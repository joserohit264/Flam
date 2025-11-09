#!/usr/bin/env python3
"""
queuectl — a minimal CLI-based background job queue with workers, retries (exponential backoff),
DLQ, persistence (SQLite), and configuration management.

Features
- enqueue jobs with JSON payload
- multiple worker processes (graceful stop)
- retry on non‑zero exit statuses with exponential backoff delay = base ** attempts
- move to DLQ after max_retries
- persistent SQLite storage under ~/.queuectl/queue.db (overridable via QUEUECTL_DB_PATH)
- config get/set for retry base, default max_retries, and job timeout
- list jobs by state, status summary, DLQ list & retry

Usage examples
    ./queuectl.py enqueue '{"id":"job1","command":"echo hello"}'
    ./queuectl.py worker start --count 3
    ./queuectl.py status
    ./queuectl.py list --state pending
    ./queuectl.py dlq list
    ./queuectl.py dlq retry job1
    ./queuectl.py config set base 2
    ./queuectl.py config get

Notes
- "graceful stop": run `./queuectl.py worker stop` — workers finish current job then exit.
- Commands are executed in /bin/sh via subprocess with shell=True. Exit code 0 = success.
- For delayed (backoff) retries we use a run_at timestamp the worker respects.
- Basic output logging is stored in job_logs table.
"""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from multiprocessing import Process, current_process
from typing import Optional, Dict, Any

APP_DIR = os.path.expanduser("~/.queuectl")
DB_PATH = os.environ.get("QUEUECTL_DB_PATH", os.path.join(APP_DIR, "queue.db"))
STOP_FILE = os.path.join(APP_DIR, "workers.stop")
DEFAULT_BASE = 2  # backoff base
DEFAULT_MAX_RETRIES = 3
DEFAULT_TIMEOUT = 300  # seconds

ISO = "%Y-%m-%dT%H:%M:%SZ"

# ----------------------------- Utility ----------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

@contextmanager
def db():
    os.makedirs(APP_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)  # autocommit mode
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with db() as conn:
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                run_at TEXT NOT NULL,
                locked_by TEXT,
                locked_at TEXT
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dlq (
                id TEXT PRIMARY KEY,
                failed_at TEXT NOT NULL,
                reason TEXT
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS job_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        # defaults
        c.execute("INSERT OR IGNORE INTO config(key,value) VALUES('base',?),('default_max_retries',?),('timeout',?)",
                  (str(DEFAULT_BASE), str(DEFAULT_MAX_RETRIES), str(DEFAULT_TIMEOUT)))


def get_config() -> Dict[str, Any]:
    with db() as conn:
        cur = conn.execute("SELECT key, value FROM config")
        out = {r[0]: r[1] for r in cur.fetchall()}
        # cast ints where applicable
        out["base"] = int(out.get("base", DEFAULT_BASE))
        out["default_max_retries"] = int(out.get("default_max_retries", DEFAULT_MAX_RETRIES))
        out["timeout"] = int(out.get("timeout", DEFAULT_TIMEOUT))
        return out


def set_config(key: str, value: str):
    with db() as conn:
        conn.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, value))


# ----------------------------- Enqueue -----------------------------------

def enqueue_job(payload: Dict[str, Any]):
    cfg = get_config()
    now = utcnow().strftime(ISO)
    job_id = payload.get("id") or f"job-{int(time.time()*1000)}"
    command = payload["command"]
    max_retries = int(payload.get("max_retries", cfg["default_max_retries"]))
    state = payload.get("state", "pending")
    attempts = int(payload.get("attempts", 0))
    run_at = payload.get("run_at") or now

    with db() as conn:
        try:
            conn.execute(
                """
                INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, run_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (job_id, command, state, attempts, max_retries, now, now, run_at)
            )
        except sqlite3.IntegrityError:
            print(f"Job with id '{job_id}' already exists.", file=sys.stderr)
            sys.exit(1)
    print(job_id)

# ----------------------------- Worker ------------------------------------

def graceful_stop_requested() -> bool:
    return os.path.exists(STOP_FILE)


def request_workers_stop():
    os.makedirs(APP_DIR, exist_ok=True)
    with open(STOP_FILE, "w") as f:
        f.write(str(int(time.time())))


def clear_workers_stop_flag():
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)


def claim_next_job(conn: sqlite3.Connection, worker_id: str) -> Optional[sqlite3.Row]:
    """Atomically move one eligible job from pending->processing and return it."""
    now = utcnow().strftime(ISO)
    # BEGIN IMMEDIATE takes a reserved lock to avoid races across processes
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        """
        SELECT id, command, attempts, max_retries FROM jobs
        WHERE state='pending' AND run_at<=?
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (now,)
    ).fetchone()
    if not row:
        conn.execute("COMMIT")
        return None
    updated = conn.execute(
        """
        UPDATE jobs SET state='processing', locked_by=?, locked_at=?, updated_at=?
        WHERE id=? AND state='pending'
        """,
        (worker_id, now, now, row["id"])
    )
    if updated.rowcount != 1:
        conn.execute("ROLLBACK")
        return None
    conn.execute("COMMIT")
    return row


def complete_job(conn: sqlite3.Connection, job_id: str):
    now = utcnow().strftime(ISO)
    conn.execute(
        "UPDATE jobs SET state='completed', updated_at=?, locked_by=NULL, locked_at=NULL WHERE id=?",
        (now, job_id),
    )


def fail_job_with_retry(conn: sqlite3.Connection, job_id: str, attempts: int, max_retries: int, reason: str):
    cfg = get_config()
    base = cfg["base"]
    now_dt = utcnow()
    next_attempt = attempts + 1
    if next_attempt > max_retries:
        # move to DLQ
        conn.execute("UPDATE jobs SET state='dead', attempts=?, updated_at=?, locked_by=NULL, locked_at=NULL WHERE id=?",
                     (next_attempt, now_dt.strftime(ISO), job_id))
        conn.execute("INSERT OR REPLACE INTO dlq(id, failed_at, reason) VALUES(?,?,?)",
                     (job_id, now_dt.strftime(ISO), reason[:500]))
        return
    delay_seconds = base ** attempts  # attempts before increment (0,1,2,..)
    run_at = (now_dt + timedelta(seconds=delay_seconds)).strftime(ISO)
    conn.execute(
        """
        UPDATE jobs
        SET state='pending', attempts=?, run_at=?, updated_at=?, locked_by=NULL, locked_at=NULL
        WHERE id=?
        """,
        (next_attempt, run_at, now_dt.strftime(ISO), job_id),
    )


def log_job_result(conn: sqlite3.Connection, job_id: str, exit_code: int, stdout: str, stderr: str):
    conn.execute(
        "INSERT INTO job_logs(job_id, ts, exit_code, stdout, stderr) VALUES(?,?,?,?,?)",
        (job_id, utcnow().strftime(ISO), exit_code, stdout, stderr),
    )


def run_command(command: str, timeout: int) -> (int, str, str):
    try:
        completed = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            executable="/bin/sh" if os.name != 'nt' else None,
            text=True,
        )
        return completed.returncode, completed.stdout, completed.stderr
    except subprocess.TimeoutExpired as e:
        return 124, e.stdout or "", e.stderr or "timeout"
    except FileNotFoundError as e:
        return 127, "", str(e)
    except Exception as e:
        return 1, "", str(e)


def worker_loop():
    init_db()
    cfg = get_config()
    timeout = cfg["timeout"]
    worker_id = f"{os.getpid()}"
    signal.signal(signal.SIGTERM, lambda *args: None)  # allow graceful exit check via file
    with db() as conn:
        idle_sleep = 1.0
        while True:
            if graceful_stop_requested():
                # finish current cycle then exit
                break
            job = claim_next_job(conn, worker_id)
            if not job:
                time.sleep(idle_sleep)
                continue
            job_id = job["id"]
            attempts = int(job["attempts"])
            max_retries = int(job["max_retries"])

            exit_code, out, err = run_command(job["command"], timeout)
            log_job_result(conn, job_id, exit_code, out, err)
            if exit_code == 0:
                complete_job(conn, job_id)
            else:
                fail_job_with_retry(conn, job_id, attempts, max_retries, reason=f"exit_code={exit_code}: {err}")
        # loop end


def start_workers(count: int):
    init_db()
    clear_workers_stop_flag()
    procs = []
    for _ in range(count):
        p = Process(target=worker_loop)
        p.daemon = False
        p.start()
        procs.append(p)
    # print PIDs so the caller can track
    print("started:", " ".join(str(p.pid) for p in procs))


def stop_workers():
    request_workers_stop()
    print("Stop requested. Workers will exit after finishing current jobs.")

# ----------------------------- Introspection -----------------------------

def status():
    init_db()
    with db() as conn:
        cur = conn.execute("SELECT state, COUNT(*) as c FROM jobs GROUP BY state")
        counts = {r[0]: r[1] for r in cur.fetchall()}
        total = sum(counts.values())
        print("Jobs:")
        for st in ["pending","processing","completed","failed","dead"]:
            print(f"  {st:10s} {counts.get(st,0)}")
        print(f"  {'total':10s} {total}")
        # show stop flag
        print(f"Workers stop flag: {'present' if graceful_stop_requested() else 'absent'}")


def list_jobs(state: Optional[str]):
    init_db()
    with db() as conn:
        if state:
            cur = conn.execute(
                "SELECT id, command, state, attempts, max_retries, run_at, updated_at FROM jobs WHERE state=? ORDER BY created_at",
                (state,))
        else:
            cur = conn.execute(
                "SELECT id, command, state, attempts, max_retries, run_at, updated_at FROM jobs ORDER BY created_at")
        rows = cur.fetchall()
        for r in rows:
            print(json.dumps({
                "id": r["id"],
                "command": r["command"],
                "state": r["state"],
                "attempts": r["attempts"],
                "max_retries": r["max_retries"],
                "run_at": r["run_at"],
                "updated_at": r["updated_at"],
            }))


def dlq_list():
    init_db()
    with db() as conn:
        cur = conn.execute("SELECT id, failed_at, reason FROM dlq ORDER BY failed_at DESC")
        for r in cur.fetchall():
            print(json.dumps({"id": r["id"], "failed_at": r["failed_at"], "reason": r["reason"] or ""}))


def dlq_retry(job_id: str):
    init_db()
    with db() as conn:
        job = conn.execute("SELECT id FROM dlq WHERE id=?", (job_id,)).fetchone()
        if not job:
            print(f"No DLQ job with id '{job_id}'.", file=sys.stderr)
            sys.exit(1)
        now = utcnow().strftime(ISO)
        conn.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        conn.execute(
            """
            UPDATE jobs SET state='pending', attempts=0, run_at=?, updated_at=?, locked_by=NULL, locked_at=NULL
            WHERE id=?
            """,
            (now, now, job_id),
        )
        print(f"Requeued {job_id}")

# ----------------------------- Config CLI --------------------------------

def config_get(key: Optional[str]):
    cfg = get_config()
    if key:
        if key not in cfg:
            print(f"Unknown key '{key}'. Known: base, default_max_retries, timeout", file=sys.stderr)
            sys.exit(1)
        print(f"{key}={cfg[key]}")
    else:
        for k in ["base","default_max_retries","timeout"]:
            print(f"{k}={cfg[k]}")


def config_set(key: str, value: str):
    if key not in {"base","default_max_retries","timeout"}:
        print("Allowed keys: base, default_max_retries, timeout", file=sys.stderr)
        sys.exit(1)
    # basic validation
    try:
        ival = int(value)
        if ival < 0:
            raise ValueError
    except Exception:
        print("Value must be a non‑negative integer", file=sys.stderr)
        sys.exit(1)
    set_config(key, str(ival))
    print(f"set {key}={ival}")

# ----------------------------- CLI wiring --------------------------------

def parse_json_or_file(arg: str) -> Dict[str, Any]:
    # If arg is a path to a file, read it; else parse as JSON
    if os.path.exists(arg):
        with open(arg, 'r', encoding='utf-8') as f:
            return json.load(f)
    return json.loads(arg)


def main():
    init_db()
    parser = argparse.ArgumentParser(prog="queuectl", description="Minimal job queue with workers and DLQ (SQLite)")
    sub = parser.add_subparsers(dest="cmd")

    p_enq = sub.add_parser("enqueue", help="Enqueue a new job with JSON payload or JSON file path")
    p_enq.add_argument("payload", help="JSON string or path to JSON file")

    p_worker = sub.add_parser("worker", help="Manage workers")
    subw = p_worker.add_subparsers(dest="wcmd")
    p_ws = subw.add_parser("start", help="Start N workers")
    p_ws.add_argument("--count", type=int, default=1, help="Number of worker processes")
    subw.add_parser("stop", help="Gracefully stop workers")

    p_status = sub.add_parser("status", help="Show counts by state and stop flag")

    p_list = sub.add_parser("list", help="List jobs (optionally filtered by state)")
    p_list.add_argument("--state", choices=["pending","processing","completed","failed","dead"], required=False)

    p_dlq = sub.add_parser("dlq", help="DLQ commands")
    subd = p_dlq.add_subparsers(dest="dcmd")
    subd.add_parser("list", help="List DLQ entries")
    p_dr = subd.add_parser("retry", help="Retry a DLQ job by id")
    p_dr.add_argument("job_id")

    p_cfg = sub.add_parser("config", help="Get/Set configuration")
    subc = p_cfg.add_subparsers(dest="ccmd")
    p_cget = subc.add_parser("get", help="Get all or one key")
    p_cget.add_argument("key", nargs="?")
    p_cset = subc.add_parser("set", help="Set a key")
    p_cset.add_argument("key", choices=["base","default_max_retries","timeout"])
    p_cset.add_argument("value")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        payload = parse_json_or_file(args.payload)
        enqueue_job(payload)
    elif args.cmd == "worker":
        if args.wcmd == "start":
            start_workers(args.count)
        elif args.wcmd == "stop":
            stop_workers()
        else:
            p_worker.print_help()
    elif args.cmd == "status":
        status()
    elif args.cmd == "list":
        list_jobs(args.state)
    elif args.cmd == "dlq":
        if args.dcmd == "list":
            dlq_list()
        elif args.dcmd == "retry":
            dlq_retry(args.job_id)
        else:
            p_dlq.print_help()
    elif args.cmd == "config":
        if args.ccmd == "get":
            config_get(args.key)
        elif args.ccmd == "set":
            config_set(args.key, args.value)
        else:
            p_cfg.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
