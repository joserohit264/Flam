# Flam
# QueueCTL — CLI-Based Background Job Queue

QueueCTL is a lightweight CLI job queue written in Python.  
It supports multiple worker processes, retries with exponential backoff, a Dead Letter Queue (DLQ), SQLite persistence, and a demo script that exercises the main features.

---

## Repository files

- `queuectl.py` — Main CLI program. Implements:
  - `enqueue` — add jobs (JSON payload or JSON file)
  - `worker start/stop` — run/stop worker processes
  - `status` — show job counts and stop flag
  - `list` — list jobs (optionally by state)
  - `dlq list` / `dlq retry` — inspect and retry DLQ jobs
  - `config get/set` — change backoff base, default max_retries, and timeout  
- `demo.sh` — Demo script that automatically:
  1. Enqueues three jobs (one purposely failing)
  2. Starts workers
  3. Shows status & lists jobs
  4. Shows DLQ and retries a DLQ job
  5. Stops workers and prints final state
- `Dockerfile` — Containerizes the app using `python:3.11-alpine`.
- `README.md` — instructions and examples.

---

## Quick overview of how it works

- Jobs are stored in an SQLite DB at `~/.queuectl/queue.db` (can be overridden).
- Workers claim a `pending` job whose `run_at <= now` and set it to `processing`.
- If a job fails, `attempts` increments and `run_at` is set using exponential backoff `delay = base ** attempts`.
- After `max_retries` is exceeded the job is moved to the `dlq` table and marked `dead`.
- You can requeue DLQ jobs manually via `dlq retry <job_id>`.

---

## Prerequisites

- Local usage:
  - Python 3.8+ (3.11 recommended)
  - `bash`
  - `sqlite3` (optional for direct DB inspection)
- Docker usage:
  - Docker engine installed

---

## Local usage — step by step

1. Make scripts executable (run from repo root):

```bash
chmod +x queuectl.py demo.sh
```
2. These are the key commands to test and run QueueCTL end to end 👇

```bash
# Run demo locally
./demo.sh

# Build Docker image
docker build -t queuectl:latest .

# Run demo inside container
docker run --rm -it --entrypoint bash queuectl:latest ./demo.sh

# Run demo inside container with persistent database
docker run --rm -it -v $HOME/.queuectl:/root/.queuectl --entrypoint bash queuectl:latest ./demo.sh

# Use a custom database path (local run)
export QUEUECTL_DB_PATH=/path/to/custom.db
./queuectl.py status
```
