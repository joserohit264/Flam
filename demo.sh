#!/usr/bin/env bash
# Demo script for QueueCTL — runs basic functional tests

set -e

echo "🚀 Starting QueueCTL demo..."
echo "==============================="

# clean old data
rm -rf ~/.queuectl
mkdir -p ~/.queuectl

# 1️⃣ Enqueue a few jobs
echo "[1] Enqueuing jobs..."
./queuectl.py enqueue '{"id":"job1","command":"echo Job 1 works!"}'
./queuectl.py enqueue '{"id":"job2","command":"bash -c \"exit 1\""}'
./queuectl.py enqueue '{"id":"job3","command":"sleep 2 && echo Finished job3"}'

# 2️⃣ Check queue status before workers start
echo "[2] Initial status:"
./queuectl.py status

# 3️⃣ Start workers (2 parallel)
echo "[3] Starting 2 workers..."
./queuectl.py worker start --count 2 &
WORKER_PID=$!
sleep 10

# 4️⃣ Show current job states
echo "[4] Status after some processing:"
./queuectl.py status

# 5️⃣ List all jobs
echo "[5] Listing all jobs:"
./queuectl.py list

# 6️⃣ Show DLQ (should contain job2 after retries)
echo "[6] DLQ list:"
./queuectl.py dlq list

# 7️⃣ Retry job2 from DLQ
echo "[7] Retrying DLQ job2..."
./queuectl.py dlq retry job2

# 8️⃣ Wait and re-check
sleep 4
./queuectl.py status
./queuectl.py list

# 9️⃣ Stop workers gracefully
echo "[9] Stopping workers..."
./queuectl.py worker stop
sleep 2
kill $WORKER_PID 2>/dev/null || true

# 🔟 Final report
echo "[10] Final DLQ list:"
./queuectl.py dlq list

echo "==============================="
echo "✅ Demo complete!"
