
#!/bin/bash
set -euo pipefail
echo "[spark_job] Starting spark job runner"
# simple pyspark run (local mode inside container)
python3 /app/spark_job.py
