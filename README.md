# Waiting for Multiple S3 Files in Airflow (by Pattern)

Very often in data pipelines, files arrive gradually — and we want the DAG to continue **only after enough files have landed**.

Example filename pattern:

branch_analytics_<something>.csv

Goal:

> **Wait until at least 5 files matching that pattern exist in S3.**

Airflow’s built-in S3 sensors:

- `S3KeySensor`
- `S3PrefixSensor`

do **not** support “wait for N files” — they only check whether *at least one file exists*.

To solve this, we use a **PythonSensor** that:

1. Lists keys in S3  
2. Filters by filename pattern  
3. Succeeds once the count meets the threshold  

---

## 🛠️ PythonSensor solution

```python
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = "my-bucket"
PREFIX = "branch_analytics_"
SUFFIX = ".csv"
REQUIRED_COUNT = 5


def wait_for_files():
    hook = S3Hook(aws_conn_id="my_s3_conn")

    keys = hook.list_keys(
        bucket_name=BUCKET,
        prefix=PREFIX,
    ) or []

    matching = [k for k in keys if k.endswith(SUFFIX)]

    print(f"Found {len(matching)} matching files: {matching}")
    return len(matching) >= REQUIRED_COUNT


wait_for_branch_files = PythonSensor(
    task_id="wait_for_branch_files",
    python_callable=wait_for_files,
    poke_interval=60,
    timeout=60 * 60,
    mode="reschedule",
)
