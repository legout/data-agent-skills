# Prefect Orchestration

Prefect is a Python workflow orchestration tool with a simple, elegant API. Prefect 3.x (2024) introduced improvements while maintaining backward compatibility with 2.x patterns.

## Installation

```bash
pip install prefect

# Optional: Prefect server (self-hosted)
pip install prefect-server

# For cloud features: Sign up at app.prefect.io and set PREFECT_API_KEY
```

## Core Concepts

- **Flow**: A Python function decorated with `@flow` representing a workflow
- **Task**: A function decorated with `@task` - atomic unit of work
- **Deployment**: Configured flow that can be triggered manually or on schedule
- **Agent**: Process that pulls scheduled flows from Prefect server/cloud and executes them

## Basic Flow

```python
from prefect import flow, task
import polars as pl
import duckdb
from datetime import datetime

@task
def extract_from_parquet(path: str) -> pl.DataFrame:
    """Extract data from Parquet file."""
    return pl.read_parquet(path)

@task
def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    """Apply transformations."""
    return (
        df
        .filter(pl.col("value") > 0)
        .group_by("category")
        .agg(pl.col("value").sum().alias("total"))
        .sort("total", descending=True)
    )

@task
def load_to_duckdb(df: pl.DataFrame, table_name: str):
    """Load to DuckDB with proper connection management."""
    with duckdb.connect("analytics.db") as con:
        con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    return len(df)

@flow(name="daily-etl")
def run_etl(
    source_path: str = "data/sales.parquet",
    target_table: str = "daily_summary"
):
    """Orchestrated ETL pipeline."""
    # Task dependencies are implicit via function calls
    raw = extract_from_parquet(source_path)
    transformed = transform_data(raw)
    row_count = load_to_duckdb(transformed, target_table)

    return {"rows_loaded": row_count, "table": target_table}

# Execute locally
if __name__ == "__main__":
    result = run_etl()
    print(f"Pipeline completed: {result}")
```

## Advanced Features

### Retries and Error Handling

```python
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)

@task(
    retries=3,
    retry_delay_seconds=60,
    retry_condition=lambda task, task_run, state: not isinstance(state.result(), ValueError)
)
def unreliable_api_call(url: str):
    """Task with Prefect-managed retries."""
    import requests
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

# Custom retry with tenacity for more control
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def custom_retry_task():
    # Your logic
    pass
```

### Caching

```python
from prefect.task_runners import SequentialTaskRunner

@task(cache_key_fn=lambda task_inputs: str(task_inputs["date"]), cache_expiration=timedelta(hours=24))
def extract_daily_data(date: datetime):
    """Cache results for 24 hours based on date parameter."""
    # Expensive extraction
    return expensive_read(date)
```

### Parameters

```python
from prefect import flow

@flow
def parameterized_flow(
    source_path: str = "data/default.parquet",
    target_table: str = "default_table",
    batch_size: int = 1000,
    dry_run: bool = False
):
    """Flow with typed parameters."""
    # Parameters show up in Prefect UI
    if dry_run:
        logger.info("Dry run - skipping write")
        return
    # ... pipeline logic
```

### Logging

```python
from prefect import get_run_logger

@task
def process_data(df: pl.DataFrame):
    logger = get_run_logger()
    logger.info(f"Processing {len(df)} rows")
    logger.debug(f"Schema: {df.schema}")
    # Use logger.info(), .warning(), .error()
```

## Scheduling & Deployments

```bash
# Create/update deployment (Prefect 3)
prefect deploy --name "daily-etl"

# Optional: use prefect.yaml schedules (cron)
# schedules:
#   - cron: "0 2 * * *"  # Daily at 2 AM
#     timezone: "UTC"

# Trigger manually
prefect deployment run "run-etl/daily-etl"

# Or with parameters
prefect deployment run "run-etl/daily-etl" \
  --params '{"source_path": "data/new.parquet"}'
```

## Prefect Cloud vs Server

- **Prefect Cloud**: Hosted SaaS at app.prefect.io, full API, no infrastructure to manage
- **Prefect Server**: Self-hosted, open-source, requires PostgreSQL + Redis
- **Prefect Worker**: Pulls scheduled runs from work pools and executes them on target infrastructure

---

## Best Practices

1. ✅ **Keep tasks atomic** - Single responsibility, idempotent
2. ✅ **Use retries** for transient failures (network, API limits)
3. ✅ **Parameterize flows** for flexibility
4. ✅ **Log with context** using `get_run_logger()`
5. ✅ **Return results** from tasks for downstream consumption
6. ❌ **Don't** write files to local disk in tasks (use cloud storage or Prefect artifacts)
7. ❌ **Don't** use global state - Prefect may run tasks in parallel across workers
8. ❌ **Don't** hardcode credentials - use Prefect Secrets or environment variables

---

## Production Patterns

### State Persistence
```python
# Use Prefect's result storage for large results
from prefect.filesystems import S3Bucket

s3_block = S3Bucket.load("my-s3-bucket")

@task(result_storage_key="extracted-data-{date}")
def extract(date):
    data = expensive_extract(date)
    return data  # Stored in S3, passed to downstream tasks as reference
```

### Notifications
```python
from prefect.notifications import email_notifier

@flow(on_failure=[email_notifier(
    subject="Pipeline Failed: {flow_run_name}",
    recipients=["team@example.com"]
)])
def critical_pipeline():
    pass
```

### Concurrency Limits
```python
from prefect import task

@task(
    task_run_timeout=timedelta(hours=2),
    retry_delay_seconds=10,
    retries=2,
)
def bounded_task():
    # Limited execution time and retries
    pass

# Or use PREFECT_TASK_MAX_RETRIES, PREFECT_TASK_RETRY_DELAY_SECONDS in env
```

---

## References

- [Prefect Documentation](https://docs.prefect.io/latest/)
- [Prefect Tasks](https://docs.prefect.io/latest/concepts/tasks/)
- [Prefect Flows](https://docs.prefect.io/latest/concepts/flows/)
- [Prefect Deployments](https://docs.prefect.io/latest/deploy/)
