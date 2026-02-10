# Advanced FlowerPower Patterns

Production-grade pipeline patterns integrating data-engineering best practices: medallion architecture, Delta Lake, incremental loads, data quality, and cloud storage.

## Table of Contents

1. [Medallion Architecture](#medallion)
2. [Delta Lake with Schema Evolution](#delta-schema)
3. [Incremental Loads with Watermarks](#watermarks)
4. [Data Quality with Pandera](#pandera)
5. [S3 + fsspec Integration](#s3)
6. [Error Handling & Retries](#error-handling)
7. [Performance Tuning](#performance)

---

## <a name="medallion"></a>1. Medallion Architecture

Implement Bronze → Silver → Gold layers with FlowerPower.

### Project Structure

```
medallion-project/
├── conf/
│   ├── project.yml
│   └── pipelines/
│       ├── bronze_ingest.yml
│       ├── silver_clean.yml
│       └── gold_aggregate.yml
├── pipelines/
│   ├── bronze_ingest.py
│   ├── silver_clean.py
│   └── gold_aggregate.py
└── hooks/
```

### Bronze Pipeline (Raw Ingestion)

```python
# pipelines/bronze_ingest.py
from pathlib import Path
from hamilton.function_modifiers import parameterize
import polars as pl
from flowerpower.cfg import Config

PARAMS = Config.load(Path(__file__).parents[1], "bronze_ingest").pipeline.h_params

@parameterize(**PARAMS.source)
def source_uri(uri: str) -> str:
    """Source data location (supports S3, GCS, local)."""
    return uri

def raw_data(source_uri: str) -> pl.LazyFrame:
    """Read raw data without transformations."""
    # fsspec handles cloud URIs automatically
    return pl.scan_parquet(source_uri)

def add_metadata(raw_data: pl.LazyFrame, source_uri: str) -> pl.LazyFrame:
    """Add ingestion metadata for lineage."""
    return raw_data.with_columns([
        pl.lit(pl.datetime.now()).alias("_ingestion_timestamp"),
        pl.lit(source_uri).alias("_source_file"),
        pl.date_range(
            start=pl.col("event_date").min(),
            end=pl.col("event_date").max(),
            interval="1d"
        ).alias("_ingestion_date")  # Partition column
    ])

def write_bronze(add_metadata: pl.LazyFrame, bronze_path: str) -> str:
    """Write to Delta Lake bronze layer (append-only)."""
    add_metadata.write_delta(
        bronze_path,
        mode="append",
        partition_by=["_ingestion_date"]  # Partition by date for pruning
    )
    count = add_metadata.select(pl.count()).collect()[0, 0]
    return f"Bronze: wrote {count} rows to {bronze_path}"
```

**Configuration (`conf/pipelines/bronze_ingest.yml`):**
```yaml
params:
  source:
    uri: "s3://raw-data/events/"
  bronze_path: "s3://lakehouse/bronze/events/"

run:
  final_vars:
    - write_bronze
  executor:
    type: threadpool
    max_workers: 8
  retry:
    max_retries: 3
    retry_delay: 2.0
  log_level: INFO
```

---

### Silver Pipeline (Cleaned & Validated)

```python
# pipelines/silver_clean.py
from pathlib import Path
import polars as pl
import pandera as pa
from pandera.polars import DataFrameSchema, Column
from flowerpower.cfg import Config

PARAMS = Config.load(Path(__file__).parents[1], "silver_clean").pipeline.h_params

def bronze_source(bronze_path: str) -> pl.LazyFrame:
    """Read from bronze Delta table."""
    return pl.scan_delta(bronze_path)

def filter_valid(bronze_source: pl.LazyFrame) -> pl.LazyFrame:
    """Filter out corrupt records early."""
    return bronze_source.filter(
        pl.col("event_id").is_not_null() &
        (pl.col("event_date") >= pl.date(2024, 1, 1))
    )

def validate_schema(filter_valid: pl.LazyFrame) -> pl.LazyFrame:
    """Pandera validation - raises if invalid."""
    schema = DataFrameSchema({
        "event_id": Column(pl.Utf8, nullable=False, unique=True),
        "user_id": Column(pl.Int64, pa.Check.gt(0)),
        "event_type": Column(pl.Utf8, pa.Check.isin(["click", "view", "purchase"])),
        "amount": Column(pl.Float64, pa.Check.ge(0)),
        "event_date": Column(pl.Date)
    })

    # Collect for validation (Pandera works on eager DataFrames)
    df = filter_valid.collect()
    validated = schema.validate(df, lazy=True)
    return validated.lazy()

def standardize(validate_schema: pl.LazyFrame) -> pl.LazyFrame:
    """Standardize formats, deduplicate, enrich."""
    return validate_schema.with_columns([
        pl.col("event_type").str.to_lowercase(),
        pl.col("amount").round(2)
    ]).unique(subset=["event_id"], keep="last")  # Dedupe

def write_silver(standardize: pl.LazyFrame, silver_path: str) -> str:
    """Write to Silver Delta table (idempotent upsert via overwrite partition)."""
    # Assume partitioned by event_date
    standardize.write_delta(
        silver_path,
        mode="overwrite",  # Replace partition for given date range
        partition_filters=[("event_date", ">=", "2024-01-01")]
    )
    count = standardize.select(pl.count()).collect()[0, 0]
    return f"Silver: wrote {count} clean records"
```

**Config:**
```yaml
params:
  bronze_path: "s3://lakehouse/bronze/events/"
  silver_path: "s3://lakehouse/silver/events/"

run:
  final_vars:
    - write_silver
  executor:
    type: processpool  # CPU-bound validation
    max_workers: 4
```

---

### Gold Pipeline (Aggregated)

```python
# pipelines/gold_aggregate.py
from pathlib import Path
import polars as pl
from flowerpower.cfg import Config

PARAMS = Config.load(Path(__file__).parents[1], "gold_aggregate").pipeline.h_params

def silver_source(silver_path: str) -> pl.LazyFrame:
    return pl.scan_delta(silver_path)

def daily_sales(silver_source: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate daily sales by region."""
    return silver_source.group_by(["event_date", "region"]).agg([
        pl.sum("amount").alias("total_sales"),
        pl.count().alias("transaction_count"),
        pl.n_unique("user_id").alias("unique_customers")
    ])

def rolling_7d(daily_sales: pl.LazyFrame) -> pl.LazyFrame:
    """Add 7-day rolling average."""
    return daily_sales.with_columns([
        pl.col("total_sales").rolling_mean(window_size=7).over("region").alias("rolling_7d_sales")
    ])

def write_gold(rolling_7d: pl.LazyFrame, gold_path: str) -> str:
    """Write Gold as Parquet (read-optimized, no ACID needed)."""
    rolling_7d.collect().write_parquet(
        gold_path,
        compression="zstd",
        statistics=True  # Enable min/max for pruning
    )
    return f"Gold: wrote aggregated table to {gold_path}"
```

---

## <a name="delta-schema"></a>2. Delta Lake Schema Evolution

Handle schema changes gracefully with `mergeSchema`.

```python
def evolving_source(source_path: str) -> pl.LazyFrame:
    """Read data that might have new columns."""
    return pl.scan_parquet(source_path)

def append_with_evolution(evolving_source: pl.LazyFrame, delta_table: str) -> str:
    """Append new data, automatically adding new columns."""
    # delta_write_options={"schema_mode": "merge"} adds new columns
    evolving_source.write_delta(
        delta_table,
        mode="append",
        delta_write_options={"schema_mode": "merge"}
    )
    return "Appended with schema evolution"

def alter_schema_explicit(delta_table: str) -> None:
    """Explicitly add column via DeltaTable API."""
    from deltalake import DeltaTable
    dt = DeltaTable(delta_table)
    dt.alter_table.add_columns({
        "new_feature": "double",
        "experiment_group": "string"
    })
```

**Config pattern:**
```yaml
params:
  delta_table: "s3://lakehouse/silver/events/"

run:
  # Enable recompute if schema changes force node re-execution
  cache:
    recompute:
      - evolving_source
```

---

## <a name="watermarks"></a>3. Incremental Loads with Watermarks

Track high-water marks to load only deltas.

### Watermark Table Setup

```sql
-- Run once in DuckDB/Postgres
CREATE TABLE watermark_table (
    table_name VARCHAR PRIMARY KEY,
    watermark_value TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Watermark Functions

```python
# pipelines/incremental_load.py
from datetime import datetime
import duckdb
import polars as pl
from flowerpower.cfg import Config

PARAMS = Config.load(Path(__file__).parents[1], "incremental_load").pipeline.h_params

def get_watermark(con, table_name: str) -> datetime:
    """Fetch last processed watermark."""
    result = con.execute(f"""
        SELECT watermark_value
        FROM watermark_table
        WHERE table_name = '{table_name}'
    """).fetchone()
    return result[0] if result else datetime(1970, 1, 1)

def set_watermark(con, table_name: str, value: datetime) -> None:
    """Upsert watermark."""
    con.execute("""
        INSERT OR REPLACE INTO watermark_table (table_name, watermark_value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    """, [table_name, value])

def incremental_etl(source_table: str, target_table: str, timestamp_col: str = "updated_at"):
    """Main pipeline function."""
    con = duckdb.connect(":memory:")

    # 1. Get old watermark
    old_wm = get_watermark(con, target_table)

    # 2. Determine new watermark from source
    new_wm = pl.scan_delta(source_table).select(
        pl.max(timestamp_col)
    ).collect()[0, 0]

    # 3. Load incremental data
    df = pl.scan_delta(source_table).filter(
        (pl.col(timestamp_col) > old_wm) &
        (pl.col(timestamp_col) <= new_wm)
    )

    # 4. Write to target (append)
    df.write_delta(target_table, mode="append")

    # 5. Update watermark
    set_watermark(con, target_table, new_wm)

    return f"Loaded {len(df.collect())} rows from {old_wm} to {new_wm}"
```

---

## <a name="pandera"></a>4. Data Quality with Pandera

Define reusable Pandera schemas and integrate into FlowerPower pipelines.

### Schema Module (`schemas.py`)

```python
# schemas.py - Shared across pipelines
import pandera as pa
from pandera.polars import DataFrameSchema, Column
import polars as pl

order_schema = DataFrameSchema({
    "order_id": Column(pl.Int64, nullable=False, unique=True),
    "customer_id": Column(pl.Int32, pa.Check.gt(0)),
    "amount": Column(pl.Float64, pa.Check.in_range(0, 1000000)),
    "order_date": Column(pl.Date, pa.Check(lambda s: s >= "2020-01-01")),
    "status": Column(pl.Utf8, pa.Check.isin(["pending", "shipped", "delivered", "cancelled"]))
})

def validate_orders(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and return DataFrame or raise."""
    return order_schema.validate(df, lazy=True)
```

### Use in Pipeline

```python
from schemas import validate_orders

def process_orders(raw_orders: pl.LazyFrame) -> pl.LazyFrame:
    df = raw_orders.collect()
    validated = validate_orders(df)
    return validated.lazy()
```

**Fail-fast vs. quarantine:**

```python
def validate_with_quarantine(raw_orders: pl.LazyFrame, quarantine_path: str) -> pl.LazyFrame:
    """Validate and write failures to quarantine."""
    df = raw_orders.collect()
    try:
        validated = order_schema.validate(df, lazy=True)
        return validated.lazy()
    except pa.errors.SchemaErrors as e:
        failures = e.failure_cases
        failures.write_parquet(quarantine_path)
        print(f"Wrote {len(failures)} invalid records to {quarantine_path}")
        raise
```

---

## <a name="s3"></a>5. S3 + fsspec Integration

Access cloud storage with proper authentication.

### Configuration

```yaml
params:
  s3_uri: "s3://my-bucket/raw/"
  aws_region: "us-east-1"
  # Credentials from environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  # Or IAM role on EC2/ECS

run:
  executor:
    type: threadpool
    max_workers: 16  # For many small files
```

### Pipeline Code

```python
def list_s3_files(s3_uri: str) -> list[str]:
    """List all Parquet files under S3 prefix."""
    import fsspec
    fs = fsspec.filesystem('s3', region=PARAMS.aws_region)
    return fs.glob(f"{s3_uri}**/*.parquet")

def read_s3_file(file_path: str) -> pl.LazyFrame:
    """Read single file from S3."""
    import fsspec
    fs = fsspec.filesystem('s3')
    with fs.open(file_path, 'rb') as f:
        return pl.read_parquet(f)

def process_all_files(list_s3_files: list[str]) -> pl.LazyFrame:
    """Union all files efficiently."""
    # Parallel read with threadpool executor
    frames = []
    for file_path in list_s3_files:
        frames.append(read_s3_file(file_path))
    return pl.concat(frames)

def write_to_s3(process_all_files: pl.LazyFrame, output_uri: str) -> str:
    """Write partitioned output to S3."""
    process_all_files.write_delta(
        output_uri,
        mode="overwrite",
        partition_by=["year", "month"],
        # Use storage_options if passing explicit credentials
        # storage_options={"AWS_REGION": "us-east-1"}
    )
    return f"Wrote to {output_uri}"
```

---

## <a name="error-handling"></a>6. Error Handling & Retries

Configure retries in YAML and implement exponential backoff.

### YAML Retry Config

```yaml
run:
  retry:
    max_retries: 3
    retry_delay: 1.0          # Initial delay in seconds
    jitter_factor: 0.1        # Randomize to avoid thundering herd
    retry_exceptions:
      - requests.exceptions.HTTPError
      - ConnectionError
      - TimeoutError
```

### Custom Retry Logic in Code

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def fetch_external_api(url: str) -> dict:
    """Fetch with exponential backoff."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def api_enrichment(data: pl.LazyFrame) -> pl.LazyFrame:
    """Enrich with external API data (retrying)."""
    # This node will be retried automatically if configured in YAML
    # or use custom @retry decorator
    return data.with_columns([
        pl.struct(["user_id"]).map_elements(
            lambda row: fetch_external_api(f"https://api.example.com/users/{row['user_id']}")["score"],
            return_dtype=pl.Float64
        ).alias("user_score")
    ])
```

---

## <a name="performance"></a>7. Performance Tuning

### Executor Selection

| Workload | Executor | Workers |
|----------|----------|---------|
| I/O-bound (S3, DB) | `threadpool` | 8-16 |
| CPU-bound (transformations) | `processpool` | CPU cores |
| Distributed (cluster) | `ray` / `dask` | Cluster nodes |
| Simple debugging | `synchronous` | 1 |

### Optimize Polars Operations

```python
def optimized_transform(df: pl.LazyFrame) -> pl.LazyFrame:
    """Use lazy evaluation, predicate pushdown, column pruning."""
    return df.filter(
        pl.col("amount") > 0
    ).select([
        "order_id", "customer_id", "amount", "event_date"  # Only needed cols
    ]).with_columns([
        pl.col("amount").round(2)
    ]).cache()  # Cache intermediate result if reused
```

### File Size Control

For Delta Lake, control file size via `data_file_size`:

```yaml
params:
  target_file_size_mb: 256

run:
  # Not directly exposed in FlowerPower yet - use delta_write_options
  # In code:
  # df.write_delta(path, delta_write_options={"data_file_size": "256mb"})
```

---

## Testing FlowerPower Pipelines

```python
# test_my_pipeline.py
import tempfile
from pathlib import Path
from flowerpower import FlowerPowerProject

def test_bronze_to_silver():
    """End-to-end test with temporary storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Write test data
        test_data = pl.DataFrame({
            "event_id": ["1", "2"],
            "user_id": [1, 2],
            "amount": [10.0, 20.0]
        })
        test_data.write_parquet(f"{tmpdir}/raw.parquet")

        # 2. Initialize project
        project = FlowerPowerProject.init(name="test", base_dir=tmpdir)

        # 3. Create pipeline (or use existing)
        # ... pipeline creation logic

        # 4. Run pipeline
        result = project.run("bronze_ingest", overrides={
            "source_uri": f"{tmpdir}/raw.parquet"
        })

        # 5. Assert outputs
        output = pl.read_delta(f"{tmpdir}/bronze/")
        assert len(output) == 2
```

---

## Deployment Patterns

### Systemd Service

```ini
# /etc/systemd/system/flowerpower-pipeline.service
[Unit]
Description=FlowerPower Pipeline
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/pipelines
Environment="AWS_ACCESS_KEY_ID=..."
Environment="AWS_SECRET_ACCESS_KEY=..."
ExecStart=/opt/pipelines/.venv/bin/flowerpower pipeline run my_pipeline
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Cron Job

```bash
# crontab -e
0 2 * * * cd /opt/pipelines && /opt/pipelines/.venv/bin/flowerpower pipeline run daily_aggregate >> /var/log/pipeline.log 2>&1
```

### Docker Container

```dockerfile
FROM python:3.11-slim
RUN pip install uv && uv pip install flowerpower[io,ui]
COPY . /app
WORKDIR /app
CMD ["flowerpower", "pipeline", "run", "my_pipeline"]
```

---

## Monitoring & Observability

### Hamilton Tracker

Enable tracking to MLflow or custom backend:

```yaml
# conf/project.yml
adapter:
  hamilton_tracker:
    enabled: true
    api_url: "http://localhost:8000"
```

### Custom Success/Failure Hooks

```yaml
run:
  on_success:
    function: "hooks.notify_success"
    kwargs:
      slack_webhook: "https://hooks.slack.com/..."
  on_failure:
    function: "hooks.notify_failure"
    kwargs:
      slack_webhook: "https://hooks.slack.com/..."
```

```python
# hooks/notification.py
import requests
from flowerpower.lifecycle import LifecycleCallback

def notify_success(result: dict, **kwargs):
    webhook = kwargs.get("slack_webhook")
    if webhook:
        requests.post(webhook, json={"text": f"Pipeline succeeded: {result}"})

def notify_failure(error: Exception, **kwargs):
    webhook = kwargs.get("slack_webhook")
    if webhook:
        requests.post(webhook, json={"text": f"Pipeline failed: {error}"})
```

---

## References

- `@data-engineering-best-practices` - Medallion architecture, partitioning, incremental loads
- `@data-engineering-storage-lakehouse` - Delta Lake, Iceberg operations
- `@data-engineering-storage-remote-access` - Cloud storage (S3, GCS, Azure)
- `@data-engineering-quality` - Data validation with Pandera, Great Expectations
- [FlowerPower Documentation](https://legout.github.io/flowerpower/)
- [Hamilton DAG Tutorial](https://hamilton.apache.org/tutorials/)
