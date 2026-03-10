# Dagster Orchestration

Dagster is an orchestration platform centered around data assets. It provides strong typing, asset lineage, data quality checks, and reproducibility.

## Installation

```bash
pip install dagster
# Optional: web UI
pip install dagster-webserver
```

## Core Concepts

- **Asset**: A data object (table, file, model) that can be materialized (produced)
- **Op**: A unit of computation (similar to Prefect task)
- **Job**: A set of ops/assets to execute
- **Sensor**: Triggers jobs based on external events
- **Schedule**: Time-based triggers
- **Resource**: Shared objects (DB connections, API clients) injected into ops/assets

## Asset-Based Pipeline (New Style)

```python
from dagster import asset, AssetsDefinition, define_asset_job, Definitions
import polars as pl
import duckdb
from pathlib import Path

@asset
def raw_events() -> pl.DataFrame:
    """Source asset - reads raw data."""
    return pl.read_parquet("data/events.parquet")

@asset
def processed_events(raw_events: pl.DataFrame) -> pl.DataFrame:
    """Transformation asset - depends on raw_events."""
    return (
        raw_events
        .filter(pl.col("status") == "active")
        .with_columns([
            pl.col("timestamp").str.to_datetime(),
            pl.col("value").fill_null(0)
        ])
    )

@asset
def daily_summary(processed_events: pl.DataFrame) -> pl.DataFrame:
    """Aggregation asset."""
    return (
        processed_events
        .group_by(pl.col("timestamp").dt.date().alias("date"))
        .agg([
            pl.col("value").sum().alias("total_value"),
            pl.col("id").count().alias("event_count")
        ])
    )

@asset
def load_to_duckdb(daily_summary: pl.DataFrame):
    """Sink asset - loads to database."""
    with duckdb.connect("analytics.db") as con:
        con.sql("""
            CREATE OR REPLACE TABLE daily_summary AS
            SELECT * FROM daily_summary
        """)

# Define job that materializes all assets
daily_etl_job = define_asset_job(
    name="daily_etl",
    selection="*"  # All assets
)

# Export for Dagster UI / Dagit
defs = Definitions(
    assets=[raw_events, processed_events, daily_summary, load_to_duckdb],
    jobs=[daily_etl_job]
)
```

### Running Dagster

```bash
# Launch local development UI
dagster dev -f path/to/your_file.py

# In UI, materialize assets by clicking "Materialize All"
# Or from code:
from dagster import materialize
materialize(to_execute_defs=[defs])
```

## Ops & Jobs (Legacy Style)

```python
from dagster import op, job, In, Out

@op(out=Out(pl.DataFrame))
def extract():
    return pl.read_parquet("data/events.parquet")

@op(ins={"df": In(pl.DataFrame)}, out=Out(pl.DataFrame))
def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("value") > 0)

@op(ins={"df": In(pl.DataFrame)})
def load(df: pl.DataFrame):
    with duckdb.connect("analytics.db") as con:
        con.sql("CREATE TABLE events AS SELECT * FROM df")

@job
def etl_pipeline():
    data = extract()
    transformed = transform(data)
    load(transformed)

# Execute
result = etl_pipeline.execute_in_process()
```

## Resources (Shared Connections)

```python
from dagster import resource, ConfigurableResource
import duckdb

class DuckDBResource(ConfigurableResource):
    """DuckDB connection resource."""
    database_path: str = "analytics.db"

    def get_connection(self):
        return duckdb.connect(self.database_path)

@asset(required_resource_keys={"duckdb"})
def db_asset(context) -> pl.DataFrame:
    conn = context.resources.duckdb.get_connection()
    result = conn.sql("SELECT * FROM events").pl()
    conn.close()  # Or use context manager
    return result

defs = Definitions(
    assets=[db_asset],
    resources={"duckdb": DuckDBResource(database_path="analytics.db")}
)
```

## Schemas and Types

```python
from dagster import Field, String, Int
from dagster import Config, config_schema

@asset
def typed_asset(config: Config):
    """Asset with config schema."""
    # Access config fields with type hints
    pass

# Or using Python dataclasses
from dagster import ConfigurableResource
from pydantic import BaseModel

class MyConfig(BaseModel):
    start_date: str
    batch_size: int = 1000

@asset
def configurable_asset(config: MyConfig):
    date = datetime.fromisoformat(config.start_date)
    # ...
```

## Sensors & Schedules

```python
from dagster import schedule, SensorResult, RunRequest, RunConfig, asset_key
from dagster._time import get_timezone

@schedule(
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC"
)
def daily_schedule(context):
    return RunRequest(run_key=datetime.now().isoformat())

# File-based sensor
@sensor(asset_keys=[asset_key("raw_events")])
def new_file_sensor(context):
    # Check if new file exists
    if new_file_detected():
        return RunRequest(run_key="new-file")
    return SensorResult.skip()
```

## Partitioned Assets

```python
from dagster import MonthlyPartitionsDefinition

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-01-01")
)
def monthly_partitioned_asset(context) -> pl.DataFrame:
    partition_date = context.partition_key  # e.g., "2024-01-01"
    return pl.read_parquet(f"s3://bucket/data/{partition_date}.parquet")

# Materialize specific partitions
job = define_asset_job(
    "monthly_job",
    selection=AssetSelection.keys("monthly_partitioned_asset"),
    partitions_def=MonthlyPartitionsDefinition(...)
)
```

## Dagster + dbt Integration

See `@data-engineering-orchestration/dbt.md` for detailed guide. Quick example:

```python
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets
from dagster import Definitions

dbt_project = DbtProject(project_dir=Path("my_dbt_project"))
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project.project_dir)

@dbt_assets(manifest=dbt_project.manifest_path)
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

defs = Definitions(
    assets=[my_dbt_assets],
    resources={"dbt": dbt_resource}
)
```

## Best Practices

1. ✅ **Assets over ops** - Asset-based programming gives better lineage
2. ✅ **Typed inputs/outputs** - Use type hints (`-> pl.DataFrame`) for compile-time checking
3. ✅ **Use resources** for shared state (DB connections, API clients)
4. ✅ **Partition large assets** for incremental materialization
5. ✅ **Add metadata** to assets (description, owners, tags)
6. ❌ **Don't** write to local filesystem in assets (use resources like S3)
7. ❌ **Don't** ignore partitions - they're key to incremental processing
8. ❌ **Don't** use global variables - Dagster may run assets in parallel

```python
# Good: Asset with metadata
@asset(
    metadata={
        "owner": "data-team@example.com",
        "description": "Daily customer transactions summary"
    }
)
def well_documented_asset():
    pass
```

---

## References

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster Assets](https://docs.dagster.io/concepts/assets/)
- [Dagster Resources](https://docs.dagster.io/concepts/resources/)
- [Dagster + dbt](https://docs.dagster.io/integrations/dbt/)
