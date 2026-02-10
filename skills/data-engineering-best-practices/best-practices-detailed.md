# Data Engineering Best Practices (Detailed Reference)

# Data Engineering Best Practices

Comprehensive guide to building production-grade data pipelines: dataset lifecycle management, medallion architecture, partitioning strategies, file sizing, schema evolution, CRUD operations across all major tools, data quality testing, and cost optimization.

## Table of Contents

1. [Medallion Architecture](#medallion-architecture)
2. [Dataset Lifecycle Management](#dataset-lifecycle)
3. [Partitioning Strategies](#partitioning)
4. [File Sizing & Performance](#file-sizing)
5. [CRUD Operations Across Tools](#crud-operations-across-tools)
6. [Schema Evolution](#schema-evolution)
7. [Data Quality Testing](#data-quality)
8. [Observability & Monitoring](#observability)
9. [Cost Optimization](#cost)
10. [Testing Strategies](#testing)

---

## Medallion Architecture

### Overview
The **medallion architecture** organizes data into quality tiers:

- **Bronze** (raw): Original ingested data, immutable, for re-playability
- **Silver** (validated): Cleaned, standardized, validated data
- **Gold** (curated): Business-ready aggregates and summaries

```
[Source Systems]
        ↓
   ┌──────────┐
   │  Bronze  │  (raw, immutable, full fidelity)
   └─────┬────┘
         │ cleansing, validation, standardization
         ↓
   ┌──────────┐
   │  Silver  │  (clean, reliable, conformed dimensions)
   └─────┬────┘
         │ aggregation, business logic
         ↓
   ┌──────────┐
   │  Gold    │  (optimized for specific use cases)
   └──────────┘
```

### Implementation Patterns

#### Bronze Layer
- Store as **append-only** (never update raw data)
- Keep original schema; add ingestion metadata (`_ingestion_timestamp`, `_source_file`)
- Format: **Parquet** or **Delta Lake** (if ACID needed)
- Partition by: **date** of ingestion or event date

```python
# Polars: Append to Bronze Delta
df.write_delta(
    "s3://lakehouse/bronze/orders/",
    mode="append",
    partition_by=["ingestion_date"]
)
```

#### Silver Layer
- Apply data quality checks (nulls, duplicates, referential integrity)
- Standardize data types (normalize dates, currencies)
- Denormalize dimension keys (snowflake → star schema)
- Format: **Delta Lake** or **Iceberg** for ACID guarantees

```python
# PyArrow: Write Silver Parquet with partitioning
import pyarrow.dataset as ds

ds.write_dataset(
    silver_table,
    "s3://lakehouse/silver/orders/",
    format="parquet",
    partitioning=ds.partitioning(
        ["order_date_year", "order_date_month"],
        flavor="hive"
    )
)
```

#### Gold Layer
- Pre-aggregated tables (daily sales by region, ML features)
- Optimized for specific queries (materialized views)
- Format: Parquet (read-heavy) or materialized views in DuckDB

```python
# DuckDB: Materialized Gold view
con.execute("""
CREATE OR REPLACE VIEW gold_daily_sales AS
SELECT
    order_date,
    region,
    SUM(amount) AS total_sales,
    COUNT(*) AS order_count
FROM silver_orders
GROUP BY 1, 2
""")
con.execute("COPY gold_daily_sales TO 's3://lakehouse/gold/daily_sales.parquet' (FORMAT PARQUET)")
```

---

## Dataset Lifecycle Management

### Stages

| Stage | Description | Retention | Format |
|-------|-------------|-----------|--------|
| **Raw/Staging** | Ingested data, minimal processing | 7-30 days (then archive/delete) | Parquet/CSV/JSON |
| **Curated/Silver** | Validated, cleaned, business-ready | Indefinite (versioned) | Delta/Iceberg |
| **Archive** | Historical data, rarely accessed | Long-term (cold storage) | Parquet (compressed) |

### Data Retention Policies

```python
# Example: Archive Bronze older than 30 days to Glacier, delete > 365
import boto3
from datetime import datetime, timedelta

s3 = boto3.client('s3')
cutoff = datetime.now() - timedelta(days=30)

# List objects with prefix (simplified)
objects = s3.list_objects_v2(
    Bucket='lakehouse',
    Prefix='bronze/'
)['Contents']

for obj in objects:
    # Parse date from path: bronze/year=2024/month=01/day=01/
    # Move to archive/ or delete based on age
    if obj['LastModified'] < cutoff:
        # Copy to archive, then delete
        pass
```

### Metadata Tracking

Maintain a **data catalog** table for all datasets:

```sql
CREATE TABLE dataset_catalog (
    dataset_name VARCHAR PRIMARY KEY,
    layer VARCHAR,  -- bronze/silver/gold
    format VARCHAR, -- parquet/delta/iceberg
    location VARCHAR,
    schema JSON,    -- Avro schema or struct
    owner VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    retention_days INTEGER,
    pii_level VARCHAR  -- high/medium/low
);
```

---

## Partitioning Strategies

### Single-Column Partitioning

Good for high-cardinality filter columns:

```python
# Polars: write partitioned Delta
df.write_delta(
    "s3://lakehouse/silver/transactions/",
    mode="overwrite",
    partition_by=["transaction_date"]  # DATE column
)
```

### Multi-Column Partitioning

Hierarchical directories: `year=2024/month=01/region=US/`

```python
# PyArrow
import pyarrow.dataset as ds

partitioning = ds.partitioning(
    schema=pa.schema([
        ("year", pa.int16()),
        ("month", pa.int8()),
        ("region", pa.string())
    ]),
    flavor="hive"
)

ds.write_dataset(
    table,
    "s3://lakehouse/silver/orders/",
    format="parquet",
    partitioning=partitioning
)
```

### Partition Pruning

Query only needed partitions:

```python
# Polars lazy scan with predicate on partition columns
lazy_df = pl.scan_parquet("s3://lakehouse/silver/orders/")
filtered = lazy_df.filter(
    (pl.col("year") == 2024) &
    (pl.col("month") == 1) &
    (pl.col("region") == "US")
).collect()
# Only reads files under year=2024/month=01/region=US/
```

---

## File Sizing & Performance

### Optimal File Size

**Target: 1 GB per file** (range: 256 MB - 2 GB). Smaller files (<128 MB) cause metadata overhead and slow listing operations.

#### Why not too small?
- Each file is an object in S3/GCS; listing 100k small files is slow
- Query engines open many file handles → latency
- Parquet footer (metadata) overhead becomes significant

#### Why not too large?
- Single file becomes bottleneck for concurrent reads
- Partition skew if one partition has one massive file
- Memory pressure during decompression

### Calculating Row Count

For a table with N columns, estimate rows per GB:

```
rows_per_gb ≈ (1_000_000_000 bytes) / (row_size_in_bytes)

row_size ≈ sum(column_avg_size) + overhead
```

Example table with avg row size 500 bytes:
```
rows_per_gb = 1e9 / 500 = 2,000,000 rows/GB
```

### Controlling File Size in Writers

**Polars / Delta:**
```python
# Delta uses delta-rs; file size controlled by target_size_mb
df.write_delta(
    "s3://bucket/table",
    mode="overwrite",
    delta_write_options={
        "data_file_size": "256mb"  # Target file size
    }
)
```

**PyArrow:**
```python
# Control via max_rows_per_file (~1M rows for 1GB)
ds.write_dataset(
    table,
    "output.parquet",
    format="parquet",
    max_rows_per_file=1_000_000,
    partitioning=...
)
```

**Spark (if used in stack):**
```python
df.write \
  .option("maxRecordsPerFile", 1000000) \
  .partitionBy("year", "month") \
  .parquet("s3://...")
```

---

## CRUD Operations Across Tools

### Append

#### Polars → Delta
```python
df.write_delta("s3://bucket/table", mode="append")
```

#### PyArrow → Parquet (existing dataset)
```python
# No direct append; read existing, concat, rewrite
existing = ds.dataset("s3://bucket/table").to_table()
combined = pa.concat_tables([existing, new_table])
ds.write_dataset(combined, "s3://bucket/table", format="parquet")
```

#### DuckDB → Parquet/Delta
```sql
COPY (SELECT * FROM new_data) TO 's3://bucket/table.parquet' (FORMAT PARQUET, APPEND true);
-- For Delta:
INSERT INTO delta_table SELECT * FROM new_data;
```

#### PyIceberg → Iceberg
```python
table.append(new_arrow_table)
```

---

### Overwrite (Full Table)

#### Polars
```python
df.write_delta("s3://bucket/table", mode="overwrite")
```

#### PyArrow
```python
# Delete directory first, then write
import shutil
shutil.rmtree("s3://bucket/table")  # Or use fs.rm with recursive
ds.write_dataset(table, "s3://bucket/table", format="parquet")
```

#### DuckDB
```sql
COPY table TO 's3://bucket/table.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);
-- For Delta: use overwrite mode or VACUUM
```

#### PyIceberg
```python
table.overwrite(new_arrow_table)
```

---

### Overwrite (Partition)

Replace specific partitions only.

#### Polars/Delta
```python
df.write_delta(
    "s3://bucket/table",
    mode="overwrite",
    partition_filters=[("year", "=", "2024")]
)
```

#### PyArrow
```python
# Identify partition paths, replace files in those partitions
partition_path = "s3://bucket/table/year=2024/"
fs.delete(partition_path, recursive=True)
ds.write_dataset(
    table_filtered,
    partition_path,
    format="parquet"
)
```

---

### Upsert / Merge

#### Polars (Delta Lake only)
```python
df.write_delta(
    "s3://bucket/table",
    mode="merge",
    delta_merge_options={
        "predicate": "source.id = target.id",
        "source_alias": "source",
        "target_alias": "target"
    }
).when_matched_update_all() \
 .when_not_matched_insert_all() \
 .execute()
```

#### DuckDB (Any format with SQL)
```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    col1 = source.col1,
    col2 = source.col2
WHEN NOT MATCHED THEN INSERT (id, col1, col2)
VALUES (source.id, source.col1, source.col2);
```

#### PyIceberg
```python
table.merge(
    source_table,
    predicate="target.id = source.id"
).when_matched_update_all() \
 .when_not_matched_insert_all() \
 .execute()
```

#### DuckDB Lakehouse (Delta)
```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Perform merge directly on Delta table
con.execute("""
MERGE INTO delta_scan('s3://bucket/delta_table') AS target
USING source_data AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ... 
WHEN NOT MATCHED THEN INSERT ...
""")
```

---

## Schema Evolution

### Adding Columns (Backward Compatible)

**Delta Lake:**
```python
# Option 1: mergeSchema during write
df.write_delta(
    table_uri,
    mode="append",
    delta_write_options={"schema_mode": "merge"}  # Adds new columns
)

# Option 2: explicit ALTER TABLE
from deltalake import DeltaTable
dt = DeltaTable(table_uri)
dt.alter_table.add_columns(
    {"new_column": "string", "another_new": "int"}
)
```

**Iceberg:**
```python
# Using PyIceberg
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType

table.update_schema().add_column("new_column", StringType()).commit()
# Or via Spark SQL:
# ALTER TABLE my_table ADD COLUMN new_column STRING;
```

**DuckDB (Parquet with schema inference):**
DuckDB reads Parquet with extra columns gracefully (missing values become NULL). For persistent schema changes, update table definition:
```sql
-- DuckDB stores schema in its catalog; for external Parquet, use VIEW
CREATE OR REPLACE VIEW my_view AS
SELECT *, NULL::VARCHAR AS new_column FROM read_parquet('data.parquet');
```

---

### Changing Column Types (Type Widening)

**Delta Lake:**
```python
dt.alter_table.change_column(
    column_name="old_col",
    new_column_name="old_col",
    new_type="string",  # Cast to STRING (int → string OK)
    existing_nullability=True
)
```

**Iceberg:**
```python
# Update type (requires compatible widening)
table.update_schema().update_column(
    field_id=existing_field_id,
    new_type=StringType()
).commit()
```

**Best Practice:** Only widen (int → long, decimal → higher precision). Never narrow (string → int, long → int) without creating new column.

---

### Dropping Columns

**Delta Lake:**
```python
dt.alter_table.drop_column("obsolete_column")
```

**Iceberg:**
```python
table.update_schema().delete_column("obsolete_column").commit()
```

**Impact:** Old data still contains column but queries fail if accessed. Use time travel to recover.

---

### Schema Compatibility Rules

| Change | Delta Lake | Iceberg | DuckDB (Parquet) |
|--------|-----------|---------|-----------------|
| **Add column** | ✅ (mergeSchema) | ✅ | ✅ (NULL on read) |
| **Drop column** | ✅ | ✅ | ❌ (breaks if query uses) |
| **Rename column** | ✅ (renameColumn) | ✅ | ❌ (treats as new column) |
| **Type widen** | ✅ | ✅ | ✅ (if Parquet schema matches) |
| **Type narrow** | ❌ (requires rewrite) | ❌ | ❌ |

**Recommendation:** Never drop/rename columns in production tables. Add new columns instead, deprecate old ones in application code.

---

## Incremental Loads & Watermarking

### Watermark Pattern

Track high-water mark per table to load only deltas:

```python
import duckdb

con = duckdb.connect("lakehouse.db")

def incremental_load(source_table, target_path, timestamp_col="last_modified"):
    # 1. Get last watermark
    old_wm = con.execute(f"""
        SELECT watermark_value
        FROM watermark_table
        WHERE table_name = '{target_path}'
    """).fetchone()[0] or "1970-01-01"

    # 2. Get new watermark (max from source)
    new_wm = con.execute(f"""
        SELECT MAX({timestamp_col}) FROM {source_table}
    """).fetchone()[0]

    # 3. Load增量
    df = con.execute(f"""
        SELECT * FROM {source_table}
        WHERE {timestamp_col} > '{old_wm}'
          AND {timestamp_col} <= '{new_wm}'
    """).pl()  # Returns Polars DataFrame

    # 4. Append to target (Delta recommended)
    df.write_delta(target_path, mode="append")

    # 5. Update watermark
    con.execute("""
        INSERT OR REPLACE INTO watermark_table (table_name, watermark_value)
        VALUES (?, ?)
    """, [target_path, new_wm])

    return len(df)
```

### CDC (Change Data Capture)

For databases with CDC streams (Debezium, Striim):

```python
# CDC records: op (c/u/d), before/after columns, ts
def apply_cdc(df_cdc, target_table):
    # Split by operation
    inserts = df_cdc.filter(pl.col("op") == "c")
    updates = df_cdc.filter(pl.col("op") == "u")
    deletes = df_cdc.filter(pl.col("op") == "d")

    # Appends
    inserts.select(["after.*"]).write_delta(target_table, mode="append")

    # Updates → MERGE
    if len(updates) > 0:
        updates.select(["after.*"]).write_delta(
            target_table,
            mode="merge",
            delta_merge_options={"predicate": "target.id = source.id"}
        ).when_matched_update_all().execute()

    # Deletes → merge with when_matched_delete()
    if len(deletes) > 0:
        deletes.select(["before.*"]).write_delta(
            target_table,
            mode="merge",
            delta_merge_options={"predicate": "target.id = source.id"}
        ).when_matched_delete().execute()
```

---

## Data Quality Testing

### Validation Frameworks

| Tool | Best For | Integration |
|------|----------|-------------|
| **Pandera** | DataFrame schemas (Pandas, Polars, Spark) | In-code validation |
| **Great Expectations** | Full suite (expectations, docs, validation) | Batch validation |
| **Pydantic** | Row-level contracts, API inputs | Data contracts |
| **DuckDB constraints** | Simple checks at query time | SQL-based |

### Pandera Example (Polars Backend)

```python
import pandera as pa
from pandera.polars import DataFrameSchema, Column

schema = DataFrameSchema({
    "order_id": Column(pa.Int64, nullable=False, unique=True),
    "customer_id": Column(pa.Int32, nullable=False),
    "amount": Column(pa.Float64, pa.Check.ge(0)),
    "order_date": Column(pa.Date, pa.Check(lambda s: s >= "2024-01-01"))
})

# Validate at Silver layer
silver_df = pl.read_delta("s3://lakehouse/silver/orders/")
validated = schema.validate(silver_df, lazy=True)  # Collect all errors
```

### Great Expectations Checkpoint

```yaml
# great_expectations/checkpoints/silver_orders.yml
run_name: silver_orders_validation
validations:
  - batch_request:
      datasource_name: silver_delta
      data_connector_name: delta_connector
      data_asset_name: orders
      batch_identifiers:
        path: s3://lakehouse/silver/orders/
    expectation_suite_name: silver_orders_suite
```

Run:
```bash
great_expectations checkpoint run silver_orders
```

### Data Contracts with Pydantic

```python
from pydantic import BaseModel, Field, validator
from datetime import date

class OrderContract(BaseModel):
    order_id: int = Field(gt=0)
    customer_id: int = Field(gt=0)
    amount: float = Field(ge=0)
    order_date: date

    @validator("order_date")
    def not_future(cls, v):
        assert v <= date.today(), "order_date cannot be in future"
        return v

# Validate each row (or batch via parse_obj_list)
for record in df.to_dicts():
    OrderContract(**record)  # Raises ValidationError if invalid
```

---

## Observability & Monitoring

### Metrics to Track

- **Pipeline success rate** (% of runs successful)
- **Data freshness** (time since last update)
- **Row counts** (detect drops/spikes)
- **Schema changes** (add/drop columns)
- **Null rate** per column
- **Duplicate rate**
- **Latency** (ingest → curated)

### Implementation with OpenTelemetry

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer("data-pipeline")

meter = metrics.get_meter("data-pipeline")
row_counter = meter.create_counter(
    "rows.processed",
    description="Number of rows processed"
)

@tracer.start_as_current_span("bronze_to_silver")
def transform_bronze():
    df = pl.scan_delta("s3://bronze/").collect()
    row_counter.add(len(df), {"layer": "bronze"})
    # ... transform
```

### Alerting

Set thresholds on metrics (Prometheus + Alertmanager):

```yaml
# alerts.yml
- alert: DataFreshnessStale
  expr: time() - data_freshness_timestamp > 3600  # 1 hour
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Dataset {{ $labels.dataset }} stale"
    description: "No new data for > 1 hour"
```

---

## Cost Optimization

### Storage Tiering

| Access Frequency | Storage Class | Use Case |
|------------------|---------------|----------|
| Hot (daily) | S3 Standard / GCS Standard | Bronze, Silver, recent Gold |
| Warm (weekly) | S3 IA / GCS Nearline | Gold older than 1 month |
| Cold (rare) | S3 Glacier / GCS Archive | Bronze archive, compliance |

Automate lifecycle policies:

```python
# AWS S3 Lifecycle policy (JSON)
{
  "Rules": [{
    "ID": "Move Bronze to Glacier after 90 days",
    "Status": "Enabled",
    "Prefix": "bronze/",
    "Transitions": [{
      "Days": 90,
      "StorageClass": "GLACIER"
    }]
  }]
}
```

### Query Optimization

- **Predicate pushdown**: Filter on partition columns first
- **Column pruning**: Select only needed columns
- **Caching**: Materialize frequently accessed Gold aggregates
- **Compression**: Zstd level 3 for balance

### Avoiding Small Files

Monitor file counts:

```python
import pyarrow.fs as fs

s3 = fs.S3FileSystem()
infos = s3.get_file_info(fs.FileSelector("s3://lakehouse/silver/", recursive=True))
print(f"Total files: {len(infos)}")
print(f"Average size: {sum(info.size for info in infos)/len(infos):.0f} bytes")

# If too many small files, re-partition
if len(infos) > 100_000:
    # Rewrite with larger target_size_mb
    pass
```

---

## Testing Strategies

### Unit Tests (Logic)

Test transformation functions in isolation:

```python
# test_transforms.py
def test_normalize_customer_name():
    input_df = pl.DataFrame({"name": ["  John  ", "Jane"]})
    result = normalize_name(input_df)
    expected = pl.DataFrame({"name": ["john", "jane"]})
    assert result.equals(expected)
```

### Integration Tests (Full Pipeline)

Test end-to-end with temporary storage:

```python
import tempfile
import shutil

def test_bronze_to_silver_pipeline():
    tmpdir = tempfile.mkdtemp()
    try:
        # 1. Write test bronze data
        bronze_path = f"{tmpdir}/bronze/"
        test_data.write_delta(bronze_path, mode="overwrite")

        # 2. Run pipeline (call main function)
        run_pipeline(bronze_path, f"{tmpdir}/silver/")

        # 3. Validate outputs
        silver = pl.read_delta(f"{tmpdir}/silver/")
        assert len(silver) > 0
        assert schema.validate(silver)
    finally:
        shutil.rmtree(tmpdir)
```

### Data Quality Tests

Integrate Pandera/Great Expectations into CI:

```python
def test_silver_data_quality():
    df = pl.read_delta("s3://lakehouse/silver/orders/")
    result = schema.validate(df, lazy=True)
    assert result.errors is None, f"Validation failed: {result.errors}"
```

---

## Recovery & Time Travel

### Delta Lake

```python
from deltalake import DeltaTable

dt = DeltaTable("s3://bucket/table")

# List versions
dt.history()  # Returns DataFrame with version, timestamp, operation

# Time travel query
dt = DeltaTable("s3://bucket/table@v5")  # Version 5
df = dt.to_pandas()

# Restore previous version
dt.restore(version=10)
```

### Iceberg

```python
from pyiceberg.table import Table

# Snapshot history
table.history()  # List of Snapshot objects

# Read previous snapshot
prev_snapshot = table.snapshots()[-2]
table = Table(identifier, snapshot_id=prev_snapshot.snapshot_id)

# Use Spark:
# SELECT * FROM my_table VERSION AS OF 10;
```

### DuckDB (Parquet only)

DuckDB doesn't track versions natively. Implement app-level versioning:

```sql
-- Store each batch with batch_id column
CREATE TABLE my_data AS SELECT *, 1 AS batch_id FROM read_parquet('batch1.parquet');

-- Query specific batch
SELECT * FROM my_data WHERE batch_id = 1;
```

---

## References

- `@data-engineering-storage-lakehouse` - Delta Lake, Iceberg, DuckLake deep dive
- `@data-engineering-storage-remote-access` - S3, GCS, Azure backend details
- `@data-engineering-storage-formats` - Parquet, Arrow, Lance file formats
- `@data-engineering-quality` - Great Expectations, Pandera setup
- **`@data-engineering-catalogs`** - Data catalog systems, Iceberg catalogs, DuckDB multi-source pattern
- [Delta Lake Schema Evolution](https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/)
- [Apache Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [DuckDB MERGE INTO](https://duckdb.org/docs/stable/sql/statements/merge_into.html)
- [PyArrow Dataset API](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html)
