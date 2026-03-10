# Delta Lake

Delta Lake is an open-source storage layer that brings ACID transactions, schema evolution, and time travel to data lakes. It's the most widely adopted lakehouse format, especially in the Databricks ecosystem.

## Installation

```bash
# Pure-Python API (recommended for non-Spark workflows)
pip install deltalake pyarrow

# OR PySpark integration
pip install delta-spark pyspark
```

## Pure-Python API (deltalake)

The `deltalake` package provides a native Python interface without requiring Spark.

### Basic Operations
```python
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# Create Delta table from PyArrow Table
data = pa.table({
    "id": [1, 2, 3],
    "value": [100.0, 200.0, 150.0],
    "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"]
})

write_deltalake(
    "data/delta-table",
    data,
    mode="overwrite"
)

# Read Delta table
dt = DeltaTable("data/delta-table")
df = dt.to_pandas()  # or dt.to_pyarrow_table(), dt.to_arrow_table()

# Query with predicates (pushdown)
df_filtered = dt.to_pandas(filters=[("timestamp", ">=", "2024-01-02")])
```

### Updates and Merges
```python
# Update rows (ACID transaction)
from deltalake import DeltaTable
from pyarrow import compute as pc

dt = DeltaTable("data/delta-table")

# Update using expression
dt.update(
    predicate="value < 150",
    updates={"value": pc.multiply(pa.scalar(1.1), "value")}
)

# Upsert (merge)
new_data = pa.table({
    "id": [2, 4],
    "value": [250.0, 400.0]
})

dt.merge(
    source=new_data,
    predicate="target.id = source.id"
).when_matched_update_all() \
 .when_not_matched_insert_all() \
 .execute()
```

### Time Travel
```python
# Read by version
dt_v1 = DeltaTable("data/delta-table")
dt_v1.load_version(1)  # Load version 1
df_v1 = dt_v1.to_pandas()

# Read by timestamp
dt_v2 = DeltaTable("data/delta-table")
dt_v2.load_with_datetime("2024-01-02T12:00:00Z")
df_v2 = dt_v2.to_pandas()

# Get history
history = dt.history()  # Returns Arrow table with commit metadata
print(history.to_pandas()[["version", "timestamp", "operation"]])
```

### Maintenance
```python
# Vacuum old files (retain last 24 hours)
dt.vacuum(retention_hours=24)

# Optimize compaction (combine small files)
dt.optimize().execute()

# Show file list
files = dt.files()
print(files)  # List of Parquet files in the table
```

## PySpark Integration

If you're already in a Spark environment, use the `delta-spark` package.

```python
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

builder = SparkSession.builder \
    .appName("DeltaExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Write
df = spark.createDataFrame([(1, "A", 100.0)], ["id", "category", "value"])
df.write.format("delta") \
    .mode("overwrite") \
    .save("s3://bucket/delta-table")

# Read
df = spark.read.format("delta").load("s3://bucket/delta-table")

# Upsert (merge)
delta_table = DeltaTable.forPath(spark, "s3://bucket/delta-table")

delta_table.alias("target").merge(
    df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Time travel
df_v1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("s3://bucket/delta-table")

# Vacuum
delta_table.vacuum(24)  # Retain 24 hours
```

## Cloud Storage Integration

See `@data-engineering-storage-remote-access/integrations/delta-lake` for S3, GCS, Azure configuration using `storage_options` or PyArrow filesystem.

## Best Practices

1. **Partition by date/region** for efficient querying
2. **Vacuum regularly** to remove obsolete files (retain based on recovery needs)
3. **Optimize** to compact small files (before vacuum)
4. **Use time travel** for audit trails, reproducibility, rollback
5. **Batch writes** for throughput (avoid single-row transactions)
6. **Monitor table history size** - large histories slow down metadata operations; archive old versions

## Common Pitfalls

- ❌ **Don't** vacuum too aggressively (you might lose ability to time travel)
- ❌ **Don't** disable file cleanup entirely (storage bloat)
- ❌ **Don't** use Delta on HDFS (not supported - use S3, GCS, Azure, local)
- ❌ **Don't** skip `OPTIMIZE` on high-churn tables (small file problem)

---

## References

- [Delta Lakedocs](https://docs.delta.io/latest/index.html)
- [deltalake Python API](https://delta-io.github.io/delta-rs/python/quickstart.html)
- [PySpark Delta Guide](https://docs.delta.io/latest/delta-batch.html)
