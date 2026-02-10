# Apache Hudi

Apache Hudi (Hadoop Upserts Deletes and Incrementals) is a data lake storage framework focused on change data capture (CDC) and streaming upserts. It's optimized for low-latency upserts and incremental data processing.

## Key Characteristics

- **CDC-First**: Built for streaming ingestion with upsert and delete support
- **Write Optimizations**: Copy-on-write (CoW) and Merge-on-Read (MoR)
- **Indexes**: Bloom filters, HBase indexing for fast upserts
- **Ecosystem**: Primarily Spark-based; no pure-Python library exists

## Installation

Hudi is distributed as Spark JARs, not a pip package.

```bash
# Set environment variables
export SPARK_VERSION=3.5
export HUDI_VERSION=1.1.1
export SCALA_VERSION=2.13

# Launch PySpark with Hudi packages
pyspark --packages \
  org.apache.hudi:hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}:${HUDI_VERSION},\
  org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.hive.convertMetastoreParquet=false'
```

Or use `spark-shell`/`spark-submit` with `--packages`.

## Core Concepts

- **Hoodie Table**: The table format with built-in indexing
- **Write Operation**: INSERT (bulk), UPSERT (incremental), DELETE
- **Indexing**: Bloom filters, global/local indexes for fast upserts
- **Compaction**: Merge-on-Read requires compaction for optimal reads

## PySpark Operations

### Write with Upsert Support
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HudiExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Sample data
df = spark.createDataFrame([
    (1, "Alice", 100.0, "2024-01-01"),
    (2, "Bob", 200.0, "2024-01-02")
], ["id", "name", "value", "ts"])

# Write (creates table, converts to Hudi format)
df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .mode("overwrite") \
    .save("s3://bucket/hudi-table")
```

### Upsert (Incremental) Operation
```python
# New updates/changes
updates_df = spark.createDataFrame([
    (1, "Alice Updated", 150.0, "2024-01-01T12:00:00"),  # Update existing
    (3, "Charlie", 300.0, "2024-01-03")                  # Insert new
], ["id", "name", "value", "ts"])

updates_df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save("s3://bucket/hudi-table")
```

### Delete Operation
```python
deletes_df = spark.createDataFrame([(2,)], ["id"])

deletes_df.write.format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append") \
    .save("s3://bucket/hudi-table")
```

### Query
```python
# Read latest snapshot (default)
df = spark.read.format("hudi").load("s3://bucket/hudi-table")

# Read incremental feed (for streaming pipelines)
incremental_df = spark.read.format("hudi") \
    .option("hoodie.datasource.read.begin_commit", "100") \
    .option("hoodie.datasource.read.end_commit", "latest") \
    .load("s3://bucket/hudi-table")

# Read prepped (MoR mode - reads merged view)
df_prepped = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "read_prepped") \
    .load("s3://bucket/hudi-table")
```

## Table Types

Hudi supports two table types:

1. **Copy-on-Write (CoW)**: Writers rewrite entire files on update. Simpler, good for read-heavy workloads.
2. **Merge-on-Read (MoR)**: Writers append delta logs, compaction merges later. Good for write-heavy/streaming.

```python
# Create CoW table
df.write.format("hudi") \
    .option("hoodie.table.name", "cow_table") \
    .option("hoodie.table.type", "COPY_ON_WRITE") \
    .mode("overwrite") \
    .save("s3://bucket/hudi-cow-table")

# Create MoR table (with compaction schedule)
df.write.format("hudi") \
    .option("hoodie.table.name", "mor_table") \
    .option("hoodie.table.type", "MERGE_ON_READ") \
    .option("hoodie.compact.inline", "true") \
    .option("hoodie.compact.inline.max.delta.commits", "4") \
    .mode("overwrite") \
    .save("s3://bucket/hudi-mor-table")
```

## Use Cases

- ✅ **CDC from databases**: Replicate database changes to data lake
- ✅ **Streaming upserts**: Kafka → Hudi → Lake
- ✅ **Incremental processing**: Query only new/changed data since last run
- ✅ **Upsert-heavy workloads**: Frequent updates to existing records

## Limitations

- ❌ No pure-Python library - requires Spark
- ❌ More complex operational overhead than Delta/Iceberg
- ❌ Smaller community (though still active)
- ❌ Limited engine support (primarily Spark)

---

## References

- [Apache Hudi Documentation](https://hudi.apache.org/docs/)
- [Hudi Spark Quick Start](https://hudi.apache.org/docs/spark-quick-start-guide)
- [Hudi Architecture](https://hudi.apache.org/blog/2021/01/04/hudi-architecture/)
