# Parquet Deep Dive

## Table of Contents
1. [When to Use Parquet](#when-to-use-parquet)
2. [Layout Concepts](#layout-concepts)
3. [Writer Settings](#writer-settings)
4. [Read Optimization](#read-optimization)
5. [Partitioning Patterns](#partitioning-patterns)
6. [Common Pitfalls](#common-pitfalls)

---

## When to Use Parquet

Use Parquet for analytical workloads where you need:
- column pruning (`SELECT col_a, col_b`)
- predicate pushdown (`WHERE date >= ...`)
- strong ecosystem compatibility (Spark, DuckDB, Polars, PyArrow, Trino)
- good compression with fast scans

Avoid Parquet for high-frequency row-level transactional updates; use Delta/Iceberg/Hudi table formats instead.

---

## Layout Concepts

Parquet file internals:
- **Row groups**: horizontal chunks of rows
- **Column chunks**: one per column per row group
- **Pages**: internal encoded/compressed blocks
- **Footer metadata**: schema + per-row-group stats (min/max/null count)

Why this matters:
- Predicate pushdown uses row-group stats to skip irrelevant data
- Columnar storage enables reading only requested columns

---

## Writer Settings

### PyArrow (full control)

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "id": [1, 2, 3],
    "amount": [10.5, 20.0, 30.25],
    "region": ["eu", "us", "eu"],
})

pq.write_table(
    table,
    "events.parquet",
    compression="zstd",          # good balance
    compression_level=3,
    row_group_size=250_000,       # tune by workload
    use_dictionary=["region"],   # dictionary-encode low-cardinality columns
)
```

### Polars (simple)

```python
import polars as pl

df = pl.DataFrame({"id": [1, 2], "value": [100.0, 200.0]})
df.write_parquet("events.parquet", compression="zstd")
```

Recommended defaults:
- compression: `zstd` (or `snappy` for maximum read/write speed)
- target file size: ~256MB to 1GB
- avoid huge numbers of tiny files

---

## Read Optimization

### Column pruning + filters

```python
import polars as pl

out = (
    pl.scan_parquet("s3://bucket/events/")
    .select(["id", "amount", "event_date"])
    .filter(pl.col("event_date") >= pl.date(2024, 1, 1))
    .collect()
)
```

### PyArrow dataset scanner

```python
import pyarrow.dataset as ds

dataset = ds.dataset("s3://bucket/events/", format="parquet")
result = dataset.to_table(
    columns=["id", "amount"],
    filter=ds.field("year") == 2024,
)
```

---

## Partitioning Patterns

Use hive-style partitions for large datasets:

```text
data/
  year=2024/month=01/day=15/part-000.parquet
```

Good partition keys:
- date/time columns used in filters
- tenant/region keys with moderate cardinality

Avoid:
- high-cardinality partition keys (e.g., user_id)
- over-partitioning that creates many tiny files

---

## Common Pitfalls

1. **Too many small files**
   - Symptoms: slow listing + scan overhead
   - Fix: periodic compaction

2. **Wrong row-group size**
   - Too small: metadata overhead
   - Too large: reduced skipping granularity

3. **Using CSV for analytics pipelines**
   - Convert to Parquet as early as possible

4. **No partition strategy**
   - Forces full scans for time-based queries

5. **Assuming Parquet provides ACID**
   - It does not. Use Delta/Iceberg/Hudi for transactional semantics.
