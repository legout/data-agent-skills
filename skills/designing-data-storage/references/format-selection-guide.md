# Format Selection Guide

Comprehensive decision guide for selecting the right data storage format for your use case. Covers six file formats (Parquet, Arrow/Feather, Lance, Zarr, Avro, ORC) and three lakehouse table formats (Delta Lake, Apache Iceberg, Apache Hudi).

## Table of Contents
1. [Quick Decision Matrix](#quick-decision-matrix)
2. [File Format Selection](#file-format-selection)
3. [Lakehouse Format Selection](#lakehouse-format-selection)
4. [Use Case Scenarios](#use-case-scenarios)
5. [Compression Guidelines](#compression-guidelines)
6. [Migration Patterns](#migration-patterns)

---

## Quick Decision Matrix

| Use Case | Recommended Format | Alternative | Avoid |
|----------|-------------------|-------------|-------|
| Data lake analytics (batch) | **Delta Lake** | Parquet, Iceberg | Avro, ORC |
| Multi-engine analytics | **Apache Iceberg** | Delta Lake | Hudi |
| CDC / streaming ingestion | **Apache Hudi** | Delta Lake | Parquet alone |
| ML training pipelines | **Lance** | Arrow/Feather | Parquet |
| Vector/embeddings storage | **Lance** | Parquet + vector DB | Raw Parquet |
| Geospatial / N-dim arrays | **Zarr** | HDF5 | Parquet |
| Inter-process communication | **Arrow IPC** | Feather | Parquet |
| Kafka/streaming events | **Avro** | JSON | Parquet |
| Spark/Databricks ecosystem | **Delta Lake** | Parquet | Iceberg* |
| Hive/Hadoop legacy | **ORC** | Parquet | Delta Lake |

*Iceberg works but Delta has better Spark integration

---

## File Format Selection

### Parquet (The Default Choice)

**Choose when:**
- You need broad ecosystem compatibility (Spark, DuckDB, Polars, pandas, Trino)
- Running analytical queries with column pruning and predicate pushdown
- Building partitioned data lakes (Hive-style: `year=2024/month=01/`)
- Need mature tooling with good compression options

**Avoid when:**
- High-frequency row-level updates (use Delta/Iceberg/Hudi instead)
- Real-time streaming ingestion (use Avro first, convert to Parquet)
- N-dimensional array data (use Zarr)
- Pure ML training pipelines (use Lance or Arrow)

**Performance tuning:**
- Row group size: 100K-1M rows for optimal skipping
- Compression: Zstd level 3 for balance, Snappy for speed
- Dictionary encoding: Enable for low-cardinality strings
- Target file size: ~256MB to 1GB

### Apache Arrow / Feather (In-Memory & IPC)

**Choose when:**
- Zero-copy sharing between Python/R/Java processes
- Fast serialization for ML training data
- In-memory analytics before writing to persistent storage
- Need to leverage Arrow compute kernels (vectorized operations)

**Avoid when:**
- Long-term archival storage (no built-in partitioning)
- Data lake storage (use Parquet or table formats)
- Cross-language persistence beyond Arrow ecosystem

**Key insight:** Arrow is an in-memory format; Feather and IPC are the on-disk serialization formats. They offer the fastest read/write for Arrow-native tools.

### Lance (ML-Native)

**Choose when:**
- Storing embeddings/vectors alongside structured data
- Multi-modal datasets (images + text + vectors in one table)
- Versioned datasets with Git-like branching
- Cloud-native storage without separate metadata catalog
- Need built-in vector search (IVF_PQ, HNSW indexes)

**Avoid when:**
- Pure SQL analytics (Parquet/Delta are better)
- Non-ML use cases without vectors
- Teams not doing ML/AI work

**Advantages over Parquet for ML:**
- Built-in vector indexes (no separate Pinecone/Milvus needed)
- Zero-copy memory mapping
- Version control for datasets
- Optimized for cloud object storage (S3/GCS)

### Zarr (Chunked N-Dimensional Arrays)

**Choose when:**
- N-dimensional arrays (tensors, satellite imagery, medical scans)
- Chunked, compressed scientific data
- Parallel reads/writes across chunks
- Cloud-optimized array storage (each chunk = separate object)

**Avoid when:**
- Tabular/relational data (use Parquet)
- Need SQL querying (use Parquet + DuckDB)
- Small datasets (overhead not worth it)

**Common use cases:**
- Satellite imagery (geospatial)
- Medical imaging (DICOM alternative)
- Climate/weather model outputs
- Deep learning training data (large tensors)

### Avro (Row-Based Streaming)

**Choose when:**
- Kafka/Kinesis streaming ingestion
- Schema evolution is critical (backward/forward compatibility)
- Row-based access patterns
- Need to serialize complex nested objects

**Avoid when:**
- Analytical queries (no column pruning)
- Large-scale data lakes (use Parquet)
- Need predicate pushdown

**Common pattern:**
```
Kafka (Avro) → Stream Processor → Data Lake (Parquet/Delta)
```

### ORC (Hive/Hadoop Legacy)

**Choose when:**
- Working in Hive/Hadoop ecosystem
- Need Hive ACID transactions
- Legacy big data pipelines requiring ORC

**Avoid when:**
- Modern analytics stack (Spark 3.x, DuckDB, Polars)
- Starting new projects (use Parquet instead)
- Need broad engine support

**Note:** ORC and Parquet are similar; Parquet has won the modern ecosystem.

---

## Lakehouse Format Selection

### Delta Lake (The Safe Default)

**Choose when:**
- You're in Spark/Databricks ecosystem
- Want simplest mental model for ACID + time travel
- Need pure-Python library (`deltalake`) without Spark
- Want mature, battle-tested solution

**Key strengths:**
- Best Spark/Databricks integration
- Simple pure-Python API (`deltalake`)
- Time travel: `load_version()`, `load_with_datetime()`
- Mature ecosystem with good tooling

**Tradeoffs:**
- Less flexible catalog system (tied to Spark catalog)
- Partition evolution requires table rewrite
- Limited schema rename support

### Apache Iceberg (Engine-Agnostic)

**Choose when:**
- Multi-engine environment (Spark, Trino, Flink, DuckDB)
- Need advanced schema evolution (column rename, branching)
- Want dynamic partition evolution
- Prefer explicit catalog abstraction

**Key strengths:**
- True engine-agnostic design
- Dynamic partition evolution
- Advanced schema evolution (rename, reorder)
- Pluggable catalog (Hive, Glue, REST, Nessie)

**Tradeoffs:**
- More complex setup (must configure catalog)
- Slightly steeper learning curve
- PyIceberg less mature than `deltalake`

### Apache Hudi (CDC-First)

**Choose when:**
- Building CDC pipelines from databases
- Streaming ingestion with upserts
- Low-latency incremental processing
- Need merge-on-read for write-heavy workloads

**Key strengths:**
- Built for CDC and streaming
- Copy-on-Write vs Merge-on-Read options
- Incremental query support (read only changes)
- Bloom filter indexing for fast upserts

**Tradeoffs:**
- No pure-Python library (Spark only)
- More operational complexity
- Smaller community than Delta/Iceberg
- Primarily Spark ecosystem

---

## Use Case Scenarios

### Scenario 1: Modern Data Lake (Batch Analytics)

**Setup:** Company building data lake on S3, using Spark/DuckDB/Polars

**Recommendation:** Delta Lake or Iceberg on Parquet

**Rationale:**
- ACID transactions for data quality
- Time travel for reproducibility
- Broad engine support
- Partitioning and optimization

### Scenario 2: Real-Time ML Feature Store

**Setup:** Need to store embeddings and serve low-latency similarity search

**Recommendation:** Lance

**Rationale:**
- Built-in vector indexes
- Version control for feature snapshots
- Cloud-native (no separate metadata service)
- Multi-modal support

### Scenario 3: Kafka → Data Lake Pipeline

**Setup:** Streaming events from Kafka, need durable lake storage

**Recommendation:** Hudi or Delta Lake

**Rationale:**
- Both support streaming ingestion
- Hudi better for pure CDC use case
- Delta simpler if already in Spark ecosystem

### Scenario 4: Scientific Computing / Geospatial

**Setup:** Satellite imagery, climate models, large tensor datasets

**Recommendation:** Zarr

**Rationale:**
- N-dimensional array support
- Chunked compression
- Parallel I/O
- xarray/Dask integration

### Scenario 5: Cross-Service Communication

**Setup:** Microservices sharing data, need fast serialization

**Recommendation:** Arrow IPC / Feather

**Rationale:**
- Zero-copy between processes
- Language-agnostic
- Fastest serialization for Arrow-native tools

---

## Compression Guidelines

### General Rules

| Priority | Codec | Use When |
|----------|-------|----------|
| Speed | **Snappy** | Real-time queries, default choice |
| Balance | **Zstd** | General purpose, good compression ratio |
| Maximum | **Gzip/Brotli** | Archival, cold storage |
| Streaming | **LZ4** | Kafka, real-time pipelines |

### Format-Specific Recommendations

**Parquet:**
- Default: `zstd` level 3
- Speed critical: `snappy`
- Cold storage: `gzip`

**Arrow/Feather:**
- Default: `lz4` (speed) or `zstd` (size)

**Lance:**
- Default: `zstd`
- Vectors: Often uncompressed (already dense floats)

**Zarr:**
- Default: `Blosc` with `zstd` or `lz4`
- Tune `clevel` 3-5 for balance

**Avro:**
- Default: `snappy` or `deflate`

---

## Migration Patterns

### Avro → Parquet (ETL Pipeline)

```python
import polars as pl

# Stream processing: convert to Parquet for analytics
df = pl.read_avro("kafka-events.avro")
df.write_parquet("s3://lake/events/2024/01/15/data.parquet")
```

### Parquet → Delta Lake (Upgrade to ACID)

```python
from deltalake import write_deltalake
import pyarrow.parquet as pq

# Convert existing Parquet to Delta
table = pq.read_table("legacy-data.parquet")
write_deltalake("s3://lake/delta-table", table, mode="overwrite")
```

### CSV → Parquet (Never use CSV for analytics)

```python
import polars as pl

df = pl.read_csv("data.csv")
df.write_parquet("data.parquet", compression="zstd")
```

### Parquet → Lance (Add Vector Search)

```python
import lancedb
import pyarrow.parquet as pq

table = pq.read_table("embeddings.parquet")
db = lancedb.connect("./feature-store.lance")
db.create_table("embeddings", table)
```

---

## Summary Checklist

Before selecting a format, ask:

1. **Do you need ACID transactions?** → Use Delta Lake, Iceberg, or Hudi
2. **Do you need vector search?** → Use Lance
3. **Do you have N-dimensional arrays?** → Use Zarr
4. **Are you streaming from Kafka?** → Use Avro first, then convert
5. **Is this for long-term analytics?** → Use Parquet or table format
6. **Do you need multi-engine support?** → Prefer Iceberg
7. **Are you in Spark/Databricks?** → Prefer Delta Lake
8. **Is this for CDC/streaming ingestion?** → Prefer Hudi
9. **Do you need zero-copy IPC?** → Use Arrow/Feather
10. **Is this legacy Hive?** → Use ORC (but consider migrating)

**When in doubt:**
- Start with **Parquet** for file-based analytics
- Upgrade to **Delta Lake** when you need ACID
- Use **Lance** for ML/vector workloads
