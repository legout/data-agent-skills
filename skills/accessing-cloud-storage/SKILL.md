---
name: accessing-cloud-storage
description: "Access cloud storage (S3, GCS, Azure) in Python using fsspec, pyarrow.fs, or obstore. Includes DataFrame integrations (Polars, DuckDB, Pandas, PyArrow), performance optimization, patterns for incremental loading, partitioned writes, and cross-cloud copy."
dependsOn: ["@data-engineering-core", "@data-engineering-storage-authentication", "@designing-data-storage"]
---

# Accessing Cloud Storage

Comprehensive guide to accessing cloud storage (S3, GCS, Azure) and remote filesystems in Python. Covers three major libraries - **fsspec**, **pyarrow.fs**, and **obstore** - and their integration with data engineering tools.

## Quick Comparison

| Feature | fsspec | pyarrow.fs | obstore |
|---------|--------|------------|---------|
| **Best For** | Broad compatibility, ecosystem integration | Arrow-native workflows, Parquet | High-throughput, performance-critical |
| **Backends** | S3, GCS, Azure, HTTP, FTP, 20+ more | S3, GCS, HDFS, local | S3, GCS, Azure, local |
| **Performance** | Good (with caching) | Excellent for Parquet | **9x faster** for concurrent ops |
| **Dependencies** | Backend-specific (s3fs, gcsfs) | Bundled with PyArrow | **Zero Python deps** (Rust) |
| **Async Support** | Yes (aiohttp) | Limited | Native sync/async |
| **DataFrame Integration** | Universal | PyArrow-native | Via fsspec wrapper |
| **Maturity** | Very mature (2018+) | Mature | New (2025), rapidly evolving |

## When to Use Which?

### Use fsspec when:
- You need broad ecosystem compatibility (pandas, xarray, Dask)
- Working with multiple storage backends (S3, GCS, Azure, HTTP)
- You need protocol chaining and caching features
- Your workflow involves diverse data formats beyond Parquet

### Use pyarrow.fs when:
- Your pipeline is Arrow/Parquet-native
- You need zero-copy integration with PyArrow datasets
- Predicate pushdown and column pruning are critical
- Working with partitioned Parquet datasets

### Use obstore when:
- Performance is paramount (many small files, high concurrency)
- You need async/await support for concurrent operations
- You want minimal dependencies (Rust-based)
- Working with large-scale data ingestion/egestion

## Skill Dependencies

Prerequisites:
- `@data-engineering-core` - Polars, DuckDB, PyArrow basics
- `@data-engineering-storage-authentication` - AWS, GCP, Azure auth patterns
- `@designing-data-storage` - File formats (Parquet, Arrow, Lance) and lakehouse formats (Delta Lake, Iceberg, Hudi)

Related:
- `@data-engineering-orchestration` - dbt with cloud storage

---

## Detailed Guides

### Library Deep Dives
This skill contains detailed guidance for all three libraries:
- **fsspec** - See [fsspec Library Guide](#fsspec-library-guide) below
- **pyarrow.fs** - See [PyArrow Filesystem Guide](#pyarrow-filesystem-guide) below
- **obstore** - See [obstore Library Guide](#obstore-library-guide) below

### DataFrame Integrations
- **Polars** - Native `s3://`, `gs://`, `az://` URIs with lazy evaluation and predicate pushdown
- **DuckDB** - HTTPFS extension for SQL queries directly on remote Parquet/JSON/CSV
- **Pandas** - fsspec auto-detection for transparent cloud URI handling
- **PyArrow** - Native filesystem with dataset scanning and batch processing

For detailed patterns, see [DataFrame Integration](#dataframe-integration) below. For Delta Lake and Iceberg table formats on cloud storage:
- `@designing-data-storage` - Delta Lake and Iceberg with cloud catalogs (S3/GCS/Azure)

### Infrastructure Patterns
- `@data-engineering-storage-authentication` - AWS, GCP, Azure auth patterns, IAM roles, service principals
- See `performance.md` in this skill - Caching, concurrency, async
- See `patterns.md` in this skill - Incremental loading, partitioned writes, cross-cloud copy

### Storage Formats
- `@designing-data-storage` - Parquet, Arrow/Feather, Lance, Zarr, Avro, ORC

---

## Quick Start Example

### Library Approaches

```python
import fsspec
import pyarrow.fs as fs
import pyarrow.parquet as pq
import obstore as obs

# Method 1: fsspec (universal)
s3_fs = fsspec.filesystem('s3')
with s3_fs.open('s3://bucket/data.parquet', 'rb') as f:
    data = f.read()

# Method 2: pyarrow.fs (Arrow-native)
s3_pa = fs.S3FileSystem(region='us-east-1')
table = pq.read_table("bucket/data.parquet", filesystem=s3_pa)

# Method 3: obstore (high-performance)
from obstore.store import S3Store
store = S3Store(bucket='my-bucket', region='us-east-1')
data = obs.get(store, 'data.parquet').bytes()
```

### DataFrame Approaches

```python
import polars as pl
import duckdb

# Polars: Native cloud URI (simplest)
df = pl.read_parquet("s3://bucket/data.parquet")
lazy_df = pl.scan_parquet("s3://bucket/dataset/**/*.parquet")

# DuckDB: SQL on remote files
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
df = con.sql("SELECT * FROM read_parquet('s3://bucket/data.parquet')").pl()

# All approaches work - choose based on your performance and ecosystem needs
```

---

## Library Guides

### fsspec Library Guide

fsspec provides a unified API for local and remote filesystems, integrating seamlessly with pandas, xarray, Dask, and many other Python data tools.

#### Installation

```bash
# Core only (no remote support)
pip install fsspec

# With specific backends
pip install fsspec[s3]        # S3 via s3fs
pip install fsspec[gcs]       # GCS via gcsfs
pip install fsspec[s3,gcs,azure]  # Multiple backends

# Or install backends directly
pip install s3fs gcsfs adlfs
```

#### Basic Usage

```python
import fsspec
import pandas as pd

# List available protocols
print(fsspec.available_protocols())
# ['file', 'memory', 'http', 'https', 's3', 's3a', 'gcs', 'gs', 'abfss', ...]

# Create filesystem instances
local_fs = fsspec.filesystem('file')
s3_fs = fsspec.filesystem('s3', anon=False)  # Uses boto3 credentials
gcs_fs = fsspec.filesystem('gcs')             # Uses GCP credentials

# Basic operations
s3_fs.ls('my-bucket/data/')                   # List files
s3_fs.exists('s3://my-bucket/data/file.csv')       # Check existence
s3_fs.mkdir('my-bucket/new-folder')           # Create directory

# Read file as bytes
with s3_fs.open('s3://my-bucket/data/file.txt', 'rb') as f:
    content = f.read()

# Read CSV directly into pandas
with s3_fs.open('s3://my-bucket/data/large.csv', 'rb') as f:
    df = pd.read_csv(f, compression='gzip')
```

#### Protocol Chaining & Caching

```python
# SimpleCache: Cache remote files locally for faster repeated access
import fsspec

# First read downloads, subsequent reads use cache
cached_file = fsspec.open_local(
    "simplecache::s3://my-bucket/large-file.nc",
    simplecache={'cache_storage': '/tmp/fsspec_cache', 'compression': None}
)

# Chain multiple protocols
# Read from HTTPS, cache locally, decompress on the fly
with fsspec.open(
    "simplecache::gzip::https://example.com/data.csv.gz",
    compression='gzip'
) as f:
    df = pd.read_csv(f)

# Other useful wrappers:
# - "filecache::" - Persistent disk cache
# - "gzip::" - Decompression
# - "zip::" - Zip file access
```

#### Advanced S3 Features

```python
import s3fs

# Detailed S3 configuration
fs = s3fs.S3FileSystem(
    key='AKIA...',
    secret='...',
    token='...',              # Temporary session token
    client_kwargs={
        'region_name': 'us-east-1',
        'endpoint_url': 'https://s3-compatible.local',  # MinIO, etc.
    },
    config_kwargs={
        'max_pool_connections': 50,
        'retries': {'max_attempts': 5}
    },
    skip_instance_cache=True   # Don't cache bucket listings
)

# Async operations
import asyncio

async def read_multiple():
    fs = s3fs.S3FileSystem(asynchronous=True)
    await fs.set_session()  # Establish async session

    # Concurrent reads (use _cat_file for bytes)
    data = await asyncio.gather(
        fs._cat_file('bucket/file1.parquet'),
        fs._cat_file('bucket/file2.parquet'),
        fs._cat_file('bucket/file3.parquet')
    )
    return data

# S3-specific features
fs.find('my-bucket', prefix='data/2024')  # List with prefix
fs.du('my-bucket/data')                   # Disk usage
fs.rm('my-bucket/temp/', recursive=True)  # Recursive delete
```

#### When to Use fsspec

Choose fsspec when:
- You need broad ecosystem compatibility (pandas, xarray, Dask)
- Working with multiple storage backends (S3, GCS, Azure, HTTP)
- You need protocol chaining and caching features
- Your workflow involves diverse data formats beyond Parquet

#### Performance Considerations

- ✅ Use `filecache::` instead of `simplecache::` for persistent caching across sessions
- ✅ Increase `max_pool_connections` for high concurrency
- ✅ Use async API for many concurrent small file operations
- ⚠️ For pure Parquet workflows with high throughput, consider `pyarrow.fs` instead
- ⚠️ For maximum performance on large concurrent operations, consider `obstore`

---

### PyArrow Filesystem Guide

PyArrow provides native filesystem integration optimized for Arrow and Parquet workflows.

#### Installation

```bash
pip install pyarrow
```

#### Basic Usage

```python
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# Create filesystem instances
s3_fs = fs.S3FileSystem(region='us-east-1')
gcs_fs = fs.GcsFileSystem()
local_fs = fs.LocalFileSystem()

# Read Parquet with column pruning
table = pq.read_table(
    "bucket/data.parquet",
    filesystem=s3_fs,
    columns=["id", "value"]  # Only read needed columns
)

# Dataset scanning with predicate pushdown
dataset = ds.dataset(
    "bucket/dataset/",
    filesystem=s3_fs,
    partitioning=ds.HivePartitioning.discover()
)

# Filter at storage layer
table = dataset.to_table(
    filter=(ds.field("year") == 2024) & (ds.field("value") > 100),
    columns=["id", "value"]
)
```

#### When to Use pyarrow.fs

Choose pyarrow.fs when:
- Your pipeline is Arrow/Parquet-native
- You need zero-copy integration with PyArrow datasets
- Predicate pushdown and column pruning are critical
- Working with partitioned Parquet datasets

#### Performance Considerations

- ✅ Excellent for Parquet workflows with high throughput
- ✅ Zero-copy data transfer with Arrow-native tools
- ✅ Efficient predicate pushdown and column pruning
- ⚠️ Limited async support compared to obstore
- ⚠️ Fewer protocol options than fsspec

---

### obstore Library Guide

obstore is a high-performance Rust-based library for cloud storage access with native async support.

#### Installation

```bash
pip install obstore
```

#### Basic Usage

```python
import obstore as obs
from obstore.store import S3Store, GCSStore, AzureStore

# Create store instances
s3_store = S3Store(bucket='my-bucket', region='us-east-1')
gcs_store = GCSStore(bucket='my-bucket')
azure_store = AzureStore(container='my-container', account='myaccount')

# Get object bytes
data = obs.get(s3_store, 'path/to/file.parquet').bytes()

# List objects
objects = obs.list(s3_store, prefix='data/2024')
for obj in objects:
    print(obj["path"], obj["size"])

# Put object
obs.put(s3_store, 'output/data.parquet', data_bytes)
```

#### Async Operations

```python
import asyncio
import obstore as obs

async def fetch_multiple():
    store = S3Store(bucket='my-bucket', region='us-east-1')
    
    # Concurrent fetches
    results = await asyncio.gather(
        obs.get_async(store, 'file1.parquet'),
        obs.get_async(store, 'file2.parquet'),
        obs.get_async(store, 'file3.parquet')
    )
    return results

# Run async
results = asyncio.run(fetch_multiple())
```

#### When to Use obstore

Choose obstore when:
- Performance is paramount (many small files, high concurrency)
- You need async/await support for concurrent operations
- You want minimal dependencies (Rust-based)
- Working with large-scale data ingestion/egestion

#### Performance Considerations

- ✅ **9x faster** for concurrent operations vs fsspec
- ✅ Native sync/async support
- ✅ Zero Python dependencies
- ✅ Rust-based implementation
- ⚠️ Newer library (2025), rapidly evolving
- ⚠️ Smaller ecosystem than fsspec

---

## DataFrame Integration

DataFrame libraries provide high-level abstractions for cloud storage I/O. This section covers integration patterns for Polars, DuckDB, Pandas, and PyArrow.

### Quick Comparison

| Framework | Integration Approach | Best For |
|-----------|---------------------|----------|
| **Polars** | Native cloud URIs (`s3://`) + fsspec/PyArrow bridges | High-performance, lazy evaluation |
| **DuckDB** | HTTPFS extension + SQL interface | Analytical queries, SQL workflows |
| **Pandas** | fsspec auto-detection | Simple workflows, broad compatibility |
| **PyArrow** | Native filesystem + dataset scanning | Arrow-native pipelines, batch processing |

### When to Use Which?

- **Polars**: Best for high-performance data pipelines with lazy evaluation, predicate pushdown, and memory efficiency
- **DuckDB**: Best for SQL-centric workflows, analytical queries on remote data without loading into memory
- **Pandas**: Best for simple scripts, small-to-medium data, maximum ecosystem compatibility
- **PyArrow**: Best for Arrow-native workflows, batch processing, and as a foundation for other tools

---

### Polars

Polars provides native cloud storage support via the Rust `object_store` crate, plus fsspec and PyArrow integration for broader compatibility.

**Key approaches:**
- **Native URIs**: Direct `s3://`, `gs://`, `az://` support (recommended)
- **fsspec bridge**: For protocol chaining and caching
- **PyArrow dataset**: For Hive-partitioned datasets with complex pushdown

```python
import polars as pl

# Native cloud URIs (simplest, best performance)
df = pl.read_parquet("s3://bucket/data.parquet")
lazy_df = pl.scan_parquet("s3://bucket/dataset/**/*.parquet")

# Lazy evaluation with predicate pushdown
result = (
    lazy_df
    .filter(pl.col("date") > "2024-01-01")  # Pushed to storage layer
    .select(["id", "value"])
    .collect()
)

# Write to cloud storage
df.write_parquet("s3://bucket/output/data.parquet")

# Partitioned write (Hive-style via PyArrow)
df.write_parquet(
    "s3://bucket/output/",
    partition_by=["year", "month"],
    use_pyarrow=True
)
```

**fsspec bridge for caching:**
```python
import fsspec

# Cache wrapper for repeated access
cached_fs = fsspec.filesystem(
    "simplecache",
    target_protocol="s3"
)
df = pl.read_parquet("simplecache::s3://bucket/cached.parquet")
```

**See:** `@data-engineering-core` for Polars fundamentals, `@data-engineering-storage-authentication` for credential setup.

---

### DuckDB

DuckDB's HTTPFS extension enables direct SQL queries on remote files without loading entire datasets into memory.

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configure credentials (or use environment variables)
con.execute("SET s3_region='us-east-1';")

# Query Parquet directly from S3
df = con.sql("""
    SELECT category, SUM(value) as total
    FROM read_parquet('s3://bucket/data/*.parquet')
    WHERE date >= '2024-01-01'
    GROUP BY category
""").pl()

# Copy operations
con.sql("""
    COPY (SELECT * FROM my_table)
    TO 's3://bucket/output.parquet'
    (FORMAT PARQUET)
""")
```

**Environment-based auth (recommended):**
```python
import os
os.environ['AWS_REGION'] = 'us-east-1'
# DuckDB reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY automatically
```

**Delta Lake integration:**
```python
con.execute("INSTALL delta; LOAD delta;")
df = con.sql("SELECT * FROM delta_scan('s3://bucket/delta-table/')").pl()
```

**See:** `@data-engineering-core` for DuckDB fundamentals, `@data-engineering-storage-authentication` for credential patterns.

---

### Pandas

Pandas leverages fsspec for automatic cloud URI handling, making remote files transparent to use.

```python
import pandas as pd

# Auto-detection via fsspec
df = pd.read_parquet("s3://bucket/data.parquet")
df = pd.read_csv("s3://bucket/data.csv.gz")  # Compression auto-detected

# Explicit filesystem for control
import fsspec
fs = fsspec.filesystem("s3")
df = pd.read_parquet(
    "s3://bucket/data.parquet",
    filesystem=fs,
    columns=["id", "value"],  # Column pruning
    filters=[("date", ">=", "2024-01-01")]  # Row group filtering
)

# Partitioned writes
df.to_parquet(
    "s3://bucket/output/",
    partition_cols=["year", "month"],
    filesystem=fs
)
```

**PyArrow filesystem for better performance:**
```python
import pyarrow.fs as fs
s3_fs = fs.S3FileSystem(region="us-east-1")
df = pd.read_parquet("bucket/data.parquet", filesystem=s3_fs)
```

**See:** `@data-engineering-core` for pandas alternatives (Polars recommended for large data), `@data-engineering-storage-authentication` for credential setup.

---

### PyArrow

PyArrow provides the foundation for many DataFrame libraries with native filesystem integration and efficient dataset scanning.

```python
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as fs

# Native filesystem
s3_fs = fs.S3FileSystem(region="us-east-1")

# Read with column pruning
table = pq.read_table(
    "bucket/file.parquet",
    filesystem=s3_fs,
    columns=["id", "value"]
)

# Dataset with predicate pushdown
dataset = ds.dataset(
    "bucket/dataset/",
    filesystem=s3_fs,
    partitioning=ds.HivePartitioning.discover()
)

# Filter at storage layer
table = dataset.to_table(
    filter=(ds.field("year") == 2024) & (ds.field("value") > 100),
    columns=["id", "value"]
)

# Batch scanning for large datasets
scanner = dataset.scanner(
    filter=ds.field("value") > 0,
    batch_size=65536
)
for batch in scanner.to_batches():
    process(batch)
```

**fsspec bridge:**
```python
import fsspec
fs = fsspec.filesystem("s3")
with fs.open("s3://bucket/file.parquet", "rb") as f:
    table = pq.read_table(f)
```

**See:** `@data-engineering-core` for PyArrow fundamentals, `@data-engineering-storage-authentication` for credential patterns.

---

### Format Considerations

For detailed information on storage formats (Parquet, Arrow, Lance, Zarr, Avro, ORC) and lakehouse table formats (Delta Lake, Iceberg, Hudi), including compression, schema evolution, and format selection guidance, see **`@designing-data-storage`**. This section focuses on I/O patterns, not format internals.

---

## Authentication

All three libraries follow standard cloud authentication patterns: explicit credentials → environment variables → config files → IAM roles/Managed Identities.

**See:** `@data-engineering-storage-authentication`

## Performance Optimization

Key strategies:
- **Caching**: fsspec's `SimpleCache` for repeated access
- **Concurrency**: obstore async API for many small files
- **Predicate pushdown**: Filter at storage layer using partitioning
- **Column pruning**: Read only required columns

**See:** `performance.md` in this skill for detailed guidance.

---

## References

- [fsspec Documentation](https://filesystem-spec.readthedocs.io/)
- [PyArrow Filesystems](https://arrow.apache.org/docs/python/filesystems.html)
- [obstore Documentation](https://developmentseed.org/obstore/)
- [s3fs Documentation](https://s3fs.readthedocs.io/)
- [gcsfs Documentation](https://gcsfs.readthedocs.io/)
