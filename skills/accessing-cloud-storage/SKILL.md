---
name: accessing-cloud-storage
description: "Access cloud storage (S3, GCS, Azure) in Python using fsspec, pyarrow.fs, or obstore. Includes performance optimization, patterns for incremental loading, partitioned writes, and cross-cloud copy."
dependsOn: ["@data-engineering-core", "@data-engineering-storage-authentication", "@data-engineering-storage-formats"]
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

## Quick Start Example

```python
import fsspec
import pyarrow.fs as fs
import obstore as obs

# Method 1: fsspec (universal)
s3_fs = fsspec.filesystem('s3')
with s3_fs.open('s3://bucket/data.parquet', 'rb') as f:
    df = pl.read_parquet(f)

# Method 2: pyarrow.fs (Arrow-native)
s3_pa = fs.S3FileSystem(region='us-east-1')
table = pq.read_table("bucket/data.parquet", filesystem=s3_pa)

# Method 3: obstore (high-performance)
from obstore.store import S3Store
store = S3Store(bucket='my-bucket', region='us-east-1')
data = obs.get(store, 'data.parquet').bytes()

# All approaches work - choose based on your performance and ecosystem needs
```

---

## fsspec: Universal Filesystem Interface

fsspec provides a unified API for local and remote filesystems, integrating seamlessly with pandas, xarray, Dask, and many other Python data tools.

### Installation

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

### Basic Usage

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
s3_fs.exists('my-bucket/data/file.csv')       # Check existence
s3_fs.mkdir('my-bucket/new-folder')           # Create directory

# Read file as bytes
with s3_fs.open('s3://my-bucket/data/file.txt', 'rb') as f:
    content = f.read()

# Read CSV directly into pandas
with s3_fs.open('s3://my-bucket/data/large.csv', 'rb') as f:
    df = pd.read_csv(f, compression='gzip')
```

### Protocol Chaining & Caching

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

### Advanced S3 Features

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

### Performance Considerations

- Use `filecache::` instead of `simplecache::` for persistent caching across sessions
- Increase `max_pool_connections` for high concurrency
- Use async API for many concurrent small file operations
- For pure Parquet workflows with high throughput, consider `pyarrow.fs` instead
- For maximum performance on large concurrent operations, consider `obstore`

---

## PyArrow.fs: Native Arrow Filesystems

PyArrow provides its own filesystem abstraction optimized for Arrow/Parquet workflows with zero-copy integration.

### Installation

```bash
# Bundled with PyArrow - no extra deps
pip install pyarrow
```

### Basic Usage

```python
import pyarrow.fs as fs
from pyarrow import parquet as pq

# From URI - auto-detects filesystem type
s3_fs, path = fs.FileSystem.from_uri("s3://bucket/path/to/data/")
print(type(s3_fs))  # <class 'pyarrow._fs.S3FileSystem'>
print(path)         # 'path/to/data/'

# GCS via URI
gcs_fs, path = fs.FileSystem.from_uri("gs://my-bucket/data/")

# Local filesystem
local_fs, path = fs.FileSystem.from_uri("file:///home/user/data/")
```

### S3 Configuration

```python
import pyarrow.fs as fs
from pyarrow.fs import S3FileSystem

# Method 1: From URI with options
s3_fs = S3FileSystem(
    access_key='AKIA...',
    secret_key='...',
    session_token='...',           # For temporary credentials
    region='us-west-2',
    endpoint_override='https://minio.local:9000',  # S3-compatible
    scheme='https',
    proxy_options={'scheme': 'http', 'host': 'proxy.company.com', 'port': 8080},
    allow_bucket_creation=True,
    retry_strategy=fs.AwsStandardS3RetryStrategy(max_attempts=5)
)

# Method 2: From URI (reads from environment/AWS config)
s3_fs, path = fs.FileSystem.from_uri("s3://my-bucket/data/")

# File operations (bucket/key paths, not s3:// URIs)
info = s3_fs.get_file_info("bucket/file.parquet")
print(info.size)           # File size in bytes
print(info.mtime)          # Modification time

# Open input stream
with s3_fs.open_input_stream("bucket/file.parquet") as f:
    data = f.read()

# Open output stream for writing
with s3_fs.open_output_stream("bucket/output.parquet") as f:
    f.write(parquet_bytes)

# Copy and delete
s3_fs.copy_file("bucket/src.parquet", "bucket/dst.parquet")
s3_fs.delete_file("bucket/old.parquet")
```

### Working with Parquet Datasets

```python
import pyarrow.dataset as ds
import pyarrow.fs as fs

# Create S3 filesystem
s3_fs = fs.S3FileSystem(region='us-east-1')

# Load partitioned dataset
dataset = ds.dataset(
    "bucket/dataset/",
    filesystem=s3_fs,
    format="parquet",
    partitioning=ds.HivePartitioning.discover()
)

print(dataset.schema)
print(f"Rows: {dataset.count_rows()}")

# Filter pushdown (only reads relevant files)
table = dataset.to_table(
    filter=(ds.field("year") == 2024) & (ds.field("month") > 6),
    columns=["id", "value", "timestamp"]  # Column pruning
)

# Scan with custom options
scanner = dataset.scanner(
    filter=ds.field("value") > 100,
    batch_size=65536,
    use_threads=True
)

for batch in scanner.to_batches():
    process(batch)
```

### Azure Support via FSSpec Bridge

```python
import adlfs
import pyarrow.fs as fs
import pyarrow.dataset as ds

# Create Azure filesystem via fsspec
azure_fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    account_key="...",
    tenant_id="...",
    client_id="...",
    client_secret="..."
)

# Wrap in PyArrow filesystem
pa_fs = fs.PyFileSystem(fs.FSSpecHandler(azure_fs))

# Use with PyArrow dataset
dataset = ds.dataset(
    "container/path/",
    filesystem=pa_fs,
    format="parquet"
)
```

### Performance Considerations

- **Column pruning**: Use `columns=` parameter to read only needed columns
- **Predicate pushdown**: Filter at dataset level to skip reading irrelevant files
- **Batch scanning**: Use `scanner.to_batches()` for large datasets
- **Threading**: Enable `use_threads=True` for CPU-bound operations
- For ecosystem integration (pandas, Dask, etc.), fsspec may be more convenient
- For maximum async performance with many small files, consider obstore

---

## obstore: High-Performance Rust-Based Storage

obstore (released 2025) provides a minimal, stateless API built on Rust's `object_store` crate, offering superior performance for concurrent operations (up to 9x faster than Python-based alternatives).

### Installation

```bash
pip install obstore

# Or with conda
conda install -c conda-forge obstore
```

### Core Concepts

obstore uses **top-level functions** (not methods) and a functional API. All operations are functions like `obs.get(store, path)`, not `store.get(path)`.

### Creating Stores

```python
import obstore as obs
from obstore.store import S3Store, GCSStore, AzureStore, LocalStore

# S3 Store
s3 = S3Store(
    bucket="my-bucket",
    region="us-east-1",
    access_key_id="AKIA...",
    secret_access_key="...",
    # Or use environment credentials
)

# GCS Store
gcs = GCSStore(
    bucket="my-bucket",
    # Uses GOOGLE_APPLICATION_CREDENTIALS by default
)

# Azure Store
azure = AzureStore(
    container="my-container",
    account_name="myaccount",
    account_key="...",
    # Or use DefaultAzureCredential
)

# Local filesystem
local = LocalStore("/path/to/root")

# From environment (picks up standard env vars)
s3 = S3Store.from_env(bucket="my-bucket")
gcs = GCSStore.from_env(bucket="my-bucket")
```

### Basic Operations

```python
import obstore as obs

store = S3Store(bucket="my-bucket", region="us-east-1")

# Put object (bytes)
obs.put(store, "hello.txt", b"Hello, World!")

# Put from file
with open("local-file.csv", "rb") as f:
    obs.put(store, "data/file.csv", f)

# Get object
response = obs.get(store, "hello.txt")
print(response.bytes())   # b"Hello, World!"
print(response.meta)      # Object metadata (size, mtime, etag, etc.)

# Get range (efficient partial reads)
partial = obs.get_range(store, "large-file.bin", offset=0, length=1024)

# Stream download
stream = obs.get(store, "large-file.bin")
for chunk in stream.stream(min_chunk_size=8 * 1024 * 1024):
    process(chunk)

# List objects (streaming, no pagination needed!)
for obj in obs.list(store, prefix="data/2024/"):
    print(f"{obj['path']}: {obj['size']} bytes")

# List with delimiter (like directory listing)
result = obs.list_with_delimiter(store, prefix="data/")
print(result["common_prefixes"])  # "directories"
print(result["objects"])          # files

# Delete
obs.delete(store, "old-file.txt")

# Copy within same store
obs.copy(store, "src/file.txt", "dst/file.txt")

# Rename/move
obs.rename(store, "old-name.txt", "new-name.txt")

# Check existence (via head)
try:
    meta = obs.head(store, "file.txt")
    print(f"Exists: {meta['size']} bytes")
except obs.NotFoundError:
    print("File not found")
```

### Async API

```python
import asyncio
import obstore as obs
from obstore.store import S3Store

async def main():
    store = S3Store(bucket="my-bucket", region="us-east-1")

    # Concurrent uploads
    await asyncio.gather(
        obs.put_async(store, "file1.txt", b"content1"),
        obs.put_async(store, "file2.txt", b"content2"),
        obs.put_async(store, "file3.txt", b"content3"),
    )

    # Concurrent downloads
    responses = await asyncio.gather(
        obs.get_async(store, "file1.txt"),
        obs.get_async(store, "file2.txt"),
        obs.get_async(store, "file3.txt"),
    )

    for resp in responses:
        print(await resp.bytes_async())

asyncio.run(main())
```

### Streaming Uploads

```python
import asyncio
import obstore as obs
from obstore.store import S3Store

store = S3Store(bucket="my-bucket")

# Upload from generator (streaming, memory-efficient)
def data_generator():
    for i in range(1000):
        yield f"Row {i}\n".encode()

obs.put(store, "output.txt", data_generator())

# Upload from async iterator
async def async_data():
    for i in range(1000):
        await asyncio.sleep(0)
        yield f"Row {i}\n".encode()

async def upload_async():
    await obs.put_async(store, "output-async.txt", async_data())

asyncio.run(upload_async())

# Automatic multipart upload for large files
# (triggered automatically based on size)
with open("huge-file.bin", "rb") as f:
    obs.put(store, "huge-file.bin", f)  # Multi-part automatically
```

### Arrow Integration

```python
import obstore as obs
from obstore.store import S3Store

store = S3Store(bucket="my-bucket")

# Return list results as Arrow table (faster, more memory-efficient)
arrow_table = obs.list(store, prefix="data/", return_arrow=True)
print(arrow_table.schema)
# pyarrow.Schema
# ├── path: string
# ├── size: int64
# ├── last_modified: timestamp[ns]
# └── etag: string

# Process with PyArrow/Polars
import polars as pl
df = pl.from_arrow(arrow_table)
```

### fsspec Compatibility

obstore provides an fsspec-compatible wrapper:

```python
from obstore.fsspec import FsspecStore, register
import pyarrow.parquet as pq

# Method 1: Register as default handler for protocols
register()
# Now fsspec uses obstore internally
import fsspec
fs = fsspec.filesystem("s3", region="us-east-1")

# Method 2: Use FsspecStore directly
fs = FsspecStore("s3", bucket="my-bucket", region="us-east-1")
# or
fs = FsspecStore.from_store(s3_store_object)

# Use with PyArrow
parquet_file = pq.ParquetFile(
    "s3://bucket/data/file.parquet",
    filesystem=fs
)
```

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

**See:** `performance.md` in this skill

## Common Patterns

- **Incremental loading**: Load only new files based on checkpoint pattern
- **Partitioned writes**: Hive-partitioned datasets for efficient querying
- **Cross-cloud copy**: Copy data between S3, GCS, Azure

**See:** `patterns.md` in this skill

---

## Related Skills

### DataFrame Integrations
- `@data-engineering-storage-remote-access-integrations-polars` - Polars + cloud URIs
- `@data-engineering-storage-remote-access-integrations-duckdb` - DuckDB HTTPFS extension
- `@data-engineering-storage-remote-access-integrations-pandas` - Pandas + remote files
- `@data-engineering-storage-remote-access-integrations-pyarrow` - PyArrow datasets
- `@data-engineering-storage-remote-access-integrations-delta-lake` - Delta on S3/GCS/Azure
- `@data-engineering-storage-remote-access-integrations-iceberg` - Iceberg with cloud catalogs

### Infrastructure & Formats
- `@data-engineering-storage-authentication` - AWS, GCP, Azure auth patterns, IAM roles, service principals
- `@data-engineering-storage-formats` - Parquet, Arrow, Lance, Zarr, Avro, ORC
- `@data-engineering-storage-lakehouse` - Delta Lake, Iceberg on cloud storage

---

## References

- [fsspec Documentation](https://filesystem-spec.readthedocs.io/)
- [PyArrow Filesystems](https://arrow.apache.org/docs/python/filesystems.html)
- [obstore Documentation](https://developmentseed.org/obstore/)
- [s3fs Documentation](https://s3fs.readthedocs.io/)
- [gcsfs Documentation](https://gcsfs.readthedocs.io/)
