# Performance Optimization

Strategies for maximizing throughput when accessing remote filesystems.

## Caching Strategies

### fsspec SimpleCache

Disk-based caching for repeated access:

```python
import fsspec

cache_options = {
    'cache_storage': '/tmp/fsspec_cache',
    'expiry_time': 86400,  # 24 hours
    'check_files': False   # Don't re-check on each access
}

cached_fs = fsspec.filesystem(
    "simplecache",
    target_protocol="s3",
    target_options={'anon': False},
    **cache_options
)

# Or via protocol chaining
with fsspec.open(
    "simplecache::s3://bucket/large-file.parquet",
    simplecache=cache_options
) as f:
    df = pd.read_parquet(f)
```

### BlockCache (for random access)

More efficient block-level caching:

```python
from fsspec.implementations.cached import CachingFileSystem

cached_fs = CachingFileSystem(
    fs=s3_fs,
    cache_storage="/tmp/cache",
    block_size=10 * 1024 * 1024,  # 10MB blocks
    check_files=False
)
```

---

## Concurrent Operations

### fsspec Async

```python
import asyncio
import s3fs

async def concurrent_reads():
    fs = s3fs.S3FileSystem(asynchronous=True)
    await fs.set_session()

    files = ['s3://bucket/file1.parquet', 's3://bucket/file2.parquet']
    contents = await asyncio.gather(
        *[fs._cat_file(f) for f in files]
    )
    return contents
```

### obstore Async (Native)

Higher performance native async API:

```python
import asyncio
import obstore as obs
from obstore.store import S3Store

async def obstore_concurrent():
    store = S3Store(bucket="my-bucket")

    files = ["file1.txt", "file2.txt", "file3.txt"]
    responses = await asyncio.gather(
        *[obs.get_async(store, f) for f in files]
    )
    return responses
```

### ThreadPool for Sync Operations

```python
from concurrent.futures import ThreadPoolExecutor

fs = s3fs.S3FileSystem()
with ThreadPoolExecutor(max_workers=10) as executor:
    files = fs.find("s3://bucket/data/")
    results = list(executor.map(process_file, files))
```

---

## Parquet-Specific Optimizations

### Column Pruning & Row Group Selection

```python
import pyarrow.parquet as pq
from fsspec.parquet import open_parquet_file

with open_parquet_file(
    "s3://bucket/file.parquet",
    columns=["id", "value"],  # Read only needed columns
    row_groups=[0, 1, 2],      # Only specific row groups
    dtype_backend="pyarrow"
) as f:
    table = pq.read_table(f)
```

### Dataset Scanning with Pushdown

```python
import pyarrow.dataset as ds
import pyarrow.fs as fs

s3_fs = fs.S3FileSystem(region="us-east-1")

dataset = ds.dataset(
    "bucket/dataset/",
    filesystem=s3_fs,
    format="parquet",
    partitioning=ds.HivePartitioning.discover()
)

# Full pushdown: filter + column selection + batching
scan = dataset.scanner(
    filter=(ds.field("year") == 2024) & (ds.field("month") <= 6),
    columns=["id", "value", "timestamp"],
    batch_size=65536,
    use_threads=True
)

for batch in scan.to_batches():
    process(batch)
```

---

## Key Takeaways

- **Cache strategically**: Use SimpleCache for repeated access patterns, especially with many small files.
- **Use async for I/O-bound concurrency**: fsspec async or native obstore async for many small files.
- **Push down to storage**: Always filter and select columns at the storage layer to minimize data transfer.
- **Prefer obstore for high-throughput**: For bulk transfer of many files, obstore's Rust implementation is significantly faster (up to 9x).
- **Partition your data**: Store data partitioned (e.g., `year=2024/month=01/`) to enable partition pruning.
- **Use appropriate file sizes**: Aim for 100MB-1GB Parquet files for optimal scan performance.

---

## References

- `@data-engineering-storage-remote-access/libraries/fsspec` - fsspec caching and async
- `@data-engineering-storage-remote-access/libraries/obstore` - Native high-performance async
- `@data-engineering-storage-remote-access/libraries/pyarrow-fs` - Dataset scanning
- `@data-engineering-core` - Polars lazy evaluation for pushdown
