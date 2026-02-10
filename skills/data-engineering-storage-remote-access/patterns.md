# Remote Storage Patterns

Common patterns for working with cloud storage: incremental loading, partitioned writes, and cross-cloud copying.

## Incremental Loading

Load only new or modified files since last checkpoint:

```python
import json
from datetime import datetime
import fsspec
import os

def load_incrementally(source_path: str, checkpoint_file: str):
    """Load only new files based on checkpoint."""
    # Load checkpoint
    try:
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        last_modified = datetime.fromisoformat(checkpoint['last_modified'])
    except FileNotFoundError:
        last_modified = datetime.min

    # List all files
    fs = fsspec.filesystem("s3", anon=False)
    all_files = fs.find(source_path)

    # Filter new files
    new_files = []
    for path in all_files:
        info = fs.info(path)
        if info['LastModified'] > last_modified:
            new_files.append(path)

    # Process
    for path in new_files:
        process_file(path)

    # Update checkpoint atomically
    if new_files:
        newest = max(fs.info(f)['LastModified'] for f in new_files)
        checkpoint_data = {'last_modified': newest.isoformat()}
        with open(checkpoint_file + '.tmp', 'w') as f:
            json.dump(checkpoint_data, f)
        os.replace(checkpoint_file + '.tmp', checkpoint_file)  # Atomic replace
```

**Alternative**: Use file naming convention with timestamps (`s3://bucket/events/year=2024/month=01/day=01/`) and track processed partitions.

---

## Writing Partitioned Datasets

Write Hive-partitioned datasets for efficient querying and predicate pushdown:

```python
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs

s3_fs = fs.S3FileSystem(region="us-east-1")

# Hive partitioning by year/month/day
ds.write_dataset(
    table,
    base_dir="bucket/output/",
    filesystem=s3_fs,
    format="parquet",
    partitioning=ds.partitioning(
        pa.schema([
            ("year", pa.int16()),
            ("month", pa.int8()),
            ("day", pa.int8())
        ]),
        flavor="hive"  # year=2024/month=01/day=01/
    ),
    max_rows_per_file=1000000,  # Target ~1M rows per Parquet file
    max_partitions=1024,        # Safety limit
    existing_data_behavior="overwrite_or_ignore"
)
```

**Polars alternative**:
```python
df.write_parquet(
    "s3://bucket/output/",
    partition_by=["year", "month"],
    use_pyarrow=True
)
```

---

## Cross-Cloud Copy

Copy data between cloud providers (S3 ↔ GCS ↔ Azure):

```python
import fsspec
from tqdm import tqdm

s3_fs = fsspec.filesystem("s3")
gcs_fs = fsspec.filesystem("gcs")

# Method 1: Stream copy (memory efficient)
def copy_file(src_fs, src_path, dst_fs, dst_path, chunk_size=8 * 1024 * 1024):
    with src_fs.open(src_path, "rb") as src, dst_fs.open(dst_path, "wb") as dst:
        while chunk := src.read(chunk_size):
            dst.write(chunk)

# Copy multiple files
files = s3_fs.glob("s3://src-bucket/data/**/*.parquet")
for src_path in tqdm(files):
    dst_path = src_path.replace("s3://src-bucket", "gs://dst-bucket")
    copy_file(s3_fs, src_path, gcs_fs, dst_path)
```

```python
# Method 2: obstore async (faster for many small files)
import asyncio
import obstore as obs
from obstore.store import S3Store, GCSStore

async def copy_directory():
    s3 = S3Store(bucket="src-bucket")
    gcs = GCSStore(bucket="dst-bucket")

    # Concurrent copy
    tasks = []
    for obj in obs.list(s3, prefix="data/"):
        src_path = obj['path']
        task = asyncio.create_task(copy_file_async(s3, src_path, gcs, src_path))
        tasks.append(task)

    await asyncio.gather(*tasks)

asyncio.run(copy_directory())
```

---

## Performance Tips

- **Concurrent operations**: Use `ThreadPoolExecutor` or `asyncio` for many small files
- **Chunked transfers**: 8-64MB chunks for large files
- **Retry logic**: Wrap copies with `tenacity` retry decorator for transient errors
- **Progress bars**: Use `tqdm` to monitor long-running copies
- **Checkpointing**: Track successfully copied files to resume after failures

---

## Error Handling

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def resilient_copy(src_fs, src_path, dst_fs, dst_path):
    """Copy with retry for transient network errors."""
    try:
        src_fs.info(src_path)  # Verify source exists
        copy_file(src_fs, src_path, dst_fs, dst_path)
    except (IOError, OSError) as e:
        raise
```

---

## References

- `@data-engineering-storage-remote-access/libraries/fsspec` - Filesystem basics
- `@data-engineering-storage-remote-access/libraries/obstore` - High-performance async
- `@data-engineering-core` - DuckDB/MERGE for metadata tracking
- `@data-engineering-observability` - Monitoring long-running copy jobs
