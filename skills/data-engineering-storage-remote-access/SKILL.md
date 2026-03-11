---
name: data-engineering-storage-remote-access
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Cloud storage access in Python: fsspec, pyarrow.fs, obstore libraries, plus integrations with Polars, DuckDB, PyArrow, Delta Lake, and Iceberg."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: Remote Storage Access

**This skill has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for all cloud storage access guidance, including:

- **Library guides**: fsspec, pyarrow.fs, obstore
- **DataFrame integrations**: Polars, DuckDB, Pandas, PyArrow
- **Performance optimization**: Caching, concurrency, async patterns
- **Common patterns**: Incremental loading, partitioned writes, cross-cloud copy

## Migration Guide

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-libraries-fsspec` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-libraries-pyarrow-fs` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-libraries-obstore` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-polars` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-pandas` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-duckdb` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-pyarrow` | `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-delta-lake` | `@data-engineering-storage-lakehouse` + `@accessing-cloud-storage` |
| `@data-engineering-storage-remote-access-integrations-iceberg` | `@data-engineering-storage-lakehouse` + `@accessing-cloud-storage` |

---

## Quick Start (New Skill)

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

**See `@accessing-cloud-storage` for complete documentation.**
