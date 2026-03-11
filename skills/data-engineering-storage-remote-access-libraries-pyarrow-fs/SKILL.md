---
name: data-engineering-storage-remote-access-libraries-pyarrow-fs
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Native Arrow filesystems for PyArrow workflows."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: PyArrow Filesystem Guide

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for the complete PyArrow filesystem guide, including:

- Installation and basic usage
- S3 configuration
- Working with Parquet datasets
- Performance considerations

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-libraries-pyarrow-fs` | `@accessing-cloud-storage` (PyArrow section) |

---

## Quick Start (New Skill)

```python
import pyarrow.fs as fs
import pyarrow.parquet as pq

# Create filesystem
s3_fs = fs.S3FileSystem(region='us-east-1')

# Read Parquet
table = pq.read_table("bucket/data.parquet", filesystem=s3_fs)
```

**See `@accessing-cloud-storage` for complete documentation.**
