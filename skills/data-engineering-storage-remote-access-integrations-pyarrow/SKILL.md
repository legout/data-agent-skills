---
name: data-engineering-storage-remote-access-integrations-pyarrow
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Using PyArrow datasets with cloud storage."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: PyArrow Cloud Storage Integration

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for PyArrow cloud storage integration, including:

- Native filesystem
- Dataset scanning with predicate pushdown
- Batch processing

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-pyarrow` | `@accessing-cloud-storage` (PyArrow section) |

---

## Quick Start (New Skill)

```python
import pyarrow.parquet as pq
import pyarrow.fs as fs

s3_fs = fs.S3FileSystem(region="us-east-1")
table = pq.read_table("bucket/data.parquet", filesystem=s3_fs)
```

**See `@accessing-cloud-storage` for complete documentation.**
