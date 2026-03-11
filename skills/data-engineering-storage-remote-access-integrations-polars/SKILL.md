---
name: data-engineering-storage-remote-access-integrations-polars
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Integrating Polars with remote filesystems (S3, GCS, Azure)."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: Polars Cloud Storage Integration

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for Polars cloud storage integration, including:

- Native cloud URIs (`s3://`, `gs://`, `az://`)
- Lazy evaluation with predicate pushdown
- fsspec bridge for caching
- Partitioned writes

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-polars` | `@accessing-cloud-storage` (Polars section) |

---

## Quick Start (New Skill)

```python
import polars as pl

# Native cloud URIs
df = pl.read_parquet("s3://bucket/data.parquet")
lazy_df = pl.scan_parquet("s3://bucket/dataset/**/*.parquet")
```

**See `@accessing-cloud-storage` for complete documentation.**
