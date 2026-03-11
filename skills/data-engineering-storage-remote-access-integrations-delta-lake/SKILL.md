---
name: data-engineering-storage-remote-access-integrations-delta-lake
description: "[DEPRECATED] Will be migrated to a future storage-design skill. Delta Lake integration with cloud storage (S3, GCS, Azure)."
dependsOn: ["@accessing-cloud-storage", "@data-engineering-storage-lakehouse"]
---

# ⚠️ DEPRECATED: Delta Lake Cloud Storage Integration

**This skill is being reorganized.**

For **cloud storage access** (S3, GCS, Azure), use **`@accessing-cloud-storage`**.

For **Delta Lake table format** details, this content will be migrated to a future `storage-design` skill. In the meantime:
- See `@data-engineering-storage-lakehouse` for Delta Lake comparisons and patterns
- See `@accessing-cloud-storage` for underlying cloud storage I/O

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-delta-lake` | `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse` |

---

## Quick Start (Interim)

```python
import polars as pl
from deltalake import DeltaTable

# Read Delta table from S3
dt = DeltaTable("s3://bucket/delta-table/")
df = pl.read_delta("s3://bucket/delta-table/")

# Write to Delta
df.write_delta("s3://bucket/output/", mode="overwrite")
```

**See `@accessing-cloud-storage` for cloud storage setup and `@data-engineering-storage-lakehouse` for Delta Lake patterns.**
