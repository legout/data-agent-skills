---
name: data-engineering-storage-remote-access-integrations-iceberg
description: "[DEPRECATED] Will be migrated to a future storage-design skill. Apache Iceberg catalog configuration for cloud storage (S3, GCS, Azure)."
dependsOn: ["@accessing-cloud-storage", "@data-engineering-storage-lakehouse"]
---

# ⚠️ DEPRECATED: Iceberg Cloud Storage Integration

**This skill is being reorganized.**

For **cloud storage access** (S3, GCS, Azure), use **`@accessing-cloud-storage`**.

For **Iceberg table format** details, this content will be migrated to a future `storage-design` skill. In the meantime:
- See `@data-engineering-storage-lakehouse` for Iceberg comparisons and patterns
- See `@accessing-cloud-storage` for underlying cloud storage I/O

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-iceberg` | `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse` |

---

## Quick Start (Interim)

```python
import pyiceberg
from pyiceberg.catalog import load_catalog

# Load Iceberg catalog (Glue, REST, etc.)
catalog = load_catalog("default")

# List tables
tables = catalog.list_tables("my_namespace")

# Load table from S3
table = catalog.load_table("my_namespace.my_table")
df = table.scan().to_polars()
```

**See `@accessing-cloud-storage` for cloud storage setup and `@data-engineering-storage-lakehouse` for Iceberg patterns.**
