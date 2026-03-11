---
name: data-engineering-storage-remote-access-integrations-pandas
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Using Pandas with cloud storage via fsspec."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: Pandas Cloud Storage Integration

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for Pandas cloud storage integration, including:

- fsspec auto-detection
- Column pruning
- Partitioned writes

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-pandas` | `@accessing-cloud-storage` (Pandas section) |

---

## Quick Start (New Skill)

```python
import pandas as pd

# Auto-detection via fsspec
df = pd.read_parquet("s3://bucket/data.parquet")
```

**See `@accessing-cloud-storage` for complete documentation.**
