---
name: data-engineering-storage-remote-access-libraries-obstore
description: "[DEPRECATED] Use @accessing-cloud-storage instead. High-performance Rust-based remote filesystem library."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: obstore Library Guide

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for the complete obstore library guide, including:

- Installation and basic usage
- Async operations
- Streaming uploads
- Performance considerations

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-libraries-obstore` | `@accessing-cloud-storage` (obstore section) |

---

## Quick Start (New Skill)

```python
import obstore as obs
from obstore.store import S3Store

store = S3Store(bucket='my-bucket', region='us-east-1')
data = obs.get(store, 'data.parquet').bytes()
```

**See `@accessing-cloud-storage` for complete documentation.**
