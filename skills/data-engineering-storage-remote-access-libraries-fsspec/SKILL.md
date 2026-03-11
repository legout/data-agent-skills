---
name: data-engineering-storage-remote-access-libraries-fsspec
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Comprehensive guide to fsspec: the universal filesystem interface for Python."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: fsspec Library Guide

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for the complete fsspec library guide, including:

- Installation and basic usage
- Protocol chaining and caching
- Advanced S3 features
- Performance considerations

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-libraries-fsspec` | `@accessing-cloud-storage` (fsspec section) |

---

## Quick Start (New Skill)

```python
import fsspec

# Create filesystem instances
s3_fs = fsspec.filesystem('s3')

# Basic operations
s3_fs.ls('my-bucket/data/')
with s3_fs.open('s3://my-bucket/data/file.txt', 'rb') as f:
    content = f.read()
```

**See `@accessing-cloud-storage` for complete documentation.**
