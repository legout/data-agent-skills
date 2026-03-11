---
name: data-engineering-storage-remote-access-integrations-duckdb
description: "[DEPRECATED] Use @accessing-cloud-storage instead. Using DuckDB HTTPFS extension with cloud storage."
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: DuckDB Cloud Storage Integration

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for DuckDB cloud storage integration, including:

- HTTPFS extension setup
- SQL queries on remote Parquet/JSON/CSV
- Delta Lake integration

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-duckdb` | `@accessing-cloud-storage` (DuckDB section) |

---

## Quick Start (New Skill)

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
df = con.sql("SELECT * FROM read_parquet('s3://bucket/data.parquet')").pl()
```

**See `@accessing-cloud-storage` for complete documentation.**
