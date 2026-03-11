---
name: managing-data-catalogs
description: "Data catalogs for lakehouse architectures: Iceberg catalogs (Hive Metastore, AWS Glue, REST/Tabular), using DuckDB as a lightweight multi-source catalog, and comparisons of open-source metadata tools (Amundsen, DataHub, OpenMetadata)."
dependsOn: ["@designing-data-storage", "@accessing-cloud-storage", "@data-engineering-storage-authentication"]
---

# Managing Data Catalogs

Guide to selecting, configuring, and using data catalog systems for data discovery, governance, and unified table access across multiple query engines.

---

## When to Use This Skill

**Use this skill when:**
- Setting up an Iceberg catalog (Hive Metastore, AWS Glue, REST/Tabular)
- Comparing catalog options for your data platform
- Using DuckDB to unify queries across heterogeneous data sources
- Evaluating open-source metadata discovery tools (DataHub, OpenMetadata)
- Configuring cross-service table access (Athena, EMR, Spark, DuckDB)

**Do not use this skill for:**
- Table format selection (Delta vs Iceberg) → use `@designing-data-storage`
- Cloud storage authentication patterns → use `@accessing-cloud-storage`
- Raw storage I/O without catalog abstraction → use `@accessing-cloud-storage`
- General ETL pipeline patterns → use `@building-data-pipelines`

---

## Quick Catalog Type Comparison

| Catalog | Backend | Managed? | Best For |
|---------|---------|----------|----------|
| **Hive Metastore** | RDBMS (Postgres/MySQL) | Self-hosted | Existing Hadoop, high partition counts |
| **AWS Glue** | AWS-managed serverless | AWS-managed | AWS-native stacks (Athena, EMR) |
| **Tabular/REST** | SaaS (Nessie-backed) | Vendor-managed | Iceberg-native, Git-like branching |
| **DuckDB (embedded)** | Local file/Postgres | Self-hosted | Single-user, PoC, small teams |

---

## When to Use Which Catalog

| Scenario | Recommended Catalog | Why |
|----------|---------------------|-----|
| AWS-native (Athena, Redshift Spectrum) | **AWS Glue** | Serverless, IAM integration |
| Self-hosted Hadoop/Spark | **Hive Metastore** | Battle-tested, no vendor lock-in |
| Iceberg-first, multi-cloud | **Tabular** or **Hive** | Native Iceberg features or flexibility |
| Small team, PoC, analytics | **DuckDB** | Zero infrastructure, SQL-native |
| LinkedIn-scale metadata | **DataHub** | Enterprise lineage, scale |
| Governance-heavy workflows | **OpenMetadata** | Built-in workflows, data quality |

---

## Core Patterns

### Catalog → Table → Storage Mapping

```
Catalog (name) → Table identifier → Metadata location → Data files
                                    (schema, snapshots, partitions)
```

**Key insight:** The catalog stores only metadata pointers. Actual data lives in object storage (S3, GCS, Azure).

### Multi-Engine Access Pattern

```python
# Same table, different engines
table = catalog.load_table("db.events")

# PyIceberg → Pandas DataFrame
df = table.scan().to_pandas()

# Spark SQL (via same catalog)
spark.table("db.events")  # Same underlying data

# DuckDB (via ATTACH or REST catalog)
SELECT * FROM iceberg.db.events
```

---

## Detailed Guides

| Guide | Covers | When to Read |
|-------|--------|--------------|
| **[Hive Metastore](./hive-metastore.md)** | Docker deployment, PyIceberg integration, pros/cons | Self-hosting Hadoop/Spark |
| **[AWS Glue Catalog](./aws-glue-catalog.md)** | GlueCatalog setup, crawlers, IAM, Unity Catalog federation | AWS-native stacks |
| **[REST Catalog & Tabular](./rest-catalog.md)** | Tabular SaaS, Nessie patterns, Git-like branching | Iceberg-first, multi-cloud |
| **[DuckDB Multi-Source](./duckdb-catalog.md)** | ATTACH patterns, unified views, limitations | Single-user/PoC catalog |
| **[Open Source Tools](./open-source-catalogs.md)** | Amundsen vs DataHub vs OpenMetadata comparison | Metadata discovery/governance |

---

## Quick Reference: PyIceberg Catalog Setup

```python
from pyiceberg.catalog import load_catalog

# Hive Metastore
catalog = load_catalog("hive", **{
    "type": "hive",
    "uri": "thrift://localhost:9083",
    "warehouse": "s3://bucket/warehouse/"
})

# AWS Glue
catalog = load_catalog("glue", **{
    "type": "glue",
    "region": "us-east-1",
    "warehouse": "s3://bucket/warehouse/"
})

# REST/Tabular
catalog = load_catalog("rest", **{
    "type": "rest",
    "uri": "https://api.tabular.io/ws/...",
    "token": "tabular-token-...",
    "warehouse": "s3://bucket/warehouse/"
})

# Create and query table
table = catalog.create_table("db.events", schema=schema)
table.append(data)
df = table.scan().to_pandas()
```

---

## Best Practices

### Catalog Selection

1. **Default to Hive Metastore** if you have Hadoop investments
2. **Use AWS Glue** for pure AWS (Athena, EMR, Redshift Spectrum)
3. **Choose Tabular** for Iceberg-native with branching needs
4. **Separate concerns:** Iceberg catalog (table storage) ≠ business metadata catalog (OpenMetadata for discovery)

### DuckDB-as-Catalog (Development Only)

- Use for **single-user notebooks** or **small team PoC**
- Store catalog file in version control (encrypt credentials)
- Use **Postgres as DuckLake backend** for multi-client writes
- Back up regularly if using as primary catalog

### Security

- Never hardcode credentials in catalog configs
- Use IAM roles (AWS), service principals (Azure), or workload identity (GCP)
- Store tokens/secrets in environment variables or secret managers

---

## See Also

- `@designing-data-storage` - Delta Lake, Iceberg table formats, file format selection
- `@accessing-cloud-storage` - fsspec, pyarrow.fs, obstore for storage access
- `@data-engineering-storage-authentication` - AWS, GCP, Azure credential patterns
- `@building-data-pipelines` - ETL patterns using catalog-registered tables

---

## References

- [Apache Iceberg Catalog Documentation](https://iceberg.apache.org/docs/latest/catalog/)
- [PyIceberg Catalog API](https://py.iceberg.apache.org/reference/pyiceberg/catalog/)
- [DuckDB ATTACH Documentation](https://duckdb.org/docs/stable/sql/statements/attach.html)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
