# REST Catalog and Tabular

REST catalog implementations for Apache Iceberg, including Tabular (managed SaaS) and Nessie (open-source Git-like versioning).

---

## Tabular (Managed SaaS)

Tabular is a managed catalog service built for Iceberg with Nessie-like versioning (branching, tags, atomic multi-table operations).

### Setup

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="tabular",
    uri="https://api.tabular.io/ws/",
    warehouse="my-warehouse",
    token="tabular-token-abc123..."
)

# Create table
table = catalog.create_table(
    identifier="default.events",
    schema=schema
)

# Append data
table.append(data)
```

### Git-Like Branching Operations

```python
# Create a branch for development
catalog.create_branch("feature-branch")

# Switch to branch
catalog.set_current_branch("feature-branch")

# Make changes
table.append(new_data)

# Later, merge to main
catalog.merge_branch("feature-branch", "main")

# Or drop the branch
catalog.drop_branch("feature-branch")
```

### Tags (Immutable Snapshots)

```python
# Tag a snapshot for reproducibility
table.create_tag("release-v1.0", snapshot_id=table.current_snapshot().snapshot_id)

# Query historical tag
df = table.scan(snapshot_id=table.snapshot_by_name("release-v1.0")).to_pandas()
```

---

## Nessie Catalog (Open Source)

Nessie provides Git-like versioning for data lakes, compatible with Iceberg, Delta Lake, and Hive tables.

### Deployment

```bash
# Docker quickstart
docker run -p 19120:19120 \
  -eNESSIE_VERSION_STORE_TYPE=INMEMORY \
  projectnessie/nessie:latest

# Production: use PostgreSQL backend
docker run -p 19120:19120 \
  -e NESSIE_VERSION_STORE_TYPE=JDBC \
  -e NESSIE_VERSION_STORE_JDBC_URL=jdbc:postgresql://postgres:5432/nessie \
  projectnessie/nessie:latest
```

### PyIceberg with Nessie

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="nessie",
    uri="http://localhost:19120/api/v2",
    warehouse="s3://my-bucket/warehouse/",
    properties={
        "s3.region": "us-east-1"
    }
)

# Nessie uses "references" (branches/tags)
catalog.set_current_branch("main")

# Create and work with tables
table = catalog.create_table("db.events", schema=schema)
```

---

## Authentication Patterns

### OAuth2 / Bearer Token

```python
catalog = RestCatalog(
    name="secured",
    uri="https://catalog.example.com/api",
    token="eyJhbG...",  # JWT or OAuth token
    warehouse="s3://bucket/warehouse/"
)
```

### Credential Exchange

```python
# Some REST catalogs support credential exchange
catalog = RestCatalog(
    name="my_catalog",
    uri="https://catalog.example.com/api",
    credential="client-id:client-secret",
    warehouse="s3://bucket/warehouse/"
)
```

---

## Multi-Engine Access

The power of REST catalogs is multi-engine interoperability:

```python
# PyIceberg
catalog = RestCatalog(name="rest", uri="...")
table = catalog.load_table("db.events")
df = table.scan().to_pandas()
```

```sql
-- Spark SQL (via REST catalog)
CREATE CATALOG rest_catalog USING iceberg
WITH (
  'type' = 'rest',
  'uri' = 'https://catalog.example.com/api',
  'warehouse' = 's3://bucket/warehouse/'
);

SELECT * FROM rest_catalog.db.events;
```

```sql
-- Trino
CREATE CATALOG iceberg USING iceberg
WITH (
  'iceberg.catalog.type' = 'rest',
  'iceberg.rest-catalog.uri' = 'https://catalog.example.com/api'
);
```

```python
# DuckDB via HTTP extension
import duckdb
con = duckdb.connect()
con.execute("""
    INSTALL iceberg;
    LOAD iceberg;
    
    CREATE SECRET iceberg_token (
        TYPE 'bearer',
        TOKEN 'my-token'
    );
    
    SELECT * FROM iceberg_scan(
        'https://catalog.example.com/api/v1/namespaces/db/tables/events'
    );
""")
```

---

## Pros and Cons: Tabular

**Pros:**
- Native Iceberg features (branching, tags, atomic multi-table ops)
- No operations overhead (fully managed)
- Git-like workflow for datasets
- Multi-engine support (Spark, Trino, Flink, DuckDB, PyIceberg)

**Cons:**
- Commercial SaaS (cost scales with usage)
- Vendor lock-in to Tabular platform
- Newer ecosystem (fewer community resources)

---

## Pros and Cons: Nessie (Self-Hosted)

**Pros:**
- Open source, no vendor lock-in
- Git-like versioning for any table format
- Self-hosted = full control

**Cons:**
- Self-managed infrastructure
- Smaller community than Hive/Spark
- Additional component to maintain

---

## When to Choose REST Catalog

| Choose REST/Tabular | Consider Alternatives |
|---------------------|----------------------|
| Iceberg-first architecture | Simple AWS setup → Use Glue |
| Need branching/versioning | Just basic catalog → Use Hive or Glue |
| Multi-engine requirements | No need for versioning → Use Hive |
| Zero-ops preference (Tabular) | Cost-sensitive + need branching → Use Nessie |

---

## Catalog Comparison

| Feature | Hive | Glue | Tabular | Nessie |
|---------|------|------|---------|--------|
| Branching/Tags | ❌ | ❌ | ✅ | ✅ |
| Multi-table atomic ops | ❌ | ❌ | ✅ | ✅ |
| Self-hosted | ✅ | ❌ | ❌ | ✅ |
| Serverless | ❌ | ✅ | ✅ | ❌ |
| Multi-engine | ✅ | ✅ | ✅ | ✅ |

---

## Troubleshooting

### 401 Unauthorized

- Check token hasn't expired
- Verify token has correct scopes
- For Nessie, check credential format

### Connection Timeout

```python
# Increase timeout for slow connections
catalog = RestCatalog(
    name="rest",
    uri="...",
    warehouse="...",
    properties={
        "connect-timeout": "30s",
        "read-timeout": "60s"
    }
)
```

---

## See Also

- [Tabular Documentation](https://tabular.io/docs/)
- [Project Nessie](https://projectnessie.org/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)
