# DuckDB as a Multi-Source Catalog

**DuckDB does NOT provide a production data catalog service** (no REST API, limited concurrency). However, you can use it as a **lightweight embedded catalog** to unify queries across multiple heterogeneous sources using the `ATTACH` statement.

**Suitable for:**
- Single-user analytics notebooks
- PoC/MVP data platforms
- Local dev environments
- Small teams (< 10 users)

---

## Architecture

```
DuckDB Process
    ├── ATTACH 'postgres://...' AS postgres
    ├── ATTACH 's3://lakehouse/delta/' AS delta_uc (TYPE unity_catalog)
    ├── ATTACH 's3://lakehouse/iceberg/' AS iceberg
    ├── ATTACH 'ducklake:data/catalog.ducklake' AS ducklake
    └── CREATE VIEW unified_dataset AS
          SELECT 'postgres' AS source, * FROM postgres.public.orders
          UNION ALL
          SELECT 'delta' AS source, * FROM delta.default.orders
          UNION ALL
          SELECT 'iceberg' AS source, * FROM iceberg.db.orders
```

---

## Attaching PostgreSQL

### Query External Postgres

```sql
-- Attach external Postgres database
ATTACH 'postgres://user:pass@host:5432/mydb' AS pg_db;

-- Query Postgres tables directly
SELECT * FROM pg_db.public.orders LIMIT 10;

-- Join across sources
SELECT 
    pg.customers.name,
    pg.orders.amount
FROM pg_db.public.orders AS orders
JOIN pg_db.public.customers AS customers
  ON orders.customer_id = customers.id;
```

### DuckLake Pattern

Use Postgres as a **metadata storage backend** for a DuckLake catalog:

```sql
-- Install and load DuckLake extension
INSTALL ducklake;
LOAD ducklake;

-- Attach Postgres as DuckLake catalog
ATTACH 'postgres://user:pass@host:5432/lakehouse_catalog' AS my_lakehouse (
    DATA_PATH 's3://my-bucket/lakehouse/'
);

-- Create Delta/Iceberg tables managed by DuckLake
CREATE TABLE my_lakehouse.silver.orders (
    order_id BIGINT,
    amount DOUBLE,
    order_date DATE
) USING DELTA;  -- Or USING ICEBERG
```

**Benefits:** Single SQL catalog with ACID transactions, time travel via Postgres WAL.

---

## Attaching Delta Lake (Unity Catalog)

Use the Unity Catalog extension (experimental):

```sql
INSTALL unity_catalog;
LOAD unity_catalog;

-- Create a Unity Catalog connection
CREATE SECRET uc_cred (
    TYPE 'aws',
    REGION 'us-east-1',
    ACCESS_KEY_ID 'AKIA...',
    SECRET_ACCESS_KEY '...'
);

ATTACH 'my_uc' AS uc (
    TYPE unity_catalog,
    ENDPOINT 'https://api.uc.tabular.io',
    SECRET 'uc_cred'
);

-- Query Delta tables
SELECT * FROM uc.my_db.my_delta_table;
```

**Note:** This is experimental. For local Delta tables, use `delta_scan()`:

```sql
INSTALL delta;
LOAD delta;

SELECT * FROM delta_scan('s3://bucket/delta_table/');
```

---

## Attaching Iceberg

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

# Attach Iceberg catalog
con.execute("""
ATTACH 'iceberg_catalog' (
    TYPE iceberg,
    CATALOG 'hive',
    URI 'thrift://localhost:9083',
    WAREHOUSE 's3://bucket/warehouse/'
);
""")

# Query
con.execute("SELECT * FROM iceberg_catalog.db.my_table").fetchdf()
```

---

## Unified Multi-Source View

Create a virtual view that unions data from all sources:

```sql
CREATE VIEW unified_orders AS
SELECT
    'postgres' AS source_system,
    o.order_id,
    o.amount,
    o.order_date,
    NULL AS metadata
FROM pg_db.public.orders o
UNION ALL
SELECT
    'delta' AS source_system,
    o.order_id,
    o.amount,
    o.order_date,
    o._metadata
FROM delta_scan('s3://bucket/delta_table/') o
UNION ALL
SELECT
    'iceberg' AS source_system,
    o.order_id,
    o.amount,
    o.order_date,
    o._metadata
FROM iceberg_catalog.analytics.orders o;
```

Query the unified view:

```sql
SELECT source_system, COUNT(*) 
FROM unified_orders 
GROUP BY source_system;
```

**Use case:** Cross-platform migration validation (ensure counts match between Postgres source and Delta target).

---

## Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| **No REST API** | Only in-process access; no multi-service sharing | Run query gateway (FastAPI) that wraps DuckDB (single point of failure) |
| **Write lock** | Only one writer at a time (file-based DB) | Use Postgres as DuckLake backend for multi-client |
| **No fine-grained auth** | All queries share same DB credentials | Use separate DuckDB files per user (not shared) |
| **Scalability** | Metadata fits in memory; works for ≤ 100k tables | Large enterprises need dedicated catalog service |
| **Availability** | Single file corruption = total loss | Back up catalog DB frequently |

---

## When NOT to Use DuckDB as Catalog

**Don't use DuckDB-as-catalog when:**
- You need multi-user concurrent write access
- Production SLA requirements exist
- REST API access required from multiple services
- Table count exceeds ~100k
- Team size > 10 people

**Use instead:** AWS Glue, Hive Metastore, or Tabular.

---

## Best Practices

1. **Use for development only** - Not production multi-user
2. **Store catalog DuckDB file** in version control (encrypt credentials)
3. **Separate credentials** from catalog - use environment variables
4. **Read-only attaches** for source systems - prevent accidental writes
5. **Back up DuckDB** regularly if using as primary catalog

---

## See Also

- [DuckDB ATTACH Documentation](https://duckdb.org/docs/stable/sql/statements/attach.html)
- [DuckLake Documentation](https://ducklake.select/docs/stable/duckdb/usage/)
- `@data-engineering-core` - DuckDB fundamentals
- `@accessing-cloud-storage` - S3 authentication for DuckDB
