# DuckDB Multi-Source Catalog Pattern

## Table of Contents
1. [Step-by-Step Setup](#step-by-step-setup)
2. [Attach Sources](#attach-sources)
3. [Create Unified Views](#create-unified-views)
4. [Cross-Source Queries](#cross-source-queries)

---

## Step-by-Step Setup

### 1) Create DuckDB catalog DB

```python
import duckdb

con = duckdb.connect("catalog.duckdb")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("INSTALL delta; LOAD delta;")
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL ducklake; LOAD ducklake;")
```

## Attach Sources

### 2) Attach Postgres, Delta, Iceberg

```python
# Attach Postgres
con.execute("""
ATTACH 'postgres://user:pass@localhost:5432/source_db' AS src_pg (
    READ_ONLY true
);
""")

# Attach Delta Lake via Unity Catalog pattern (if using Tabular)
con.execute("""
ATTACH 'my_uc' AS delta_catalog (
    TYPE unity_catalog,
    ENDPOINT 'https://api.tabular.io',
    WAREHOUSE 's3://my-bucket/warehouse/'
);
""")

# Or direct delta_scan view
con.execute("""
CREATE VIEW delta_orders AS
SELECT * FROM delta_scan('s3://bucket/delta/orders/');
""")

# Attach Iceberg
con.execute("""
ATTACH 'iceberg_catalog' (
    TYPE iceberg,
    URI 'thrift://localhost:9083',
    WAREHOUSE 's3://bucket/warehouse/'
);
""")
```

## Create Unified Views

### 3) Build a unified logical view

```python
con.execute("""
CREATE VIEW unified_orders AS
SELECT
    'postgres' AS source,
    order_id,
    customer_id,
    amount,
    order_date
FROM src_pg.public.orders
UNION ALL
SELECT
    'delta' AS source,
    order_id,
    customer_id,
    amount,
    order_date
FROM delta_catalog.production.orders
UNION ALL
SELECT
    'iceberg' AS source,
    order_id,
    customer_id,
    amount,
    order_date
FROM iceberg.analytics.orders;
""")
```

## Cross-Source Queries

### 4) Query all sources through one view

```python
df = con.execute("""
SELECT
    source,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM unified_orders
WHERE order_date >= '2024-01-01'
GROUP BY source
""").df()
print(df)
```
