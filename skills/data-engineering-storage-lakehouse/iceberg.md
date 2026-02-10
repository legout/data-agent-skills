# Apache Iceberg

Apache Iceberg is a high-performance table format for large-scale analytics. It's engine-agnostic, supporting Spark, Trino, Flink, DuckDB, and more. Iceberg emphasizes schema evolution, partition evolution, and hidden partitioning.

## Installation

```bash
# Core PyIceberg with PyArrow backend
pip install "pyiceberg[pyarrow,pandas,sql-sqlite]"

# Optional extras for specific backends
pip install "pyiceberg[aws]"   # S3 + AWS Glue catalog
pip install "pyiceberg[rest]"  # REST catalog
```

## Catalog Configuration

Iceberg uses catalogs to track table metadata. Choose based on your environment:

### AWS Glue Catalog
```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "glue",
    **{
        "type": "glue",
        "s3.region": "us-east-1",
        "s3.access-key-id": "AKIA...",
        "s3.secret-access-key": "...",
    }
)
```

### REST Catalog (Tabular, etc.)
```python
catalog = load_catalog(
    "rest",
    **{
        "uri": "https://iceberg-catalog.example.com",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
    }
)
```

### Hive Metastore
```python
catalog = load_catalog(
    "hive",
    **{
        "uri": "thrift://localhost:9083",
        "s3.endpoint": "http://minio:9000",
    }
)
```

### Local Development (No Catalog)
```python
from pyiceberg.catalog import InMemoryCatalog

catalog = InMemoryCatalog("local")
# Tables are stored in a local directory by default
```

## Table Operations

### Create Table
```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType, NestedField, StringType, IntegerType, BooleanType
)

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "lon", DoubleType(), required=False),
    NestedField(4, "population", IntegerType(), required=False)
)

table = catalog.create_table(
    "default.cities",
    schema=schema
)
```

### Write Data
```python
import pyarrow.parquet as pq
import pyarrow as pa

# Read Parquet and append
parquet_table = pq.read_table("data/cities.parquet")
table.append(parquet_table)

# Or create from pandas
import pandas as pd
df = pd.DataFrame({
    "city": ["Amsterdam", "Berlin"],
    "lat": [52.37, 52.52],
    "lon": [4.89, 13.405],
    "population": [872000, 3645000]
})
table.append(df)
```

### Read Data
```python
# Full table scan
df = table.scan().to_pandas()

# With filter pushdown
df = table.scan(
    row_filter="population > 1000000",
    selected_fields=("city", "population")
).to_pandas()

# With limit
df = table.scan(limit=100).to_arrow()

# Time travel (as-of timestamp)
historical = table.scan(
    as_of_timestamp="2024-01-01T00:00:00Z"
).to_pandas()
```

### Schema Evolution
```python
# Add column (non-breaking)
with table.update_schema() as update:
    update.add_column("country", StringType(), required=False)

# Upgrade column type (e.g., int → long)
with table.update_schema() as update:
    update.upgrade_column("population", IntegerType(), required=False)

# Rename column
with table.update_schema() as update:
    update.rename_column("lat", "latitude")
```

### Partition Evolution
Iceberg supports dynamic partition evolution - you can change partition specs without rewriting data.

```python
# Set new partition spec
with table.update_spec() as update:
    # Partition by country instead of city
    update.set_partition_spec(["country"])
```

### Delete Data
```python
# Soft delete (adds to delete file)
import pyarrow as pa

key = pa.array([1])
table.delete(delete_filter="id IN (1, 2, 3)")

# Or use positional deletes
# Not directly supported in PyIceberg yet - use Spark for complex deletes
```

## Cloud Storage Integration

See `@data-engineering-storage-remote-access/integrations/iceberg` for S3/GCS/Azure catalog configuration details.

## Comparison with Delta Lake

| Feature | Iceberg | Delta Lake |
|---------|---------|------------|
| **Schema Evolution** | Branching, rename support | Add/drop columns, limited rename |
| **Partition Evolution** | Dynamic, no rewrite | Requires table rewrite (old versions still work) |
| **Engine Support** | Spark, Trino, Flink, DuckDB, Presto | Spark, Pandas (limited), DuckDB (read-only) |
| **Time Travel Syntax** | `as_of_timestamp`, `as_of_version` | `versionAsOf`, `timestampAsOf` |
| **Catalog Abstraction** | Plug-in (Hive, Glue, REST, Nessie) | Built-in (mostly Spark catalog) |
| **Python API** | PyIceberg (mature) | deltalake (mature) |

## Best Practices

1. **Use a catalog** - Never use Iceberg tables without catalog metadata tracking
2. **Leverage partition evolution** for evolving data distribution
3. **Commit regularly** - Each commit creates a snapshot; monitor snapshot count
4. **Archive old snapshots** with `expire_snapshots()` to limit metadata growth
5. **Use PyArrow backend** for best performance with Python
6. **Test locally** with `InMemoryCatalog` or file-based catalog before deploying

## Performance Tips

- ✅ Use partition pruning (Iceberg does this automatically)
- ✅ Write in batches (larger Parquet files = better compression/read)
- ✅ Cache table metadata if accessing repeatedly
- ✅ Use `selected_fields` to avoid reading unnecessary columns

## Resources

- [PyIceberg Documentation](https://pyiceberg.readthedocs.io/)
- [Iceberg Specification](https://iceberg.apache.org/spec/)
- [Iceberg Catalog Configurations](https://iceberg.apache.org/docs/latest/catalog/)
