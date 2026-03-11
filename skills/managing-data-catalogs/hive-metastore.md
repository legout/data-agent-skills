# Hive Metastore

The traditional, battle-tested catalog used by Hadoop/Hive for over a decade. Stores table metadata in a relational database.

---

## Deployment Options

### Docker Quickstart

```bash
# Run Hive Metastore with PostgreSQL backend
docker run -p 9083:9083 \
  -e METASTORE_DB_TYPE=postgresql \
  -e METASTORE_DB_URL=jdbc:postgresql://postgres:5432/metastore \
  -e METASTORE_DB_USER=hive \
  -e METASTORE_DB_PASSWORD=hive \
  apache/hive:4.0

# Initialize metastore schema (run once)
docker exec -it <container> schematool \
  -dbType postgresql -initSchema
```

### Self-Hosted Production

**Requirements:**
- PostgreSQL or MySQL backend (RDS, Cloud SQL, or self-hosted)
- HAProxy or similar for high availability
- Regular backups of the metastore database

```yaml
# docker-compose.yml example
version: '3'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: metastore
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: ${METASTORE_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  metastore:
    image: apache/hive:4.0
    ports:
      - "9083:9083"
    environment:
      METASTORE_DB_TYPE: postgresql
      METASTORE_DB_URL: jdbc:postgresql://postgres:5432/metastore
      METASTORE_DB_USER: hive
      METASTORE_DB_PASSWORD: ${METASTORE_PASSWORD}
    depends_on:
      - postgres
```

---

## PyIceberg Integration

```python
from pyiceberg.catalog.hive import HiveCatalog

catalog = HiveCatalog(
    name="my_hive",
    uri="thrift://localhost:9083",
    warehouse="s3://my-bucket/warehouse/",
    properties={
        "s3.region": "us-east-1",
        # If using S3 with IAM role, no credentials needed
        # If using keys:
        # "s3.access-key-id": "...",
        # "s3.secret-access-key": "..."
    }
)

# Create database
catalog.create_namespace("my_db")

# Create table
catalog.create_table(
    identifier=("my_db", "my_table"),
    schema=schema,  # PyArrow or PyIceberg schema
    location="s3://my-bucket/warehouse/my_db.db/my_table/"
)

# Load and query
table = catalog.load_table("my_db.my_table")
df = table.scan().to_pandas()
```

---

## Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `uri` | Thrift URI to metastore | `thrift://localhost:9083` |
| `warehouse` | Default S3/GCS location | `s3://bucket/warehouse/` |
| `s3.region` | AWS region for S3 | `us-east-1` |
| `s3.access-key-id` | AWS access key (if not using IAM) | `AKIA...` |
| `s3.secret-access-key` | AWS secret key | `...` |

---

## Performance Tuning

### Database Backend Tuning

For high partition counts (>100k):

```sql
-- PostgreSQL optimizations
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB';
ALTER SYSTEM SET work_mem = '50MB';

-- Create indexes for frequent queries
CREATE INDEX idx_partitions_tbl ON PARTITIONS(TBL_ID);
CREATE INDEX idx_sds_cd ON SDS(CD_ID);
```

### Metastore JVM Options

```bash
export HIVE_OPTS="-Xmx4g -XX:+UseG1GC"
```

---

## Pros and Cons

**Pros:**
- Mature, battle-tested (10+ years in production)
- Handles 500k+ partitions with proper DB tuning
- Works with any Hadoop ecosystem tool (Spark, Presto, Hive, Flink)
- No vendor lock-in
- Self-hosted = full control over data

**Cons:**
- Self-managed (DB operations, backups, HA setup)
- Limited Iceberg-specific features (no branching/namespace)
- Thrift protocol (legacy, less efficient)
- Requires separate infrastructure

---

## When to Choose Hive Metastore

| Choose Hive Metastore | Consider Alternatives |
|----------------------|----------------------|
| Existing Hadoop/Spark infrastructure | AWS-only stack → Use Glue |
| Need >10k partitions | Need Git-like branching → Use Tabular |
| Require self-hosted for compliance | Want serverless → Use Glue or Tabular |
| Multi-cloud deployment | Small team/PoC → Use DuckDB |

---

## Troubleshooting

### Connection Refused

```bash
# Check metastore is listening
netstat -an | grep 9083

# Check Thrift URI format
# Correct: thrift://hostname:9083
# Incorrect: http://hostname:9083
```

### S3 Access Denied

- Verify IAM permissions for S3 bucket
- Check `warehouse` path ends with `/`
- Ensure S3 credentials or IAM role is configured

---

## See Also

- [Apache Hive Metastore Docs](https://cwiki.apache.org/confluence/display/Hive/Design)
- `@designing-data-storage` - Iceberg table format details
- `@accessing-cloud-storage` - S3 authentication patterns
