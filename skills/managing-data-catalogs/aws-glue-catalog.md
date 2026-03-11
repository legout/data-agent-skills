# AWS Glue Data Catalog

Fully managed catalog by AWS, serverless, Hive-compatible API. Integrates natively with Athena, EMR, Redshift Spectrum.

---

## Setup with PyIceberg

```python
from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog(
    name="my_glue",
    region="us-east-1",
    warehouse="s3://my-bucket/warehouse/"
    # No explicit credentials needed if using IAM role
)

# Create table
catalog.create_table(
    identifier=("my_database", "events"),
    schema=schema
)

# Tables automatically appear in AWS Glue console
# Query via Athena: SELECT * FROM my_database.events
```

---

## IAM Permissions Required

### Minimum IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

### Fine-Grained with Lake Formation

```json
{
  "Effect": "Allow",
  "Action": "lakeformation:GetDataAccess",
  "Resource": "*"
}
```

---

## Crawler Configuration

For existing data in S3, use a Glue Crawler to auto-discover schema:

```python
import boto3

glue = boto3.client('glue')

# Create crawler
glue.create_crawler(
    Name='my-crawler',
    Role='AWSGlueServiceRole',
    DatabaseName='my_database',
    Targets={
        'S3Targets': [
            {'Path': 's3://my-bucket/warehouse/my_table/'}
        ]
    },
    TablePrefix='crawled_',
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'LOG'
    }
)

# Run crawler
glue.start_crawler(Name='my-crawler')
```

---

## Cross-Service Access

### Athena Integration

```sql
-- Tables registered via Glue are queryable in Athena
SELECT * FROM my_database.events
WHERE event_date >= date '2024-01-01';
```

### EMR Integration

```python
# Spark on EMR uses Glue by default
spark.conf.set("hive.metastore.client.factory.class",
               "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")

# Access same table
df = spark.table("my_database.events")
```

### Redshift Spectrum

```sql
-- Create external schema pointing to Glue catalog
CREATE EXTERNAL SCHEMA my_schema
FROM DATA CATALOG
DATABASE 'my_database'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Query Iceberg tables
SELECT * FROM my_schema.events LIMIT 10;
```

---

## Unity Catalog Federation (Databricks)

Databricks can read AWS Glue tables via federation:

```python
# In Databricks notebook
spark.conf.set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")

# Query Glue catalog table from Databricks
spark.sql("SELECT * FROM my_database.events").show()
```

Or create a foreign catalog in Unity Catalog:

```sql
-- Databricks SQL
CREATE FOREIGN CATALOG glue_catalog
FROM METASTORE
OPTIONS (
  ' metastore' = 'aws',
  'region' = 'us-east-1'
);
```

---

## Pros and Cons

**Pros:**
- Serverless, zero operations overhead
- Native integration with Athena, EMR, Redshift Spectrum
- Fine-grained IAM permissions via Lake Formation
- Works with Delta Lake, Iceberg, Hudi tables
- Managed high availability

**Cons:**
- Performance degrades >10k partitions (known issue)
- Limited metadata operations (slower than Hive for bulk ops)
- AWS lock-in
- Limited Iceberg-specific features (branching, tags)

---

## When to Choose AWS Glue

| Choose AWS Glue | Consider Alternatives |
|-----------------|----------------------|
| AWS-native stack (Athena, EMR) | Multi-cloud → Use Hive or Tabular |
| Need serverless catalog | Need Git-like branching → Use Tabular |
| Lake Formation governance | >50k partitions → Use Hive Metastore |
| Tight IAM integration | Self-hosted requirement → Use Hive |

---

## Known Limitations

### Partition Performance

- Glue struggles with >10,000 partitions per table
- Workaround: Use partition projection or partition indexes

```python
# Partition projection in table properties
properties = {
    "projection.enabled": "true",
    "projection.event_date.type": "date",
    "projection.event_date.range": "2020-01-01,NOW",
    "projection.event_date.format": "yyyy-MM-dd"
}
```

### API Rate Limits

- Default: 5 TPS for GetTable operations
- Request increase via AWS Support for high-throughput workloads

---

## Troubleshooting

### AccessDeniedException

```
botocore.errorfactory.AccessDeniedException: 
User is not authorized to perform: glue:GetTable
```

- Verify IAM role has required Glue permissions
- Check Lake Formation data lake permissions
- Ensure S3 bucket policy allows access

### EntityNotFoundException

```
glue:GetTable: Entity not found
```

- Database may not exist → Create with `catalog.create_namespace()`
- Table may be in different region
- Verify catalog configuration matches AWS region

---

## See Also

- [AWS Glue Data Catalog Docs](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- `@accessing-cloud-storage` - AWS authentication patterns
- `@data-engineering-storage-authentication` - IAM roles and credentials
