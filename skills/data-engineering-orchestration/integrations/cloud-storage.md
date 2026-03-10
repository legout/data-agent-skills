# dbt with Cloud Storage

Configuring dbt adapters (DuckDB, Postgres, Snowflake) to read/write data directly from/to cloud storage (S3, GCS, Azure).

## dbt-duckdb with S3/GCS

The DuckDB adapter supports HTTPFS for direct cloud access.

**profiles.yml**:
```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/analytics.db  # Local DuckDB file
      extensions:
        - httpfs
        - parquet
      settings:
        s3_region: us-east-1
        s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
        # For GCS: gcs_region, use Google ADC instead
```

**Model Example** (`models/staging/stg_events.sql`):
```sql
-- Read Parquet directly from S3
WITH source AS (
    SELECT * FROM read_parquet('s3://bucket/raw/events/*.parquet')
)

SELECT
    id::INTEGER as event_id,
    event_type,
    value::FLOAT as amount,
    timestamp::TIMESTAMP as created_at
FROM source
WHERE value IS NOT NULL
```

## dbt-postgres with S3

Use AWS PostgreSQL extension (`aws_s3`):

```yaml
# profiles.yml
my_project:
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: "{{ env_var('PG_PASSWORD') }}"
      dbname: analytics
      schema: raw
      # aws_s3 extension must be installed in Postgres
```

**Model using aws_s3**:
```sql
-- Import from S3 using aws_s3 extension
SELECT * FROM aws_s3.table_import_from_s3(
    'events', 'parquet', 's3://bucket/events/'
);
```

## dbt Models with External Tables

Alternative: Create external tables in warehouse first:

```sql
-- Postgres with foreign data wrapper
CREATE FOREIGN TABLE external_events (
    id INTEGER,
    event_type TEXT,
    value FLOAT,
    timestamp TIMESTAMP
) SERVER s3_server
OPTIONS (
    s3_access_key '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    s3_secret_key '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    filename 's3://bucket/events.parquet',
    format 'parquet'
);

-- dbt model selects from external table
SELECT * FROM external_events WHERE value > 0
```

## Best Practices

1. ✅ **Use environment variables** for credentials via `{{ env_var(...) }}`
2. ✅ **Install extensions** (`httpfs`, `aws_s3`) in settings
3. ✅ **Partition cloud paths** (s3://bucket/events/year=2024/month=01/)
4. ❌ **Don't** commit credentials to `profiles.yml` in version control
5. ❌ **Don't** rely on local filesystem - cloud storage should be the source of truth

## Credential Management

Production deployments should use:

- **CI/CD Secrets**: GitHub Actions secrets, GitLab CI variables, Jenkins credentials
- **Cloud Secret Managers**: AWS Secrets Manager, GCP Secret Manager, Azure Key Vault
- **dbt Cloud**: Built-in environment variable management

Example with dbt Cloud:
```yaml
# dbt Cloud UI: Set environment variables in project settings
# No need for env_var() in profiles.yml for dbt Cloud
```

---

## References

- [dbt-duckdb documentation](https://docs.getdbt.com/docs/adapters/duckdb)
- [DuckDB HTTPFS extension](https://duckdb.org/docs/extensions/httpfs)
- [dbt-postgres documentation](https://docs.getdbt.com/docs/adapters/postgres)
- `@data-engineering-orchestration/dbt.md` - Full dbt guide
- `@data-engineering-storage-remote-access/integrations/duckdb.md` - DuckDB cloud patterns
