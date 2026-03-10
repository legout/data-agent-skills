# dbt (Data Build Tool)

dbt (data build tool) is the standard for analytics engineering. It transforms raw data into analytics-ready tables using SQL and Jinja templating, with built-in testing, documentation, and lineage.

## Why dbt?

- **SQL-first**: Write transformations in SQL, not Python
- **Modularity**: `ref()` and `source()` build a DAG of dependencies
- **Testing**: Schema tests, custom tests, data quality assertions
- **Documentation**: Auto-generated data dictionary with lineage graph
- **Package ecosystem**: Share models, macros, tests across projects
- **Adapters**: Works with DuckDB, Postgres, Snowflake, BigQuery, Spark, etc.

## Installation

```bash
# Install dbt-core (library)
pip install dbt-core

# Install adapter for your data platform
pip install dbt-duckdb     # DuckDB
pip install dbt-postgres   # PostgreSQL
pip install dbt-snowflake  # Snowflake
pip install dbt-bigquery   # Google BigQuery
pip install dbt-spark      # Spark/Databricks
```

## Project Structure

```bash
my_dbt_project/
├── dbt_project.yml      # Project config
├── profiles.yml         # Connection profiles (in home dir, not repo!)
├── models/              # SQL models
│   ├── staging/
│   │   ├── stg_events.sql
│   │   └── schema.yml
│   └── marts/
│       ├── daily_summary.sql
│       └── schema.yml
├── tests/               # Custom tests (sources, singular_data_test)
├── macros/              # Reusable Jinja functions
├── seeds/               # CSV data to load
├── snapshots/           # Slowly changing dimensions (SCD)
├── analysis/            # Ad-hoc queries (not part of DAG)
└── manifest.json        # Generated (don't commit to git)
```

## Quickstart

```bash
# Initialize project
dbt init my_project
cd my_project

# Edit profiles.yml (see below)
dbt debug  # Verify connection

# Build everything (run + test)
dbt build
```

## profiles.yml Configuration

DuckDB example:
```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/analytics.db
      threads: 4
```

Postgres example:
```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: "{{ env_var('PG_USER') }}"
      password: "{{ env_var('PG_PASSWORD') }}"
      port: 5432
      dbname: analytics
      schema: dev
      threads: 4
```

**See Also:** `@data-engineering-orchestration/integrations/cloud-storage.md` for cloud storage configs.

## Models

Models are SQL SELECT statements. dbt compiles them into `CREATE OR REPLACE TABLE` statements.

`models/staging/stg_events.sql`:
```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'events') }}
)

SELECT
    id::INTEGER as event_id,
    event_type,
    value::FLOAT as amount,
    timestamp::TIMESTAMP as created_at
FROM source
WHERE value IS NOT NULL
```

`models/marts/daily_summary.sql`:
```sql
SELECT
    DATE_TRUNC('day', created_at) as day,
    event_type,
    SUM(amount) as total_value,
    COUNT(*) as event_count
FROM {{ ref('stg_events') }}
GROUP BY 1, 2
```

**Key jinja macros:**
- `{{ ref('model_name') }}` - Reference another model (builds dependency)
- `{{ source('source_name', 'table_name') }}` - Reference raw source
- `{{ config(...) }}` - Model-specific config (materialization, partition_by)
- `{{ var('variable_name') }}` - Project variables

## Sources

Define raw tables in YAML:

`models/schema.yml`:
```yaml
version: 2

sources:
  - name: raw
    database: production
    schema: raw
    tables:
      - name: events
        columns:
          - name: id
            tests:
              - unique
              - not_null
```

Then reference: `{{ source('raw', 'events') }}`

## Tests

### Schema Tests (in YAML)
```yaml
models:
  - name: stg_events
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id
      - name: amount
        tests:
          - not_null
          - dbt_utils.greater_than_zero:
              compare_to: 0
          - accepted_range:
              min_value: 0
              max_value: 1000000
```

### Singular Tests (Python/SQL)
`tests/custom_positive_amount.sql`:
```sql
SELECT * FROM {{ ref('stg_events') }}
WHERE amount <= 0
```

### Generic Tests (macros)
```sql
-- tests/generic/not_null.sql
SELECT * FROM {{ ref(model) }}
WHERE {{ column }} IS NULL
```

Run tests:
```bash
dbt test
```

## Snapshots (SCD Type 2)

Track slowly changing dimensions:

`snapshots/customers_snapshot.sql`:
```sql
{% snapshot customers_snapshot %}

{{
    config(
        target_database='analytics',
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

## Seeds

Load static CSV data:

```bash
dbt seed  # Loads all CSVs in seeds/ directory as tables
```

`seeds/countries.csv` becomes `analytics.seeds.countries` table.

## Commands

```bash
dbt debug           # Check connection
dbt deps            # Install packages (from packages.yml)
dbt compile         # Compile SQL (creates manifest.json)
dbt run             # Execute models
dbt test            # Run tests
dbt snapshot        # Execute snapshots
dbt seed            # Load seed CSVs
dbt build           # Run + test + snapshot + seed
dbt docs generate   # Generate documentation site
dbt docs serve      # Serve docs locally at localhost:8080
dbt run-operation   # Execute a macro
dbt source freshness # Check freshness of sources
```

## Configuration

`dbt_project.yml`:
```yaml
name: my_project
version: "1.0.0"
config-version: 2

profile: my_project  # Matches profile name

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]

models:
  my_project:
    staging:
      +materialized: view  # Default materialization for staging models
      +tags: ["staging"]
    marts:
      +materialized: table
      +tags: ["mart"]
```

## Materializations

- **view** (default): `CREATE VIEW` - no storage, computed on read
- **table**: `CREATE TABLE` - stored, faster queries, costs storage
- **incremental**: Only insert/update changed rows (requires `is_incremental()` logic)
- **ephemeral**: CTE, never materialized (used as subquery only)

Example incremental model:
```sql
{{ config(materialized='incremental') }}

SELECT
    id,
    value,
    updated_at
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

## Python API

```python
from dbt.cli.main import dbtRunner

dbt = dbtRunner()
result = dbt.invoke(["run", "--select", "tag:daily"])

if result.success:
    print("dbt run succeeded")
else:
    print("dbt run failed")
    # Inspect result.exception, result.result
```

## dbt + Dagster

See `@data-engineering-orchestration/dagster.md` or `dagster-dbt` package.

## Best Practices

1. ✅ **One model per file** - Clear naming: `stg_`, `int_`, `dim_`, `fct_` prefixes
2. ✅ **Use staging layer** - Raw → Staging (clean/type) → Intermediate → Marts
3. ✅ **Add tests** - Not null, unique, relationships, custom
4. ✅ **Document** - Descriptions in YAML appear in data catalog
5. ✅ **Tag models** - Use tags (`+tags: ["daily"]`) for selective runs
6. ❌ **Don't** use `SELECT *` in models (explicit columns only)
7. ❌ **Don't** put business logic in macros - keep in models
8. ❌ **Don't** commit `profiles.yml` to repo (use env vars or CI/CD secrets)

---

## References

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-Labs/dbt-core](https://github.com/dbt-labs/dbt-core)
- [dbt Adapters](https://docs.getdbt.com/docs/available-adapters/)
- `@data-engineering-orchestration/integrations/cloud-storage.md` - dbt with cloud storage
