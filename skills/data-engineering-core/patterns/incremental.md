# Incremental Loading Patterns

Strategies for loading only new or changed data in ETL pipelines. Covers watermark-based incremental loads, change data capture (CDC), and merge patterns.

## Why Incremental?

- **Efficiency**: Process only changed data, not full dataset
- **Latency**: Faster pipeline runs (hours → minutes)
- **Cost**: Less compute and storage I/O
- **Scalability**: Handle growing datasets without linear time growth

## Common Patterns

### 1. Timestamp Watermark

Track the last processed timestamp and fetch only newer records.

```python
import duckdb
from datetime import datetime
from contextlib import contextmanager

@contextmanager
def get_connection(db_path: str = "etl.db"):
    con = duckdb.connect(db_path)
    try:
        yield con
    finally:
        con.close()

def get_last_watermark(con, table_name: str, timestamp_col: str = "updated_at") -> datetime:
    """Get max timestamp from target table."""
    result = con.sql(f"SELECT MAX({timestamp_col}) FROM {table_name}").fetchone()
    return result[0] if result[0] else datetime(1900, 1, 1)

def incremental_load(source_path: str, target_table: str):
    """Incremental load from Parquet file using timestamp watermark."""
    with get_connection() as con:
        watermark = get_last_watermark(con, target_table)

        # ✅ FIXED: Parameterized query prevents SQL injection
        query = """
            SELECT * FROM read_parquet(?)
            WHERE updated_at > ?
        """
        new_data = con.sql(query, [source_path, watermark]).pl()

        # Append or upsert
        con.sql("INSERT INTO target_table SELECT * FROM new_data")
```

**Pros**: Simple, works for append-only or slowly changing data
**Cons**: Misses updates to old records (only new ones); relies on accurate timestamps

---

### 2. Version/Sequence Number

For databases with version columns or incrementing IDs:

```python
def incremental_by_version(con, source_table: str, target_table: str, version_col: str = "version"):
    """Track last processed version number."""
    last_version = con.sql(f"SELECT MAX({version_col}) FROM {target_table}").fetchone()[0] or 0

    new_data = con.sql(f"""
        SELECT * FROM {source_table}
        WHERE {version_col} > {last_version}
    """)

    con.sql(f"INSERT INTO {target_table} SELECT * FROM new_data")
```

**Pros**: Guarantees no gaps; handles updates if versions increase on update
**Cons**: Requires monotonic version column

---

### 3. CDC (Change Data Capture)

Capture all INSERT, UPDATE, DELETE operations from database transaction logs.

**Tools**: Debezium (Kafka Connect), pglogical (PostgreSQL), SQL Server CDC

```python
# Example: Consuming Debezium JSON from Kafka topic
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cdc-consumer',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['postgres.public.events.debezium'])

for msg in consumer:
    event = json.loads(msg.value().decode('utf-8'))
    op = event['payload']['op']  # 'c'=create, 'u'=update, 'd'=delete

    if op == 'c':
        insert_record(event['payload']['after'])
    elif op == 'u':
        update_record(event['payload']['after'])
    elif op == 'd':
        delete_record(event['payload']['before']['id'])
```

**Pros**: Captures all changes, including deletes; real-time
**Cons**: Complex setup; requires source database configuration

---

### 4. Merge/Upsert Pattern

Combine new data with existing using key matching (UPSERT).

```python
def upsert_data(con, source_df, target_table: str, key_columns: list):
    """
    Merge source data into target table.
    Requires DuckDB MERGE support (v0.8+) or Spark Delta.
    """
    # Materialize source
    con.sql("CREATE OR REPLACE TABLE staging AS SELECT * FROM source_df")

    # Build merge condition
    merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)

    con.sql(f"""
        MERGE INTO {target_table} AS target
        USING staging AS source
        ON {merge_cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

**Pros**: Handles updates and inserts; idempotent
**Cons**: Requires database/engine that supports MERGE (DuckDB, Spark, Postgres)

---

## Implementation Patterns

### Checkpoint Persistence

Store watermark/metadata in separate tracking table:

```python
def load_with_checkpoint(
    source_path: str,
    target_table: str,
    checkpoint_table: str = "pipeline_checkpoints"
):
    """Incremental load with explicit checkpoint management."""
    with get_connection() as con:
        # Get last checkpoint
        result = con.sql(f"""
            SELECT last_value FROM {checkpoint_table}
            WHERE pipeline_name = 'daily_events'
        """).fetchone()
        last_checkpoint = result[0] if result else datetime(1900, 1, 1)

        # Load new data based on checkpoint
        new_data = con.sql("""
            SELECT * FROM read_parquet(?)
            WHERE updated_at > ?
        """, [source_path, last_checkpoint]).pl()

        if len(new_data) > 0:
            con.sql(f"INSERT INTO {target_table} SELECT * FROM new_data")

            # Update checkpoint to max timestamp in new data
            new_max = new_data["updated_at"].max()
            con.sql(f"""
                INSERT OR REPLACE INTO {checkpoint_table}
                VALUES ('daily_events', '{new_max}')
            """)
```

### Idempotent Design

Ensure pipeline can be safely re-run:

```python
def idempotent_incremental(
    con,
    source_path: str,
    target_table: str,
    merge_keys: list
):
    """
    Idempotent incremental: re-running processes same data.
    Use MERGE with DISTINCT to avoid duplicates.
    """
    # Read source, deduplicate by merge keys
    source = con.sql(f"""
        SELECT DISTINCT ON ({', '.join(merge_keys)}) *
        FROM read_parquet(?)
        ORDER BY {', '.join(merge_keys)}, updated_at DESC
    """, [source_path])

    # Merge
    con.sql(f"""
        CREATE OR REPLACE TABLE staging AS SELECT * FROM source
    """)

    merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    con.sql(f"""
        DELETE FROM {target_table}
        WHERE EXISTS (
            SELECT 1 FROM staging
            WHERE {merge_cond}
        )
    """)

    con.sql(f"INSERT INTO {target_table} SELECT * FROM staging")
```

---

## Anti-Patterns

❌ **Using row count as watermark** - Counts change non-monotonically
❌ **No deduplication** - Duplicate processing on retries leads to duplicates
❌ **Missing transaction boundaries** - Partial loads on failure cause data loss
❌ **No backfill capability** - Can't re-process specific date range
❌ **Relying on file modification times** - Not reliable across systems

---

## Best Practices

1. ✅ **Use timestamps or versions** - Monotonic, reliable
2. ✅ **Make idempotent** - Safe to retry, no duplicate side effects
3. ✅ **Track metadata explicitly** - Separate checkpoint table, not embedded in data
4. ✅ **Include backfill strategy** - Ability to re-process date ranges manually
5. ✅ **Handle out-of-order data** - Late-arriving records (use watermark lag buffer)
6. ✅ **Test reprocessing scenarios** - What happens if we re-run yesterday's pipeline?
7. ❌ **Don't** rely on record counts (non-monotonic)
8. ❌ **Don't** skip transactions - Use transactions or atomic operations
9. ❌ **Don't** assume exactly-once - Design for at-least-once

---

## References

- `@data-engineering-core` - DuckDB MERGE patterns
- `@data-engineering-orchestration` - Scheduling incremental jobs
- `@data-engineering-storage-lakehouse` - Delta/Iceberg time travel for backfills
- [AWS DMS CDC Best Practices](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html)
- [Debezium Documentation](https://debezium.io/documentation/)
