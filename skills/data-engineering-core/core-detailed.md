# Core Data Engineering (Detailed Reference)

# Core Data Engineering

Foundational libraries and patterns for building robust, efficient data pipelines in Python. Covers Polars, DuckDB, PyArrow, relational database integration, ETL patterns, error handling, and performance optimization.

## Quick Reference

| Library | Primary Use | Key Strength |
|---------|-------------|--------------|
| **Polars** | DataFrame operations | Lazy evaluation, multi-threading, 10-50x faster than pandas |
| **DuckDB** | In-process SQL analytics | Embedded OLAP, zero-copy Arrow integration |
| **PyArrow** | Data interchange | Apache Arrow format, zero-copy transfers |
| **psycopg2** | PostgreSQL access | CDC, bulk operations, connection pooling |

## Related Skills

- `@data-engineering-storage-lakehouse` - Delta Lake, Iceberg, Hudi
- `@data-engineering-streaming` - Kafka, MQTT, NATS
- `@data-engineering-orchestration` - Prefect, Dagster, dbt
- `@data-engineering-quality` - Great Expectations, Pandera
- `@data-engineering-observability` - OpenTelemetry, Prometheus
- `@data-engineering-ai-ml` - Embeddings, vector DBs, RAG
- `@data-engineering-storage-remote-access` - fsspec, pyarrow.fs, obstore for cloud access

---

## 1. Polars: High-Performance DataFrames

Polars is a Rust-based DataFrame library with lazy evaluation and automatic parallelization.

### Installation
```bash
pip install polars
```

### Basic Operations
```python
import polars as pl

# Create DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3],
    "category": ["A", "B", "A"],
    "value": [100, 200, 150]
})

# Filter and aggregate (eager)
result = df.filter(pl.col("category") == "A").group_by("category").agg(
    pl.col("value").sum().alias("total")
)

# Lazy evaluation for large datasets
lazy_df = (
    pl.scan_csv("large_file.csv")
    .filter(pl.col("date") > "2024-01-01")
    .group_by("category")
    .agg(pl.col("value").mean())
)

# Materialize only when needed
result = lazy_df.collect()
```

### Best Practices
- ✅ **Use lazy evaluation** (`scan_*`) to defer computation and enable query optimization
- ✅ **Chain operations** to allow Polars to optimize the entire query
- ✅ **Leverage `explain()`** to see the optimized query plan
- ✅ **Use `pl.Categorical`** for string columns to reduce memory by 5-10x
- ✅ **Process large files in chunks** using `read_csv_batched()` for memory constraints

```python
# See query plan before execution
lazy_df.explain()

# Streaming for very large datasets (batched processing)
reader = pl.read_csv_batched("huge_file.csv", batch_size=100_000)
while (batches := reader.next_batches(1)):
    for batch in batches:
        process(batch)
```

### Anti-Patterns
- ❌ Don't load entire large datasets eagerly (`pl.read_parquet()` on multi-GB files)
- ❌ Don't process row-by-row with `iter_rows()` - use vectorized operations
- ❌ Don't chain excessive operations on eager DataFrames - switch to lazy

---

## 2. DuckDB: Embedded SQL Analytics

DuckDB is an in-process SQL OLAP database with zero-copy integration with Polars and PyArrow. DuckDB 1.0+ (2024) is production-ready for enterprise OLAP workloads.

### Installation
```bash
pip install duckdb
```

### Basic SQL Operations
```python
import duckdb

# Query directly on files (Parquet, CSV, JSON)
result = duckdb.sql("""
    SELECT category, SUM(value) as total
    FROM 'sales.parquet'
    WHERE date > '2024-01-01'
    GROUP BY category
    ORDER BY total DESC
""")

# Create tables and insert data
duckdb.sql("CREATE TABLE events (id INT, timestamp TIMESTAMP, value DOUBLE)")
duckdb.sql("INSERT INTO events VALUES (1, '2024-01-01', 100.5)")

# Convert results to different formats
df_pandas = result.df()   # pandas DataFrame
df_polars = result.pl()  # Polars DataFrame
df_arrow = result.arrow()  # PyArrow Table
```

### Integration with Polars
```python
import duckdb
import polars as pl

# Query Polars DataFrames directly with SQL
df = pl.DataFrame({
    "id": [1, 2, 3],
    "category": ["A", "B", "A"],
    "value": [100, 200, 300]
})

result = duckdb.sql("""
    SELECT id, value,
           SUM(value) OVER (ORDER BY id) as running_total
    FROM df
""").pl()

# Multi-library workflow: PyArrow → Polars → DuckDB
import pyarrow.parquet as pq
table = pq.read_table("data.parquet")
df = pl.from_arrow(table)
processed = df.filter(pl.col("value") > 100)
duckdb.sql("SELECT * FROM processed WHERE category = 'A'")
```

### Connection Management
```python
# ✅ DO: Use context manager for automatic cleanup
with duckdb.connect("analytics.db") as con:
    con.sql("CREATE TABLE ...")
    result = con.sql("SELECT * FROM table").pl()

# ✅ DO: Close connections explicitly if not using context manager
con = duckdb.connect("analytics.db")
try:
    con.sql("...")
finally:
    con.close()

# ❌ DON'T: Leak connections
con = duckdb.connect("analytics.db")
con.sql("...")  # Never closed → connection leak
```

### Parameterized Queries (SQL Injection Prevention)
```python
# ✅ DO: Use parameterized queries
def get_events(con, min_value: float, category: str):
    return con.sql("""
        SELECT * FROM events
        WHERE value > ? AND category = ?
    """, [min_value, category]).pl()

# ❌ DON'T: Use f-strings or concatenation
def vulnerable(con, user_category: str):
    # SQL INJECTION RISK!
    con.sql(f"SELECT * FROM events WHERE category = '{user_category}'")
```

---

## 3. PyArrow: Data Interchange Format

PyArrow provides zero-copy data transfer between systems using Apache Arrow format.

### Installation
```bash
pip install pyarrow
```

### Basic Operations
```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import polars as pl

# Create Arrow tables
table = pa.table({
    "id": [1, 2, 3],
    "value": [100.0, 200.0, 150.0]
})

# Read/write Parquet
pq.write_table(table, "data.parquet")
table = pq.read_table("data.parquet")

# Read with row group filtering (efficient partial reads)
parquet_file = pq.ParquetFile("data.parquet")
table = parquet_file.read_row_groups([0, 1])  # Specific row groups only

# IPC for Arrow data interchange
with ipc.new_file("data.arrow", table.schema) as writer:
    writer.write(table)

# Convert to other formats
pandas_df = table.to_pandas()
polars_df = pl.from_arrow(table)
```

---

## 4. PostgreSQL Integration

### 4.1 Basic CRUD with psycopg2

For **Change Data Capture (CDC)** from PostgreSQL, consider [Debezium](https://debezium.io/) or `pglogical` for streaming change events to Kafka.

**Installation:**
```bash
pip install psycopg2-binary pandas
```

**Operations:**
```python
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from contextlib import contextmanager

@contextmanager
def get_connection():
    """Context manager for connection cleanup."""
    conn = psycopg2.connect(
        host="localhost",
        database="mydb",
        user="user",
        password="pass"
    )
    try:
        yield conn
    finally:
        conn.close()

# Create table
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    conn.commit()

# Insert single record
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO events (name, value) VALUES (%s, %s)",
            ("event1", 100.50)  # Parameterized → safe from SQL injection
        )
    conn.commit()

# Bulk insert
data = [("event2", 200.75), ("event3", 300.00), ("event4", 150.25)]
with get_connection() as conn:
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO events (name, value) VALUES %s",
            data
        )
    conn.commit()

# Query to DataFrame
with get_connection() as conn:
    df = pd.read_sql("SELECT * FROM events WHERE value > 100", conn)

# Update with transaction
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE events SET value = %s WHERE id = %s",
            (250.00, 2)  # Parameterized
        )
    conn.commit()
```

### 4.2 DuckDB + PostgreSQL (Fixed SQL Injection)

> **Note**: `INSTALL postgres` requires network access on first run to download the extension.

**Incremental Loading Pattern:**
```python
import duckdb
from datetime import datetime
from contextlib import contextmanager

@contextmanager
def get_duckdb_connection(db_path: str = "etl.db"):
    """Context manager for DuckDB connections."""
    con = duckdb.connect(db_path)
    try:
        yield con
    finally:
        con.close()

def incremental_sync(pg_conn_str: str, table_name: str):
    """Sync PostgreSQL table incrementally using parameterized queries."""
    with get_duckdb_connection("local_duckdb.db") as con:
        # Install and load PostgreSQL extension
        con.sql("INSTALL postgres; LOAD postgres;")

        # Attach PostgreSQL database (read-only)
        con.sql(f"""
            ATTACH '{pg_conn_str}' AS pg
            (TYPE POSTGRES, READ_ONLY)
        """)

        # Get watermark using parameterized query
        result = con.sql("SELECT MAX(updated_at) FROM sync_log").fetchone()
        last_sync = result[0] if result[0] else datetime(1900, 1, 1)

        # ✅ FIXED: Use parameterized query instead of string formatting
        con.sql("""
            CREATE OR REPLACE TABLE events AS
            SELECT * FROM pg.events
            WHERE updated_at > ?
        """, [last_sync])

        # Log sync time (also parameterized)
        con.sql("INSERT INTO sync_log VALUES (?)", [datetime.utcnow()])

incremental_sync(
    "postgresql://user:pass@host:5432/mydb",
    "events"
)
```

---

## 5. ETL Pipeline Patterns

### 5.1 Basic ETL Structure
```python
import polars as pl
import duckdb
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from typing import Protocol, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataSource(Protocol):
    """Protocol for data sources."""
    def extract(self) -> pl.LazyFrame:
        ...

class ETLPipeline:
    """
    Production-ready ETL pipeline with lazy evaluation and proper resource management.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.raw_path = Path(config["raw_path"])
        self.processed_path = Path(config["processed_path"])
        self.duckdb_path = Path(config.get("duckdb_path", "analytics.db"))

    def extract(self, source: str | Path) -> pl.LazyFrame:
        """Extract data from source (CSV or Parquet)."""
        source = str(source)
        if source.endswith(".csv"):
            return pl.scan_csv(source)
        elif source.endswith(".parquet"):
            return pl.scan_parquet(source)
        else:
            raise ValueError(f"Unsupported format: {source}")

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply transformations (lazy - not executed yet)."""
        return (
            df
            .filter(pl.col("value").is_not_null())
            .with_columns([
                pl.col("date").str.to_date(),
                pl.col("amount").cast(pl.Float64),
                pl.col("category").str.to_lowercase().alias("category_cleaned")
            ])
            .group_by(["category_cleaned", "date"])
            .agg([
                pl.col("amount").sum().alias("total_amount"),
                pl.col("id").count().alias("count")
            ])
        )

    def load(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Load materialized DataFrame to destination."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.processed_path / f"output_{timestamp}.parquet"

        # Write to Parquet
        df.write_parquet(output_path)

        # Load to DuckDB (use context manager to avoid leaks)
        with duckdb.connect(self.duckdb_path) as con:
            con.sql("""
                CREATE TABLE IF NOT EXISTS daily_summary AS
                SELECT * FROM df
            """)

        return {
            "output_path": str(output_path),
            "rows_written": len(df),
            "timestamp": datetime.now().isoformat()
        }

    def run(self, source_path: str | Path) -> Dict[str, Any]:
        """Execute full ETL pipeline."""
        logger.info(f"Starting ETL from {source_path}")

        try:
            # Extract (lazy)
            raw = self.extract(source_path)

            # Estimate row count without full collect (Polars 0.19+)
            # For Parquet: metadata.num_rows
            # For CSV: peek at file or use head + len
            logger.info("Extraction stage initialized (lazy)")

            # Transform (lazy)
            transformed = self.transform(raw)

            # Collect (materialize)
            result_df = transformed.collect()
            logger.info(f"Transformed data: {len(result_df)} rows")

            # Load
            load_result = self.load(result_df)
            logger.info(f"ETL completed: {load_result}")

            return load_result

        except Exception as e:
            logger.error(f"ETL failed: {e}", exc_info=True)
            raise

# Usage
if __name__ == "__main__":
    config = {
        "raw_path": "data/raw",
        "processed_path": "data/processed",
        "duckdb_path": "analytics.db"
    }
    pipeline = ETLPipeline(config)
    result = pipeline.run("data/input.parquet")
    print(result)
```

### 5.2 Incremental ETL with Watermark (Fixed)
```python
class IncrementalETL:
    """
    incremental loading with proper resource management and SQL injection prevention.
    """
    def __init__(self, db_path: str = "etl.db", watermark_col: str = "updated_at"):
        self.db_path = db_path
        self.watermark_col = watermark_col

    def get_watermark(self, table_name: str) -> datetime:
        """Get last sync timestamp from metadata table."""
        with duckdb.connect(self.db_path) as con:
            result = con.sql(f"""
                SELECT MAX({self.watermark_col}) FROM {table_name}
            """).fetchone()
            return result[0] if result[0] else datetime(1900, 1, 1)

    def extract_incremental(self, source_path: str, table_name: str) -> pl.DataFrame:
        """Extract only new/changed records using parameterized query."""
        watermark = self.get_watermark(table_name)

        with duckdb.connect(self.db_path) as con:
            # ✅ FIXED: Parameterized query prevents SQL injection
            query = f"""
                SELECT * FROM read_parquet(?)
                WHERE {self.watermark_col} > ?
            """
            result = con.sql(query, [source_path, watermark])
            return result.pl()

    def upsert(
        self,
        source_df: pl.DataFrame,
        target_table: str,
        key_columns: list[str]
    ):
        """Merge changes into target table with proper cleanup."""
        with duckdb.connect(self.db_path) as con:
            # Create staging table (materialize)
            con.sql("CREATE OR REPLACE TABLE staging AS SELECT * FROM source_df")

            # Build merge condition dynamically but safely (column names are identifiers, not values)
            merge_condition = " AND ".join(
                f"target.{col} = source.{col}" for col in key_columns
            )

            # Execute merge
            con.sql(f"""
                MERGE INTO {target_table} AS target
                USING staging AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

            row_count = con.sql("SELECT COUNT(*) FROM staging").fetchone()[0]
            logger.info(f"Merged {row_count} records into {target_table}")

# Usage
etl = IncrementalETL()
new_data = etl.extract_incremental("s3://bucket/events.parquet", "events")
etl.upsert(new_data, "events", ["id"])
```

---

## 6. Error Handling & Resilience

### 6.1 Retry Pattern
```python
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
import botocore.exceptions
import requests.exceptions

class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((
        IOError,
        botocore.exceptions.ClientError,
        requests.exceptions.ConnectionError
    ))
)
def reliable_extract(source: str) -> pl.DataFrame:
    """Extract with automatic retry for transient errors."""
    logger.info(f"Extracting from {source}")
    try:
        df = pl.read_parquet(source)
        logger.info(f"Extracted {len(df)} rows")
        return df
    except (IOError, FileNotFoundError) as e:
        logger.warning(f"Transient error reading {source}: {e}")
        raise
```

### 6.2 Circuit Breaker (Fixed)
```python
import time
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker:
    """
    Fixed implementation with proper state management and timeout reset.
    """
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = CircuitState.CLOSED

    def call(self, func, *args, **kwargs):
        """
        Execute function with circuit breaker protection.

        Raises:
            CircuitOpenError: When circuit is open
            Original exception: On failure when closed/half-open
        """
        if self.state == CircuitState.OPEN:
            # Check if timeout has elapsed to transition to half-open
            if time.time() - self.last_failure_time >= self.timeout:
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker transitioning to HALF_OPEN")
            else:
                raise RuntimeError("Circuit breaker is OPEN - fast failing")

        try:
            result = func(*args, **kwargs)
            # Success: reset failure count and close circuit
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful call."""
        self.failures = max(0, self.failures - 1)  # Degrade gradually
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            logger.info("Circuit breaker CLOSED - recovery successful")

    def _on_failure(self):
        """Handle failed call."""
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker OPEN - {self.failures} failures")

# Usage
import requests

circuit = CircuitBreaker(threshold=5, timeout=60)

def fetch_from_api(url: str):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

payload = circuit.call(fetch_from_api, "https://api.example.com/events")
```

### 6.3 Validation
```python
def validate_schema(df: pl.DataFrame, expected_schema: Dict[str, pl.DataType]) -> bool:
    """
    Validate DataFrame matches expected schema.

    Args:
        df: Polars DataFrame
        expected_schema: Mapping of column name to Polars data type

    Returns:
        True if valid, raises exception otherwise
    """
    for col, dtype in expected_schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        if df[col].dtype != dtype:
            raise TypeError(
                f"Column '{col}' has wrong type: {df[col].dtype} != {dtype}"
            )
    return True
```

---

## 7. Performance Optimization

### 7.1 Memory Management
```python
import psutil

def memory_usage_mb() -> float:
    """Get current process memory usage in MB."""
    process = psutil.Process()
    return process.memory_info().rss / 1024 ** 2

# Chunked processing for files larger than memory
def process_large_file_chunked(
    filepath: str,
    chunk_size: int = 100_000
) -> int:
    """Process large CSV in chunks to avoid OOM."""
    total_processed = 0
    reader = pl.read_csv_batched(filepath, batch_size=chunk_size)

    while (batches := reader.next_batches(1)):
        for chunk in batches:
            # Process chunk
            processed = chunk.filter(pl.col("value") > 0)
            total_processed += len(processed)
            del chunk  # Explicit cleanup

    return total_processed
```

### 7.2 Query Optimization
```python
# ✅ DO: Use lazy evaluation with predicate pushdown
lazy_df = pl.scan_parquet("large_data.parquet")
filtered = (
    lazy_df
    .filter(pl.col("date") > "2024-01-01")  # Pushed to I/O
    .filter(pl.col("value") > 0)
    .select(["id", "date", "value"])  # Column pruning
)
result = filtered.collect()  # Single pass

# ✅ DO: Use DuckDB EXPLAIN to analyze queries
import duckdb
plan = duckdb.sql("""
    EXPLAIN SELECT category, SUM(value)
    FROM 'data.parquet'
    GROUP BY category
""").pl()
print(plan)
```

---

## 8. Common Patterns & Anti-Patterns

### ✅ Recommended Patterns
```python
# Lazy evaluation for ETL
def etl_lazy():
    return (
        pl.scan_parquet("data.parquet")
        .filter(pl.col("value") > 0)
        .group_by("category")
        .agg(pl.col("value").sum())
        .collect()
    )

# Type-specific casting
def enforce_types(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col("id").cast(pl.Int64),
        pl.col("category").cast(pl.Categorical),
        pl.col("date").str.to_date(),
    ])

# DuckDB for complex SQL
def complex_sql():
    return duckdb.sql("""
        SELECT DATE_TRUNC('day', timestamp) as day,
               category,
               SUM(value) as total
        FROM read_parquet('data.parquet')
        WHERE status = 'active'
        GROUP BY 1, 2
        HAVING SUM(value) > 1000
        ORDER BY 3 DESC
    """).df()

# Incremental processing
def incremental_load(last_checkpoint: datetime) -> pl.DataFrame:
    return duckdb.sql("""
        SELECT * FROM read_parquet('data.parquet')
        WHERE updated_at > ?
    """, [last_checkpoint]).pl()
```

### ❌ Anti-Patterns to Avoid
```python
# pandas for large files → OOM risk
def bad_pandas():
    import pandas as pd
    df = pd.read_csv("large_file.csv")  # Loads everything into memory
    return df.groupby("category")["value"].sum()

# Eager chaining → multiple passes
def bad_eager():
    df = pl.read_parquet("data.parquet")  # Full load
    df = df.filter(...)  # Second pass
    df = df.group_by(...)  # Third pass
    return df

# Row-by-row processing → very slow
def bad_iterrows():
    results = []
    for row in df.iter_rows():  # Python loop, no vectorization
        results.append(process_row(row))
    return results

# No error handling → crashes on missing files
def no_error_handling():
    return pl.read_csv("missing.csv")  # Raises FileNotFoundError
```

---

## 9. Templates

A production-ready, fully documented ETL pipeline template is available in `templates/complete_etl_pipeline.py`. It includes:

- Lazy evaluation for memory efficiency
- Context manager for resource cleanup
- Incremental loading support
- Structured logging with `exc_info=True`
- Custom exception hierarchy
- Configuration via JSON
- Watermark tracking
- Type hints and docstrings

**See file:** `templates/complete_etl_pipeline.py`

---

## 10. Complete Example: Multi-Source ETL

Here's a consolidated example demonstrating best practices:

```python
"""
Production ETL Pipeline: Extract from multiple sources, transform, load to DuckDB.
"""
import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path
import logging
from typing import List
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    raw_path: Path
    processed_path: Path
    duckdb_path: Path
    start_date: datetime

class ProductionETL:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.config.raw_path.mkdir(parents=True, exist_ok=True)
        self.config.processed_path.mkdir(parents=True, exist_ok=True)

    def extract_sources(self, sources: List[Path]) -> pl.LazyFrame:
        """Extract and union multiple Parquet sources."""
        lazy_frames = [pl.scan_parquet(str(src)) for src in sources]
        return pl.concat(lazy_frames)

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply business logic transformations."""
        return (
            df
            .filter(pl.col("timestamp") >= self.config.start_date)
            .filter(pl.col("value").is_not_null())
            .with_columns([
                pl.col("category").str.to_lowercase(),
                pl.col("value").round(2)
            ])
            .group_by(["category", pl.col("timestamp").dt.date()])
            .agg([
                pl.col("value").sum().alias("total_value"),
                pl.col("id").count().alias("transaction_count")
            ])
        )

    def load(self, df: pl.DataFrame):
        """Load to DuckDB with proper connection management."""
        output_file = (
            self.config.processed_path /
            f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        df.write_parquet(output_file)

        with duckdb.connect(self.config.duckdb_path) as con:
            con.sql("""
                CREATE TABLE IF NOT EXISTS daily_summary AS
                SELECT * FROM df
            """)
            con.sql(f"""
                INSERT INTO daily_summary
                SELECT * FROM read_parquet('{output_file}')
            """)

        logger.info(f"Loaded {len(df)} rows to {output_file}")

    def run(self, sources: List[Path]) -> None:
        """Execute full pipeline."""
        logger.info(f"Starting ETL with {len(sources)} sources")

        try:
            # Extract
            raw = self.extract_sources(sources)
            logger.info("Extraction initialized (lazy)")

            # Transform
            transformed = self.transform(raw)

            # Collect and load
            result = transformed.collect()
            self.load(result)

            logger.info("ETL completed successfully")

        except Exception as e:
            logger.error(f"ETL failed: {e}", exc_info=True)
            raise

# Example usage
if __name__ == "__main__":
    config = PipelineConfig(
        raw_path=Path("data/raw"),
        processed_path=Path("data/processed"),
        duckdb_path=Path("analytics.db"),
        start_date=datetime(2024, 1, 1)
    )

    etl = ProductionETL(config)
    sources = list(config.raw_path.glob("*.parquet"))
    etl.run(sources)
```

---

## 11. Testing & Validation

While `@data-engineering-quality` covers Great Expectations and Pandera, here's a lightweight pattern for unit testing Polars transformations:

```python
import pytest
from datetime import datetime

def test_transform_filters_by_date():
    """Test that transformation filters old data."""
    # Arrange
    input_data = pl.DataFrame({
        "timestamp": [
            datetime(2023, 12, 1),
            datetime(2024, 1, 1),
            datetime(2024, 2, 1)
        ],
        "value": [100, 200, 300]
    })
    pipeline = ProductionETL(PipelineConfig(
        start_date=datetime(2024, 1, 1),
        raw_path=Path("."),
        processed_path=Path("."),
        duckdb_path=Path(":memory:")
    ))

    # Act
    lazy_input = input_data.lazy()
    result = pipeline.transform(lazy_input).collect()

    # Assert
    assert len(result) == 2  # Only 2024+ dates
    assert result["total_value"].sum() == 500

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

---

## References

- [Polars Documentation](https://pola.rs/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Tenacity (retrying)](https://tenacity.readthedocs.io/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
