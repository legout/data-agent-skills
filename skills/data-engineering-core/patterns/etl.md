# ETL Pipeline Patterns

Common structured patterns for Extract, Transform, Load pipelines using Polars, DuckDB, and PyArrow.

## Basic ETL Skeleton

```python
import polars as pl
import duckdb
from pathlib import Path
from typing import Protocol
import logging

logger = logging.getLogger(__name__)

class ETLPipeline:
    """Reusable ETL framework."""

    def __init__(self, config: dict):
        self.config = config
        self.raw_path = Path(config["raw_path"])
        self.processed_path = Path(config["processed_path"])

    def extract(self, source: str) -> pl.LazyFrame:
        """Read source data as LazyFrame."""
        if source.endswith(".csv"):
            return pl.scan_csv(source)
        elif source.endswith(".parquet"):
            return pl.scan_parquet(source)
        else:
            raise ValueError(f"Unsupported format: {source}")

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply transformations (lazy)."""
        return (
            df
            .filter(pl.col("value").is_not_null())
            .with_columns([
                pl.col("date").str.to_date(),
                pl.col("amount").cast(pl.Float64),
                pl.col("category").str.to_lowercase()
            ])
            .group_by(["category", "date"])
            .agg([
                pl.col("amount").sum().alias("total_amount"),
                pl.col("id").count().alias("count")
            ])
        )

    def load(self, df: pl.DataFrame) -> dict:
        """Materialize and store results."""
        output_path = self.processed_path / f"output_{datetime.now():%Y%m%d_%H%M%S}.parquet"
        df.write_parquet(output_path)

        with duckdb.connect(self.config.get("duckdb_path", "analytics.db")) as con:
            con.sql("CREATE OR REPLACE TABLE daily_summary AS SELECT * FROM df")

        return {"output": str(output_path), "rows": len(df)}

    def run(self, source_path: str) -> dict:
        """Execute full pipeline."""
        logger.info(f"Starting ETL: {source_path}")

        try:
            raw = self.extract(source_path)
            transformed = self.transform(raw)
            result_df = transformed.collect()
            return self.load(result_df)
        except Exception as e:
            logger.error(f"ETL failed: {e}", exc_info=True)
            raise

# Usage
pipeline = ETLPipeline({
    "raw_path": "data/raw",
    "processed_path": "data/processed",
    "duckdb_path": "analytics.db"
})
pipeline.run("data/input.parquet")
```

## Executor Pattern

Separate pipeline definition from execution:

```python
class PipelineExecutor:
    """Runs pipelines with different backends."""

    def __init__(self, executor: str = "local"):
        self.executor = executor

    def execute(self, pipeline: ETLPipeline, **kwargs):
        if self.executor == "local":
            return pipeline.run(**kwargs)
        elif self.executor == "dask":
            return self._execute_dask(pipeline, **kwargs)
        elif self.executor == "prefect":
            return self._deploy_prefect(pipeline, **kwargs)
        # Add other backends

executor = PipelineExecutor(executor="local")
result = executor.execute(pipeline, source_path="data.parquet")
```

---

## Best Practices

1. ✅ **Lazy evaluation**: Use `scan_*` and chain operations before `collect()`
2. ✅ **Context managers**: Manage DuckDB connections with `with` statement
3. ✅ **Type hints**: Document function signatures for clarity
4. ✅ **Structured logging**: Include contextual information (source, row counts)
5. ✅ **Error handling**: Catch exceptions at pipeline boundary, log with `exc_info=True`
6. ❌ **Don't** Eagerly collect intermediate results - defeats lazy evaluation
7. ❌ **Don't** Use global state - keep pipeline self-contained
8. ❌ **Don't** Skip validation - verify schemas before/after transformations

---

## References

- `@data-engineering-core` - Polars, DuckDB API details
- `@data-engineering-orchestration` - Prefect/Dagster orchestration
- `@data-engineering-core/patterns/incremental.md` - Incremental loading
- `../templates/complete_etl_pipeline.py` - Production template
