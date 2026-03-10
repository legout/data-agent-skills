"""
Complete ETL Pipeline Template

A production-ready template for building ETL pipelines with Polars, DuckDB, and PyArrow.
Features:
- Lazy evaluation for memory efficiency
- Context manager for resource cleanup
- Incremental loading support
- Structured logging
- Error handling

Usage:
    with DataPipeline("config.json") as pipeline:
        result = pipeline.run("data/input.parquet")
        print(pipeline.get_summary(7))
"""
import polars as pl
import duckdb
import json
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Production-grade ETL pipeline template.
    
    Args:
        config_path: Path to JSON configuration file
        
    Example config.json:
        {
            "duckdb_path": "analytics.db",
            "raw_path": "data/raw",
            "processed_path": "data/processed"
        }
    """
    
    def __init__(self, config_path: str = "pipeline_config.json"):
        with open(config_path) as f:
            self.config = json.load(f)
        
        self.duckdb = duckdb.connect(self.config["duckdb_path"])
        self._init_duckdb_tables()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection cleanup."""
        self.duckdb.close()
    
    def _init_duckdb_tables(self):
        """Initialize DuckDB schema."""
        self.duckdb.sql("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id VARCHAR,
                event_type VARCHAR,
                value DOUBLE,
                timestamp TIMESTAMP,
                metadata VARCHAR
            )
        """)
        
        self.duckdb.sql("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                date DATE,
                event_type VARCHAR,
                total_value DOUBLE,
                event_count BIGINT,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """)
    
    def extract(self, source: str) -> pl.LazyFrame:
        """
        Extract data from source.
        
        Supports:
        - Local Parquet files
        - S3 paths (requires credentials)
        - Local CSV files
        
        Args:
            source: Path to data file (parquet or csv)
            
        Returns:
            LazyFrame for efficient processing
        """
        if source.endswith(".csv"):
            return pl.scan_csv(source)
        elif source.startswith("s3://"):
            return pl.scan_parquet(source)
        else:
            return pl.scan_parquet(source)
    
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply transformations to raw data.
        
        Override this method to customize transformations.
        
        Args:
            df: LazyFrame from extract stage
            
        Returns:
            Transformed LazyFrame
        """
        return (
            df
            .with_columns([
                pl.col("timestamp").str.to_datetime(),
                pl.col("value").fill_null(0)
            ])
            .filter(pl.col("value") > 0)
            .filter(pl.col("timestamp") >= datetime(2024, 1, 1))
        )
    
    def load(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Load transformed data to destination.
        
        Args:
            df: Materialized DataFrame to load
            
        Returns:
            Dict with load statistics
        """
        # Insert raw events
        self.duckdb.sql("INSERT INTO raw_events SELECT * FROM df")
        
        # Create daily summary
        summary = (
            df.group_by(pl.col("timestamp").dt.date().alias("date"), "event_type")
            .agg([
                pl.col("value").sum().alias("total_value"),
                pl.col("id").count().alias("event_count")
            ])
            .sort("date")
        )
        
        self.duckdb.sql("INSERT INTO daily_summary SELECT * FROM summary")
        
        return {
            "events_loaded": len(df),
            "summary_rows": len(summary),
            "timestamp": datetime.now().isoformat()
        }
    
    def run(self, source_path: str) -> Dict[str, Any]:
        """
        Execute full ETL pipeline.
        
        Args:
            source_path: Path to source data file
            
        Returns:
            Dict with pipeline execution results
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info(f"Starting pipeline from {source_path}")
        
        try:
            # Extract
            raw = self.extract(source_path)
            logger.info(f"Source has ~{raw.collect().height} rows")
            
            # Transform
            transformed = self.transform(raw)
            
            # Collect and load
            result_df = transformed.collect()
            load_result = self.load(result_df)
            
            logger.info(f"Pipeline completed: {load_result}")
            return load_result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def get_summary(self, days: int = 7) -> pl.DataFrame:
        """
        Query recent summary statistics.
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame with summary data
        """
        result = self.duckdb.sql(f"""
            SELECT date, event_type, total_value, event_count
            FROM daily_summary
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY date DESC
        """)
        return result.pl()
    
    def get_watermark(self, table_name: str) -> Optional[str]:
        """
        Get last processed timestamp for incremental loading.
        
        Args:
            table_name: Name of table to check
            
        Returns:
            ISO timestamp string or None
        """
        result = self.duckdb.sql(f"""
            SELECT MAX(timestamp) FROM {table_name}
        """).fetchone()
        return result[0] if result and result[0] else None


class PipelineError(Exception):
    """Custom exception for pipeline errors."""
    pass


# Example usage
if __name__ == "__main__":
    # Using context manager for automatic cleanup
    with DataPipeline("config.json") as pipeline:
        # Run single batch
        result = pipeline.run("data/events_2024.parquet")
        print(f"Loaded {result['events_loaded']} events")
        
        # Query results
        summary = pipeline.get_summary(30)
        print("\nLast 30 days summary:")
        print(summary)
        
        # Check watermark for incremental loads
        watermark = pipeline.get_watermark("raw_events")
        print(f"\nLast processed: {watermark}")
