---
name: data-engineering
description: "Comprehensive data engineering skill suite covering core libraries (Polars, DuckDB, PyArrow), lakehouse formats, cloud storage, orchestration, streaming, quality, observability, and AI/ML pipelines."
---

# Data Engineering Hub

Welcome to the comprehensive data engineering skill suite. This hub organizes all data engineering knowledge into logical, non-overlapping domains.

## Skill Map

| Domain | Skills | When to Use |
|--------|--------|-------------|
| **Core** | `@data-engineering-core` | Polars, DuckDB, PyArrow fundamentals; ETL patterns; error handling; performance optimization |
| **Storage** | `@designing-data-storage` | File formats (Parquet, Arrow, Lance, Zarr, Avro, ORC) and lakehouse table formats (Delta Lake, Iceberg, Hudi) |
|  | `@accessing-cloud-storage` | fsspec, pyarrow.fs, obstore; cloud access patterns |
|  | `@data-engineering-storage-authentication` | AWS, GCP, Azure auth - IAM roles, managed identity, secrets management |
| **Orchestration** | `@data-engineering-orchestration` | Prefect, Dagster, dbt, workflow scheduling |
| **Streaming** | `@data-engineering-streaming` | Kafka, MQTT, NATS JetStream for real-time data |
| **Quality & Observability** | `@assuring-data-pipelines` | Great Expectations, Pandera, OpenTelemetry, Prometheus for validation and monitoring |
| **AI/ML** | `@data-engineering-ai-ml` | Embeddings, vector databases, RAG pipelines |
| **Best Practices** | `@data-engineering-best-practices` | Medallion architecture, partitioning, file sizing, incremental loads, schema evolution, testing |
| **Catalogs** | `@data-engineering-catalogs` | Data catalog systems: Iceberg catalogs, DuckDB multi-source, Amundsen/DataHub/OpenMetadata |

## Quick Reference: Core Stack

| Task | Recommended Tool |
|------|------------------|
| DataFrame operations | **Polars** (10-50x faster than pandas) |
| SQL analytics | **DuckDB** (embedded OLAP, zero-copy Arrow integration) |
| Data interchange | **PyArrow** (Arrow format, zero-copy transfers) |
| Cloud storage access | **fsspec** (universal), **pyarrow.fs** (Arrow-native), **obstore** (high-performance) |
| Lakehouse format | **Delta Lake** (Spark ecosystem), **Iceberg** (engine-agnostic), **Hudi** (streaming CDC) |
| Orchestration | **Prefect** (Pythonic flows), **Dagster** (asset-based), **dbt** (SQL transformations) |
| Validation | **Pandera** (lightweight), **Great Expectations** (enterprise) |

## Getting Started

### New to Data Engineering?
Start with `@data-engineering-core` to learn the foundational libraries and patterns.

### Working with Cloud Storage?
Go to `@data-engineering-storage-remote-access` for fsspec, pyarrow.fs, and obstore.

### Building Data Lakes?
Explore `@designing-data-storage` for file formats and ACID table formats.

### Choosing a Data Catalog?
Check `@data-engineering-catalogs` for Iceberg catalogs, DuckDB multi-source patterns, and tool comparisons.

### Production-Grade Pipelines?
Read `@data-engineering-best-practices` for medallion architecture, partitioning, schema evolution, and testing strategies.

### Orchestrating Pipelines?
Check `@data-engineering-orchestration` for Prefect, Dagster, and dbt.

### Production Monitoring?
See `@assuring-data-pipelines` for validation, tracing, and monitoring.

### AI/ML Data Pipelines?
Visit `@data-engineering-ai-ml` for embeddings, vector databases, and RAG.

## Principles

1. **Lazy evaluation**: Use Polars lazy frames and DuckDB query planning for performance
2. **Zero-copy data transfer**: Leverage Arrow format for memory efficiency
3. **Pushdown optimization**: Filter at storage layer to minimize data transfer
4. **Type safety**: Use explicit schemas and type hints
5. **Resilience**: Implement retries, circuit breakers, and proper error handling
6. **Observability**: Instrument pipelines with traces and metrics
7. **Security**: Never hardcode credentials; use IAM roles and environment variables

## Migration from Legacy Skills

This restructured suite replaces the previous split organization (`data-engineering-*` and `remote-filesystems-*`). All content has been consolidated to eliminate duplication and clarify ownership.

**Legacy skill replacements:**
- `data-engineering-core` → `@data-engineering-core` (plus specific integrations)
- `data-engineering-lakehouse` / `data-engineering-storage-lakehouse` → `@designing-data-storage`
- `data-engineering-storage-formats` → `@designing-data-storage`
- `data-engineering-orchestration` → `@data-engineering-orchestration`
- `data-engineering-streaming` → `@data-engineering-streaming`
- `data-engineering-quality` → `@assuring-data-pipelines`
- `data-engineering-observability` → `@assuring-data-pipelines`
- `data-engineering-llm-pipelines` → `@data-engineering-ai-ml`
- `remote-filesystems-*` → `@accessing-cloud-storage` and integrations

All legacy skills remain functional but are deprecated. New content should be added to the new structure only.
