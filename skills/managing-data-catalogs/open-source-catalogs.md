# Open Source Catalog Tools Comparison

Comparison of open-source data discovery and governance tools: Amundsen, DataHub, and OpenMetadata.

**Note:** These tools serve a different purpose than Iceberg/Delta catalogs (Glue, Hive, Tabular). They focus on **metadata discovery, lineage, and governance** rather than table storage metadata.

---

## Amundsen (Lyft)

**Focus:** Data discovery, lightweight

### Architecture
- **Metadata store:** Neo4j (graph) + MySQL (text search)
- **Ingestion:** Python scripts or Airflow operators
- **Deployment:** Easiest (fewer services)

### Features
| Feature | Support |
|---------|---------|
| Search | ⭐⭐⭐⭐⭐ Simple keyword search |
| Lineage | ⭐⭐ Limited (no column-level) |
| Governance | ⭐⭐ Minimal |
| Data Quality | ❌ Not built-in |

### Best For
- Small teams needing basic discovery
- Quick deployment without complex infrastructure

### Current Status
⚠️ **Caution:** Slower development post-2023 acquisition by Workday. Consider for new deployments carefully.

---

## DataHub (LinkedIn)

**Focus:** Scalable, enterprise-grade metadata platform

### Architecture
- **Metadata store:** MySQL (metadata) + Kafka (streaming updates) + Elasticsearch (search)
- **Ingestion:** Python SDK, Airflow operators, or REST API
- **Deployment:** Complex (multiple services: MySQL, Kafka, Elasticsearch, Neo4j)

### Features
| Feature | Support |
|---------|---------|
| Search | ⭐⭐⭐⭐ Advanced faceted search |
| Lineage | ⭐⭐⭐⭐⭐ Full end-to-end (auto-ingested from Spark, Flink, etc.) |
| Governance | ⭐⭐⭐⭐ Strong (PII tagging, access policies) |
| Data Quality | ⭐⭐⭐ Can integrate with Great Expectations |
| Extensibility | ⭐⭐⭐⭐⭐ High (many source connectors) |

### Best For
- Large enterprises (LinkedIn-scale)
- Teams needing comprehensive lineage
- Complex data ecosystems with many sources

### Deployment Complexity
```yaml
# Services required
- DataHub Frontend
- DataHub GMS (Metadata Service)
- MySQL
- Kafka + Zookeeper
- Elasticsearch
- Neo4j (optional, for lineage graph)
```

---

## OpenMetadata

**Focus:** Unified governance & quality

### Architecture
- **Metadata store:** Postgres + Elasticsearch/OpenSearch
- **Ingestion:** Python SDK or built-in UI workflows
- **Deployment:** Moderate (needs Postgres, OpenSearch, Airflow for ingestion)

### Features
| Feature | Support |
|---------|---------|
| Search | ⭐⭐⭐⭐ Good faceted search |
| Lineage | ⭐⭐⭐⭐ Built-in, column-level |
| Governance | ⭐⭐⭐⭐⭐ Excellent (workflows, tasks, approvals) |
| Data Quality | ⭐⭐⭐⭐⭐ Native integration with tests |
| UI | ⭐⭐⭐⭐⭐ Cleanest, most modern interface |

### Best For
- Teams needing strong governance + quality testing
- Organizations wanting workflow-driven data management
- Modern data stack preferences

### Unique Features
- **Data Quality Tests:** Built-in test definitions and scheduling
- **Glossary Workflows:** Approval flows for business glossary
- **Data Insights:** Built-in analytics on metadata

---

## Comparison Summary

| Tool | Discovery | Lineage | Governance | Scale | Ops Complexity | Status |
|------|-----------|---------|------------|-------|----------------|--------|
| **Amundsen** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | Small | Low | ⚠️ Stale |
| **DataHub** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Enterprise | High | ✅ Active |
| **OpenMetadata** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Medium-Large | Moderate | ✅ Active |

---

## Selection Recommendations

| Scenario | Recommended Tool |
|----------|------------------|
| **Start new project** | **OpenMetadata** (governance + quality focus) |
| **LinkedIn-scale** | **DataHub** (massive scale, lineage automation) |
| **Basic discovery only** | Consider OpenMetadata or simple custom solution |
| **Existing Amundsen** | Plan migration to OpenMetadata or DataHub |

---

## Integration with Lakehouse Catalogs

These metadata tools **complement** (not replace) Iceberg/Delta catalogs:

```
┌─────────────────────────────────────────┐
│  OpenMetadata / DataHub                 │
│  (Discovery, Lineage, Governance)       │
└─────────────┬───────────────────────────┘
              │ Ingests metadata from
┌─────────────▼───────────────────────────┐
│  Iceberg/Delta Catalog                  │
│  (Glue, Hive, Tabular)                  │
│  → Table schemas, locations             │
└─────────────┬───────────────────────────┘
              │ Points to
┌─────────────▼───────────────────────────┐
│  Object Storage (S3, GCS)               │
│  → Actual data files                    │
└─────────────────────────────────────────┘
```

### Typical Integration

```python
# DataHub ingestion example
from datahub.ingestion.source.iceberg import IcebergSource

source = IcebergSource.create(
    config_dict={
        "catalog": {
            "type": "glue",
            "region": "us-east-1"
        }
    }
)
# Ingests all table schemas, locations into DataHub
```

---

## When to Use Each

### Choose OpenMetadata when:
- You need built-in data quality testing
- Governance workflows are important
- You want the best UI/UX
- Moderate scale (not LinkedIn-size)

### Choose DataHub when:
- You operate at massive scale
- Comprehensive lineage is critical
- You have many diverse data sources
- You need maximum extensibility

### Avoid Amundsen when:
- Starting new projects (stale development)
- You need governance or quality features
- Long-term maintenance is a concern

---

## References

- [OpenMetadata](https://open-metadata.org/)
- [DataHub](https://datahubproject.io/)
- [Amundsen](https://www.amundsen.io/)
- [Comparison: OpenMetadata vs DataHub vs Amundsen](https://atlan.com/open-source-data-catalog-tools/)
