---
id: das-0p8u
status: closed
deps: [das-llsd, das-g8hg, das-trf5, das-k0lp, das-n3x8, das-ekec, das-5ewy, das-h2mc, das-09vu]
links: []
created: 2026-03-10T15:55:10Z
closed: 2026-03-11T19:05:00Z
type: epic
priority: 4
assignee: legout
tags: [skill-refactor, data-engineering]
---
# Refactor data-engineering skills into the new workflow-centered set

Rewrite the engineering-side skills into the approved workflow-centered architecture and remove fragmentation.

## Closure Summary

✅ **All 9 dependency tickets closed**
✅ **All 14 new workflow-centered skills verified present**
✅ **No orphaned old skills remain**

### Skills Created (14 total)

**Data Engineering (9 skills):**
1. `building-data-pipelines` - Batch ETL with Polars, DuckDB, PyArrow
2. `accessing-cloud-storage` - Cloud auth and storage access patterns
3. `designing-data-storage` - File formats and lakehouse table formats
4. `managing-data-catalogs` - Data catalog architecture and metadata
5. `orchestrating-data-pipelines` - Prefect, Dagster, dbt workflows
6. `assuring-data-pipelines` - Quality + observability combined
7. `building-streaming-pipelines` - Kafka, MQTT, NATS JetStream
8. `engineering-ai-pipelines` - Embeddings, vectors, RAG, LLM monitoring
9. `flowerpower` - FlowerPower/Hamilton DAG framework

**Data Science (5 skills):**
10. `analyzing-data` - EDA + visualization unified
11. `engineering-ml-features` - ML feature engineering
12. `evaluating-ml-models` - Model validation and tuning
13. `working-in-notebooks` - Jupyter, marimo workflows
14. `building-data-apps` - Streamlit, Panel, Gradio apps

### Refactoring Achievement

- **Reduced skill count**: 29 skills → 14 skills (52% reduction)
- **Eliminated fragmentation**: Consolidated 23 data-engineering-* skills into 9 workflow-centered skills
- **Action-oriented naming**: All skills now use verb-first naming (`building-`, `accessing-`, `evaluating-`)
- **Removed duplication**: ~4,060 duplicate lines eliminated across data-science references
- **Migration documentation**: CHANGELOG.md and docs/migration-map.md provide complete transition guide


## Notes

**2026-03-11T18:10:17Z**

Epic Closure Summary:
- All 9 dependency tickets verified closed (das-llsd, das-g8hg, das-trf5, das-k0lp, das-n3x8, das-ekec, das-5ewy, das-h2mc, das-09vu)
- 14 workflow-centered skills present with SKILL.md (9 DE + 5 DS)
- Zero orphaned data-engineering-* skills remain
- Refactoring metrics: 29→14 skills (-52%), ~4,060 duplicate refs eliminated
- Commit: dacc4d7
- Gate: Clear pass (review + post-fix)
