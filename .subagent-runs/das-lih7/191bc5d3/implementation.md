# Implementation: das-lih7 - Remove Legacy Eval Manifests

## Summary
Removed legacy evaluation manifests from `eval/` and `eval/trigger-eval/` directories, keeping only the 14 target skills defined in SKILL_REFACTORING_PLAN.md Section 5.2.

## Target Skills (14)
Per the skill refactoring plan, the following skills are retained:

1. `building-data-pipelines`
2. `accessing-cloud-storage`
3. `designing-data-storage`
4. `managing-data-catalogs`
5. `orchestrating-data-pipelines`
6. `assuring-data-pipelines`
7. `building-streaming-pipelines`
8. `engineering-ai-pipelines`
9. `using-flowerpower`
10. `analyzing-data`
11. `engineering-ml-features`
12. `evaluating-ml-models`
13. `working-in-notebooks`
14. `building-data-apps`

## Deleted Legacy Manifests (29 per directory)

The following manifests were removed as they represent deprecated skills being consolidated into the 14 target skills:

### Data Engineering (15 manifests)
- `data-engineering` - Converted to non-triggerable documentation
- `data-engineering-core` - Folded into `building-data-pipelines`
- `data-engineering-best-practices` - Folded into `building-data-pipelines`
- `data-engineering-catalogs` - Folded into `managing-data-catalogs`
- `data-engineering-orchestration` - Folded into `orchestrating-data-pipelines`
- `data-engineering-quality` - Folded into `assuring-data-pipelines`
- `data-engineering-observability` - Folded into `assuring-data-pipelines`
- `data-engineering-streaming` - Folded into `building-streaming-pipelines`
- `data-engineering-ai-ml` - Folded into `engineering-ai-pipelines`
- `data-engineering-storage-authentication` - Folded into `accessing-cloud-storage`
- `data-engineering-storage-formats` - Folded into `designing-data-storage`
- `data-engineering-storage-lakehouse` - Folded into `designing-data-storage`
- `data-engineering-storage-remote-access` - Folded into `accessing-cloud-storage`
- `flowerpower` - Renamed to `using-flowerpower`

### Storage Remote Access Subskills (11 manifests)
- `data-engineering-storage-remote-access-libraries-fsspec`
- `data-engineering-storage-remote-access-libraries-pyarrow-fs`
- `data-engineering-storage-remote-access-libraries-obstore`
- `data-engineering-storage-remote-access-integrations-polars`
- `data-engineering-storage-remote-access-integrations-duckdb`
- `data-engineering-storage-remote-access-integrations-pandas`
- `data-engineering-storage-remote-access-integrations-pyarrow`
- `data-engineering-storage-remote-access-integrations-delta-lake`
- `data-engineering-storage-remote-access-integrations-iceberg`

### Data Science (6 manifests)
- `data-science-eda` - Folded into `analyzing-data`
- `data-science-visualization` - Folded into `analyzing-data`
- `data-science-feature-engineering` - Folded into `engineering-ml-features`
- `data-science-model-evaluation` - Folded into `evaluating-ml-models`
- `data-science-notebooks` - Folded into `working-in-notebooks`
- `data-science-interactive-apps` - Folded into `building-data-apps`

## Validation

### Final Counts
- `eval/`: 14 manifests (✓ matches target)
- `eval/trigger-eval/`: 14 manifests (✓ matches target)

### Remaining Manifests (both directories)
```
accessing-cloud-storage
analyzing-data
assuring-data-pipelines
building-data-apps
building-data-pipelines
building-streaming-pipelines
designing-data-storage
engineering-ai-pipelines
engineering-ml-features
evaluating-ml-models
managing-data-catalogs
orchestrating-data-pipelines
using-flowerpower
working-in-notebooks
```

## Migration Mapping

| Deleted Skill | Target Skill(s) |
|--------------|-----------------|
| data-engineering | (documentation only) |
| data-engineering-core | building-data-pipelines |
| data-engineering-best-practices | building-data-pipelines |
| data-engineering-storage-authentication | accessing-cloud-storage |
| data-engineering-storage-remote-access* | accessing-cloud-storage |
| data-engineering-storage-formats | designing-data-storage |
| data-engineering-storage-lakehouse | designing-data-storage |
| data-engineering-catalogs | managing-data-catalogs |
| data-engineering-orchestration | orchestrating-data-pipelines |
| data-engineering-quality | assuring-data-pipelines |
| data-engineering-observability | assuring-data-pipelines |
| data-engineering-streaming | building-streaming-pipelines |
| data-engineering-ai-ml | engineering-ai-pipelines |
| flowerpower | using-flowerpower |
| data-science-eda | analyzing-data |
| data-science-visualization | analyzing-data |
| data-science-feature-engineering | engineering-ml-features |
| data-science-model-evaluation | evaluating-ml-models |
| data-science-notebooks | working-in-notebooks |
| data-science-interactive-apps | building-data-apps |

## References
- SKILL_REFACTORING_PLAN.md Section 5.2 (Target skill set)
- SKILL_REFACTORING_PLAN.md Section 6 (Migration map)
