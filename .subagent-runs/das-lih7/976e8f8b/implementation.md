# Implementation: Eval Structure for 14 Target Skills

## Summary

Created the evaluation directory structure with JSON manifest templates for all 14 target skills per SKILL_REFACTORING_PLAN.md Section 10.

## Files Created

### Task Evaluation Manifests (`eval/`)

| File | Skill | Evaluations |
|------|-------|-------------|
| `building-data-pipelines.json` | Core ETL/pipeline construction | 5 |
| `accessing-cloud-storage.json` | Cloud storage auth & access | 5 |
| `designing-data-storage.json` | File formats & lakehouse | 5 |
| `managing-data-catalogs.json` | Data catalogs & metadata | 5 |
| `orchestrating-data-pipelines.json` | Prefect/Dagster/dbt | 5 |
| `assuring-data-pipelines.json` | Data quality & observability | 5 |
| `building-streaming-pipelines.json` | Kafka/MQTT/NATS | 5 |
| `engineering-ai-pipelines.json` | Embeddings/RAG/LLM ops | 5 |
| `using-flowerpower.json` | FlowerPower/Hamilton | 5 |
| `analyzing-data.json` | EDA & visualization | 5 |
| `engineering-ml-features.json` | Feature engineering | 5 |
| `evaluating-ml-models.json` | Model evaluation & tuning | 5 |
| `working-in-notebooks.json` | Jupyter/marimo workflows | 5 |
| `building-data-apps.json` | Streamlit/Panel/Gradio | 5 |

**Total: 14 task evaluation manifests with 70 task evaluations**

### Trigger Evaluation Manifests (`eval/trigger-eval/`)

| File | Skill | Evaluations |
|------|-------|-------------|
| `building-data-pipelines.json` | Core ETL/pipeline construction | 15 |
| `accessing-cloud-storage.json` | Cloud storage auth & access | 15 |
| `designing-data-storage.json` | File formats & lakehouse | 15 |
| `managing-data-catalogs.json` | Data catalogs & metadata | 15 |
| `orchestrating-data-pipelines.json` | Prefect/Dagster/dbt | 15 |
| `assuring-data-pipelines.json` | Data quality & observability | 15 |
| `building-streaming-pipelines.json` | Kafka/MQTT/NATS | 15 |
| `engineering-ai-pipelines.json` | Embeddings/RAG/LLM ops | 15 |
| `using-flowerpower.json` | FlowerPower/Hamilton | 15 |
| `analyzing-data.json` | EDA & visualization | 15 |
| `engineering-ml-features.json` | Feature engineering | 15 |
| `evaluating-ml-models.json` | Model evaluation & tuning | 15 |
| `working-in-notebooks.json` | Jupyter/marimo workflows | 15 |
| `building-data-apps.json` | Streamlit/Panel/Gradio | 15 |

**Total: 14 trigger evaluation manifests with 210 trigger evaluations**

### Documentation

| File | Purpose |
|------|---------|
| `eval/README.md` | Complete manifest format documentation for contributors |

## Manifest Format Specification

### Task Evaluation Structure
```json
{
  "skill_name": "string",
  "task_evaluations": [
    {
      "id": "eval-001",
      "name": "string",
      "description": "string",
      "prompt": "string",
      "expected_behavior": "string",
      "success_criteria": ["string"],
      "tags": ["string"]
    }
  ]
}
```

### Trigger Evaluation Structure
```json
{
  "skill_name": "string",
  "trigger_evaluations": [
    {
      "id": "trig-001",
      "prompt": "string",
      "expected_trigger": boolean,
      "rationale": "string",
      "category": "positive|negative|near-miss"
    }
  ]
}
```

## Key Design Decisions

1. **14 Target Skills**: Created manifests for the target architecture (not current 29 skills)
2. **Action-Oriented Names**: Using new skill names per Section 5.2 (e.g., `building-data-pipelines` not `data-engineering-core`)
3. **5 Task Evaluations Each**: Each skill has 5 task evaluations covering primary workflows
4. **15 Trigger Evaluations Each**: Mix of positive (6-8), negative (4-6), and near-miss (3-5) cases
5. **Clear Boundaries**: Trigger evaluations explicitly test skill boundaries to prevent overlap

## For Contributors

New skill eval files should be added to:
- Task evaluations: `eval/<skill-name>.json`
- Trigger evaluations: `eval/trigger-eval/<skill-name>.json`

See `eval/README.md` for complete format documentation and guidelines.
