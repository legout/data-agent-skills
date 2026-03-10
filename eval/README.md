# Skill Evaluation Manifests

This directory contains evaluation manifests for the 14 target skills in the refactored architecture. These manifests define how each skill is evaluated for quality and triggering accuracy.

## Directory Structure

```
eval/
├── README.md                       # This file
├── <skill-name>.json               # Task evaluation manifest for each skill
└── trigger-eval/
    └── <skill-name>.json           # Trigger evaluation manifest for each skill
```

## The 14 Target Skills

Per SKILL_REFACTORING_PLAN.md Section 5.2, this repository targets 14 skills:

### Data Engineering Skills (9)
| Skill | Task Eval | Trigger Eval |
|-------|-----------|--------------|
| `building-data-pipelines` | ✅ | ✅ |
| `accessing-cloud-storage` | ✅ | ✅ |
| `designing-data-storage` | ✅ | ✅ |
| `managing-data-catalogs` | ✅ | ✅ |
| `orchestrating-data-pipelines` | ✅ | ✅ |
| `assuring-data-pipelines` | ✅ | ✅ |
| `building-streaming-pipelines` | ✅ | ✅ |
| `engineering-ai-pipelines` | ✅ | ✅ |
| `using-flowerpower` | ✅ | ✅ |

### Data Science Skills (5)
| Skill | Task Eval | Trigger Eval |
|-------|-----------|--------------|
| `analyzing-data` | ✅ | ✅ |
| `engineering-ml-features` | ✅ | ✅ |
| `evaluating-ml-models` | ✅ | ✅ |
| `working-in-notebooks` | ✅ | ✅ |
| `building-data-apps` | ✅ | ✅ |

## Manifest Format

### Task Evaluation Manifest (`<skill-name>.json`)

Located at `eval/<skill-name>.json`, this manifest contains 3-5 task evaluations that test the skill's ability to produce correct, complete, and useful outputs.

**Structure:**

```json
{
  "skill_name": "string",
  "task_evaluations": [
    {
      "id": "string",
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

**Fields:**

- `skill_name`: The name of the skill being evaluated
- `task_evaluations`: Array of task evaluation definitions (3-5 items)
  - `id`: Unique identifier for this evaluation (e.g., "eval-001")
  - `name`: Short descriptive name
  - `description`: What this evaluation tests
  - `prompt`: The user prompt to test with
  - `expected_behavior`: What the skill should do/produce
  - `success_criteria`: List of specific criteria for a passing evaluation (3-5 items)
  - `tags`: Categorization tags (e.g., ["workflow", "error-handling", "setup"])

### Trigger Evaluation Manifest (`trigger-eval/<skill-name>.json`)

Located at `eval/trigger-eval/<skill-name>.json`, this manifest contains 10-20 trigger evaluations that test whether the skill triggers appropriately.

**Structure:**

```json
{
  "skill_name": "string",
  "trigger_evaluations": [
    {
      "id": "string",
      "prompt": "string",
      "expected_trigger": boolean,
      "rationale": "string",
      "category": "positive|negative|near-miss"
    }
  ]
}
```

**Fields:**

- `skill_name`: The name of the skill being evaluated
- `trigger_evaluations`: Array of trigger evaluation definitions (10-20 items)
  - `id`: Unique identifier (e.g., "trig-001")
  - `prompt`: The user query/prompt
  - `expected_trigger`: Whether this skill SHOULD trigger for this prompt
  - `rationale`: Why this prompt should or should not trigger the skill
  - `category`: Classification of the test case
    - `positive`: Clear trigger case - skill should definitely trigger
    - `negative`: Clear non-trigger - skill should not trigger
    - `near-miss`: Ambiguous or adjacent topic - tests boundary clarity

**Category Distribution Guidelines:**

- **Positive (6-8)**: Clear cases where the skill should trigger
- **Negative (4-6)**: Cases that belong to other skills
- **Near-miss (3-5)**: Boundary cases that test skill separation

## Adding Evaluations for New Skills

When adding a new skill to the repository:

1. **Create task evaluation file:**
   - File: `eval/<skill-name>.json`
   - Include 3-5 task evaluations covering primary use cases
   - Use sequential IDs starting from "eval-001"

2. **Create trigger evaluation file:**
   - File: `eval/trigger-eval/<skill-name>.json`
   - Include 10-20 trigger evaluations
   - Use sequential IDs starting from "trig-001"
   - Include mix of positive, negative, and near-miss cases

3. **Update the skill table** in this README

4. **Validate your JSON:**
   ```bash
   python -m json.tool eval/<skill-name>.json > /dev/null && echo "Valid JSON"
   python -m json.tool eval/trigger-eval/<skill-name>.json > /dev/null && echo "Valid JSON"
   ```

## Evaluation Methodology

### Task Evaluations

Task evaluations assess output quality:

1. **Baseline**: Run prompt without any skill
2. **With Skill**: Run prompt with the skill enabled
3. **Compare**: Evaluate against success criteria

Metrics: correctness, completeness, brevity, usefulness

### Trigger Evaluations

Trigger evaluations assess routing accuracy:

1. Run prompt through skill router
2. Check if skill triggers (yes/no)
3. Compare against expected_trigger

Metrics: precision, recall, false positive rate, boundary clarity

## Success Criteria Summary

A skill is considered fully evaluated when:

- [ ] 3-5 task evaluations defined
- [ ] 10-20 trigger evaluations defined (mix of positive, negative, near-miss)
- [ ] All manifests are valid JSON
- [ ] All IDs are unique within each manifest
- [ ] All required fields are populated
- [ ] Evaluations cover primary use cases
- [ ] Evaluations test boundary conditions

## Maintenance

- Update evaluations when skill scope changes
- Add evaluations when new features are added
- Review and refresh evaluations quarterly
- Remove obsolete evaluations when functionality is deprecated

## References

- See `SKILL_REFACTORING_PLAN.md` Section 10 for the evaluation strategy
- See individual skill manifests for specific evaluation definitions
