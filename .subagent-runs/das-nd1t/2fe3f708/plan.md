# Implementation Plan

## Goal
Create the `evaluating-ml-models` skill by refactoring model evaluation content from `analyzing-data/references/` into a self-contained skill, following the pattern from `engineering-ml-features`.

## Tasks

1. **Create skill directory structure**
   - Create `skills/evaluating-ml-models/` directory
   - Create `skills/evaluating-ml-models/references/` subdirectory
   - Acceptance: Both directories exist

2. **Copy reference files from analyzing-data**
   - Copy `skills/analyzing-data/references/cross-validation.md` → `skills/evaluating-ml-models/references/cross-validation.md`
   - Copy `skills/analyzing-data/references/metrics-guide.md` → `skills/evaluating-ml-models/references/metrics-guide.md`
   - Copy `skills/analyzing-data/references/hyperparameter-tuning.md` → `skills/evaluating-ml-models/references/hyperparameter-tuning.md`
   - Copy `skills/analyzing-data/references/experiment-tracking.md` → `skills/evaluating-ml-models/references/experiment-tracking.md`
   - Acceptance: All 4 reference files exist in new location with identical content

3. **Create main SKILL.md**
   - File: `skills/evaluating-ml-models/SKILL.md`
   - Changes: Create new file with:
     - YAML frontmatter with `name: evaluating-ml-models`, appropriate description, and `dependsOn` referencing related skills
     - Main sections: When to use, Quick tool selection table, Evaluation workflow, Core implementation rules, Common anti-patterns, Progressive disclosure section pointing to local references/, Related skills
     - Update all relative references to point to `references/` (not `../analyzing-data/references/`)
   - Acceptance: SKILL.md renders correctly, references point to correct local paths

4. **Update analyzing-data references (cleanup)**
   - File: `skills/analyzing-data/SKILL.md`
   - Changes: Remove or update the progressive disclosure section that points to the moved reference files (cross-validation.md, metrics-guide.md, hyperparameter-tuning.md, experiment-tracking.md) - they should now point to `@evaluating-ml-models` skill instead
   - Acceptance: analyzing-data SKILL.md no longer has broken references to moved files

5. **Deprecate/update data-science-model-evaluation**
   - File: `skills/data-science-model-evaluation/SKILL.md`
   - Changes: Add deprecation notice or update to reference the new `@evaluating-ml-models` skill. The progressive disclosure section currently points to `../analyzing-data/references/` which will break.
   - Acceptance: No broken references, users directed to new skill

6. **Create evals file**
   - File: `evals/evaluating-ml-models.json`
   - Changes: Create evaluation suite following `working-in-notebooks.json` pattern:
     - 4-5 task_evals covering CV, metrics, hyperparameter tuning, experiment tracking
     - 15-20 trigger_evals (mix of positive triggers for evaluating-ml-models and negative triggers that should route to other skills like engineering-ml-features or data-science-notebooks)
   - Acceptance: Valid JSON, covers key use cases, follows established pattern

## Files to Modify

### New Files
- `skills/evaluating-ml-models/SKILL.md` - Main skill file
- `skills/evaluating-ml-models/references/cross-validation.md` - Copied from analyzing-data
- `skills/evaluating-ml-models/references/metrics-guide.md` - Copied from analyzing-data
- `skills/evaluating-ml-models/references/hyperparameter-tuning.md` - Copied from analyzing-data
- `skills/evaluating-ml-models/references/experiment-tracking.md` - Copied from analyzing-data
- `evals/evaluating-ml-models.json` - Evaluation suite

### Modified Files
- `skills/analyzing-data/SKILL.md` - Remove/update progressive disclosure for moved references
- `skills/data-science-model-evaluation/SKILL.md` - Add deprecation/update references

## Dependencies

- Task 1 must complete before Task 2
- Task 2 must complete before Task 3 (SKILL.md references the files)
- Task 3 can happen in parallel with Tasks 4-5
- Task 6 (evals) can happen anytime after Task 3

## Risks

1. **Broken references in data-science-model-evaluation**: The existing skill points to `../analyzing-data/references/` - these will become broken. Must update this skill.
2. **Content duplication**: Ensure we don't duplicate content that should stay in data-science-model-evaluation vs move to evaluating-ml-models. The new skill should be the primary source for evaluation patterns.
3. **Trigger overlap**: Need careful trigger_evals to distinguish between `evaluating-ml-models` (evaluation, CV, metrics) and `engineering-ml-features` (feature engineering).
4. **Backward compatibility**: Users may have referenced the old paths - but since skills are loaded fresh each time, this is acceptable.
