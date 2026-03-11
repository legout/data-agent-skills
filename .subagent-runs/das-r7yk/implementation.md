# Implementation: das-r7yk - GitHub Actions CI Workflow

## Summary
Created a GitHub Actions workflow that integrates strict lint and eval-presence checks into CI.

## Files Created
- `.github/workflows/ci.yml` - Complete CI workflow configuration

## Workflow Details

### Triggers
- `push` to `main` branch
- `pull_request` to `main` branch

### Jobs

#### 1. lint (Skill Lint - Strict)
- **Runs on:** ubuntu-latest
- **Purpose:** Validates all SKILL.md files and markdown references
- **Steps:**
  1. Checkout repository
  2. Set up Python 3.12
  3. Install pyyaml dependency
  4. Run `python3 tools/skill_lint.py --strict`
- **Failure output:** Actionable error messages with common fixes listed

#### 2. eval-presence-check (Eval Manifest Presence)
- **Runs on:** ubuntu-latest  
- **Depends on:** lint job passing
- **Purpose:** Verifies all 14 target skill eval manifests exist
- **Validates:**
  - `eval/<skill>.json` - Standard eval manifests
  - `eval/trigger-eval/<skill>.json` - Trigger eval manifests
- **Skills verified (14 total):**
  1. accessing-cloud-storage
  2. analyzing-data
  3. assuring-data-pipelines
  4. building-data-apps
  5. building-data-pipelines
  6. building-streaming-pipelines
  7. designing-data-storage
  8. engineering-ai-pipelines
  9. engineering-ml-features
  10. evaluating-ml-models
  11. managing-data-catalogs
  12. orchestrating-data-pipelines
  13. using-flowerpower
  14. working-in-notebooks
- **Failure output:** Lists all missing files with specific paths and actionable next steps

## Actionable Failure Output Examples

### Lint Failure
```
::error::Skill lint failed. Please fix the issues above.

Common fixes:
  - Ensure all SKILL.md files have valid frontmatter (name, description)
  - Fix broken markdown references in documentation
  - Run 'python3 tools/skill_lint.py' locally to see warnings
```

### Eval Manifest Missing
```
::error::Missing 2 eval manifest(s)

Failed! The following eval manifests are missing:
  - eval/new-skill.json
  - eval/trigger-eval/new-skill.json

Action required:
  1. Create the missing eval manifest JSON files in the appropriate directory
  2. See eval/README.md for the eval manifest format specification
  3. Ensure both eval/<skill>.json AND eval/trigger-eval/<skill>.json exist
```

## Dependencies Satisfied
- ✅ `tools/skill_lint.py --strict` (from das-8erm, das-6zd4)
- ✅ 14 target-skill eval manifests in `eval/` and `eval/trigger-eval/` (from das-lih7)
