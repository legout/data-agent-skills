# Fixes Applied

## Issues from Review

| Severity | Issue | Status |
|----------|-------|--------|
| Major | Lint failure path does not reliably show actionable error block | ✅ Fixed |

## Fixes Applied

### Fixed [Major]: Lint failure exit code capture in `.github/workflows/ci.yml`

**Problem:** Under GitHub Actions' default `bash -e` mode, when `python3 tools/skill_lint.py --strict` fails, the shell exits immediately before `LINT_EXIT=$?` is executed, preventing the custom `::error::` guidance from being printed.

**Fix:** Changed from:
```bash
python3 tools/skill_lint.py --strict
LINT_EXIT=$?
```

To:
```bash
LINT_EXIT=0
python3 tools/skill_lint.py --strict || LINT_EXIT=$?
```

This ensures:
1. `LINT_EXIT` is initialized to 0 (success case)
2. If lint fails, the `||` clause captures the exit code without triggering `set -e`
3. The subsequent conditional block always runs and can emit the actionable error message

## Status

All critical and major issues resolved. 0 minor/suggestions skipped.
