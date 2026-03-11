## Fixes Applied

- Fixed [Major]: TOC check scope too broad in `tools/skill_lint.py` — Added guard `if "references" not in md_file.parts: return` at the start of `lint_toc_required()` to restrict TOC enforcement to reference documents only (`references/**/*.md`), matching ticket scope "long references without a TOC".

- Fixed [Minor]: Duplicate content total_lines calculation in `tools/skill_lint.py` — Changed `total_lines = len(block.splitlines()) * len(locations)` to `total_lines = len(block.splitlines()) * len(unique_files)` to use unique file count instead of occurrence count, matching "appears in 3+ files with >100 lines" threshold.

## Verification

```
$ python tools/skill_lint.py
...
Summary: 34 error(s), 145 warning(s)
```

- TOC warnings now only appear for `references/` files (e.g., `skills/flowerpower/references/configuration.md`)
- Duplicate content detection still works correctly with 6 unique files flagged
- Stale year detection still working

## Status
All critical and major issues resolved. 0 minor/suggestions skipped.
