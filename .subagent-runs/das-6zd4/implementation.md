# Implementation Summary: das-6zd4

## Changes Made

Modified `tools/skill_lint.py` to add three new lint checks:

### 1. Duplicate Content Detection (`lint_duplicate_content`)
- Extracts content blocks (code fences and bullet lists) from all markdown files
- Flags blocks >5 lines that appear in 3+ files with >100 total identical lines
- Shows file paths with parent directories for clarity

### 2. TOC Required Check (`lint_toc_required`)
- Flags markdown files >100 lines without a Table of Contents
- Looks for `## Table of Contents`, `## TOC`, or `## Contents` headings (case-insensitive)

### 3. Stale Year Detection (`lint_stale_year`)
- Detects `(YYYY)` year markers in h1/h2 headings (pattern `(202[0-9])`)
- Reports warning with line number and heading preview

## Helper Function
- `extract_content_blocks(text)`: Extracts code fences and bullet lists with line numbers

## Integration
All three checks are integrated into the `main()` function:
- `lint_toc_required()` and `lint_stale_year()` run per-file
- `lint_duplicate_content()` runs once with all markdown files for cross-file comparison

## Test Results
Running `python tools/skill_lint.py`:
- Found 2 stale year markers: `(2024)` in data-engineering-catalogs, `(2026)` in data-science-visualization
- Found 2 duplicate content blocks: hyperparameter-tuning.md (6 files, 126 lines), plotly-dash.md (6 files, 132 lines)
- Found 58 files missing Table of Contents (>100 lines)

## Files Changed
- `tools/skill_lint.py` - Added 3 new lint functions + helper + main loop integration
