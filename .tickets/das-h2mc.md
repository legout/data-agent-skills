---
id: das-h2mc
status: closed
deps: [das-trf5]
links: [das-trf5]
created: 2026-03-10T15:55:11Z
type: task
priority: 3
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, ai-ml]
---
# Create engineering-ai-pipelines with embeddings, vector stores, RAG, and monitoring workflows

Rewrite the AI/ML data-pipeline skill around production workflow guidance instead of topic sprawl.

## Acceptance Criteria

- new engineering-ai-pipelines skill exists with direct references for embeddings, vector stores, RAG, and monitoring
- storage-related cross-links are explicit and non-duplicative
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T16:00:11Z**

Implementation complete:

- Created engineering-ai-pipelines skill with SKILL.md (workflow guidance) + 4 reference files
- Topics covered: embedding generation (OpenAI/local), vector stores (LanceDB/pgvector/DuckDB), RAG pipelines, LLM monitoring
- Fixed 3 Major issues: SQL param mismatch, division-by-zero guard, opt-in full prompt logging
- Fixed 2 Minor issues: missing imports in code snippets
- Post-fix gate: Clear pass

Key files:
- skills/engineering-ai-pipelines/SKILL.md
- skills/engineering-ai-pipelines/references/*.md (4)
- evals/engineering-ai-pipelines.json (15 test cases)

Commit: 89f1b21
