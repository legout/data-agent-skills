# Implementation Plan

## Goal
Create the `engineering-ai-pipelines` skill by refactoring `data-engineering-ai-ml` to focus on production workflow guidance for embeddings, vector stores, RAG, and monitoring, following the pattern from `designing-data-storage`.

## Tasks

1. **Create SKILL.md with workflow-focused structure**
   - File: `skills/engineering-ai-pipelines/SKILL.md`
   - Changes: Create new skill file following the `designing-data-storage` pattern:
     - Table of Contents with clear navigation
     - "When to Use Which?" decision matrix section comparing embeddings, vector DBs, and RAG approaches
     - Quick comparison tables (like format comparison in designing-data-storage)
     - Code examples section with complete, runnable examples
     - Best practices with ✅/❌ format
     - References to related skills with `@skill` notation
   - Acceptance: SKILL.md has proper YAML frontmatter, follows the designing-data-storage structure pattern, includes cross-link to `@designing-data-storage`

2. **Create reference directory and consolidate detailed guides**
   - File: `skills/engineering-ai-pipelines/references/embeddings.md`
   - Changes: Move/duplicate detailed embedding content from `data-engineering-ai-ml/embeddings.md`
   - Acceptance: File contains OpenAI API, sentence-transformers, batch processing, and best practices

3. **Create vector storage reference guide**
   - File: `skills/engineering-ai-pipelines/references/vector-stores.md`
   - Changes: Consolidate vector database content from `data-engineering-ai-ml/vector-databases.md`
   - Acceptance: Covers LanceDB, pgvector, DuckDB with comparison table and selection criteria

4. **Create RAG pipeline reference guide**
   - File: `skills/engineering-ai-pipelines/references/rag-pipelines.md`
   - Changes: Move RAG content from `data-engineering-ai-ml/rag-pipelines.md`
   - Acceptance: Includes chunking strategies, context assembly, prompt construction, complete pipeline example

5. **Create LLM monitoring reference guide**
   - File: `skills/engineering-ai-pipelines/references/monitoring.md`
   - Changes: Move monitoring content from `data-engineering-ai-ml/monitoring.md`
   - Acceptance: Covers cost tracking, retry patterns, OpenTelemetry integration, quality evaluation

6. **Create evals file with 12-15 test cases**
   - File: `evals/engineering-ai-pipelines.json`
   - Changes: Create eval file following `evals/designing-data-storage.json` structure
   - Acceptance: Test cases cover:
     - Embedding generation (OpenAI, local models)
     - Vector database selection (LanceDB vs pgvector vs DuckDB)
     - RAG pipeline questions
     - LLM monitoring and cost tracking
     - Cross-skill references to `@designing-data-storage` and `@data-engineering-core`

7. **Update parent skill dependencies**
   - File: `skills/data-engineering/SKILL.md` (if it exists and references AI/ML)
   - Changes: Add reference to new `@engineering-ai-pipelines` skill if applicable
   - Acceptance: Cross-links are consistent and bidirectional where appropriate

## Files to Modify
- None (this is a new skill creation)

## New Files
- `skills/engineering-ai-pipelines/SKILL.md` - Main skill file with workflow guidance
- `skills/engineering-ai-pipelines/references/embeddings.md` - Embedding generation patterns
- `skills/engineering-ai-pipelines/references/vector-stores.md` - Vector database comparison
- `skills/engineering-ai-pipelines/references/rag-pipelines.md` - End-to-end RAG workflows
- `skills/engineering-ai-pipelines/references/monitoring.md` - LLM monitoring patterns
- `evals/engineering-ai-pipelines.json` - Eval test cases (12-15 cases)

## Dependencies
- Task 2-5 can be done in parallel after Task 1 structure is established
- Task 6 (evals) should be done after all content is in place
- Task 7 (parent updates) is optional and can be done last

## Risks
1. **Content duplication vs moving**: Need to decide whether to move or reference existing content. Recommendation: Create new consolidated references but leave original files in place (don't break existing references).
2. **Skill naming**: Ensure `engineering-ai-pipelines` name is finalized and consistent.
3. **Dependency updates**: The original `data-engineering-ai-ml` has dependencies on other skills; the new skill should have appropriate `dependsOn` in YAML frontmatter.
4. **Cross-skill references**: Must properly reference `@designing-data-storage` for storage guidance and avoid duplicating that content.

## Implementation Notes

### SKILL.md Structure (follow designing-data-storage pattern):
```yaml
---
name: engineering-ai-pipelines
description: "AI/ML production workflows: embedding generation, vector storage, RAG patterns, LLM monitoring, and batch inference."
dependsOn:
  - "@data-engineering-core"
  - "@designing-data-storage"
---
```

### Sections to include:
1. Table of Contents
2. Quick Comparison (Embeddings | Vector DBs | RAG approach)
3. When to Use Which? (decision matrix)
4. Detailed Reference Guides (links to references/)
5. Code Examples (embedding gen, vector search, RAG pipeline, monitoring)
6. Best Practices
7. Related Skills (with @mentions)
8. References (external links)

### Eval coverage areas:
- embedding-selection: OpenAI vs local models
- vector-db-selection: LanceDB vs pgvector vs DuckDB
- rag-pipeline: chunking, retrieval, generation
- monitoring-setup: cost tracking, retry logic
- cross-reference: storage guidance should reference @designing-data-storage
