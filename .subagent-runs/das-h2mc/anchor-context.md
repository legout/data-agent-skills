# Anchor Context: das-h2mc

## Ticket Summary
Create new `engineering-ai-pipelines` skill by refactoring `data-engineering-ai-ml` to focus on production workflow guidance for embeddings, vector stores, RAG, and monitoring. Follow the pattern from das-trf5 (designing-data-storage).

## Complexity Assessment
- **LOC Estimate**: ~200-300 (new SKILL.md + possible content reorganization)
- **Complexity**: Medium
- **Type**: Skill refactor/consolidation with eval coverage

## Dependencies
- `das-trf5` (closed) - Created `designing-data-storage` skill pattern to follow
- Existing `skills/data-engineering-ai-ml/` content to adapt

## Existing Content to Adapt
```
skills/data-engineering-ai-ml/
├── SKILL.md          # Main skill file
├── embeddings.md     # Embedding generation guidance
├── vector-databases.md  # Vector store guidance
├── rag-pipelines.md  # RAG workflow guidance
└── monitoring.md     # LLM monitoring guidance
```

## Research Gaps
**None** - Existing content provides sufficient guidance. This is a refactor/consolidation task.

## External Libraries
No new external dependencies needed - using existing skill patterns.

## Testing Requirements
- Create `evals/engineering-ai-pipelines.json` with 12-15 test cases
- Cover: embeddings, vector stores, RAG, monitoring, cross-skill references
- Pattern: Follow `evals/designing-data-storage.json` structure

## File Hints
- **Source**: `skills/data-engineering-ai-ml/`
- **Target**: `skills/engineering-ai-pipelines/` (new)
- **Evals**: `evals/engineering-ai-pipelines.json` (new)

## Recommended Path
**Path B (Standard)** - Straightforward refactor with existing content to adapt. No research needed.

### Rationale
1. Existing content provides all necessary guidance
2. Pattern from das-trf5 is well-established
3. Medium complexity, clear scope
4. Standard validation (review + test) sufficient

## Implementation Notes
1. Create `skills/engineering-ai-pipelines/SKILL.md` with workflow-focused structure
2. Either move or reference existing sub-documents from data-engineering-ai-ml
3. Add explicit cross-links to `@designing-data-storage` for storage-related guidance
4. Create eval coverage matching acceptance criteria
5. Update any references in parent skills if needed
