## Review

- What's correct
  - Implemented ticket scope as **new-skill creation** without modifying legacy skill content/files.
  - Added all planned artifacts:
    - `skills/engineering-ai-pipelines/SKILL.md`
    - `skills/engineering-ai-pipelines/references/embeddings.md`
    - `skills/engineering-ai-pipelines/references/vector-stores.md`
    - `skills/engineering-ai-pipelines/references/rag-pipelines.md`
    - `skills/engineering-ai-pipelines/references/monitoring.md`
    - `evals/engineering-ai-pipelines.json`
  - `SKILL.md` includes required frontmatter, decision matrices, comparison tables, code examples, ✅/❌ best practices, and cross-link to `@designing-data-storage`.
  - Evals cover expected areas (embeddings, vector DB choice, RAG patterns, monitoring, and cross-skill reference).

- Issue [Major]: Parameter mismatch in `monitoring.md` cache insert example will fail at runtime.  
  - File: `skills/engineering-ai-pipelines/references/monitoring.md`  
  - Description: In `LLMMonitor.log_call`, SQL uses two placeholders + `NOW()`:
    `INSERT OR IGNORE INTO prompt_cache (prompt_hash, prompt_text, created_at) VALUES (?, ?, NOW())`
    but passes **three** parameters: `[prompt_hash, prompt, datetime.now()]`. This causes a binding error.
  - Suggested fix: Pass only two parameters (`[prompt_hash, prompt]`) or change SQL to three placeholders and bind three values consistently.

- Issue [Major]: Monitoring example can throw division-by-zero when no active days are present.  
  - File: `skills/engineering-ai-pipelines/references/monitoring.md`  
  - Description: `daily_cost = stats['estimated_cost_usd'] / stats.get('active_days', 1)` still divides by zero when `active_days` exists but is `0`.
  - Suggested fix: Guard denominator explicitly, e.g. `days = max(stats.get('active_days', 0), 1)` before division.

- Issue [Major]: Monitoring guidance stores full prompts/responses by default, creating avoidable PII/security risk and contradicting stated best practices.  
  - File: `skills/engineering-ai-pipelines/references/monitoring.md`  
  - Description: The sample schema and `log_call` implementation persist `prompt_text` and `response_text` in `prompt_cache` by default, while the same doc advises not logging full prompts/responses by default.
  - Suggested fix: Default to hashed/redacted storage; make full prompt/response capture opt-in behind explicit flags and retention controls.

- Issue [Minor]: “Complete/runnable” code samples have missing imports in at least two places.  
  - Files:
    - `skills/engineering-ai-pipelines/references/embeddings.md`
    - `skills/engineering-ai-pipelines/SKILL.md`
  - Description:
    - `embeddings.md` usage calls `pl.read_parquet(...)` without importing `polars as pl` in that scope.
    - `SKILL.md` monitoring example uses `openai.OpenAI()` but lacks `import openai` in that snippet.
  - Suggested fix: Add missing imports directly in each snippet to keep examples copy/paste runnable.

- Issue [Suggestion]: Parent hub skill still points AI/ML users only to `@data-engineering-ai-ml`; new skill discoverability is limited.  
  - File: `skills/data-engineering/SKILL.md`  
  - Description: Plan’s optional cross-link consistency task was not applied, so users may miss `@engineering-ai-pipelines`.
  - Suggested fix: Add `@engineering-ai-pipelines` in AI/ML row and “Getting Started” AI/ML section (or explicitly indicate relationship between old/new skill).

- Note: Observations
  - `anchor-context.md` referenced by the task was not present in the run directory; review was performed using `implementation.md`, `plan.md`, and the introduced files.
  - Review constrained to implementation-introduced scope only.

- Gate: Uncertain
