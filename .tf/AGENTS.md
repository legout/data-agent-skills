# AGENTS

Reusable lessons learned from ticket implementations.

## Lesson: Action-Oriented Skill Naming

Skill names should start with verbs describing user actions (building-*, accessing-*, analyzing-*) rather than nouns or domains. Use consistent verb conventions: building for construction, accessing for connection, designing for architecture, managing for administration, orchestrating for coordination, assuring for quality, engineering for specialized construction, evaluating for measurement. Target 2-4 words max, kebab-case.

Discovered in: das-3jql (2026-03-10)

## Lesson: Adjacent-Skill Boundary Documentation

Discovered in: das-3jql (2026-03-10)

## Lesson: Eval Directory Scope Hygiene

When creating evaluation scaffolds for refactored architectures, ensure eval directories contain only the target skill manifests, not legacy ones. Mixed manifests (legacy + target) cause ambiguity for downstream eval tooling and make ticket behavior/metrics unclear. Remove or relocate legacy manifests before committing eval structure.

Discovered in: das-lih7 (2026-03-10)

## Lesson: Single Source of Truth for Standards

When multiple related standards exist (naming, frontmatter, templates, evaluation), create a consolidated index document that serves as the authoritative reference. This reduces drift, improves discoverability, and provides a single entry point for all standards-related questions. Include quick-reference tables, cross-links to detailed docs, decision rationales, and checklists.

Discovered in: das-ngoo (2026-03-10)

## Lesson: Shared Reference Consolidation for Skill Groups

When multiple skills in a domain (e.g., data-science-*) share identical reference files, consolidate to a single shared location (e.g., analyzing-data/references/) rather than maintaining duplicates. This reduces maintenance burden, ensures consistency, and eliminates drift. After consolidation, update SKILL.md progressive-disclosure paths to point to the shared location using correct relative paths (one level up for sibling skills, not two).

Discovered in: das-qdy8 (2026-03-10)

## Lesson: Lint Tool Scope for External Skills

When skills are created outside the repo (e.g., ~/.pi/agent/skills/), the lint tool cannot validate them from within the repo context. For post-fix verification of external skills, either: (1) add a scoped lint mode with path include/exclude, (2) run lint in the skill's owning workspace, or (3) accept manual verification as the gate. Do not rely on repo-scoped lint for external skill paths.

Discovered in: das-s0yk (2026-03-11)

## Lesson: Skill Merge Content Preservation

When merging multiple skills into one, systematically verify all unique content from each source skill is preserved before deleting source directories. Create a checklist of unique sections (workflows, anti-patterns, tool options, examples) and cross-reference against the merged skill. Common oversights include: EDA-specific workflows (duplicates, class imbalance, temporal patterns), specialized tool options (interactive exploration, large-data handling), and domain-specific anti-patterns (MCAR/MAR/MNAR for missing data).

Discovered in: das-3wu8 (2026-03-11)

## Lesson: Library Selection Layer Cohesion

When creating skills that help users choose between similar libraries (e.g., fsspec vs pyarrow.fs vs obstore), inline the library deep-dives into the main SKILL.md rather than keeping them as separate files. This creates a cohesive "library selection and usage" layer where users can: (1) see the comparison table, (2) immediately read detailed usage guidance for their chosen library, all without navigating between multiple skill files. Reserve separate reference files for supplementary topics like performance patterns or common recipes.

Discovered in: das-ix8j (2026-03-11)

## Lesson: Dependency Direction Consistency

When documenting skill relationships in the "Related skills" section, ensure the wording accurately reflects the formal `dependsOn` frontmatter. The text description should not invert the dependency relationship (e.g., saying "X depends on this skill" when frontmatter shows "this skill depends on @X"). Use directional phrasing like "Upstream X before Y" or "Downstream Y after X" to avoid ambiguity.

Discovered in: das-nd1t (2026-03-11)

## Lesson: Avoid Circular Deprecation References

When consolidating skills and creating deprecation stubs, verify that the new canonical skill does NOT reference the deprecated skills in its content. This creates "circular deprecation" where users following the canonical entry point are sent back to deprecated content. After creating deprecation stubs, audit the new skill's references to ensure all pointers point to current/active skills only.

Discovered in: das-g8hg (2026-03-11)

## Lesson: Search for Dangling References After Skill Deletion

When deleting deprecated skill directories, search the entire codebase for references to the deleted skills (using grep/find), not just in obvious SKILL.md files. References may exist in migration guides, comparison tables, documentation, or cross-skill notes. Update all found references to point to canonical replacements before committing the deletion.

Discovered in: das-9jfk (2026-03-11)

## Lesson: Canonical Skill Names in Boundary Sections

When documenting skill boundaries against related skills, use canonical skill names (e.g., `building-data-pipelines`, `orchestrating-data-pipelines`) not @-prefixed aliases (e.g., `@data-engineering-core`, `@data-engineering-orchestration`). This ensures boundary documentation matches the current taxonomy and avoids confusion when deprecated names are phased out.

Discovered in: das-09vu (2026-03-11)

## Lesson: Progressive Disclosure Code Density

For workflow-oriented skills (streaming, orchestration, pipelines), keep SKILL.md at decision/workflow level with short illustrative snippets only (~5-15 lines). Long runnable patterns (producer/consumer implementations, error handling flows) should live in reference files. Exception: library-selection skills where comparison and usage guidance should be inline for cohesion. When SKILL.md contains 50+ line code blocks, it likely duplicates reference content and should be refactored to progressive disclosure with links.

Discovered in: das-5ewy (2026-03-11)

## Lesson: Multi-Framework Skill Organization

When documenting multiple frameworks/libraries in a single skill (e.g., Streamlit, Panel, Gradio, Dash, NiceGUI), use a three-tier structure: (1) main SKILL.md with quick starts and selection guidance for 80% of use cases, (2) framework-specific reference files for deep dives (caching, callbacks, deployment), and (3) a framework-selection guide that bridges the main skill and references with decision matrices. This prevents SKILL.md from becoming unwieldy while maintaining discoverability. Each reference file should focus on advanced patterns specific to that framework rather than duplicating basics already covered in the main skill.

Discovered in: das-hoav (2026-03-11)

## Lesson: Non-Triggerable Hub Skill Pattern

For skills that serve as documentation hubs/indexes rather than providing actionable guidance, use explicit non-triggerable markers: (1) add `[DOCS ONLY - DO NOT TRIGGER]` prefix to frontmatter description, (2) add deprecation banner at top of content with routing to specific replacement skills. This prevents hub skills from being selected over actionable skills on general queries, improving user experience by directing them immediately to relevant specialized skills.

Discovered in: das-y3ig (2026-03-11)

## Lesson: Changelog Claims Scoped to Delivery State

When documenting refactoring work in changelogs, distinguish between completed work and planned target state. Use explicit markers like "(Planned)" or future-tense wording for items not yet delivered. Avoid presenting target-state goals as completed facts, which misleads users during migration and creates trust issues. Scope claims to what is actually in the commit, defer roadmap items to dedicated sections.

Discovered in: das-t14p (2026-03-11)
