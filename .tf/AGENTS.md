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
