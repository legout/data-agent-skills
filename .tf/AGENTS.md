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
