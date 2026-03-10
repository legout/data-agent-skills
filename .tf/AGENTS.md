# AGENTS

Reusable lessons learned from ticket implementations.

## Lesson: Action-Oriented Skill Naming

Skill names should start with verbs describing user actions (building-*, accessing-*, analyzing-*) rather than nouns or domains. Use consistent verb conventions: building for construction, accessing for connection, designing for architecture, managing for administration, orchestrating for coordination, assuring for quality, engineering for specialized construction, evaluating for measurement. Target 2-4 words max, kebab-case.

Discovered in: das-3jql (2026-03-10)

## Lesson: Adjacent-Skill Boundary Documentation

When skills have overlapping concerns (e.g., EDA vs Visualization, Quality vs Observability), document explicit trigger guidance with comparison tables. Merge skills that are logically adjacent but operationally difficult to separate, using internal sections to preserve conceptual boundaries while avoiding user confusion.

Discovered in: das-3jql (2026-03-10)
