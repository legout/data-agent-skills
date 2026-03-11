---
id: das-y3ig
status: closed
deps: [das-llsd, das-g8hg, das-trf5, das-k0lp, das-n3x8, das-ekec, das-5ewy, das-h2mc, das-09vu]
links: [das-01dp, das-t14p]
created: 2026-03-10T15:55:12Z
type: task
priority: 3
assignee: legout
parent: das-dpgr
tags: [skill-refactor, migration, docs, hub]
---
# Replace the data-engineering hub skill with a docs-only skill index

Convert the current broad hub behavior into documentation so it no longer competes in triggering.

## Acceptance Criteria

- docs/skill-map.md replaces the triggerable hub behavior
- old-to-new engineering skill routing is documented
- the old hub is clearly marked or removed from the trigger path


## Notes

**2026-03-11T17:13:32Z**

Implementation complete:
- Converted data-engineering skill to docs-only hub
- Added [DOCS ONLY - DO NOT TRIGGER] prefix to frontmatter description
- Added deprecation banner with routing to 8 specific replacement skills
- Review gate: clear pass (1 non-blocking suggestion skipped)
- Commit: 57e6192
