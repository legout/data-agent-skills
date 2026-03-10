I've created the anchor context for ticket das-b143. Here's a summary:

**Ticket das-b143**: Document the standard frontmatter policy and the dependsOn decision for skills

**Key findings:**
- The dependsOn decision is already made in SKILL_REFACTORING_PLAN.md (section 9.3) - remove from frontmatter
- The lint tool (tools/skill_lint.py) warns on non-standard frontmatter fields including dependsOn
- 26 of 29 current skills use dependsOn in frontmatter
- docs/skill-map.md already exists with naming rules and architecture

**Complexity**: Simple - documentation only, decision already made

**Recommended path**: A (Minimal) - extract and formalize existing decisions into authoring docs

**Output target**: Create `docs/skill-authoring.md` documenting:
1. Allowed frontmatter fields (name, description, plus optional license/compatibility/metadata/allowed-tools)
2. The dependsOn removal decision with rationale
3. Related-skill routing patterns (plain skill names, not hybrid @skill/path notation)
4. Reference standards