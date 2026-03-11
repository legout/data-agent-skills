Done. I've written the anchor context to `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

**Summary of key findings:**

1. **Ticket das-u0hp** is about creating a new `engineering-ml-features` skill as part of the data-science refactoring epic (das-68kl)

2. **Already existing:**
   - Eval files: `eval/engineering-ml-features.json` (5 task evals) and `eval/trigger-eval/engineering-ml-features.json` (15 trigger tests)
   - Reference files in `skills/analyzing-data/references/` (categorical-encoding, datetime-features, text-features, feature-selection)
   - Current source skill: `skills/data-science-feature-engineering/SKILL.md`

3. **Main work:**
   - Create new skill at `skills/engineering-ml-features/SKILL.md`
   - Update broken references (analyzing-data SKILL.md points to old name)
   - Consolidate references without duplication
   - Apply lessons from das-qdy8 (shared reference consolidation), das-3wu8 (content preservation), das-ix8j (library selection cohesion)

4. **Complexity**: Medium - eval scaffolding exists, straightforward content migration following established pattern from das-jg7i (analyzing-data creation)