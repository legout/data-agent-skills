Done! I've written the anchor context to `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

## Summary

**Ticket das-0p8u** is an epic to refactor data-engineering skills into the approved 14-skill workflow-centered architecture. Key findings:

### Status
- All 9 dependency tasks are **completed** (das-llsd, das-g8hg, das-trf5, das-k0lp, das-n3x8, das-ekec, das-5ewy, das-h2mc, das-09vu)
- The 14 new skills exist in `skills/` directory
- Data-science skills were also refactored (analyzing-data, engineering-ml-features, evaluating-ml-models, working-in-notebooks, building-data-apps)

### Research Gaps Identified
- **Eval coverage incomplete**: Only 4 of 14 skills have eval files
- Need lint verification
- Need reference integrity check for deprecated skill names

### Recommended Path: C (Deep)
Since most skill creation is complete, remaining work is verification and cleanup:
1. Verify all 14 skills exist and pass lint
2. Complete eval coverage for 10 missing skills
3. Confirm CHANGELOG.md accuracy
4. Verify docs migration references

### Lessons Applied
Relevant lessons from `.tf/AGENTS.md` including: dangling reference search (das-9jfk), skill merge preservation (das-3wu8), avoid circular deprecation (das-g8hg), progressive disclosure (das-5ewy), and lint scope for cleanup (das-01dp).