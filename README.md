# Data Engineering Agent Skills

A curated skill library for coding agents focused on data engineering workflows.

## Install with `npx skills`

List skills:

```bash
npx skills add legout/agent-skills-data-engineering --list
```

Install specific skills:

```bash
npx skills add legout/agent-skills-data-engineering \
  --skill data-engineering-core \
  --skill data-engineering-storage-remote-access
```

Install all skills:

```bash
npx skills add legout/agent-skills-data-engineering --skill '*'
```

## Repository layout

- `skills/` — installable skill directories (for `npx skills` discovery)
- `data-engineering/` — source content and references
- `flowerpower-skill/` — FlowerPower skill package and helper scripts
- `tools/` — utility scripts such as `skill_lint.py`

## Notes

This repository intentionally excludes internal development planning documents.
