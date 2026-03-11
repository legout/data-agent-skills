Done! I've created the anchor context for ticket das-nd1t at `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

## Summary

**Ticket**: das-nd1t - Create `evaluating-ml-models` skill with CV, metrics, tuning, and experiment tracking

**Key Points**:
- **Complexity**: Medium - follows the established pattern from recently completed das-u0hp (engineering-ml-features)
- **Source content already exists** in `skills/analyzing-data/references/` (cross-validation.md, metrics-guide.md, hyperparameter-tuning.md, experiment-tracking.md)
- **Main work**: Restructure into new self-contained skill + add eval coverage + fix broken references

**Structure to create**:
```
skills/evaluating-ml-models/
├── SKILL.md
└── references/
    ├── cross-validation.md
    ├── metrics-guide.md
    ├── hyperparameter-tuning.md
    └── experiment-tracking.md
```

**Testing**: Create `evals/evaluating-ml-models.json` following the working-in-notebooks.json pattern with task and trigger evals.