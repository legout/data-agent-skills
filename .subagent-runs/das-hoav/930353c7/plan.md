# Implementation Plan

## Goal
Create a new `building-data-apps` skill for Streamlit, Panel, Gradio, Dash, and NiceGUI workflows, refactoring content from `data-science-interactive-apps` with clear boundaries vs `working-in-notebooks`.

## Tasks

### 1. Create new skill directory structure
- **File**: Create `skills/building-data-apps/`
- **Changes**: Create folder and `SKILL.md` main file
- **Acceptance**: Directory exists with proper structure

### 2. Write main SKILL.md content
- **File**: `skills/building-data-apps/SKILL.md`
- **Changes**: 
  - Copy relevant content from `data-science-interactive-apps`
  - Update name to `building-data-apps`
  - Rewrite description to emphasize stakeholder-facing apps
  - Add explicit "When NOT to use" section with boundary vs `working-in-notebooks`
  - Expand tool selection guide to include Dash and NiceGUI with proper coverage
  - Add quick starts for all 5 frameworks (Streamlit, Panel, Gradio, Dash, NiceGUI)
  - Include deployment patterns section
  - Add "Related skills" table with clear boundaries
- **Acceptance**: Complete skill file following pattern from `working-in-notebooks`

### 3. Create references/ directory with advanced patterns
- **File**: `skills/building-data-apps/references/streamlit-advanced.md`
- **Changes**: Move and expand from `analyzing-data/references/streamlit-advanced.md`
  - Caching strategies (@st.cache_data, @st.cache_resource)
  - Multipage apps structure
  - Secrets management
  - Custom components
  - Session state patterns
- **Acceptance**: File has comprehensive advanced Streamlit patterns

### 4. Create Panel advanced reference
- **File**: `skills/building-data-apps/references/panel-advanced.md`
- **Changes**: 
  - Parameterized classes deep dive
  - Reactive programming patterns
  - Layout and templating
  - Linking plots and widgets
  - Deploying with `panel serve`
- **Acceptance**: File covers advanced Panel usage

### 5. Create Gradio advanced reference
- **File**: `skills/building-data-apps/references/gradio-advanced.md`
- **Changes**:
  - Interface types (Blocks, Tabbed, etc.)
  - Custom components
  - Authentication and sharing
  - Hugging Face Spaces integration
  - API generation for models
- **Acceptance**: File covers advanced Gradio usage

### 6. Create Dash advanced reference
- **File**: `skills/building-data-apps/references/dash-advanced.md`
- **Changes**: 
  - Expand from `analyzing-data/references/plotly-dash.md`
  - Callback patterns and state management
  - Layout components
  - Deployment with gunicorn
  - Production considerations
- **Acceptance**: File covers comprehensive Dash patterns

### 7. Create NiceGUI reference
- **File**: `skills/building-data-apps/references/nicegui-guide.md`
- **Changes**:
  - Core concepts (ui.* elements)
  - Event handling
  - Async/await patterns
  - Desktop vs web deployment
  - Native-like UI components
- **Acceptance**: File covers NiceGUI from basics to advanced

### 8. Create framework selection guide
- **File**: `skills/building-data-apps/references/framework-selection.md`
- **Changes**:
  - Detailed comparison matrix
  - Decision tree for choosing frameworks
  - Migration patterns between frameworks
  - When to combine frameworks
- **Acceptance**: File helps users choose the right tool

### 9. Create deployment patterns reference
- **File**: `skills/building-data-apps/references/deployment-patterns.md`
- **Changes**:
  - Streamlit Community Cloud
  - Hugging Face Spaces
  - Docker containerization
  - Cloud platforms (AWS, GCP, Azure)
  - Self-hosted options
- **Acceptance**: File covers all major deployment paths

### 10. Update data-science-interactive-apps skill
- **File**: `skills/data-science-interactive-apps/SKILL.md`
- **Changes**:
  - Add deprecation/migration notice at top
  - Update related skills to point to `building-data-apps`
  - Add note that content has moved
- **Acceptance**: Users are directed to new skill

### 11. Verify eval file alignment
- **File**: `eval/building-data-apps.json` (already exists)
- **Changes**: Verify all 5 task evaluations are covered by skill content
  - eval-001: Streamlit Dashboard → covered by quick start + advanced ref
  - eval-002: Panel Application → covered by quick start + advanced ref
  - eval-003: Gradio ML Demo → covered by quick start + advanced ref
  - eval-004: Framework Selection → covered by selection guide
  - eval-005: App Deployment → covered by deployment patterns ref
- **Acceptance**: All eval criteria can be met by skill content

### 12. Create boundary cross-references
- **Files**: 
  - Update `skills/working-in-notebooks/SKILL.md` to ensure boundary with `building-data-apps` is clear
  - Ensure `skills/building-data-apps/SKILL.md` references `working-in-notebooks` appropriately
- **Changes**: Verify bidirectional boundary documentation exists
- **Acceptance**: Users can clearly distinguish between notebook and app use cases

## Files to Modify

| File | Changes |
|------|---------|
| `skills/building-data-apps/SKILL.md` | Create new main skill file |
| `skills/building-data-apps/references/streamlit-advanced.md` | Create from existing content |
| `skills/building-data-apps/references/panel-advanced.md` | Create new |
| `skills/building-data-apps/references/gradio-advanced.md` | Create new |
| `skills/building-data-apps/references/dash-advanced.md` | Create from existing content |
| `skills/building-data-apps/references/nicegui-guide.md` | Create new |
| `skills/building-data-apps/references/framework-selection.md` | Create new |
| `skills/building-data-apps/references/deployment-patterns.md` | Create new |
| `skills/data-science-interactive-apps/SKILL.md` | Add deprecation/migration notice |

## New Files

```
skills/building-data-apps/
├── SKILL.md                              # Main skill file
└── references/
    ├── streamlit-advanced.md             # Advanced Streamlit patterns
    ├── panel-advanced.md                 # Advanced Panel patterns
    ├── gradio-advanced.md                # Advanced Gradio patterns
    ├── dash-advanced.md                  # Advanced Dash patterns
    ├── nicegui-guide.md                  # NiceGUI guide
    ├── framework-selection.md            # Framework comparison
    └── deployment-patterns.md            # Deployment options
```

## Dependencies

```
Task 1 (create directory) → All other tasks
Task 2 (main SKILL.md) → Tasks 3-9 (references), Task 12 (boundaries)
Tasks 3-9 (references) → Task 11 (verify eval alignment)
```

## Risks

1. **Content overlap with `data-science-interactive-apps`**: Must clearly mark old skill as superseded
2. **Dash/NiceGUI coverage gaps**: These had minimal coverage in source; need to write substantial new content
3. **Boundary confusion**: Must be explicit about when to use `building-data-apps` vs `working-in-notebooks`
4. **Reference file proliferation**: 7 reference files is many; ensure each has distinct value
5. **Eval alignment**: Must verify all 5 task evals are properly covered by new skill structure

## Pattern to Follow

Use `skills/working-in-notebooks/SKILL.md` as the template:
- Clear "When to use" and "When NOT to use" sections
- Explicit boundary table contrasting with related skills
- Tool selection guide with decision checklist
- Core workflow with numbered steps
- Progressive disclosure with references/ folder
- Related skills table at bottom
- No `dependsOn` in frontmatter (non-standard field)
