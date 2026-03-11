# Implementation: building-data-apps Skill

## Summary

Created a new `building-data-apps` skill that replaces and expands upon `data-science-interactive-apps`. The new skill covers all 5 major Python data app frameworks (Streamlit, Panel, Gradio, Dash, NiceGUI) with clear boundaries versus `working-in-notebooks`.

## Files Created

### Main skill
- `skills/building-data-apps/SKILL.md` (11,674 bytes)
  - Clear "When to use" and "When NOT to use" sections
  - Boundary table contrasting with `working-in-notebooks`
  - Tool selection guide with quick decision checklist
  - Quick start examples for all 5 frameworks
  - Progressive disclosure pointing to reference files

### Reference files
1. `references/streamlit-advanced.md` (7,271 bytes)
   - Caching strategies (@st.cache_data, @st.cache_resource)
   - Multipage apps structure
   - Secrets management
   - Session state patterns
   - Custom components
   - Layout patterns

2. `references/panel-advanced.md` (9,534 bytes)
   - Parameterized classes
   - Reactive programming (@pn.depends, bind, rx)
   - Layout and templating
   - Linked plots and widgets
   - Deployment with panel serve

3. `references/gradio-advanced.md` (11,179 bytes)
   - Interface types (Interface, Blocks, Tabbed, ChatInterface)
   - Input/output components
   - Event handling and state management
   - Authentication and sharing
   - Hugging Face Spaces integration
   - API generation

4. `references/dash-advanced.md` (11,876 bytes)
   - Callback patterns and state management
   - Multiple inputs/outputs
   - dcc.Store for client-side state
   - Pattern-matching callbacks
   - Multi-page apps
   - Deployment with Gunicorn

5. `references/nicegui-guide.md` (11,022 bytes)
   - Core concepts (ui.* namespace)
   - Event handling
   - Async/await patterns
   - Desktop vs web deployment
   - Native-like UI components

6. `references/framework-selection.md` (9,022 bytes)
   - Detailed comparison matrix
   - Decision framework with 4 key questions
   - Framework profiles (strengths/limitations)
   - Migration patterns
   - Selection checklist

7. `references/deployment-patterns.md` (11,344 bytes)
   - Streamlit Community Cloud
   - Hugging Face Spaces
   - Docker containerization (all frameworks)
   - Cloud platforms (AWS, GCP, Azure)
   - Self-hosted options
   - Security and performance checklists

### Updated file
- `skills/data-science-interactive-apps/SKILL.md`
  - Added deprecation notice at top
  - Updated description to indicate deprecated status
  - Points users to `building-data-apps`

## Key Design Decisions

### 1. Clear boundaries with working-in-notebooks
- Used the same boundary pattern as `working-in-notebooks`
- Bidirectional cross-references ensure users can navigate correctly
- "Quick boundary check" section for rapid decision-making

### 2. Progressive disclosure structure
- Main SKILL.md: Quick starts and tool selection for 80% of use cases
- references/: Deep dives for specific needs
- Framework selection guide bridges main skill and references

### 3. Coverage of all 5 frameworks
- Original skill only covered 3 frameworks well (Streamlit, Panel, Gradio)
- Added substantial new content for Dash and NiceGUI
- Each framework has appropriate depth based on complexity

### 4. Eval alignment verification
All 5 task evaluations from `eval/building-data-apps.json` are covered:
- eval-001: Streamlit Dashboard → Main skill + streamlit-advanced.md
- eval-002: Panel Application → Main skill + panel-advanced.md
- eval-003: Gradio ML Demo → Main skill + gradio-advanced.md
- eval-004: Framework Selection → framework-selection.md
- eval-005: App Deployment → deployment-patterns.md

## Statistics

- Total new lines of documentation: ~82,000 bytes
- New files: 8 (1 skill + 7 references)
- Modified files: 1 (deprecation notice)
- Frameworks covered: 5 (up from 3)
- Reference guides: 7

## Migration Path for Users

Users of the old `data-science-interactive-apps` skill should:
1. Use `@building-data-apps` for all new projects
2. Reference `framework-selection.md` when choosing between frameworks
3. Use `deployment-patterns.md` for production deployment guidance
4. Note that Dash and NiceGUI are now fully documented options
