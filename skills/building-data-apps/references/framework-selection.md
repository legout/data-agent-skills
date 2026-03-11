# Framework Selection Guide

Choosing the right framework for your data application depends on your specific requirements, team skills, and deployment constraints.

## Quick Decision Matrix

| Factor | Streamlit | Panel | Gradio | Dash | NiceGUI |
|--------|-----------|-------|--------|------|---------|
| **Learning curve** | Easiest | Moderate | Easy | Moderate | Easy |
| **Layout flexibility** | Limited | Excellent | Limited | Excellent | Good |
| **ML model demos** | Good | Good | Excellent | Good | Fair |
| **Complex dashboards** | Fair | Excellent | Fair | Excellent | Good |
| **Jupyter integration** | None | Excellent | None | None | None |
| **Community size** | Largest | Medium | Large | Large | Small |
| **Async support** | Limited | Good | Limited | Limited | Excellent |
| **Desktop apps** | No | No | No | No | Yes |
| **HoloViz ecosystem** | No | Yes | No | No | No |
| **React ecosystem** | No | No | No | Yes | No |

## Decision Framework

### Question 1: Who is your audience?

**Non-technical stakeholders who need a simple interface**
- → **Streamlit** — fastest to build, easiest to understand
- → **Gradio** — if it's an ML model demo with Hugging Face sharing

**Technical users who need complex dashboards**
- → **Panel** — flexible layouts, excellent for complex reactive UIs
- → **Dash** — production control, fine-grained customization

**Mixed audience (desktop + web)**
- → **NiceGUI** — single codebase for both

### Question 2: What is your data source?

**Large datasets requiring real-time interaction**
- → **Panel** — Datashader integration for millions of points
- → **Dash** — efficient callbacks and state management

**Live streaming data**
- → **Streamlit** — simple `st.empty()` pattern
- → **Panel** — reactive streams with HoloViews
- → **NiceGUI** — excellent async support

**Multiple data sources with complex joins**
- → **Panel** — parameterized workflows
- → **Dash** — flexible callback chains

### Question 3: What is your deployment target?

**Free cloud hosting**
- → **Streamlit** — Streamlit Community Cloud
- → **Gradio** — Hugging Face Spaces

**Enterprise/self-hosted**
- → **Dash** — proven production patterns with Gunicorn
- → **Panel** — mature server deployment
- → **Streamlit** — Docker + any cloud

**Desktop application**
- → **NiceGUI** — native window mode

### Question 4: What is your team's background?

**Pure Python data scientists**
- → **Streamlit** — minimal web knowledge required
- → **Panel** — if coming from Jupyter workflows

**ML engineers sharing models**
- → **Gradio** — purpose-built for ML demos
- → **Streamlit** — flexible for any data app

**Frontend developers who know React**
- → **Dash** — React-based, familiar patterns

**Full-stack Python developers**
- → **NiceGUI** — modern Python with async
- → **Dash** — full control over frontend/backend

## Detailed Framework Profiles

### Streamlit

**Best for:** Rapid prototyping, ML demos, simple dashboards

**Strengths:**
- Fastest time to first prototype
- Largest community and ecosystem
- Simple mental model (rerun on every interaction)
- Excellent caching decorators
- Built-in secrets management
- Streamlit Community Cloud for free hosting

**Limitations:**
- Rerun model can be inefficient for complex apps
- Layout flexibility limited (top-down flow)
- Not ideal for complex reactive UIs
- No native desktop support

**Use when:** You need to ship something today, audience is non-technical, app is relatively simple.

### Panel

**Best for:** Complex dashboards, reactive UIs, Jupyter workflows

**Strengths:**
- Most flexible layout system
- Excellent reactive programming model
- Deep HoloViz ecosystem integration (hvPlot, HoloViews, Datashader)
- Works in Jupyter notebooks and as standalone apps
- Parameterized classes for clean architecture
- Handles large datasets with Datashader

**Limitations:**
- Steeper learning curve than Streamlit
- Smaller community than Streamlit/Dash
- Documentation can be fragmented

**Use when:** Building complex dashboards, need linked/interactive visualizations, working with large data, already using HoloViz tools.

### Gradio

**Best for:** ML model demos, quick sharing, Hugging Face integration

**Strengths:**
- Purpose-built for ML model interfaces
- Built-in sharing (temporary URLs)
- Excellent Hugging Face Spaces integration
- Automatic API generation
- Great for quick prototypes of ML functionality
- Built-in flagging/feedback collection

**Limitations:**
- Less flexible for non-ML apps
- Layout system less powerful than competitors
- Opinionated about UI patterns

**Use when:** Sharing ML models, creating quick demos, deploying to Hugging Face Spaces.

### Dash

**Best for:** Production dashboards, enterprise apps, fine control

**Strengths:**
- Most mature production patterns
- React-based frontend (familiar to web developers)
- Extensive component library
- Fine-grained callback control
- Excellent state management (dcc.Store)
- Multi-page app support
- Long callbacks for background tasks

**Limitations:**
- More verbose than Streamlit/Gradio
- Requires more web development knowledge
- Callback model can be complex for beginners

**Use when:** Building production-grade dashboards, need React ecosystem, enterprise deployment, complex state management.

### NiceGUI

**Best for:** Desktop + web apps, modern Python async, native-like UI

**Strengths:**
- Single codebase for web and desktop
- Excellent async/await support
- Modern UI components (Vue.js + Quasar)
- Tailwind CSS integration
- Simple yet powerful API
- Great for Python developers who want modern UIs

**Limitations:**
- Smallest community of the major frameworks
- Newer, less battle-tested in production
- Fewer third-party extensions

**Use when:** Need desktop app, async operations are important, want modern UI without web dev knowledge.

## Migration Patterns

### Streamlit → Panel

**Why migrate:** Need more layout flexibility, complex reactive UIs, better handling of large data.

**Migration path:**
1. Replace `st.*` widgets with Panel widgets
2. Replace caching with Panel's reactive patterns
3. Convert `st.columns` to Panel layouts (`pn.Row`, `pn.Column`)
4. Use `@pn.depends` instead of Streamlit's rerun model

### Streamlit → Dash

**Why migrate:** Need production-grade control, React ecosystem, complex state management.

**Migration path:**
1. Convert widgets to Dash Core Components
2. Replace caching with `dcc.Store` or memoization
3. Convert layout to HTML/Dash components
4. Add explicit callbacks for all interactions

### Any → Gradio

**Why migrate:** Specifically for ML model sharing, Hugging Face Spaces.

**Migration path:**
1. Identify the core prediction function
2. Wrap in `gr.Interface` or `gr.Blocks`
3. Map inputs/outputs to Gradio components
4. Add examples for user guidance

## Combining Frameworks

Sometimes combining frameworks makes sense:

### Streamlit + Gradio

Use Gradio Blocks within Streamlit for specific ML model interfaces:

```python
import streamlit as st
from gradio import Blocks

st.title("My App")
# ... Streamlit content ...

# Embed Gradio for specific model demo
st.components.v1.iframe("https://huggingface.co/spaces/user/space", height=600)
```

### Panel + HoloViz ecosystem

Panel works seamlessly with hvPlot, HoloViews, and Datashader for advanced visualizations.

### Dash + custom React components

Dash allows embedding custom React components for specialized needs.

## Framework Selection Checklist

Before choosing a framework, answer these questions:

- [ ] **Timeline:** How quickly do you need to ship?
  - Immediate → Streamlit or Gradio
  - Medium → Panel or NiceGUI
  - Long-term project → Dash or Panel

- [ ] **Complexity:** How complex is the UI?
  - Simple forms/charts → Streamlit
  - Complex linked visualizations → Panel
  - Production dashboard → Dash

- [ ] **Audience:** Who will use this?
  - Internal stakeholders → Streamlit
  - ML researchers → Gradio
  - External customers → Dash
  - Desktop users → NiceGUI

- [ ] **Data size:** How much data?
  - Small (< 10k rows) → Any framework
  - Medium (10k-1M rows) → Panel with care
  - Large (> 1M rows) → Panel with Datashader

- [ ] **Team skills:** What does your team know?
  - Only Python data science → Streamlit
  - Jupyter + Python → Panel
  - React experience → Dash
  - Modern Python async → NiceGUI

- [ ] **Deployment:** Where will this run?
  - Free cloud → Streamlit Cloud or Hugging Face
  - Enterprise self-hosted → Dash or Panel
  - Desktop → NiceGUI

## Recommendations by Use Case

| Use Case | Recommended | Alternatives |
|----------|-------------|--------------|
| Quick ML demo | Gradio | Streamlit |
| Executive dashboard | Streamlit | Panel |
| Complex analytics tool | Panel | Dash |
| Production customer portal | Dash | Streamlit |
| Data exploration tool | Panel | Streamlit |
| ML research sharing | Gradio | Streamlit |
| Desktop data tool | NiceGUI | Panel |
| Real-time monitoring | Panel | Dash |
| Jupyter widget extension | Panel | — |
