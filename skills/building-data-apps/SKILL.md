---
name: building-data-apps
description: "Build interactive web applications for data science and ML: Streamlit, Panel, Gradio, Dash, and NiceGUI. Use for creating stakeholder-facing dashboards, ML model demos, and internal data tools that non-technical users can interact with."
---

# Building Data Apps

Use this skill to create interactive web applications that let stakeholders explore data, interact with ML models, and access analytics without writing code.

## When to use this skill

- **Stakeholder dashboards** — executives, product managers, or clients need self-service data access
- **ML model demos** — let users test predictions with their own inputs
- **Internal data tools** — operations teams need forms, filters, and reporting
- **Data exploration for non-coders** — business users need to drill into datasets
- **Prototyping before full engineering** — validate UX quickly with Python
- **A/B testing interfaces** — experiment with different presentations of results
- **Multi-user analytics** — shared tools accessed via browser (not notebooks)

## When NOT to use this skill

Use a different skill for these related but distinct tasks:

| Instead of... | Use this skill | Because... |
|---------------|----------------|------------|
| Creating reproducible analysis notebooks | `working-in-notebooks` | Notebooks are for analysts; apps are for stakeholders |
| Jupyter-style interactive exploration | `working-in-notebooks` | Use Jupyter/marimo when the user is writing code |
| Exploratory data analysis patterns | `analyzing-data` | EDA methodology (profiling, statistical tests) belongs there |
| Choosing visualization libraries | `analyzing-data` | Chart types and library deep-dives are covered there |
| Production ML feature engineering | `engineering-ml-features` | Feature engineering logic is domain-specific |
| Model evaluation and cross-validation | `evaluating-ml-models` | Model comparison and metrics belong there |

### Quick boundary check

- **Data app** = deployed web interface with widgets, accessed via URL, used by non-coders
- **Notebook** = code cells + outputs, run interactively by people who write code
- If the user mentions "dashboard," "app," "users clicking buttons," or "share with stakeholders" → use this skill
- If the user mentions "notebook," "Jupyter," "analysis," or "explore data interactively" → use `working-in-notebooks`

## Tool selection guide

### Quick decision checklist

| Question | If yes, consider |
|----------|------------------|
| Need the simplest possible API? | **Streamlit** |
| Need ML model sharing with built-in hosting? | **Gradio** |
| Need complex reactive dashboards with flexible layouts? | **Panel** |
| Need production-grade control + React ecosystem? | **Dash** |
| Need native-like UI with async support? | **NiceGUI** |
| Deploying to Hugging Face Spaces? | **Gradio or Streamlit** |
| Already using HoloViz ecosystem (hvPlot, HoloViews)? | **Panel** |
| Need desktop + web from same codebase? | **NiceGUI** |

### Framework comparison

| Framework | Best For | Key Strength | Deployment |
|-----------|----------|--------------|------------|
| **Streamlit** | Rapid prototyping, ML demos | Simplest API, largest community | Streamlit Cloud, Docker |
| **Panel** | Complex dashboards, reactive UIs | Flexible layouts, Jupyter integration | Panel serve, Cloud Run |
| **Gradio** | ML model demos, quick sharing | Built-in sharing, Hugging Face integration | Spaces, self-hosted |
| **Dash** | Production dashboards, fine control | React backend, extensive components | Gunicorn, cloud platforms |
| **NiceGUI** | Desktop + web apps, async workflows | Native-like UI, modern Python async | Native, Docker, cloud |

## Core workflow: Building a data app

### Step 1: Choose your framework

See the decision checklist above. For most ML demos and simple dashboards → Streamlit. For complex reactive layouts → Panel. For quick ML model sharing → Gradio.

### Step 2: Set up the project structure

```
my-app/
├── app.py              # Main entry point
├── requirements.txt    # Dependencies
├── .env               # Environment variables (not committed)
├── data/              # Data files
└── utils/             # Helper modules
```

### Step 3: Build the minimum viable app

Start with one widget and one output. Test with real users before adding complexity.

### Step 4: Handle secrets properly

```python
# ✅ Use environment variables or framework secrets
import os
api_key = os.environ.get("OPENAI_API_KEY")

# Streamlit specific:
# api_key = st.secrets["openai_api_key"]

# ❌ Never hardcode secrets
# api_key = "sk-abc123..."
```

### Step 5: Add caching for performance

See framework-specific advanced references for caching patterns.

### Step 6: Deploy

Choose based on your needs: free tier (Streamlit Cloud, Hugging Face Spaces), containerized (Docker), or enterprise cloud (AWS, GCP, Azure).

## Quick start: Streamlit

```python
# app.py
import streamlit as st
import pandas as pd
import plotly.express as px

st.title("Sales Dashboard")

# Sidebar controls
region = st.sidebar.selectbox("Region", ["All", "North", "South", "East", "West"])

# Load data (use caching in production)
df = pd.read_parquet("sales.parquet")
if region != "All":
    df = df[df['region'] == region]

# Metrics row
col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${df['sales'].sum():,.0f}")
col2.metric("Orders", len(df))
col3.metric("Avg Order", f"${df['sales'].mean():.2f}")

# Visualization
fig = px.line(df.groupby('date')['sales'].sum().reset_index(), x='date', y='sales')
st.plotly_chart(fig, use_container_width=True)

# Data table
st.dataframe(df.head(100))
```

Run: `streamlit run app.py`

## Quick start: Gradio

```python
import gradio as gr
from transformers import pipeline

# Load model (example: sentiment analysis)
classifier = pipeline("sentiment-analysis")

def predict(text):
    result = classifier(text)[0]
    return result['label'], result['score']

interface = gr.Interface(
    fn=predict,
    inputs=gr.Textbox(lines=2, placeholder="Enter text..."),
    outputs=[gr.Label(label="Sentiment"), gr.Number(label="Confidence")],
    title="Sentiment Analysis",
    description="Enter text to analyze sentiment",
    examples=["I love this!", "This is terrible."]
)

interface.launch()
```

## Quick start: Panel

```python
import panel as pn
import hvplot.pandas
import pandas as pd

pn.extension()

df = pd.read_parquet("data.parquet")

# Widgets
region = pn.widgets.Select(name='Region', options=['All'] + df['region'].unique().tolist())
metric = pn.widgets.RadioBoxGroup(name='Metric', options=['sales', 'profit', 'units'])

# Reactive function
@pn.depends(region, metric)
def plot(region, metric):
    data = df if region == 'All' else df[df['region'] == region]
    return data.hvplot.line(x='date', y=metric, title=f'{metric.title()} by Date')

# Layout
app = pn.Column(
    "# Sales Dashboard",
    pn.Row(region, metric),
    plot
)

app.servable()
```

Run: `panel serve app.py --show`

## Quick start: Dash

```python
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px

app = Dash(__name__)

df = px.data.gapminder()

app.layout = html.Div([
    html.H1("Dashboard"),
    dcc.Dropdown(df.country.unique(), 'Canada', id='country'),
    dcc.Graph(id='graph')
])

@callback(
    Output('graph', 'figure'),
    Input('country', 'value')
)
def update_graph(country):
    return px.line(df[df.country == country], x='year', y='pop')

if __name__ == '__main__':
    app.run(debug=True)
```

## Quick start: NiceGUI

```python
from nicegui import ui
import pandas as pd

df = pd.read_parquet("data.parquet")

# Simple UI with async support
async def load_data():
    ui.notify(f"Loaded {len(df)} rows")
    table.update_rows(df.head(20).to_dict('records'))

ui.label('Sales Dashboard').classes('text-2xl font-bold')

ui.button('Load Data', on_click=load_data)

table = ui.table(
    columns=[{'name': col, 'label': col} for col in df.columns],
    rows=[]
).classes('w-full')

ui.run()
```

## Core design principles

### 1) Start simple, iterate

- MVP with one widget + one visualization
- Add complexity only when needed
- Test with real users early

### 2) Optimize for the audience

| Audience | Approach |
|----------|----------|
| Executives | Key metrics, simple filters, clean layout |
| Data scientists | Raw data access, parameter tuning, debug info |
| Operations | Refresh buttons, alerts, mobile-friendly |

### 3) Handle state carefully

Each framework has different state management:

```python
# Streamlit: session_state for persistence
if 'counter' not in st.session_state:
    st.session_state.counter = 0

# Dash: dcc.Store component
# Panel: param.Parameter with @depends
# NiceGUI: reactive variables or app.storage
```

### 4) Never expose secrets

```python
# ✅ Use environment variables
import os
api_key = os.environ.get("OPENAI_API_KEY")

# Streamlit Cloud specific:
# api_key = st.secrets["openai_api_key"]

# ❌ Never hardcode
# api_key = "sk-..."
```

## Validation and feedback loop

### Self-check questions

Before deploying:

1. [ ] Can a non-technical user understand the interface?
2. [ ] Are secrets loaded from environment variables?
3. [ ] Is data cached appropriately (no repeated loading)?
4. [ ] Are there loading states for slow operations?
5. [ ] Does it handle edge cases (empty data, errors)?
6. [ ] Is it responsive on mobile if needed?
7. [ ] Are dependencies pinned in requirements.txt?

### Performance checklist

- [ ] Data loading is cached or happens once
- [ ] Long computations don't block the UI
- [ ] Large datasets use pagination or sampling
- [ ] Images/assets are optimized

## Progressive disclosure

### Core references (in this skill)

- `references/streamlit-advanced.md` — Caching, multipage apps, secrets, custom components
- `references/panel-advanced.md` — Parameterized classes, reactive programming, layouts
- `references/gradio-advanced.md` — Interface types, custom components, Hugging Face Spaces
- `references/dash-advanced.md` — Callback patterns, state management, production deployment
- `references/nicegui-guide.md` — Core concepts, async patterns, desktop vs web
- `references/framework-selection.md` — Detailed comparison, decision framework, migration paths
- `references/deployment-patterns.md` — Streamlit Cloud, Hugging Face, Docker, cloud platforms

### Related skills

| Skill | Relationship | When to use |
|-------|--------------|-------------|
| `working-in-notebooks` | Distinct boundary | Creating analysis notebooks for coders — **not** stakeholder apps |
| `analyzing-data` | Complementary | EDA patterns, visualization library selection |
| `evaluating-ml-models` | Complementary | Model metrics and comparison for app display |
| `engineering-ml-features` | Complementary | Feature engineering behind app predictions |

## Common anti-patterns

- ❌ Loading data on every interaction (use caching)
- ❌ Blocking the UI with long computations (use async or progress indicators)
- ❌ No error handling for edge cases (empty data, network failures)
- ❌ Hardcoded file paths or credentials (use environment variables)
- ❌ Too many widgets (cognitive overload — prioritize)
- ❌ No mobile consideration when audience uses phones
- ❌ Confusing notebook code with app code (different paradigms)

## External resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Panel Documentation](https://panel.holoviz.org/)
- [Gradio Documentation](https://www.gradio.app/docs)
- [Dash Documentation](https://dash.plotly.com/)
- [NiceGUI Documentation](https://nicegui.io/)
- [Hugging Face Spaces](https://huggingface.co/spaces)
