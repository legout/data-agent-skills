# Streamlit Advanced Patterns

## Caching Strategies

Streamlit provides two caching decorators for different use cases:

### @st.cache_data — for data

Use for DataFrames, arrays, or any serializable data that changes.

```python
import streamlit as st
import pandas as pd

@st.cache_data
def load_data(path):
    """Cache data loading — invalidated when path changes."""
    return pd.read_parquet(path)

@st.cache_data(ttl=3600)  # Refresh after 1 hour
def fetch_api_data(endpoint):
    """Cache API calls with TTL."""
    import requests
    return requests.get(endpoint).json()

@st.cache_data(persist=True)  # Persist to disk
def expensive_computation(query):
    """Cache survives app restarts."""
    return run_expensive_query(query)
```

### @st.cache_resource — for resources

Use for ML models, database connections, or other non-serializable resources.

```python
import streamlit as st
import pickle
from transformers import pipeline

@st.cache_resource
def load_model():
    """Model loaded once, reused across sessions."""
    return pipeline("sentiment-analysis")

@st.cache_resource
def get_database_connection():
    """Database connection persisted across reruns."""
    import psycopg2
    return psycopg2.connect(os.environ["DATABASE_URL"])
```

### Cache clear patterns

```python
# Clear specific cache
load_data.clear()

# Clear all data caches
st.cache_data.clear()

# Clear all resource caches
st.cache_resource.clear()
```

## Multipage Apps

Structure for larger applications:

```
my-app/
├── streamlit_app.py      # Entry point
├── pages/
│   ├── 1_📊_Dashboard.py
│   ├── 2_🤖_Model_Demo.py
│   └── 3_⚙️_Settings.py
├── utils/
│   └── helpers.py
└── data/
    └── sample.parquet
```

### Entry point (streamlit_app.py)

```python
import streamlit as st

st.set_page_config(
    page_title="My App",
    page_icon="🚀",
    layout="wide"
)

st.title("Welcome")
st.sidebar.success("Select a page above.")
```

### Page file (pages/1_📊_Dashboard.py)

```python
import streamlit as st

st.set_page_config(page_title="Dashboard")
st.title("📊 Dashboard")

# Page-specific content
data = load_data()
st.dataframe(data)
```

### Programmatic page navigation

```python
# Navigate to another page
st.switch_page("pages/2_🤖_Model_Demo.py")

# Create sidebar links
st.page_link("pages/1_📊_Dashboard.py", label="Dashboard")
```

## Secrets Management

### Local development (.streamlit/secrets.toml)

```toml
# .streamlit/secrets.toml (add to .gitignore!)
openai_api_key = "sk-..."
[database]
host = "localhost"
port = 5432
password = "secret"
```

```python
import streamlit as st

# Access secrets
api_key = st.secrets["openai_api_key"]
db_host = st.secrets["database"]["host"]

# Or use dot notation
db_config = st.secrets.database
```

### Streamlit Cloud deployment

1. Go to your app dashboard
2. Click "Manage app" → "Secrets"
3. Add secrets as TOML:

```toml
openai_api_key = "sk-..."
```

### Environment variable fallback

```python
import os
import streamlit as st

# Try secrets first, fall back to env vars
try:
    api_key = st.secrets["openai_api_key"]
except KeyError:
    api_key = os.environ.get("OPENAI_API_KEY")
```

## Session State

### Basic usage

```python
import streamlit as st

# Initialize
if 'counter' not in st.session_state:
    st.session_state.counter = 0

# Use
st.session_state.counter += 1
st.write(f"Count: {st.session_state.counter}")

# Reset
if st.button("Reset"):
    st.session_state.counter = 0
    st.rerun()
```

### Callback pattern

```python
import streamlit as st

if 'items' not in st.session_state:
    st.session_state.items = []

def add_item():
    st.session_state.items.append(st.session_state.new_item)
    st.session_state.new_item = ""  # Clear input

st.text_input("New item", key="new_item", on_change=add_item)
st.write(st.session_state.items)
```

### Widget state persistence

```python
# Widget values are automatically stored in session_state by their key
name = st.text_input("Name", key="user_name")
# Value accessible as st.session_state.user_name
```

## Custom Components

### Popular community components

```bash
pip install streamlit-aggrid streamlit-echarts streamlit-lottie
```

```python
from st_aggrid import AgGrid, GridOptionsBuilder
import streamlit_echarts

# AgGrid for advanced tables
df = load_data()
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination()
gb.configure_selection('multiple')
AgGrid(df, gridOptions=gb.build())

# ECharts for advanced visualizations
options = {
    "xAxis": {"type": "category", "data": ["Mon", "Tue", "Wed"]},
    "yAxis": {"type": "value"},
    "series": [{"data": [120, 200, 150], "type": "bar"}]
}
streamlit_echarts.st_echarts(options)
```

### Creating custom components

For custom JavaScript/TypeScript components, see:
- [Streamlit Components API](https://docs.streamlit.io/develop/concepts/custom-components/intro)
- [Component template](https://github.com/streamlit/component-template)

## Layout Patterns

### Columns

```python
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.line_chart(data)

with col2:
    st.metric("Revenue", "$12M")
    st.metric("Growth", "+8%")

with col3:
    st.button("Refresh")
    st.download_button("Export", data.to_csv())
```

### Tabs

```python
tab1, tab2 = st.tabs(["Chart", "Data"])

with tab1:
    st.line_chart(data)

with tab2:
    st.dataframe(data)
```

### Expander

```python
with st.expander("Advanced Options"):
    threshold = st.slider("Threshold", 0, 100, 50)
    method = st.selectbox("Method", ["A", "B", "C"])
```

### Sidebar

```python
with st.sidebar:
    st.header("Settings")
    model = st.selectbox("Model", ["gpt-4", "gpt-3.5"])
    temperature = st.slider("Temperature", 0.0, 1.0, 0.7)
```

### Containers

```python
# Dynamic content container
placeholder = st.empty()

# Later in code...
placeholder.info("Processing...")
# ... work happens ...
placeholder.success("Done!")
```

## Progress and Status

```python
import time

# Progress bar
progress_bar = st.progress(0)
for i in range(100):
    time.sleep(0.1)
    progress_bar.progress(i + 1)

# Status messages
st.info("This is informational")
st.warning("This is a warning")
st.error("This is an error")
st.success("This succeeded!")
st.spinner("Loading...")  # Context manager

with st.spinner("Training model..."):
    train_model()
```

## Forms

```python
with st.form("my_form"):
    name = st.text_input("Name")
    email = st.text_input("Email")
    submitted = st.form_submit_button("Submit")
    
    if submitted:
        save_to_database(name, email)
        st.success("Saved!")
```

## Performance Tips

1. **Use caching** — Always cache data loading and model initialization
2. **Limit data** — Use pagination or sampling for large datasets
3. **Defer loading** — Use `st.spinner()` for slow operations
4. **Optimize charts** — Use `use_container_width=True` and limit data points
5. **Avoid nested reruns** — Structure callbacks to minimize reruns

## Deployment Checklist

- [ ] Secrets configured in Streamlit Cloud (or environment)
- [ ] requirements.txt includes all dependencies
- [ ] No local file paths (use relative or cloud storage)
- [ ] Caching configured for data and models
- [ ] Error handling for edge cases
- [ ] README with setup instructions
