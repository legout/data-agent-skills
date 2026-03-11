# Panel Advanced Patterns

## Parameterized Classes

Panel's core pattern is parameterized classes that automatically generate widgets and reactive updates.

### Basic Parameterized class

```python
import param
import panel as pn
import pandas as pd

pn.extension()

class DataExplorer(param.Parameterized):
    """Parameterized dashboard with automatic widget generation."""
    
    # Parameters automatically create widgets
    region = param.Selector(default='All', objects=['All', 'North', 'South'])
    metric = param.Selector(default='sales', objects=['sales', 'profit'])
    smoothing = param.Number(default=0, bounds=(0, 10))
    
    def __init__(self, data, **params):
        self.data = data
        super().__init__(**params)
    
    @param.depends('region', 'metric', 'smoothing')
    def plot(self):
        """Automatically updates when parameters change."""
        df = self.data if self.region == 'All' else self.data[self.data.region == self.region]
        
        if self.smoothing > 0:
            df = df.rolling(window=self.smoothing).mean()
        
        return df.hvplot.line(x='date', y=self.metric, title=f'{self.metric.title()} Over Time')
    
    @param.depends('region')
    def stats(self):
        """Statistics panel that updates with region."""
        df = self.data if self.region == 'All' else self.data[self.data.region == self.region]
        return pn.pane.Markdown(f"""
        ### Statistics for {self.region}
        - Total: ${df[self.metric].sum():,.0f}
        - Mean: ${df[self.metric].mean():.2f}
        - Count: {len(df):,}
        """)

# Usage
df = pd.read_parquet('data.parquet')
explorer = DataExplorer(df)

# Layout with parameter widgets automatically generated
app = pn.Column(
    '# Data Explorer',
    pn.Row(
        pn.panel(explorer.param),  # Auto-generated widgets
        pn.Column(explorer.plot, explorer.stats)
    )
)

app.servable()
```

### Parameter types reference

```python
import param
import datetime

class ParametersDemo(param.Parameterized):
    # Numeric
    integer = param.Integer(default=5, bounds=(0, 100))
    number = param.Number(default=3.14, bounds=(0, 10))
    
    # Selection
    choice = param.Selector(default='A', objects=['A', 'B', 'C'])
    multi = param.ListSelector(default=['A'], objects=['A', 'B', 'C'])
    
    # Boolean
    flag = param.Boolean(default=True)
    
    # Text
    text = param.String(default='Hello')
    
    # Dates
    date = param.Date(default=datetime.date.today())
    
    # Color
    color = param.Color(default='#ff0000')
    
    # File path
    path = param.Filename()
    
    # Dynamic objects
    dynamic = param.Dynamic(default=1)  # Can be any type
```

## Reactive Programming Patterns

### @pn.depends decorator

```python
import panel as pn
import hvplot.pandas

pn.extension()

df = pd.read_parquet('sales.parquet')

# Widgets
region_widget = pn.widgets.Select(name='Region', options=['All'] + df.region.unique().tolist())
metric_widget = pn.widgets.RadioBoxGroup(name='Metric', options=['sales', 'profit'])

@pn.depends(region_widget, metric_widget)
def update_plot(region, metric):
    """Function re-runs when either widget changes."""
    data = df if region == 'All' else df[df.region == region]
    return data.hvplot.line(x='date', y=metric)

@pn.depends(region_widget)
def update_stats(region):
    data = df if region == 'All' else df[df.region == region]
    return pn.pane.Markdown(f"Total: ${data.sales.sum():,.0f}")

app = pn.Column(
    pn.Row(region_widget, metric_widget),
    update_plot,
    update_stats
)

app.servable()
```

### bind() for functional reactivity

```python
import panel as pn

# Bind function to widgets
slider = pn.widgets.FloatSlider(name='Value', start=0, end=100)
text = pn.widgets.TextInput(name='Format', value='Value: {value:.2f}')

def format_value(value, template):
    return template.format(value=value)

# bind creates a reactive pane
reactive_text = pn.bind(format_value, slider, text)

app = pn.Column(slider, text, reactive_text)
app.servable()
```

### Reactive expressions (pn.rx)

```python
import panel as pn
import pandas as pd

pn.extension()

df = pd.read_parquet('data.parquet')

# Create reactive widgets
region = pn.widgets.Select(name='Region', options=['All'] + df.region.unique().tolist())

# Create a reactive reference to the widget value
region_rx = pn.rx(lambda: region.value)

# Reactive filtering - properly react to widget value changes
filtered_df = pn.rx(lambda: df if region.value == 'All' else df[df.region == region.value])

# Alternative using bind for cleaner syntax
def filter_data(selected_region):
    return df if selected_region == 'All' else df[df.region == selected_region]

filtered_df = pn.bind(filter_data, region)

# Reactive aggregation
@pn.depends(region)
def show_total():
    data = filter_data(region.value)
    return pn.pane.Markdown(f"Total: ${data.sales.sum():,.0f}")

app = pn.Column(
    region,
    show_total
)
```

## Layout and Templating

### Built-in templates

```python
import panel as pn

pn.extension()

# FastGridTemplate for dashboard layouts
template = pn.template.FastGridTemplate(
    title='Analytics Dashboard',
    sidebar=['# Settings', widget1, widget2],
    theme='dark'
)

template.main[
    0:3, 0:6  # Row slice, Column slice
] = plot1
template.main[
    0:3, 6:12
] = plot2
template.main[
    3:6, 0:12
] = table

template.servable()
```

### Custom layouts

```python
import panel as pn

# Accordion
accordion = pn.Accordion(
    ('Section 1', pn.pane.Markdown('Content 1')),
    ('Section 2', pn.pane.Markdown('Content 2'))
)

# Tabs
tabs = pn.Tabs(
    ('Plot', plot_pane),
    ('Data', table_pane),
    ('Settings', settings_column)
)

# Card with header
card = pn.Card(
    content,
    title='Metrics',
    collapsed=True  # Collapsible
)

# Column with scroll
col = pn.Column(
    *many_items,
    scroll=True,
    height=400
)

# GridBox
grid = pn.GridBox(
    *plots,
    ncols=3
)
```

### Template gallery

```python
# MaterialTemplate
pn.template.MaterialTemplate(title='My App')

# BootstrapTemplate
pn.template.BootstrapTemplate(title='My App')

# ReactTemplate (grid-based)
pn.template.ReactTemplate(title='My App')

# FastListTemplate (sidebar + main)
pn.template.FastListTemplate(title='My App')

# GoldenTemplate (multi-pane)
pn.template.GoldenTemplate(title='My App')
```

## Linking Plots and Widgets

### Linked brushing

```python
import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd

pn.extension()
hv.extension('bokeh')

df = pd.read_parquet('large_dataset.parquet')

# Create linked plots
scatter = hv.Scatter(df, 'x', 'y')
hist = hv.Histogram(df, 'value')

# Link selection
link_selections = hv.link_selections.instance()
linked_scatter = link_selections(scatter)
linked_hist = link_selections(hist)

app = pn.Column(linked_scatter, linked_hist)
```

### Cross-filtering with param

```python
import param
import panel as pn

class CrossFilter(param.Parameterized):
    category = param.Selector(objects=['A', 'B', 'C'])
    
    def __init__(self, data, **params):
        self.data = data
        super().__init__(**params)
    
    @param.depends('category')
    def filtered_data(self):
        return self.data[self.data.category == self.category]
    
    @param.depends('filtered_data')
    def plot1(self):
        return self.filtered_data().hvplot.scatter(x='x', y='y')
    
    @param.depends('filtered_data')
    def plot2(self):
        return self.filtered_data().hvplot.hist('value')

filter_app = CrossFilter(df)
app = pn.Column(
    filter_app.param,
    pn.Row(filter_app.plot1, filter_app.plot2)
)
```

## Deploying with panel serve

### Basic serve

```bash
# Serve a script
panel serve app.py

# Serve with auto-reload (development)
panel serve app.py --autoreload

# Serve with show
panel serve app.py --show

# Serve on specific port
panel serve app.py --port 5006

# Serve multiple apps
panel serve app1.py app2.py

# Serve directory
panel serve apps/
```

### Serve options

```bash
# Production deployment example
panel serve app.py \
    --port 5006 \
    --address 0.0.0.0 \
    --allow-websocket-origin myapp.example.com \
    --num-procs 4 \
    --ssl-certfile cert.pem \
    --ssl-keyfile key.pem
```

> **⚠️ Security Note:** Always use explicit hostnames for `--allow-websocket-origin` in production (e.g., `myapp.example.com`). Using wildcards (`*`) is only acceptable for local development as it weakens CSRF/origin protections.

### Programmatic serving

```python
import panel as pn

# In the script
if __name__.startswith('bokeh'):
    # Running under panel serve
    pass

# Or check
def is_servable():
    return __name__.startswith('bokeh') or __name__ == '__main__'
```

## Serving with Bokeh server directly

```python
# For embedded deployment
from bokeh.server.server import Server
from tornado.ioloop import IOLoop

def modify_doc(doc):
    app = create_app()
    doc.add_root(app)

server = Server({'/app': modify_doc}, num_procs=1)
server.start()

if __name__ == '__main__':
    IOLoop.current().start()
```

## Exporting to static HTML

```python
import panel as pn

app = create_app()

# Save as HTML
app.save('output.html', embed=True)

# Embed state
app.embed_state('./embed_states/')
```

## Performance Optimization

### Datashader for large datasets

```python
import datashader as ds
import holoviews.operation.datashader as hd

# Automatically rasterize large scatter plots
rasterized = hd.rasterize(hv.Scatter(df, 'x', 'y'))
```

### Lazy loading

```python
import panel as pn

# Lazy load expensive components
@pn.cache
def expensive_plot():
    return create_expensive_plot()

# Only computes when first accessed
plot = pn.panel(expensive_plot)
```

### Debouncing inputs

```python
import panel as pn

text = pn.widgets.TextInput(value='Initial')
# Debounce: only trigger after user stops typing
text.param.value.throttle = 500  # milliseconds
```

## Debugging Tips

```python
import panel as pn

# Show param values
pn.panel(obj.param).servable()

# Print param changes
def print_change(event):
    print(f"{event.name} changed to {event.new}")

obj.param.watch(print_change, 'region')

# Param print all
print(obj.param.values())
```
