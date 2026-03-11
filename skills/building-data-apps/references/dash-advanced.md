# Dash Advanced Patterns

## Callback Patterns and State Management

### Basic callback

```python
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px

app = Dash(__name__)

df = px.data.gapminder()

app.layout = html.Div([
    dcc.Dropdown(df.country.unique(), 'Canada', id='country'),
    dcc.Graph(id='graph')
])

@callback(
    Output('graph', 'figure'),
    Input('country', 'value')
)
def update_graph(country):
    return px.line(df[df.country == country], x='year', y='pop')
```

### Multiple inputs and outputs

```python
from dash import Dash, html, dcc, callback, Output, Input

app = Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(['A', 'B'], 'A', id='category'),
    dcc.Slider(0, 10, 1, value=5, id='threshold'),
    html.Div(id='summary'),
    dcc.Graph(id='chart')
])

@callback(
    Output('summary', 'children'),
    Output('chart', 'figure'),
    Input('category', 'value'),
    Input('threshold', 'value')
)
def update(category, threshold):
    summary_text = f"Category: {category}, Threshold: {threshold}"
    fig = create_chart(category, threshold)
    return summary_text, fig
```

### PreventUpdate for conditional updates

```python
from dash import Dash, html, dcc, callback, Output, Input
from dash.exceptions import PreventUpdate

@callback(
    Output('output', 'children'),
    Input('input', 'value')
)
def conditional_update(value):
    if value is None or len(value) < 3:
        raise PreventUpdate  # Don't update output
    return process(value)
```

### no_update for partial updates

```python
from dash import Dash, html, dcc, callback, Output, Input, no_update

@callback(
    Output('graph1', 'figure'),
    Output('graph2', 'figure'),
    Input('dropdown', 'value')
)
def update(value):
    if value == 'A':
        return create_fig_a(), no_update  # Don't update graph2
    return create_fig_b(), create_fig_c()
```

### State for non-triggering inputs

```python
from dash import Dash, html, dcc, callback, Output, Input, State

app.layout = html.Div([
    dcc.Input(id='username', placeholder='Username'),
    dcc.Input(id='password', type='password', placeholder='Password'),
    html.Button('Login', id='login-btn', n_clicks=0),
    html.Div(id='output')
])

@callback(
    Output('output', 'children'),
    Input('login-btn', 'n_clicks'),
    State('username', 'value'),
    State('password', 'value'),
    prevent_initial_call=True
)
def login(n_clicks, username, password):
    if authenticate(username, password):
        return f"Welcome, {username}!"
    return "Invalid credentials"
```

### dcc.Store for client-side state

```python
from dash import Dash, html, dcc, callback, Output, Input

app.layout = html.Div([
    # Store data in browser
    dcc.Store(id='store-data', storage_type='memory'),  # 'memory', 'local', or 'session'
    
    dcc.Dropdown(['A', 'B'], id='selection'),
    html.Button('Save', id='save-btn'),
    html.Div(id='display')
])

@callback(
    Output('store-data', 'data'),
    Input('save-btn', 'n_clicks'),
    Input('selection', 'value'),
    prevent_initial_call=True
)
def save_data(n_clicks, selection):
    return {'selection': selection, 'timestamp': time.time()}

@callback(
    Output('display', 'children'),
    Input('store-data', 'data')
)
def display_data(data):
    if data is None:
        return "No data saved"
    return f"Saved: {data['selection']} at {data['timestamp']}"
```

## Layout Components

### Core HTML components

```python
from dash import html

html.Div([
    html.H1('Title'),
    html.H2('Subtitle'),
    html.P('Paragraph text'),
    html.Div([
        html.Span('Inline text'),
        html.Br(),  # Line break
        html.A('Link', href='https://example.com'),
        html.Img(src='/assets/image.png')
    ], className='container'),
    html.Ul([html.Li('Item 1'), html.Li('Item 2')]),  # List
    html.Table([
        html.Thead(html.Tr([html.Th('Col1'), html.Th('Col2')])),
        html.Tbody([
            html.Tr([html.Td('A'), html.Td('B')]),
            html.Tr([html.Td('C'), html.Td('D')])
        ])
    ])
])
```

### Dash Core Components (dcc)

```python
from dash import dcc

# Inputs
dcc.Input(type='text', placeholder='Enter text...')
dcc.Input(type='number', min=0, max=100, step=1)
dcc.Textarea(placeholder='Multi-line text')

# Selections
dcc.Dropdown(
    options=[{'label': 'A', 'value': 'a'}, {'label': 'B', 'value': 'b'}],
    value='a',
    multi=False
)
dcc.RadioItems(options=[{'label': 'X', 'value': 'x'}], value='x')
dcc.Checklist(options=[{'label': 'Y', 'value': 'y'}], value=['y'])
dcc.Slider(min=0, max=10, step=1, value=5, marks={0: '0', 10: '10'})
dcc.RangeSlider(min=0, max=100, value=[20, 80])

# Date
dcc.DatePickerSingle(date=date.today())
dcc.DatePickerRange(start_date=date.today(), end_date=date.today())

# Upload
dcc.Upload(
    children=html.Div(['Drag and Drop or ', html.A('Select File')]),
    style={
        'width': '100%', 'height': '60px', 'lineHeight': '60px',
        'borderWidth': '1px', 'borderStyle': 'dashed',
        'borderRadius': '5px', 'textAlign': 'center'
    }
)

# Other
dcc.Markdown('''# Markdown support
- Lists
- **Bold** and *italic*
''')
dcc.ConfirmDialog(message='Are you sure?', displayed=False)
dcc.Loading(html.Div(id='loading-output'))
```

### Multi-page apps with dash.page_container

```python
# app.py
from dash import Dash, html, dcc
import dash

app = Dash(__name__, use_pages=True, pages_folder='pages')

app.layout = html.Div([
    html.H1('Multi-page App'),
    html.Div([
        dcc.Link(page['name'], href=page['relative_path'])
        for page in dash.page_registry.values()
    ], style={'display': 'flex', 'gap': '10px'}),
    dash.page_container  # Renders the current page
])

if __name__ == '__main__':
    app.run(debug=True)

# pages/home.py
import dash
from dash import html

dash.register_page(__name__, path='/')

layout = html.Div('Home page content')

# pages/analysis.py
import dash
from dash import html

dash.register_page(__name__, path='/analysis')

layout = html.Div('Analysis page content')
```

## Pattern-matching callbacks

For dynamic number of components:

```python
from dash import Dash, html, dcc, callback, Output, Input, State, MATCH, ALL

app = Dash(__name__)

app.layout = html.Div([
    html.Button('Add Filter', id='add-filter', n_clicks=0),
    html.Div(id='filters-container'),
    html.Div(id='output')
])

@callback(
    Output('filters-container', 'children'),
    Input('add-filter', 'n_clicks'),
    State('filters-container', 'children'),
    prevent_initial_call=True
)
def add_filter(n_clicks, children):
    new_filter = html.Div([
        dcc.Dropdown(
            ['A', 'B', 'C'],
            id={'type': 'filter-dropdown', 'index': n_clicks}
        ),
        html.Button('X', id={'type': 'remove-btn', 'index': n_clicks})
    ])
    children = children or []
    return children + [new_filter]

# Match all dropdowns with type='filter-dropdown'
@callback(
    Output('output', 'children'),
    Input({'type': 'filter-dropdown', 'index': ALL}, 'value')
)
def update_output(values):
    return f"Selected: {values}"
```

## Clientside callbacks (JavaScript)

For fast, client-side updates without server roundtrip:

```python
from dash import Dash, html, dcc, clientside_callback, Input, Output

app = Dash(__name__)

app.layout = html.Div([
    dcc.Input(id='input', value='Hello'),
    html.Div(id='output')
])

clientside_callback(
    """
    function(value) {
        return value.toUpperCase();
    }
    """,
    Output('output', 'children'),
    Input('input', 'value')
)
```

## Styling with Dash

### Inline styles

```python
html.Div(
    'Content',
    style={
        'backgroundColor': '#f0f0f0',
        'padding': '20px',
        'borderRadius': '5px'
    }
)
```

### External CSS

```python
app = Dash(__name__, external_stylesheets=['https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css'])
```

### Assets folder

Place CSS files in `assets/` folder:

```
project/
├── app.py
└── assets/
    └── style.css
```

```css
/* assets/style.css */
.custom-class {
    background-color: #f0f0f0;
    padding: 20px;
}
```

### Dash Bootstrap Components

```python
import dash_bootstrap_components as dbc

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1('Title'), width=12)
    ]),
    dbc.Row([
        dbc.Col(dbc.Card([dbc.CardBody('Content')]), width=6),
        dbc.Col(dbc.Card([dbc.CardBody('Content')]), width=6)
    ]),
    dbc.Button('Click me', color='primary', className='mt-3')
], fluid=True)
```

## Deployment with Gunicorn

### Basic Procfile

```
web: gunicorn app:server
```

### requirements.txt

```
dash
gunicorn
pandas
plotly
```

### app.py for deployment

```python
from dash import Dash, html

app = Dash(__name__)
server = app.server  # Expose Flask server for Gunicorn

app.layout = html.Div('Hello World')

if __name__ == '__main__':
    app.run(debug=False)
```

### Running locally with Gunicorn

```bash
gunicorn app:server -b :8050 -w 4
```

### Docker deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8050

CMD ["gunicorn", "app:server", "-b", "0.0.0.0:8050", "-w", "4"]
```

## Production Considerations

### Disable debug mode

```python
if __name__ == '__main__':
    app.run(debug=False)  # Never use debug=True in production
```

### Environment variables

```python
import os

debug = os.environ.get('DASH_DEBUG', 'False').lower() == 'true'
app.run(debug=debug)
```

### Error handling

```python
from dash import callback, Output, Input

@callback(
    Output('output', 'children'),
    Input('input', 'value')
)
def safe_update(value):
    try:
        result = process(value)
        return result
    except Exception as e:
        return html.Div(f"Error: {str(e)}", style={'color': 'red'})
```

### Long callbacks (background tasks)

```python
from dash import Dash, html, dcc, callback, Output, Input
from dash.long_callback import DiskcacheLongCallbackManager
import diskcache

cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

app = Dash(__name__, long_callback_manager=long_callback_manager)

app.layout = html.Div([
    html.Button('Run', id='run-btn'),
    html.Div(id='output'),
    html.Div(id='progress')
])

@callback(
    Output('output', 'children'),
    Input('run-btn', 'n_clicks'),
    running=[(Output('run-btn', 'disabled'), True, False)],
    progress=Output('progress', 'children'),
    prevent_initial_call=True
)
def long_task(set_progress, n_clicks):
    for i in range(10):
        time.sleep(1)
        set_progress(f"Progress: {i+1}/10")
    return "Complete!"
```

## Testing Dash apps

```python
from dash.testing.application_runners import import_app

def test_app(dash_duo):
    app = import_app('app')
    dash_duo.start_server(app)
    
    # Find element
    dash_duo.find_element('#input')
    
    # Input text
    dash_duo.clear_input('#input')
    dash_duo.send_keys_to_element('#input', 'test')
    
    # Wait for output
    dash_duo.wait_for_text_to_equal('#output', 'expected', timeout=10)
    
    # Take screenshot
    dash_duo.take_snapshot('test')
```

## Best Practices

1. **Use `prevent_initial_call=True`** for callbacks that shouldn't run on page load
2. **Minimize callback inputs** — only trigger when necessary
3. **Use `dcc.Store`** for client-side state to reduce server load
4. **Use `no_update`** and `PreventUpdate` to avoid unnecessary updates
5. **Structure multi-page apps** with `dash.register_page()`
6. **Use Bootstrap or other CSS frameworks** for responsive layouts
7. **Always disable debug mode** in production
8. **Use Gunicorn with multiple workers** for production deployments
9. **Add error handling** in callbacks for robustness
