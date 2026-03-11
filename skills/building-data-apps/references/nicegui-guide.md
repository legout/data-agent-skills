# NiceGUI Guide

NiceGUI is a Python UI framework that creates both web and desktop applications with a simple, intuitive API. It uses Vue.js and Quasar on the frontend, providing modern UI components.

## Core Concepts

### Hello World

```python
from nicegui import ui

ui.label('Hello World!')
ui.button('Click me', on_click=lambda: ui.notify('Clicked!'))

ui.run()
```

Run with `python app.py` and open `http://localhost:8080`.

### The ui.* namespace

NiceGUI provides UI elements through the `ui` module:

```python
from nicegui import ui

# Text elements
ui.label('Plain text')
ui.html('<strong>HTML content</strong>')
ui.markdown('# Markdown support')

# Input elements
ui.input('Name', placeholder='Enter name')
ui.number('Age', value=25)
ui.slider(min=0, max=100, value=50)
ui.select(['Option 1', 'Option 2'], label='Choose')
ui.checkbox('Agree to terms')
ui.switch('Enable feature')

# Buttons
ui.button('Submit', on_click=lambda: ui.notify('Submitted'))
ui.button('Primary', color='primary')
ui.button('With icon', icon='favorite')

# Display data
ui.table(columns=[{'name': 'col1', 'label': 'Column 1'}], rows=[])
ui.json({'key': 'value'})
ui.code('print("Hello")')

# Media
ui.image('https://example.com/image.png')
ui.video('https://example.com/video.mp4')
```

## Event Handling

### Button clicks

```python
from nicegui import ui

def on_click():
    ui.notify('Button clicked!')

ui.button('Click me', on_click=on_click)
```

### Input events

```python
from nicegui import ui

# Value change
name = ui.input('Name', on_change=lambda e: ui.notify(f'Changed to: {e.value}'))

# Validation
password = ui.input('Password', password=True)
ui.button('Submit', on_click=lambda: validate(password.value))
```

### Debounced input

```python
from nicegui import ui

# Only triggers after user stops typing (300ms default)
ui.input('Search', on_change=handle_search).props('debounce=500')
```

## Async/Await Patterns

NiceGUI has excellent async support for non-blocking operations:

```python
import asyncio
from nicegui import ui

async def long_task():
    ui.notify('Starting...')
    await asyncio.sleep(3)  # Non-blocking
    ui.notify('Complete!')

ui.button('Run async task', on_click=long_task)

ui.run()
```

### Loading states

```python
from nicegui import ui
import asyncio

async def process():
    button.disable()
    spinner.visible = True
    
    try:
        await asyncio.sleep(2)
        ui.notify('Done!')
    finally:
        spinner.visible = False
        button.enable()

button = ui.button('Process', on_click=process)
spinner = ui.spinner().classes('invisible')
```

### Concurrent tasks

```python
import asyncio
from nicegui import ui

async def background_task():
    while True:
        await asyncio.sleep(5)
        ui.notify('Background tick')

# Start background task
ui.timer(5.0, lambda: ui.notify('Timer tick'))

# Or with create_task
asyncio.create_task(background_task())
```

## Layout and Styling

### Containers

```python
from nicegui import ui

# Card container
with ui.card():
    ui.label('Card title').classes('text-h6')
    ui.label('Card content')

# Row layout
with ui.row():
    ui.button('Button 1')
    ui.button('Button 2')
    ui.button('Button 3')

# Column layout
with ui.column():
    ui.label('Item 1')
    ui.label('Item 2')
    ui.label('Item 3')

# Grid layout
with ui.grid(columns=3):
    for i in range(9):
        ui.label(f'Item {i}')

# Expansion panel
with ui.expansion('Click to expand'):
    ui.label('Hidden content')

# Tabs
with ui.tabs() as tabs:
    ui.tab('Tab 1')
    ui.tab('Tab 2')

with ui.tab_panels(tabs):
    with ui.tab_panel('Tab 1'):
        ui.label('Content 1')
    with ui.tab_panel('Tab 2'):
        ui.label('Content 2')
```

### Tailwind CSS classes

NiceGUI uses Tailwind CSS for styling:

```python
from nicegui import ui

ui.label('Bold and large').classes('text-2xl font-bold')
ui.label('Colored text').classes('text-blue-500')
ui.button('Centered').classes('mx-auto')

# Responsive
ui.label('Hidden on mobile').classes('hidden md:block')

# Spacing
ui.label('With padding').classes('p-4 m-2')

# Flexbox
with ui.row().classes('justify-between items-center'):
    ui.label('Left')
    ui.label('Right')
```

Common Tailwind classes:
- `text-{size}`: `text-sm`, `text-lg`, `text-2xl`
- `font-{weight}`: `font-normal`, `font-bold`
- `p-{size}`, `m-{size}`: padding and margin
- `bg-{color}-{shade}`: `bg-blue-500`, `bg-gray-100`
- `text-{color}`: `text-red-500`
- `w-{size}`, `h-{size}`: width and height
- `flex`, `grid`: layout modes
- `hidden`, `block`, `inline`: display

### Custom CSS

```python
from nicegui import ui

ui.add_css('''
    .my-custom-class {
        background-color: #f0f0f0;
        border-radius: 8px;
    }
''')

ui.label('Custom styled').classes('my-custom-class')
```

## Reactive State

### Bind values

```python
from nicegui import ui

label = ui.label('Hello')
input_field = ui.input('Name', value='World')

# Bind input value to label
input_field.bind_value(label, 'text')
```

### app.storage

Persist data across sessions:

```python
from nicegui import ui, app

# User-specific storage (stored in browser)
@app.get('/')
def index():
    name = app.storage.user.get('name', 'Guest')
    ui.label(f'Hello, {name}!')
    
    ui.input('Your name', value=name).bind_value(
        app.storage.user, 'name'
    )

# General storage (server-side)
counter = app.storage.general.get('counter', 0)
app.storage.general['counter'] = counter + 1
```

## Data Display

### Tables

```python
from nicegui import ui

columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'sortable': True},
    {'name': 'age', 'label': 'Age', 'field': 'age', 'sortable': True},
]

rows = [
    {'name': 'Alice', 'age': 30},
    {'name': 'Bob', 'age': 25},
]

table = ui.table(columns=columns, rows=rows, row_key='name')

# Add new row
def add_row():
    rows.append({'name': 'Charlie', 'age': 35})
    table.rows = rows

ui.button('Add row', on_click=add_row)
```

### Charts (with echarts)

```python
from nicegui import ui

chart = ui.echart({
    'xAxis': {'type': 'category', 'data': ['Mon', 'Tue', 'Wed']},
    'yAxis': {'type': 'value'},
    'series': [{'data': [120, 200, 150], 'type': 'bar'}]
})

# Update chart
def update():
    chart.options['series'][0]['data'] = [300, 250, 400]
    chart.update()

ui.button('Update', on_click=update)
```

### Plotly integration

```python
from nicegui import ui
import plotly.express as px

df = px.data.gapminder().query("country=='Canada'")
fig = px.line(df, x='year', y='pop')

ui.plotly(fig).classes('w-full h-64')
```

## Dialogs and Notifications

### Notifications

```python
from nicegui import ui

ui.button('Success', on_click=lambda: ui.notify('Done!', type='positive'))
ui.button('Error', on_click=lambda: ui.notify('Failed!', type='negative'))
ui.button('Warning', on_click=lambda: ui.notify('Caution!', type='warning'))
ui.button('Info', on_click=lambda: ui.notify('Note', type='info'))

# With position
ui.button('Top right', on_click=lambda: ui.notify('Hello', position='top-right'))

# With timeout
ui.button('Persistent', on_click=lambda: ui.notify('Stay', timeout=None))
```

### Dialogs

```python
from nicegui import ui

with ui.dialog() as dialog, ui.card():
    ui.label('Are you sure?')
    with ui.row():
        ui.button('Yes', on_click=lambda: dialog.submit('yes'))
        ui.button('No', on_click=lambda: dialog.submit('no'))

async def confirm():
    result = await dialog
    ui.notify(f'You chose: {result}')

ui.button('Delete', on_click=confirm, color='negative')
```

### Custom dialogs

```python
from nicegui import ui

with ui.dialog(value=True) as dialog:
    with ui.card().classes('w-96'):
        ui.label('Login').classes('text-h5')
        username = ui.input('Username')
        password = ui.input('Password', password=True)
        with ui.row().classes('justify-end'):
            ui.button('Cancel', on_click=dialog.close)
            ui.button('Login', on_click=lambda: ui.notify(f'Welcome {username.value}'))
```

## Desktop vs Web Deployment

### Web mode (default)

```python
from nicegui import ui

ui.label('Web app')

ui.run(
    host='0.0.0.0',
    port=8080,
    title='My App',
    favicon='🚀'
)
```

### Desktop mode (native window)

```python
from nicegui import ui, native

ui.label('Desktop app')

ui.run(
    native=True,  # Creates native window
    window_size=(800, 600),
    fullscreen=False,
    frameless=False,
)
```

### Hybrid approach

```python
import sys
from nicegui import ui

ui.label('My App')

# Detect if running as web or desktop
if '--web' in sys.argv:
    ui.run(host='0.0.0.0', port=8080)
else:
    ui.run(native=True, window_size=(1024, 768))
```

## Pages and Routing

### Multiple pages

```python
from nicegui import ui

@ui.page('/')
def home():
    ui.label('Home page')
    ui.link('Go to About', '/about')

@ui.page('/about')
def about():
    ui.label('About page')
    ui.link('Go Home', '/')

ui.run()
```

### Page parameters

```python
from nicegui import ui

@ui.page('/user/{user_id}')
def user_page(user_id: str):
    ui.label(f'User ID: {user_id}')

ui.run()
```

### Page decorators with layout

```python
from nicegui import ui

def layout():
    with ui.header():
        ui.label('My App').classes('text-h5')
    with ui.left_drawer():
        ui.link('Home', '/')
        ui.link('Settings', '/settings')

@ui.page('/')
def home():
    layout()
    ui.label('Home content')

@ui.page('/settings')
def settings():
    layout()
    ui.label('Settings content')

ui.run()
```

## File Handling

### File upload

```python
from nicegui import ui

async def handle_upload(e):
    # e.content is bytes
    text = e.content.decode('utf-8')
    ui.notify(f'Uploaded {len(text)} characters')

ui.upload(on_upload=handle_upload, label='Upload file')
```

### File download

```python
from nicegui import ui
from pathlib import Path

content = 'Hello, World!'
ui.download(content.encode(), 'hello.txt')

# Or from file
ui.download(Path('/path/to/file.pdf'))
```

## Best Practices

1. **Use async functions** for I/O operations to avoid blocking
2. **Leverage Tailwind classes** for consistent styling
3. **Use `app.storage`** for user-specific state persistence
4. **Test both web and desktop modes** if supporting both
5. **Use `ui.run()` at the end** of your script
6. **Leverage the context manager syntax** (`with ui.card():`) for clean layout code
7. **Use `bind_value()`** for simple reactive updates
8. **Prefer `native=True`** for standalone desktop tools

## When to Choose NiceGUI

| Use NiceGUI when... | Consider alternatives when... |
|--------------------|------------------------------|
| You need a native desktop feel | You need the largest community (Streamlit) |
| Async/await is important | You need ML-specific features (Gradio) |
| Modern UI components matter | You need Jupyter integration (Panel) |
| Web + desktop from same code | You need React ecosystem (Dash) |
| Simplicity over features | Building complex dashboards |
