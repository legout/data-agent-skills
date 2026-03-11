# Gradio Advanced Patterns

## Interface Types

### gr.Interface — Simplest API

For simple functions with clear inputs and outputs:

```python
import gradio as gr

def greet(name, intensity):
    return "Hello, " + name + "!" * intensity

interface = gr.Interface(
    fn=greet,
    inputs=[gr.Textbox(label="Name"), gr.Slider(1, 10, label="Intensity")],
    outputs=gr.Textbox(label="Greeting"),
    title="Greeter",
    description="Enter your name for a custom greeting!"
)

interface.launch()
```

### gr.Blocks — Flexible Layouts

For complex UIs with custom layouts:

```python
import gradio as gr

def generate_text(prompt, temperature, max_tokens):
    # Your generation logic
    return f"Result for: {prompt}"

with gr.Blocks() as demo:
    gr.Markdown("# Text Generator")
    
    with gr.Row():
        with gr.Column():
            prompt = gr.Textbox(label="Prompt", lines=3)
            temperature = gr.Slider(0, 2, value=1, label="Temperature")
            max_tokens = gr.Slider(1, 2048, value=512, label="Max Tokens")
            generate_btn = gr.Button("Generate", variant="primary")
        
        with gr.Column():
            output = gr.Textbox(label="Output", lines=10)
            examples = gr.Examples(
                examples=["Write a poem", "Explain quantum physics"],
                inputs=prompt
            )
    
    generate_btn.click(
        fn=generate_text,
        inputs=[prompt, temperature, max_tokens],
        outputs=output
    )

demo.launch()
```

### gr.TabbedInterface — Multi-page Apps

```python
import gradio as gr

def function1(x):
    return x

def function2(x):
    return x * 2

tab1 = gr.Interface(function1, gr.Textbox(), gr.Textbox())
tab2 = gr.Interface(function2, gr.Number(), gr.Number())

demo = gr.TabbedInterface([tab1, tab2], ["Tab 1", "Tab 2"])
demo.launch()
```

### gr.ChatInterface — Chatbots

```python
import gradio as gr

def chat(message, history):
    """Simple chatbot that echoes with prefix."""
    return f"Bot: You said '{message}'"

demo = gr.ChatInterface(
    fn=chat,
    title="Simple Chatbot",
    description="A basic chat interface",
    examples=["Hello!", "How are you?"]
)

demo.launch()
```

## Input/Output Components

### Text inputs

```python
import gradio as gr

textbox = gr.Textbox(
    label="Input",
    placeholder="Enter text here...",
    lines=3,  # Multi-line
    max_lines=10,
    value="Default text"
)

textarea = gr.TextArea(label="Long Text")  # Alias for lines>1
```

### Numeric inputs

```python
slider = gr.Slider(
    minimum=0,
    maximum=100,
    value=50,
    step=1,
    label="Select value"
)

number = gr.Number(value=10, label="Number")
```

### Selection inputs

```python
dropdown = gr.Dropdown(
    choices=["Option 1", "Option 2", "Option 3"],
    value="Option 1",
    label="Select"
)

radio = gr.Radio(
    choices=["A", "B", "C"],
    label="Choose one"
)

checkbox_group = gr.CheckboxGroup(
    choices=["Feature 1", "Feature 2"],
    label="Features"
)
```

### Media inputs

```python
image = gr.Image(
    type="pil",  # Returns PIL Image
    label="Upload Image"
)

audio = gr.Audio(
    type="filepath",  # Returns file path
    label="Upload Audio"
)

video = gr.Video(label="Upload Video")

file = gr.File(label="Upload File")
```

### Data inputs

```python
dataframe = gr.DataFrame(
    value=pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
    label="Data"
)

json_input = gr.JSON(label="JSON Input")
```

### Output components

```python
# Text
label = gr.Label(label="Prediction")  # For classification
json_output = gr.JSON(label="Results")
html = gr.HTML(label="Formatted Output")

# Media
image_out = gr.Image(label="Output Image")
audio_out = gr.Audio(label="Output Audio")

# Data
df_out = gr.DataFrame(label="Results Table")
plot = gr.Plot(label="Visualization")

# Special
gallery = gr.Gallery(label="Image Gallery")  # Multiple images
markdown = gr.Markdown()  # Formatted text
```

## Event Handling

### Button clicks

```python
import gradio as gr

with gr.Blocks() as demo:
    name = gr.Textbox()
    output = gr.Textbox()
    btn = gr.Button("Greet")
    
    def greet(n):
        return f"Hello {n}!"
    
    btn.click(greet, inputs=name, outputs=output)
```

### Input change events

```python
import gradio as gr

with gr.Blocks() as demo:
    text = gr.Textbox()
    output = gr.Textbox()
    
    # Trigger on change (debounced)
    text.change(lambda x: x.upper(), inputs=text, outputs=output)
    
    # Trigger on submit (Enter key)
    text.submit(process, inputs=text, outputs=output)
```

### Multiple event handlers

```python
import gradio as gr

with gr.Blocks() as demo:
    input_text = gr.Textbox()
    output_text = gr.Textbox()
    btn1 = gr.Button("Process 1")
    btn2 = gr.Button("Process 2")
    
    btn1.click(process1, inputs=input_text, outputs=output_text)
    btn2.click(process2, inputs=input_text, outputs=output_text)
```

### State management

```python
import gradio as gr

def update_chat(message, history):
    history = history or []
    history.append((message, f"Response to: {message}"))
    return history, history

with gr.Blocks() as demo:
    chatbot = gr.Chatbot()
    msg = gr.Textbox()
    state = gr.State()  # Stores history between calls
    
    msg.submit(update_chat, inputs=[msg, state], outputs=[chatbot, state])

demo.launch()
```

## Custom Components

### Using existing custom components

```python
# Popular community components
# pip install gradio-image-prompter
# pip install gradio-model3d

import gradio as gr
from gradio_image_prompter import image_prompter

demo = gr.Interface(
    fn=process_image,
    inputs=image_prompter(),
    outputs="image"
)
```

### Creating custom components

See [Gradio Custom Components](https://www.gradio.app/guides/custom-components-in-five-minutes) for full documentation.

## Authentication and Sharing

### Built-in authentication

```python
import gradio as gr

# Simple password
gr.Interface(...).launch(auth=("username", "password"))

# Multiple users
gr.Interface(...).launch(
    auth=[("user1", "pass1"), ("user2", "pass2")]
)

# Custom auth function

def auth_fn(username, password):
    # Check credentials against database
    return username if valid else None

gr.Interface(...).launch(auth=auth_fn)
```

### Temporary public sharing

```python
import gradio as gr

# Creates temporary public URL (72 hours)
gr.Interface(...).launch(share=True)
```

### Persistent hosting with Hugging Face Spaces

1. Create a Space at [huggingface.co/spaces](https://huggingface.co/spaces)
2. Choose Gradio as the SDK
3. Push your code:

```
space/
├── app.py           # Your Gradio app
├── requirements.txt # Dependencies
└── README.md        # Space description
```

### Private Spaces

- Create a private Space on Hugging Face
- Only accessible to you and collaborators
- Free tier: unlimited private Spaces (within compute limits)

## API Generation

Gradio automatically generates an API for every interface:

### Viewing the API

1. Click "Use via API" at the bottom of your Gradio app
2. Or access `/api` endpoint directly

### Python API client

```python
from gradio_client import Client

client = Client("http://localhost:7860")  # Or your Space URL

# Predict
result = client.predict("Hello", api_name="/predict")

# With specific parameters
result = client.predict(
    message="Hello",
    temperature=0.7,
    api_name="/chat"
)
```

### JavaScript API

```javascript
import { client } from "@gradio/client";

const app = await client("http://localhost:7860");
const result = await app.predict("/predict", ["Hello"]);
```

### HTTP API (curl)

```bash
curl -X POST http://localhost:7860/api/predict \
  -H "Content-Type: application/json" \
  -d '{"data": ["Hello"]}'
```

## File Handling

### Uploading files

```python
import gradio as gr

def process_file(file_path):
    # file_path is a temporary file path
    with open(file_path, 'r') as f:
        content = f.read()
    return content

gr.Interface(
    fn=process_file,
    inputs=gr.File(label="Upload"),
    outputs=gr.Textbox()
).launch()
```

### Returning files

```python
import gradio as gr
import tempfile

def create_file():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("Generated content")
        return f.name

gr.Interface(
    fn=create_file,
    inputs=None,
    outputs=gr.File(label="Download")
).launch()
```

### Image processing

```python
import gradio as gr
from PIL import Image

def process_image(input_image):
    # input_image is PIL Image (if type="pil")
    gray = input_image.convert('L')
    return gray

gr.Interface(
    fn=process_image,
    inputs=gr.Image(type="pil"),
    outputs=gr.Image()
).launch()
```

## Examples and Flagging

### Adding examples

```python
import gradio as gr

gr.Interface(
    fn=predict,
    inputs=gr.Textbox(),
    outputs=gr.Label(),
    examples=[
        ["Example 1"],
        ["Example 2"],
        ["Example 3"]
    ],
    examples_per_page=3
).launch()
```

### In Blocks

```python
with gr.Blocks() as demo:
    with gr.Row():
        with gr.Column():
            input_text = gr.Textbox()
            examples = gr.Examples(
                examples=["Hello", "World"],
                inputs=input_text
            )
        with gr.Column():
            output = gr.Textbox()
    
    btn = gr.Button("Submit")
    btn.click(predict, inputs=input_text, outputs=output)
```

### Flagging (feedback collection)

```python
import gradio as gr

gr.Interface(
    fn=predict,
    inputs="text",
    outputs="label",
    allow_flagging="manual",  # or "auto"
    flagging_options=["correct", "incorrect", "unclear"],
    flagging_dir="flagged"  # Where flagged data is saved
).launch()
```

## Queuing and Concurrency

### Basic queue

```python
import gradio as gr

gr.Interface(...).queue().launch()
```

### Queue configuration

```python
demo = gr.Blocks()

with demo:
    # Your UI
    pass

# Configure queue
demo.queue(
    concurrency_count=3,  # Number of concurrent workers
    max_size=10,          # Max queue size
    default_enabled=True
).launch()
```

### Event-specific queue

```python
btn.click(
    fn=slow_function,
    inputs=input,
    outputs=output,
    queue=True  # Use queue for this specific event
)
```

## Best Practices

1. **Always use `queue()`** for production apps to handle multiple users
2. **Set reasonable limits** on inputs (max length, file size)
3. **Add examples** to help users understand expected inputs
4. **Use `gr.State()`** for multi-turn conversations or persistent data
5. **Test the API** — Gradio's automatic API is powerful for integrations
6. **Consider Hugging Face Spaces** for easy, free hosting
7. **Add error handling** in your functions to show friendly messages

## Deployment Checklist

- [ ] `demo.queue()` enabled for production
- [ ] Dependencies in `requirements.txt`
- [ ] Hugging Face Space created (if using Spaces)
- [ ] Authentication configured (if needed)
- [ ] Examples added for user guidance
- [ ] API tested if integrating with other systems
- [ ] Error handling in all functions
- [ ] No hardcoded secrets (use environment variables)
