# Deployment Patterns

This guide covers deploying data applications built with Streamlit, Panel, Gradio, Dash, and NiceGUI to various platforms.

## Streamlit Community Cloud

**Best for:** Free hosting, GitHub integration, quick sharing

### Setup

1. Push your code to GitHub
2. Sign in at [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repository
4. Select branch and main file

### Repository structure

```
my-app/
├── streamlit_app.py      # Main entry point
├── requirements.txt      # Dependencies
├── packages.txt          # System dependencies (optional)
└── .streamlit/
    └── config.toml       # Streamlit configuration
```

### requirements.txt

```
streamlit
pandas
plotly
```

### Secrets configuration

```toml
# .streamlit/secrets.toml (for local dev)
openai_api_key = "sk-..."

# On Streamlit Cloud: Settings → Secrets
[openai]
api_key = "sk-..."
```

```python
import streamlit as st
api_key = st.secrets["openai"]["api_key"]
```

### Limitations

- Public by default (private apps require paid plan)
- 1 GB memory limit
- Sleeps after inactivity (cold start on wake)
- GitHub repo must be public (for free tier)

## Hugging Face Spaces

**Best for:** ML demos, Gradio apps, community sharing

### Setup

1. Create account at [huggingface.co](https://huggingface.co)
2. Go to [huggingface.co/spaces](https://huggingface.co/spaces) → "Create new Space"
3. Select SDK (Gradio, Streamlit, or Docker)
4. Choose visibility (Public or Private)

### Gradio SDK structure

```
space/
├── app.py              # Entry point (must create demo or app)
├── requirements.txt    # Dependencies
└── README.md          # Space description
```

```python
# app.py
import gradio as gr

def greet(name):
    return f"Hello {name}!"

# Must assign to demo or app
demo = gr.Interface(fn=greet, inputs="text", outputs="text")
demo.launch()
```

### Streamlit SDK structure

Same structure, but Streamlit handles the app creation.

### Docker SDK (any framework)

```dockerfile
# Dockerfile
FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]
```

### Hardware upgrades

Free tier: 2 vCPU, 16 GB RAM
Paid upgrades available for GPU access.

## Docker Containerization

**Best for:** Reproducible deployments, any cloud platform, self-hosting

### Generic Dockerfile

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including curl for health checks
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY . .

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8080/ || exit 1

# Run command (framework-specific)
```

### Streamlit Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
# Build and run
docker build -t my-streamlit-app .
docker run -p 8501:8501 my-streamlit-app
```

### Panel Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 5006

# Use explicit hostname for production security
# Replace myapp.example.com with your actual domain
CMD ["panel", "serve", "app.py", "--port=5006", "--address=0.0.0.0", "--allow-websocket-origin=myapp.example.com"]
```

> **⚠️ Security Note:** For local development only, you may use `--allow-websocket-origin='*'`, but always use explicit hostnames in production to maintain proper CSRF/origin protections.

### Gradio Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 7860

CMD ["python", "app.py"]
```

```python
# app.py
import gradio as gr
import os

def greet(name):
    return f"Hello {name}!"

demo = gr.Interface(fn=greet, inputs="text", outputs="text")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
```

### Dash Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8050

CMD ["gunicorn", "app:server", "-b", "0.0.0.0:8050", "-w", "4"]
```

```python
# app.py
from dash import Dash, html

app = Dash(__name__)
server = app.server  # Required for Gunicorn

app.layout = html.Div("Hello World")

if __name__ == '__main__':
    app.run(debug=False)
```

### NiceGUI Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8080

CMD ["python", "app.py"]
```

```python
# app.py
from nicegui import ui, app
import os

ui.label('Hello from Docker')

port = int(os.environ.get('PORT', 8080))
ui.run(host='0.0.0.0', port=port)
```

## Cloud Platforms

### AWS

**Elastic Beanstalk (easiest)**

```yaml
# .ebextensions/python.config
option_settings:
  aws:elasticbeanstalk:container:python:
    WSGIPath: app:server  # For Dash
```

```
my-app/
├── application.py    # or app.py
├── requirements.txt
├── Dockerfile       # Optional
└── .ebextensions/
    └── python.config
```

**ECS/Fargate (containers)**

1. Push Docker image to ECR
2. Create ECS task definition
3. Configure Fargate service
4. Set up Application Load Balancer

**EC2 (self-managed)**

```bash
# On EC2 instance
sudo apt update
sudo apt install python3-pip nginx

# Clone app
git clone https://github.com/user/app.git
cd app
pip3 install -r requirements.txt

# Setup systemd service
sudo tee /etc/systemd/system/myapp.service > /dev/null <<EOF
[Unit]
Description=My App
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/app
ExecStart=/usr/local/bin/streamlit run app.py --server.port 8501
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable myapp
sudo systemctl start myapp

# Configure Nginx reverse proxy
sudo tee /etc/nginx/sites-available/myapp > /dev/null <<EOF
server {
    listen 80;
    server_name mydomain.com;
    
    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_cache_bypass \$http_upgrade;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/myapp /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### Google Cloud Platform

**Cloud Run (recommended for serverless)**

```bash
# Build and deploy
gcloud builds submit --tag gcr.io/PROJECT-ID/my-app
gcloud run deploy my-app --image gcr.io/PROJECT-ID/my-app --platform managed
```

**App Engine**

```yaml
# app.yaml
runtime: python39

entrypoint: streamlit run app.py --server.port $PORT

handlers:
- url: /.*
  script: auto
```

```
gcloud app deploy
```

**Compute Engine**

Similar to AWS EC2 setup with systemd + nginx.

### Azure

**Container Instances**

```bash
az container create \
  --resource-group myResourceGroup \
  --name myapp \
  --image myregistry.azurecr.io/myapp:latest \
  --ports 8501
```

**App Service**

```
# .deployment
[config]
SCM_DO_BUILD_DURING_DEPLOYMENT=true
```

```bash
az webapp up --sku B1 --name myapp
```

### Heroku

```
# Procfile
web: streamlit run app.py --server.port $PORT
```

```bash
# Create and deploy
heroku create my-app
git push heroku main
```

**Note:** Heroku's free tier has been discontinued. Paid dynos start at $7/month.

### Railway / Render / Fly.io

These platforms offer simpler deployment experiences:

**Railway**
1. Connect GitHub repo
2. Railway auto-detects Python and deploys

**Render**
1. Create Web Service
2. Connect GitHub repo
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `streamlit run app.py`

**Fly.io**

```bash
fly launch  # Creates fly.toml
fly deploy
```

## Self-Hosted Options

### VPS (DigitalOcean, Linode, Vultr)

1. Provision Ubuntu server
2. Install Python, Nginx, Supervisor
3. Clone application
4. Setup systemd service or Supervisor
5. Configure Nginx reverse proxy
6. Setup SSL with Let's Encrypt

### Kubernetes

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: myregistry/my-app:latest
        ports:
        - containerPort: 8501
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: my-app
spec:
  selector:
    app: my-app
  ports:
  - port: 80
    targetPort: 8501
  type: ClusterIP
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: my-app
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt"
spec:
  tls:
  - hosts:
    - app.example.com
    secretName: my-app-tls
  rules:
  - host: app.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: my-app
            port:
              number: 80
```

## Framework-Specific Deployment Notes

### Streamlit

- Use `--server.headless true` for server deployments
- Set `--browser.gatherUsageStats false` to disable telemetry
- Use `--server.enableCORS false` when behind reverse proxy

```bash
streamlit run app.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
```

### Panel

- Use explicit hostnames for `--allow-websocket-origin` (never wildcards in production)
- Consider `--num-procs` for multi-process deployment

```bash
# Production example with explicit hostname
panel serve app.py \
  --port 5006 \
  --address 0.0.0.0 \
  --allow-websocket-origin myapp.example.com \
  --num-procs 4
```

> **⚠️ Important:** The `--allow-websocket-origin` flag controls which domains can establish WebSocket connections. Using `'*'` in production disables origin validation and weakens security. Always specify your actual domain(s).

### Gradio

- Set `server_name="0.0.0.0"` in `launch()`
- Use `share=False` in production (no temporary URLs)

```python
demo.launch(
    server_name="0.0.0.0",
    server_port=int(os.environ.get("PORT", 7860)),
    share=False
)
```

### Dash

- Always expose `server` for WSGI
- Use Gunicorn in production (not `app.run()`)
- Set `debug=False`

### NiceGUI

- Set `host='0.0.0.0'` in `ui.run()`
- Use `reload=False` in production
- Desktop mode incompatible with containerized deployment

## Security Checklist

- [ ] Secrets stored in environment variables, not code
- [ ] HTTPS enabled (Let's Encrypt, CloudFlare, etc.)
- [ ] Authentication implemented if needed
- [ ] CORS properly configured
- [ ] WebSocket origins explicitly specified (no wildcards in production)
- [ ] Rate limiting for public-facing apps
- [ ] Input validation on all user inputs
- [ ] Dependency scanning (Dependabot, Snyk)
- [ ] Container images from trusted sources
- [ ] Regular security updates

## Performance Optimization

### Caching strategies

- **Streamlit:** `@st.cache_data`, `@st.cache_resource`
- **Panel:** Param caching, `pn.cache` decorator
- **Dash:** Flask-Caching, Redis
- **Gradio:** Manual caching with LRU cache
- **NiceGUI:** `functools.lru_cache`, async patterns

### Scaling

| Platform | Scaling Strategy |
|----------|-----------------|
| Streamlit Cloud | Upgrade plan, no manual scaling |
| Hugging Face Spaces | Upgrade hardware, duplicate Spaces |
| Docker/Cloud | Horizontal scaling with load balancer |
| Kubernetes | HPA based on CPU/memory |

### CDN for static assets

Serve large assets (images, datasets) from CDN:
- AWS CloudFront
- CloudFlare
- Google Cloud CDN
