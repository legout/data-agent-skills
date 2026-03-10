# LLM Monitoring & Observability

Tracking LLM API calls, costs, performance, and output quality in production pipelines.

## What to Monitor?

| Metric | Why | Tools |
|--------|-----|-------|
| Token usage | Cost control | OpenAI API response headers, logging |
| Latency | User experience | Timing in code, OpenTelemetry |
| Error rate | Reliability | Exception tracking, retries |
| Cost | Budget tracking | Token counting × pricing |
| Output quality | Model drift | Human evaluation, LLM-as-a-judge |
| Cache hit rate | Efficiency | Prompt cache implementation |

## 1. Cost & Usage Tracking

```python
import duckdb
from datetime import datetime

class LLMMonitor:
    def __init__(self, db_path: str = "llm_monitoring.db"):
        self.conn = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        """Initialize monitoring tables."""
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS llm_calls (
                call_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                model VARCHAR,
                function_name VARCHAR,
                prompt_hash VARCHAR,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                total_tokens INTEGER,
                latency_ms INTEGER,
                status VARCHAR,
                error_message VARCHAR
            )
        """)

        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS prompt_cache (
                prompt_hash VARCHAR PRIMARY KEY,
                prompt_text VARCHAR,
                response_text VARCHAR,
                created_at TIMESTAMP,
                hit_count INTEGER DEFAULT 0
            )
        """)

    def log_call(
        self,
        model: str,
        function_name: str,
        prompt: str,
        response: dict = None,
        latency_ms: int = None,
        error: Exception = None
    ):
        """Log LLM call for observability."""
        import hashlib

        call_id = hashlib.md5(
            f"{datetime.utcnow().isoformat()}{prompt[:100]}".encode()
        ).hexdigest()[:16]

        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()[:16]

        # Cache prompt if not already seen
        self.conn.execute("""
            INSERT OR IGNORE INTO prompt_cache (prompt_hash, prompt_text, created_at)
            VALUES (?, ?, NOW())
        """, [prompt_hash, prompt])

        # Log call
        if error:
            status = "error"
            tokens = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            error_msg = str(error)[:500]
        else:
            status = "success"
            tokens = response.get('usage', {})
            error_msg = None
            # Cache response if prompt cache enabled
            self.conn.execute("""
                UPDATE prompt_cache
                SET response_text = ?, hit_count = hit_count + 1
                WHERE prompt_hash = ?
            """, [response['choices'][0]['message']['content'], prompt_hash])

        self.conn.execute("""
            INSERT INTO llm_calls VALUES (
                ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            call_id, model, function_name, prompt_hash,
            tokens.get('prompt_tokens', 0),
            tokens.get('completion_tokens', 0),
            tokens.get('total_tokens', 0),
            latency_ms, status, error_msg
        ])

    def get_usage_stats(self, days: int = 7) -> dict:
        """Get token usage and cost estimates."""
        result = self.conn.sql(f"""
            SELECT
                COUNT(*) as total_calls,
                SUM(prompt_tokens) as total_prompt_tokens,
                SUM(completion_tokens) as total_completion_tokens,
                SUM(total_tokens) as total_tokens,
                AVG(latency_ms) as avg_latency_ms,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                SUM(CASE
                    WHEN model = 'gpt-4o' THEN total_tokens * 0.0025 / 1000
                    WHEN model = 'gpt-4o-mini' THEN total_tokens * 0.00015 / 1000
                    ELSE total_tokens * 0.002 / 1000
                END) as estimated_cost_usd
            FROM llm_calls
            WHERE timestamp > NOW() - INTERVAL '{days} days'
        """).fetchone()

        return {
            'total_calls': result[0],
            'prompt_tokens': result[1],
            'completion_tokens': result[2],
            'total_tokens': result[3],
            'avg_latency_ms': result[4],
            'error_count': result[5],
            'error_rate': result[5] / result[0] if result[0] else 0,
            'estimated_cost_usd': result[6] or 0
        }

monitor = LLMMonitor()
```

## 2. Retry Pattern with Monitoring

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def monitored_llm_call(prompt: str, model: str = "gpt-4o") -> str:
    """LLM call with retry and monitoring."""
    import time
    from openai import OpenAI

    client = OpenAI()
    start = time.time()
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}]
        )
        latency_ms = int((time.time() - start) * 1000)

        monitor.log_call(
            model=model,
            function_name="generate_answer",
            prompt=prompt,
            response=response.model_dump(),
            latency_ms=latency_ms
        )
        return response.choices[0].message.content

    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        monitor.log_call(
            model=model,
            function_name="generate_answer",
            prompt=prompt,
            latency_ms=latency_ms,
            error=e
        )
        raise
```

## 3. OpenTelemetry Integration

```python
from openai import OpenAI
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("llm_pipeline")
client = OpenAI()

def traced_llm_call(prompt: str):
    with tracer.start_as_current_span("llm_inference") as span:
        span.set_attribute("model", "gpt-4o")
        span.set_attribute("prompt_length", len(prompt))

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )

        span.set_attribute("tokens.prompt", response.usage.prompt_tokens)
        span.set_attribute("tokens.completion", response.usage.completion_tokens)
        span.set_attribute("latency_ms", response.response_ms)

        if response.usage.total_tokens > 1000:
            span.set_attribute("expensive_call", True)

        return response.choices[0].message.content
```

## 4. Quality Evaluation

### LLM-as-a-Judge
```python
from typing import List
from openai import OpenAI

client = OpenAI()

def evaluate_response(
    query: str,
    response: str,
    ground_truth: str = None,
    criteria: List[str] = ["correctness", "relevance", "safety"]
) -> dict:
    """Use GPT-4 to judge response quality."""
    judge_prompt = f"""Evaluate the response to the query.

Query: {query}
Response: {response}
{"Ground Truth: " + ground_truth if ground_truth else ""}

Score 1-5 on: {', '.join(criteria)}. Provide brief justification."""

    scores = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": judge_prompt}]
    )
    return {
        "scores": scores.choices[0].message.content,
        "raw_judge_response": scores.model_dump()
    }
```

---

## Best Practices

1. ✅ **Log every call** - Success and failures; include model, tokens, latency
2. ✅ **Hash prompts** - Deduplicate cache, avoid recounting
3. ✅ **Track costs** - Aggregate by model, daily/weekly reports
4. ✅ **Set alerts** - Error rate > 5%, latency > 30s, cost spike
5. ✅ **Sample slow queries** - Log full prompt/response for debugging
6. ✅ **Use structured logging** - JSON format for easy parsing
7. ❌ **Don't** log full prompts/responses by default (PII, cost)
8. ❌ **Don't** ignore cache - Cache identical prompts to reduce cost
9. ❌ **Don't** forget to monitor fallback models (rate limits)

---

## References

- [OpenAI Token Pricing](https://openai.com/pricing)
- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- `@data-engineering-observability` - General pipeline monitoring
- `@data-engineering-ai-ml/rag-pipelines.md` - End-to-end RAG
