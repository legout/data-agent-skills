---
name: engineering-ai-pipelines
description: "AI/ML production workflows: embedding generation, vector storage, RAG patterns, LLM monitoring, and batch inference."
dependsOn:
  - "@data-engineering-core"
  - "@designing-data-storage"
---

# Engineering AI Pipelines

Production-ready patterns for AI/ML data pipelines: generating embeddings, selecting vector databases, building RAG systems, and monitoring LLM usage. Covers local and cloud-based approaches with cost and performance considerations.

## Table of Contents

- [Quick Comparison](#quick-comparison)
  - [Embedding Approaches](#embedding-approaches)
  - [Vector Databases](#vector-databases)
- [When to Use Which?](#when-to-use-which)
  - [Embedding Generation](#embedding-generation)
  - [Vector Storage](#vector-storage)
  - [RAG Architecture](#rag-architecture)
- [Pipeline Selection Matrix](#pipeline-selection-matrix)
- [Detailed Reference Guides](#detailed-reference-guides)
- [Code Examples](#code-examples)
  - [OpenAI Embeddings](#openai-embeddings)
  - [Local Embeddings](#local-embeddings)
  - [Vector Search with LanceDB](#vector-search-with-lancedb)
  - [Complete RAG Pipeline](#complete-rag-pipeline)
  - [LLM Monitoring](#llm-monitoring)
- [Best Practices](#best-practices)
  - [Embeddings](#embeddings)
  - [Vector Storage](#vector-storage-1)
  - [RAG Pipelines](#rag-pipelines)
  - [Monitoring](#monitoring)
- [Related Skills](#related-skills)
- [References](#references)

## Quick Comparison

### Embedding Approaches

| Approach | Provider | Dimensions | Cost | Speed | Best For |
|----------|----------|------------|------|-------|----------|
| **OpenAI 3-small** | OpenAI | 1536 | $0.00002/1K tokens | ⚡⚡⚡ Fast | Production, convenience |
| **OpenAI 3-large** | OpenAI | 3072 | $0.00013/1K tokens | ⚡⚡ | Higher quality needs |
| **MiniLM-L6** | Local (sentence-transformers) | 384 | Free | ⚡⚡ | Privacy, offline, cost control |
| **MPNet-base** | Local (sentence-transformers) | 768 | Free | ⚡⚡ | Better quality than MiniLM |
| **OpenAI ada-002** | OpenAI | 1536 | $0.00010/1K tokens | ⚡⚡⚡ | Legacy compatibility |

### Vector Databases

| Feature | LanceDB | pgvector | DuckDB | Pinecone/Weaviate |
|---------|---------|----------|--------|-------------------|
| **Deployment** | Embedded file | PostgreSQL | Embedded | Managed service |
| **Scale** | Millions | Tens of millions | <100K | Billions |
| **Index Types** | IVF_PQ, HNSW | HNSW, IVFFlat | None | HNSW, IVF |
| **Cloud Native** | ✅ S3/GCS/Azure | Via RDS | ❌ | ✅ |
| **ACID** | ✅ | ✅ ✅ | ✅ | Varies |
| **Cost** | Free | Postgres cost | Free | Usage-based |
| **Best For** | RAG apps, prototyping | Production + existing PG | Quick experiments | Enterprise scale |

## When to Use Which?

### Embedding Generation

**Choose OpenAI API when:**
- You need production reliability without managing infrastructure
- Cost is acceptable ($0.02-0.13 per 1M tokens)
- You don't have GPU resources for local inference
- Token limits (8K) fit your use case
- You want state-of-the-art quality without tuning

**Choose Local Models (sentence-transformers) when:**
- Data privacy is critical (healthcare, finance)
- You need offline/air-gapped operation
- You have high volume (millions of documents)
- You want to avoid API rate limits
- You have GPU resources available

**Choose OpenAI 3-small vs 3-large:**
- Use **3-small** for most use cases (cost-effective, good quality)
- Use **3-large** when you need maximum retrieval accuracy

### Vector Storage

**Choose LanceDB when:**
- Building RAG applications or prototypes
- Need embedded deployment (no server management)
- Working with multi-modal data (text + images + vectors)
- Want Arrow-native integration with Polars/PyArrow
- Need cloud storage (S3/GCS) without complex setup

**Choose pgvector when:**
- Already using PostgreSQL for transactional data
- Need ACID guarantees with vector search
- Complex joins between vectors and relational data
- Cannot tolerate separate vector DB infrastructure

**Choose DuckDB when:**
- Quick experiments or small datasets (<100K vectors)
- Already using DuckDB for analytics
- Don't need approximate nearest neighbor (ANN) indexing
- Simple cosine similarity is sufficient

**Choose Managed Services (Pinecone, Weaviate) when:**
- Billions of vectors
- Need managed infrastructure
- Enterprise SLA requirements
- Hybrid search (vector + keyword) out of the box

### RAG Architecture

**Simple RAG (single stage):**
```
Query → Embedding → Vector Search → Context → LLM → Answer
```
- Best for: Quick prototypes, well-structured documents
- Tools: LanceDB + OpenAI

**Advanced RAG (multi-stage):**
```
Query → Embedding → Vector Search → Re-ranking → Context → LLM → Answer
```
- Best for: Production systems requiring high accuracy
- Add: Cross-encoder re-ranking, metadata filtering

**Hybrid RAG:**
```
Query → [Vector Search + Keyword/BM25] → Fusion → Context → LLM → Answer
```
- Best for: Domain-specific terminology, acronyms
- Tools: LanceDB hybrid search or specialized services

## Pipeline Selection Matrix

| Use Case | Embedding | Vector DB | RAG Pattern | Monitoring |
|----------|-----------|-----------|-------------|------------|
| Internal knowledge base | OpenAI 3-small | LanceDB | Simple RAG | Basic cost tracking |
| Customer-facing chatbot | OpenAI 3-large | pgvector | Advanced (re-ranking) | Full observability |
| Healthcare (HIPAA) | Local MiniLM | LanceDB (on-prem) | Simple RAG | Local logging only |
| Financial documents | OpenAI 3-small | pgvector | Hybrid RAG | Cost + audit logs |
| Research paper search | Local MPNet | LanceDB | Advanced RAG | Basic metrics |
| High-volume (>10M docs) | OpenAI 3-small | Managed (Pinecone) | Simple RAG | Full observability |
| Multi-modal (text + images) | CLIP/Local | LanceDB | Simple RAG | Basic tracking |

## Detailed Reference Guides

### Core Patterns

- **`references/embeddings.md`** - OpenAI API, sentence-transformers, batch processing, model selection
- **`references/vector-stores.md`** - LanceDB, pgvector, DuckDB comparison and selection
- **`references/rag-pipelines.md`** - Chunking strategies, context assembly, retrieval patterns
- **`references/monitoring.md`** - Cost tracking, retry logic, OpenTelemetry, quality evaluation

## Code Examples

### OpenAI Embeddings

```python
import openai
import polars as pl
import tiktoken

class OpenAIEmbeddingPipeline:
    def __init__(self, model: str = "text-embedding-3-small"):
        self.client = openai.OpenAI()
        self.model = model
        self.encoding = tiktoken.encoding_for_model(model)

    def generate_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings in batches of 100."""
        all_embeddings = []
        batch_size = 100

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            response = self.client.embeddings.create(
                model=self.model,
                input=batch
            )
            all_embeddings.extend([e.embedding for e in response.data])

        return all_embeddings

    def process_dataframe(self, df: pl.DataFrame, text_col: str) -> pl.DataFrame:
        """Add embeddings column to Polars DataFrame."""
        texts = df[text_col].to_list()
        embeddings = self.generate_embeddings(texts)
        return df.with_columns(pl.Series("embedding", embeddings))

# Usage
pipeline = OpenAIEmbeddingPipeline()
df = pl.DataFrame({"text": ["Hello world", "Goodbye world"]})
df_with_embeddings = pipeline.process_dataframe(df, "text")
```

### Local Embeddings

```python
from sentence_transformers import SentenceTransformer
import polars as pl

model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dimensions

texts = ["Hello world", "Goodbye world"]
embeddings = model.encode(texts, show_progress_bar=True)

df = pl.DataFrame({
    "text": texts,
    "embedding": embeddings.tolist()
})
```

### Vector Search with LanceDB

```python
import lancedb
import polars as pl

# Connect and create table
db = lancedb.connect("./.lancedb")
df = pl.DataFrame({
    "id": [1, 2, 3],
    "text": ["Machine learning basics", "Deep learning intro", "Neural networks"],
    "embedding": [[0.1] * 384, [0.2] * 384, [0.3] * 384]
})

table = db.create_table("documents", df.to_arrow())

# Create index for faster search
table.create_index(
    vector_column_name="embedding",
    metric="cosine",
    index_type="IVF_PQ",
    num_partitions=256,
    num_sub_vectors=96
)

# Search with filtering
query_embedding = [0.15] * 384
results = (
    table.search(query_embedding)
    .where("id > 1")
    .limit(5)
    .to_pandas()
)
```

### Complete RAG Pipeline

```python
import lancedb
from sentence_transformers import SentenceTransformer
import openai

class RAGPipeline:
    def __init__(self, db_path: str, embedding_model: str = "all-MiniLM-L6-v2"):
        self.embedder = SentenceTransformer(embedding_model)
        self.db = lancedb.connect(db_path)
        self.table = self.db.open_table("documents")
        self.openai_client = openai.OpenAI()

    def retrieve(self, query: str, k: int = 5) -> list[dict]:
        """Retrieve relevant documents."""
        query_embedding = self.embedder.encode([query])[0].tolist()
        results = self.table.search(query_embedding).limit(k).to_pandas()
        return results.to_dict('records')

    def generate(self, query: str, context_docs: list[dict]) -> str:
        """Generate answer with LLM."""
        context_text = "\n\n---\n\n".join([doc['text'] for doc in context_docs])

        response = self.openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Answer based on the provided context. Cite sources using [1], [2], etc."},
                {"role": "user", "content": f"Context:\n{context_text}\n\nQuestion: {query}\n\nAnswer:"}
            ],
            temperature=0.1
        )
        return response.choices[0].message.content

    def run(self, query: str, k: int = 5) -> dict:
        """Full RAG pipeline."""
        docs = self.retrieve(query, k)
        answer = self.generate(query, docs)

        return {
            "answer": answer,
            "sources": [{"id": d['id'], "text": d['text'][:200]} for d in docs],
            "source_count": len(docs)
        }

# Usage
rag = RAGPipeline("./.lancedb")
result = rag.run("What is machine learning?")
print(result["answer"])
```

### LLM Monitoring

```python
import duckdb
from datetime import datetime
import time

class LLMMonitor:
    def __init__(self, db_path: str = "llm_monitoring.db"):
        self.conn = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS llm_calls (
                call_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                model VARCHAR,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                total_tokens INTEGER,
                cost_usd DOUBLE,
                latency_ms INTEGER,
                status VARCHAR
            )
        """)

    def log_call(self, model: str, prompt_tokens: int, completion_tokens: int, latency_ms: int):
        # Approximate cost (update with current pricing)
        cost_per_1k = {
            "gpt-4o": 0.0025,
            "gpt-4o-mini": 0.00015,
            "text-embedding-3-small": 0.00002
        }.get(model, 0.001)

        total_tokens = prompt_tokens + completion_tokens
        cost = total_tokens / 1000 * cost_per_1k

        call_id = f"{datetime.now().timestamp():.6f}"
        self.conn.execute("""
            INSERT INTO llm_calls VALUES (?, NOW(), ?, ?, ?, ?, ?, ?, ?)
        """, [call_id, model, prompt_tokens, completion_tokens, total_tokens, cost, latency_ms, "success"])

    def get_usage_stats(self, days: int = 7) -> dict:
        result = self.conn.sql(f"""
            SELECT
                COUNT(*) as total_calls,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost,
                AVG(latency_ms) as avg_latency
            FROM llm_calls
            WHERE timestamp > NOW() - INTERVAL '{days} days'
        """).fetchone()

        return {
            'total_calls': result[0],
            'total_tokens': result[1],
            'total_cost_usd': result[2],
            'avg_latency_ms': result[3]
        }

# Usage with retry pattern
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

monitor = LLMMonitor()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def monitored_llm_call(prompt: str, model: str = "gpt-4o") -> str:
    client = openai.OpenAI()
    start = time.time()

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    latency_ms = int((time.time() - start) * 1000)
    usage = response.usage

    monitor.log_call(model, usage.prompt_tokens, usage.completion_tokens, latency_ms)
    return response.choices[0].message.content
```

## Best Practices

### Embeddings

1. ✅ **Batch API calls** - OpenAI accepts up to 100 texts per request
2. ✅ **Chunk long texts** - Split on token limits, respect natural boundaries
3. ✅ **Cache embeddings** - Don't regenerate identical text (hash-based cache)
4. ✅ **Normalize embeddings** - Most similarity metrics expect normalized vectors
5. ✅ **Use appropriate model** - Trade off quality vs cost/latency
6. ✅ **Count tokens before sending** - Use tiktoken to avoid truncation
7. ❌ **Don't** embed rich text (HTML, markdown) - Extract plain text first
8. ❌ **Don't** mix embedding models in same index - Different dimensions break search

### Vector Storage

1. ✅ **Build index after bulk load** - Not incremental for best performance
2. ✅ **Use metadata filtering** - Filter on attributes before vector search
3. ✅ **Choose right index type** - IVF_PQ for disk efficiency, HNSW for recall
4. ✅ **Normalize for cosine** - Ensure embeddings are normalized if using cosine metric
5. ✅ **Include source metadata** - Need attribution for RAG answers
6. ❌ **Don't** store raw vectors without context - metadata essential
7. ❌ **Don't** rebuild indexes frequently - expensive operation
8. ❌ **Don't** use DuckDB for >100K vectors - no ANN indexing

### RAG Pipelines

1. ✅ **Chunk intelligently** - Don't split mid-sentence; respect semantic boundaries
2. ✅ **Include metadata** - Filter on date, source, author before vector search
3. ✅ **Add source citations** - Include [1], [2] references in answers
4. ✅ **Track token budget** - Don't exceed LLM context window
5. ✅ **Handle "I don't know"** - Train model to admit when context insufficient
6. ❌ **Don't** retrieve too many chunks - quality degrades with noise
7. ❌ **Don't** skip re-ranking for production - cross-encoder improves accuracy
8. ❌ **Don't** put PII in vector DB without encryption

### Monitoring

1. ✅ **Log every call** - Success and failures; model, tokens, latency
2. ✅ **Hash prompts** - Enable deduplication and caching
3. ✅ **Track costs by model** - Different pricing tiers add up
4. ✅ **Set up alerts** - Error rate > 5%, latency > 30s, cost spike
5. ✅ **Implement retry logic** - Exponential backoff for rate limits
6. ✅ **Use structured logging** - JSON format for easy parsing
7. ❌ **Don't** log full prompts by default - PII and cost concerns
8. ❌ **Don't** ignore cache hit rate - Measure and optimize

## Related Skills

- `@designing-data-storage` - File formats (Parquet, Lance) and lakehouse formats for storing embeddings
- `@data-engineering-core` - Polars, DuckDB for data processing in pipelines
- `@data-engineering-orchestration` - Scheduling and batch processing for embedding generation
- `@data-engineering-quality` - Data validation and quality checks for ML pipelines
- `@data-engineering-observability` - General pipeline monitoring with OpenTelemetry

## References

- [OpenAI Embeddings Guide](https://platform.openai.com/docs/guides/embeddings)
- [Sentence Transformers](https://www.sbert.net/)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [pgvector Documentation](https://github.com/pgvector/pgvector)
- [LangChain RAG Guide](https://python.langchain.com/docs/tutorials/rag/)
- [LlamaIndex Documentation](https://docs.llamaindex.ai/)
