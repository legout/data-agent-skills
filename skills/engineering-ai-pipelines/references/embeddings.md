# Embeddings Generation

Generating vector embeddings from text for use in RAG, semantic search, and ML pipelines. Covers OpenAI API and local models (sentence-transformers).

## OpenAI Embeddings API

### Installation
```bash
pip install openai tiktoken
```

### Batch Embedding Pipeline
```python
import openai
from typing import List
import tiktoken

class OpenAIEmbeddingPipeline:
    def __init__(self, model: str = "text-embedding-3-small"):
        self.client = openai.OpenAI()
        self.model = model
        self.encoding = tiktoken.encoding_for_model(model)

    def chunk_text(self, text: str, max_tokens: int = 8000) -> List[str]:
        """Split text into token-safe chunks."""
        tokens = self.encoding.encode(text)
        return [
            self.encoding.decode(tokens[i:i + max_tokens])
            for i in range(0, len(tokens), max_tokens)
        ]

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
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

    def process_dataframe(self, df, text_col: str):
        """Add embeddings column to Polars DataFrame."""
        import polars as pl
        texts = df[text_col].to_list()
        embeddings = self.generate_embeddings(texts)
        return df.with_columns(pl.Series("embedding", embeddings))

# Usage
import polars as pl

pipeline = OpenAIEmbeddingPipeline()
df = pipeline.process_dataframe(pl.read_parquet("documents.parquet"), "text")
```

### Costs & Limits
| Model | Dimensions | Cost per 1M tokens | Max tokens |
|-------|------------|-------------------|------------|
| `text-embedding-3-small` | 1536 | $0.020 | 8192 |
| `text-embedding-3-large` | 3072 | $0.130 | 8192 |
| `text-embedding-ada-002` | 1536 | $0.100 | 8191 |

## Local Embeddings (sentence-transformers)

### Installation
```bash
pip install sentence-transformers
```

### Usage
```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dim, fast

# Encode list of texts
texts = ["Hello world", "Goodbye world"]
embeddings = model.encode(texts, show_progress_bar=True)  # numpy arrays

# Convert to list for storage
embeddings_list = embeddings.tolist()

# For Polars DataFrame
import polars as pl
df = pl.DataFrame({
    "text": texts,
    "embedding": embeddings_list
})
```

### Model Selection
| Model | Dimensions | Size | Speed | Quality | Best For |
|-------|------------|------|-------|---------|----------|
| `all-MiniLM-L6-v2` | 384 | 80MB | ⚡⚡⚡ | Good | Speed, low resources |
| `all-mpnet-base-v2` | 768 | 420MB | ⚡⚡ | Better | Quality priority |
| `multi-qa-mpnet-base-dot-v1` | 768 | 420MB | ⚡⚡ | QA-optimized | Question-answering |
| `paraphrase-multilingual-MiniLM-L12-v2` | 384 | 120MB | ⚡⚡ | Good | Non-English text |

## Batch Processing with Polars

```python
from sentence_transformers import SentenceTransformer
import polars as pl

model = SentenceTransformer('all-MiniLM-L6-v2')

# Process in batches to avoid OOM
batch_size = 1000
reader = pl.read_csv_batched("large_corpus.csv", batch_size=batch_size)

all_embeddings = []
for batches in reader:
    for batch in batches:
        batch_embeddings = model.encode(batch["text"].to_list())
        all_embeddings.extend(batch_embeddings.tolist())

# Create final DataFrame
df = pl.DataFrame({
    "embedding": all_embeddings
})
df.write_parquet("corpus_with_embeddings.parquet")
```

## Caching Strategy

```python
import hashlib
import json
from pathlib import Path

class EmbeddingCache:
    def __init__(self, cache_dir: str = ".embedding_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_key(self, text: str, model: str) -> str:
        return hashlib.md5(f"{model}:{text}".encode()).hexdigest()

    def get(self, text: str, model: str) -> list[float] | None:
        cache_key = self._get_cache_key(text, model)
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text())
        return None

    def set(self, text: str, model: str, embedding: list[float]):
        cache_key = self._get_cache_key(text, model)
        cache_file = self.cache_dir / f"{cache_key}.json"
        cache_file.write_text(json.dumps(embedding))

# Usage
cache = EmbeddingCache()

for text in texts:
    cached = cache.get(text, "all-MiniLM-L6-v2")
    if cached:
        embedding = cached
    else:
        embedding = model.encode([text])[0].tolist()
        cache.set(text, "all-MiniLM-L6-v2", embedding)
```

## Token Counting

```python
import tiktoken

def count_tokens(text: str, model: str = "text-embedding-3-small") -> int:
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))

# Check before sending
for text in texts:
    if count_tokens(text) > 8192:
        # Split into chunks
        chunks = chunk_text(text, max_tokens=8000)
        # Process chunks separately
```

## Best Practices

1. ✅ **Batch API calls** - OpenAI accepts up to 100 texts per request
2. ✅ **Chunk long texts** - Split on token limits, respect natural boundaries
3. ✅ **Cache embeddings** - Don't regenerate identical text (hash-based cache)
4. ✅ **Normalize embeddings** - Most similarity metrics expect normalized vectors
5. ✅ **Use appropriate model** - Trade off quality vs cost/latency
6. ❌ **Don't** embed rich text (HTML, markdown) - Extract plain text first
7. ❌ **Don't** mix embedding models in same index - Different dimensionalities break similarity

---

## References

- [OpenAI Embeddings](https://platform.openai.com/docs/guides/embeddings)
- [Sentence Transformers](https://www.sbert.net/)
- `@engineering-ai-pipelines/references/vector-stores.md` - Storing and querying embeddings
