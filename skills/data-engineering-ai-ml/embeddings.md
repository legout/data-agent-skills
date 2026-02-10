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
        texts = df[text_col].to_list()
        embeddings = self.generate_embeddings(texts)
        return df.with_columns(pl.Series("embedding", embeddings))

# Usage
pipeline = OpenAIEmbeddingPipeline()
df = pipeline.process_dataframe(pl.read_parquet("documents.parquet"), "text")
```

### Costs & Limits
- `text-embedding-3-small`: $0.00002 / 1K tokens (1536 dim)
- `text-embedding-3-large`: $0.00013 / 1K tokens (3072 dim)
- `text-embedding-ada-002`: $0.00010 / 1K tokens (1536 dim)
- Max input: 8192 tokens (3-small/large), 8191 (ada-002)

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
df = pl.DataFrame({
    "text": texts,
    "embedding": embeddings_list
})
```

### Model Selection
| Model | Dimensions | Size | Speed | Quality |
|-------|------------|------|-------|---------|
| `all-MiniLM-L6-v2` | 384 | 80MB | ⚡⚡⚡ | Good |
| `all-mpnet-base-v2` | 768 | 420MB | ⚡⚡ | Better |
| `multi-qa-mpnet-base-dot-v1` | 768 | 420MB | ⚡⚡ | QA-optimized |
| `text-embedding-3-small` | 1536 | - | ⚡ | OpenAI |

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
- `@data-engineering-ai-ml/vector-databases.md` - Storing and querying embeddings
