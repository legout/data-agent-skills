# Vector Databases

Storing and querying vector embeddings for similarity search. Covers LanceDB (embedded, cloud-native), pgvector (PostgreSQL extension), and DuckDB (list-based) for different scale and deployment needs.

## LanceDB: Embedded Cloud-Native

### Why LanceDB?
- **Arrow-native**: Zero-copy, integrates with Polars/PyArrow
- **Embedded**: Single file `.lancedb` directory (no server)
- **Scales**: Same API for local dev and cloud (S3, GCS)
- **Rich types**: Supports images, point clouds, multimodal

### Installation
```bash
pip install lancedb
```

### Quickstart
```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
import polars as pl

# Connect (creates directory if not exists)
db = lancedb.connect("./.lancedb")

# Define schema with Pydantic
class Document(LanceModel):
    id: int
    text: str
    vector: Vector(1536)  # embedding dimension

# Create table
table = db.create_table("documents", schema=Document)

# Add data (embeddings must be list[float])
table.add([
    {"id": 1, "text": "hello world", "vector": [0.1] * 1536},
    {"id": 2, "text": "goodbye world", "vector": [0.2] * 1536},
])

# Vector search
query = [0.1] * 1536
results = table.search(query).limit(5).to_pandas()
print(results)
```

### Auto-Embedding with Registry
```python
from lancedb.embeddings import get_registry

embedder = get_registry().get("sentence-transformers").create(
    model="all-MiniLM-L6-v2"
)

class AutoDoc(LanceModel):
    text: str = embedder.SourceField()  # Auto-encoded on add()
    vector: Vector(embedder.ndims()) = embedder.VectorField()

table = db.create_table("auto_docs", schema=AutoDoc)
table.add([{"text": "LanceDB makes vector search easy"}])
# Embeddings generated automatically
```

### Indexing
```python
# IVF_PQ (disk-efficient, good for <100M vectors)
table.create_index(
    metric="cosine",
    num_partitions=256,      # ~ num_rows / 8192
    num_sub_vectors=96       # ~ dimension / 8
)

# HNSW (high recall, higher memory)
table.create_index(
    metric="cosine",
    index_type="hnsw",
    m=16,
    ef_construction=200
)

# Wait for async build
table.wait_for_index()
```

### Search Tuning
```python
# Increase accuracy at cost of latency
results = (
    table.search(query)
    .limit(10)
    .nprobes(50)        # Scan more IVF partitions
    .refine_factor(25)  # Rerank more candidates
    .to_pandas()
)
```

### Cloud Storage
```python
db = lancedb.connect("s3://bucket/lancedb")
# Works with s3://, gs://, az://
# Credentials from env (AWS_ACCESS_KEY_ID, etc.)
```

### Polars Integration
```python
# Convert between LanceDB and Polars
arrow_table = table.search().to_arrow()
df = pl.from_arrow(arrow_table)

# Write Polars DataFrame to LanceDB
table.add(df.to_arrow())
```

---

## pgvector: PostgreSQL Extension

### When to Use pgvector
- Need ACID transactions with vectors
- Already using PostgreSQL
- Need complex joins with vector search
- Downtime not acceptable (server always running)

### Installation
```sql
-- PostgreSQL with pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Or Docker: timescale/timescaledb:latest-pg15 includes pgvector
```

### Schema
```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    content TEXT,
    embedding VECTOR(1536),  -- OpenAI embedding dimension
    metadata JSONB
);

-- HNSW index for cosine similarity
CREATE INDEX ON documents
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);
```

### Python Operations
```python
import psycopg2
import numpy as np

def store_embeddings(conn, documents: list, embeddings: list):
    """Store documents with embeddings."""
    with conn.cursor() as cur:
        for doc, emb in zip(documents, embeddings):
            cur.execute(
                """
                INSERT INTO documents (content, embedding, metadata)
                VALUES (%s, %s::vector, %s::jsonb)
                """,
                (doc['content'], emb.tolist(), doc.get('metadata', {}))
            )
    conn.commit()

def similarity_search(conn, query_embedding: list, top_k: int = 5):
    """Cosine similarity search."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, content, metadata,
                   1 - (embedding <=> %s::vector) as similarity
            FROM documents
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """, (query_embedding, query_embedding, top_k))
        return cur.fetchall()
```

---

## DuckDB: Simple Vector Search

DuckDB doesn't have a native vector type, but you can use array operations for small-scale use:

```python
import duckdb
import numpy as np

# Create table with FLOAT[] (list of floats)
con = duckdb.connect()
con.sql("""
    CREATE TABLE documents (
        id INTEGER,
        content TEXT,
        embedding FLOAT[1536]
    )
""")

# Insert (using numpy)
embedding = np.random.rand(1536).tolist()
con.sql("INSERT INTO documents VALUES (1, 'hello', ?)", [embedding])

# Cosine similarity (custom function needed)
con.sql("""
    SELECT id, content,
           list_cosine_similarity(embedding, ?) as similarity
    FROM documents
    ORDER BY similarity DESC
    LIMIT 5
""", [query_embedding])
```

**Note**: DuckDB's vector search is not optimized for large-scale production. Use LanceDB or pgvector for production vector workloads.

---

## Comparison

| Feature | LanceDB | pgvector | DuckDB |
|---------|---------|----------|--------|
| **Deployment** | Embedded file or cloud | PostgreSQL server | Embedded process |
| **Scale** | Millions of vectors | Tens of millions | <100k vectors |
| **Index Types** | IVF_PQ, HNSW | HNSW, IVFFlat | None |
| **Cloud Native** | ✅ (S3/GCS/Azure) | Via Postgres RDS | ❌ |
| **Transactions** | ✅ (ACID via Lance format) | ✅ (PostgreSQL) | ✅ |
| **Python API** | Native, friendly | SQL via psycopg2 | SQL |
| **Best For** | RAG apps, prototyping | Production with existing PG | Quick experiments |

---

## Best Practices

1. ✅ **LanceDB**: Build index after bulk ingestion, use HNSW for high recall
2. ✅ **pgvector**: Use `vector_cosine_ops` for cosine, tune `m` and `ef_construction`
3. ✅ **Dimensions**: 1536 (OpenAI ada/small) or 384 (MiniLM) common
4. ✅ **Normalize**: Ensure embeddings are normalized if using cosine
5. ✅ **Metadata filtering**: Filter on metadata before vector search wherever possible
6. ❌ **Don't** store non-indexed large vectors in pgvector (use `vector` type, not `bytea`)
7. ❌ **Don't** rebuild indexes frequently (expensive)
8. ❌ **Don't** use DuckDB for serious vector workloads (no indexes)

---

## References

- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [pgvector GitHub](https://github.com/pgvector/pgvector)
- `@data-engineering-ai-ml/embeddings.md` - Embedding generation
- `@data-engineering-ai-ml/rag-pipelines.md` - End-to-end RAG patterns
