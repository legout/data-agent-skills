# RAG (Retrieval-Augmented Generation) Pipelines

Building RAG systems: document chunking, embedding generation, vector retrieval, context assembly, and LLM prompting patterns.

## Pipeline Flow

```
Raw Documents → Chunking → Embeddings → Vector DB → Query → Context Assembly → LLM → Answer
```

## 1. Document Chunking Strategies

Chunking balances context window limits (3K-128K tokens) with semantic coherence.

### By Tokens (Simple)
```python
from typing import List
import tiktoken

def chunk_by_tokens(text: str, max_tokens: int = 500, overlap: int = 50) -> List[str]:
    """Split text into overlapping chunks."""
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    tokens = encoding.encode(text)
    chunks = []

    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunks.append(encoding.decode(chunk_tokens))

    return chunks
```

### By Paragraphs (Preserves Structure)
```python
import re

def chunk_by_paragraphs(text: str, max_chars: int = 2000) -> List[str]:
    """Split on paragraph boundaries, max size."""
    paragraphs = re.split(r'\n\s*\n', text)
    chunks, current = [], ""

    for para in paragraphs:
        if len(current) + len(para) < max_chars:
            current += para + "\n\n"
        else:
            if current:
                chunks.append(current.strip())
            current = para + "\n\n"

    if current:
        chunks.append(current.strip())
    return chunks
```

### Semantic Chunking (Advanced)
Use embeddings to detect semantic boundaries. Libraries: `semantic-router`, `langchain.text_splitter.RecursiveCharacterTextSplitter`.

```python
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""]
)

chunks = splitter.split_text(long_document)
```

---

## 2. Context Assembly

Build context within LLM token budget (e.g., 3K-16K tokens).

```python
from dataclasses import dataclass
from typing import List
import tiktoken

@dataclass
class RAGContext:
    query: str
    chunks: List[str]
    sources: List[dict]
    token_count: int

class RAGAssembler:
    def __init__(self, max_tokens: int = 3000, model: str = "gpt-3.5-turbo"):
        self.max_tokens = max_tokens
        self.encoding = tiktoken.encoding_for_model(model)

    def assemble(self, query: str, results: List[dict]) -> RAGContext:
        """Build context within token budget."""
        chunks, sources = [], []
        total_tokens = len(self.encoding.encode(query))

        for res in results:
            chunk = res['text']
            chunk_tokens = len(self.encoding.encode(chunk))

            if total_tokens + chunk_tokens > self.max_tokens:
                break

            chunks.append(chunk)
            sources.append({
                'id': res['id'],
                'similarity': res['similarity'],
                'metadata': res.get('metadata', {})
            })
            total_tokens += chunk_tokens

        return RAGContext(
            query=query,
            chunks=chunks,
            sources=sources,
            token_count=total_tokens
        )
```

---

## 3. Prompt Construction

```python
def build_rag_prompt(context: RAGContext, system_prompt: str = None) -> str:
    """Construct prompt with retrieved context."""
    context_text = "\n\n---\n\n".join(context.chunks)

    prompt = system_prompt or """Answer the question based on the provided context.
If the answer is not in the context, say 'I don't know'."""

    return f"""<|system|>
{prompt}

Context:
{context_text}

<|user|>
{context.query}

<|assistant|>"""
```

---

## 4. Complete RAG Pipeline

```python
"""
Production RAG pipeline with vector DB, retrieval, and LLM generation.
"""
import polars as pl
import lancedb
from sentence_transformers import SentenceTransformer
import openai
from typing import List, Dict

class RAGPipeline:
    def __init__(
        self,
        db_path: str,
        embedding_model: str = "all-MiniLM-L6-v2",
        openai_model: str = "gpt-4o"
    ):
        # Load embedding model (local)
        self.embedder = SentenceTransformer(embedding_model)

        # Connect to vector DB
        self.db = lancedb.connect(db_path)
        self.table = self.db.open_table("documents")

        # LLM client
        self.openai_client = openai.OpenAI()
        self.openai_model = openai_model

    def retrieve(self, query: str, k: int = 5, filters: Dict = None) -> List[Dict]:
        """Retrieve relevant documents."""
        query_embedding = self.embedder.encode([query])[0].tolist()

        search = self.table.search(query_embedding).limit(k)

        if filters:
            # Build where clause
            conditions = " AND ".join(f"{k} = '{v}'" for k, v in filters.items())
            search = search.where(conditions)

        results = search.to_pandas()
        return results.to_dict('records')

    def generate(self, query: str, context_docs: List[Dict]) -> str:
        """Generate answer with LLM."""
        context_text = "\n\n---\n\n".join([doc['text'] for doc in context_docs])

        response = self.openai_client.chat.completions.create(
            model=self.openai_model,
            messages=[
                {"role": "system", "content": """Answer based on the provided context.
Cite sources using [1], [2], etc. If uncertain, say so."""},
                {"role": "user", "content": f"""Context:
{context_text}

Question: {query}

Answer:"""}
            ],
            temperature=0.1
        )
        return response.choices[0].message.content

    def run(self, query: str, k: int = 5, filters: Dict = None) -> dict:
        """Full RAG pipeline."""
        docs = self.retrieve(query, k, filters)
        answer = self.generate(query, docs)

        return {
            "answer": answer,
            "sources": [{"id": d['id'], "text": d['text'][:200]} for d in docs],
            "source_count": len(docs)
        }

# Usage
rag = RAGPipeline("./.lancedb")
result = rag.run("What is RAG?")
print(result["answer"])
print(f"Sources: {result['source_count']} documents")
```

---

## 5. Advanced: Re-ranking

Cross-encoders improve retrieval quality by re-ranking initial results.

```python
from sentence_transformers import CrossEncoder

# Load re-ranker model
reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

def rerank_results(query: str, retrieved_docs: List[dict]) -> List[dict]:
    """Re-rank retrieved documents using cross-encoder."""
    pairs = [(query, doc['text']) for doc in retrieved_docs]
    scores = reranker.predict(pairs)

    # Sort by re-ranker score
    for doc, score in zip(retrieved_docs, scores):
        doc['rerank_score'] = score

    return sorted(retrieved_docs, key=lambda x: x['rerank_score'], reverse=True)

# In pipeline: retrieve more, then re-rank to top_k
docs = rag.retrieve(query, k=20)  # Retrieve more
docs = rerank_results(query, docs)[:5]  # Re-rank and take top 5
```

---

## 6. Evaluation

### Relevance Scoring
```python
def evaluate_retrieval(query: str, relevant_doc_ids: set, retrieved_docs: List[Dict]):
    """Calculate precision@k."""
    retrieved_ids = {doc['id'] for doc in retrieved_docs}
    precision = len(relevant_doc_ids & retrieved_ids) / len(retrieved_ids)
    return precision
```

### LLM-as-a-Judge
```python
from openai import OpenAI

client = OpenAI()

def evaluate_answer(query: str, answer: str, ground_truth: str) -> float:
    """Use LLM to score answer quality."""
    judge_prompt = f"""Score the following answer on a scale of 1-5 for correctness, relevance, and completeness.

Question: {query}
Answer: {answer}
Expected: {ground_truth}

Provide only the numeric score (1-5):"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": judge_prompt}]
    )
    # Parse score from response
    try:
        return float(response.choices[0].message.content.strip())
    except:
        return 0.0
```

---

## Best Practices

1. ✅ **Chunk intelligently** - Don't split mid-sentence; respect semantic boundaries
2. ✅ **Include metadata** - Filter on date, source, author before vector search
3. ✅ **Hybrid search** - Combine vector + keyword/BM25 for better recall
4. ✅ **Re-rank top results** - Use cross-encoder to rerank initial retrieval
5. ✅ **Track token usage** - Costs add up, especially with long contexts
6. ✅ **Add citations** - Include source IDs in answer for verifiability
7. ❌ **Don't** use last-layer embeddings for retrieval if you fine-tuned model
8. ❌ **Don't** retrieve everything - Use metadata filters to narrow first
9. ❌ **Don't** put PII in vector DB without encryption

---

## References

- [LangChain RAG Guide](https://python.langchain.com/docs/tutorials/rag/)
- [LlamaIndex RAG](https://docs.llamaindex.ai/en/stable/module_guides/deploying/query_pipeline/root/)
- `@engineering-ai-pipelines/references/embeddings.md` - Embedding generation
- `@engineering-ai-pipelines/references/vector-stores.md` - Vector storage
- `@engineering-ai-pipelines/references/monitoring.md` - LLM monitoring
