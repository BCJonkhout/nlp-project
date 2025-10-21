Dutch Law RAG Evaluation
========================

Goal
----
- Evaluate which retrieval context works best for answering questions about Dutch law: BM25 (keyword) vs text embeddings.
- Keep the generator G constant (same LLM and prompt across both conditions).
- Provide a simple A/B evaluation UI where users see two shuffled answers and pick the better one.

Repository Contents (relevant)
------------------------------
- `scraper/deep_crawler_wetten.py`: Crawl wetten.overheid.nl (HTML and PDF) and dump content.
- `scraper/convert_wetten_crawled_to_markdown.py`: Convert crawl dumps into structured Markdown.
- `rag-pipeline/rag-pipeline.py`: Partial code that references Weaviate ingestion/querying; not a complete service.
- `docker-compose.yml`: Orchestrates the evaluation stack.
- `services/api`: FastAPI app serving the A/B evaluation UI and APIs.
- `scripts/ingest.py`: Ingest Markdown into OpenSearch (BM25) and Weaviate (embeddings).

Architecture
------------
- `OpenSearch` stores chunks for BM25 retrieval.
- `Weaviate` stores chunks with vectors using `text2vec-google` (Gemini embeddings) for near-text retrieval.
- `API (FastAPI)` exposes endpoints to:
  - start an evaluation: run both retrievers, use same LLM to generate answers, return shuffled options A/B
  - submit a choice: persist which option (and thus which method) was preferred
- `SQLite` persists evaluations in a lightweight file (`/data/evaluations.sqlite`).

Query Expansion (Natural Language → Retrieval)
---------------------------------------------
- The API expands natural-language questions before retrieval:
  - BM25: Gemini (or a local fallback) extracts key terms and short phrases. The BM25 query uses a bool with boosted `match_phrase` and `match` clauses, ensuring at least one term matches.
  - Embeddings: The same expansion yields multiple `nearText` concepts for Weaviate to improve recall/precision.
- This allows asking natural questions while BM25 still benefits from precise term/phrase matching.
- Environment vars (optional):
  - `GEMINI_MODEL` influences both answering and expansion; by default a Gemini Flash model is used.

Prerequisites
-------------
- Docker and Docker Compose.
- Google Cloud service account JSON for Vertex AI access (Gemini).
  - Place the JSON at `secrets/gcp.json` (project root). It is mounted into containers at `/secrets/gcp.json`.
  - Set `GCP_PROJECT` and `GCP_LOCATION` in your `.env`.

Quick Start
-----------
1) Copy `.env.example` to `.env` and edit if needed.

2) Start the stack:
   docker compose up -d --build

   Services:
   - API/UI: http://localhost:8000
   - Weaviate: http://localhost:8080
   - OpenSearch: http://localhost:9200

3) Ingest data (Markdown from the provided scraper/converter):
   python scripts/ingest.py path/to/wetten.md --os-host localhost --w-host localhost --w-grpc-port 50051

   Notes:
   - This splits Markdown into ~1000-char chunks (200 overlap) and indexes into both OpenSearch and Weaviate.
   - Default OpenSearch index is `laws_bm25`, Weaviate class `DocumentChunk`.
   - Weaviate collection is created with `text2vec-google` vectorizer (server-side Gemini embeddings).
   - For OpenSearch 3.x images, an initial admin password may be required by the Security plugin. The compose file disables the plugin for local use and sets `DISABLE_INSTALL_DEMO_CONFIG=true`. If you choose to enable security, set `OPENSEARCH_INITIAL_ADMIN_PASSWORD` and configure `OPENSEARCH_USER`/`OPENSEARCH_PASSWORD` in `.env`.

4) Run an evaluation:
   - Open http://localhost:8000
   - Enter a question about Dutch law and click "Generate Options".
   - Review Options A and B (answers + sources) and pick the better one.
   - The app stores your choice and reveals which method you picked.

Keeping G Constant (Generator)
-----------------------------
- The API uses Google Gemini (Vertex AI) for both retrieval conditions with the same prompt/config.
- Provide a Google Cloud service account JSON and set:
  - `GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp.json` (mounted file)
  - `GCP_PROJECT` and `GCP_LOCATION` (e.g., `europe-west4`)
  - Optional: `GEMINI_MODEL` (default `gemini-2.5-flash`)
- If Gemini is not configured, the API returns a deterministic fallback string so you can still test the flow.

Endpoints
---------
- `POST /evaluate/start` { question, top_k?, window_size? }
  - Returns: `evaluation_id`, and two options (A/B) with answers and sources.

- `POST /evaluate/submit` { evaluation_id, choice: 'A'|'B'|'N' }
  - Returns: confirmation with the chosen method (`bm25`, `embeddings`, or `neutral`).

Data Model (SQLite)
-------------------
- Table `evaluations`: stores `evaluation_id`, timestamps, question, A/B payloads (JSON), and user choice.

Configuration
-------------
- `.env.example` includes:
  - `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT`, `GCP_LOCATION`, `GEMINI_MODEL`, `GEMINI_EMBEDDING_MODEL`
  - `OPENSEARCH_HOST/PORT`, `BM25_INDEX`
  - `OPENSEARCH_INITIAL_ADMIN_PASSWORD` (for 3.x security), optional `OPENSEARCH_USER`/`OPENSEARCH_PASSWORD`
  - `WEAVIATE_HOST/PORT`, `WEAVIATE_CLASS`
  - `EVAL_DB_PATH`

Relation to rag-pipeline
------------------------

- A self-contained ingestion and evaluation stack is provided via `scripts/ingest.py` and `services/api`, using a `DocumentChunk`-like schema (`content`, `document_id`, `chunk_index`, `source`).

Troubleshooting
---------------
- Weaviate with `text2vec-google` needs a valid GCP project and service account; ensure the secret is mounted and `GCP_PROJECT` is set.
- If OpenSearch is red/initializing, wait ~30s before ingestion/search.
- If you see placeholder answers, confirm GCP credentials are configured or check logs for Gemini errors.
 - If BM25 results feel too broad, reduce `top_k` or adjust your query; the expansion layer is additive, but you can refine the question to steer terms/phrases.
 - If ingesting from host, ensure Weaviate gRPC port 50051 is exposed (compose maps `50051:50051`) and pass `--w-grpc-port 50051` to the ingest script.

Next Steps / Ideas
------------------
- Add batch evaluation mode with CSV/JSON of questions and a simple results dashboard.
- Add richer logging: response latency, token usage, and retrieval scores.
- Extend ingestion to support ZIPs and PDFs directly (using the existing scraper outputs).
