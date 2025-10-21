import os
import random
import uuid
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from .schemas import StartEvalRequest, StartEvalResponse, SubmitEvalRequest, SubmitEvalResponse
from .retrievers import BM25Retriever, VectorRetriever
from .llm import generate_answer, expand_query
from .store import EvalStore
from .logging_config import configure_logging


APP_TITLE = "Dutch Law RAG Evaluation"

configure_logging()
app = FastAPI(title=APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# Initialize dependencies (lazy connect inside methods)
bm25 = BM25Retriever(
    host=os.getenv("OPENSEARCH_HOST", "opensearch"),
    port=int(os.getenv("OPENSEARCH_PORT", "9200")),
    index=os.getenv("BM25_INDEX", "laws_bm25"),
)
vector = VectorRetriever(
    host=os.getenv("WEAVIATE_HOST", "weaviate"),
    port=int(os.getenv("WEAVIATE_PORT", "8080")),
    class_name=os.getenv("WEAVIATE_CLASS", "DocumentChunk"),
)

store = EvalStore(db_path=os.getenv("EVAL_DB_PATH", "./evaluations.sqlite"))
store.init()

log = logging.getLogger(__name__)


@app.get("/", response_class=HTMLResponse)
def index_page():
    # Serve the static UI
    with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/health")
def health():
    # Soft healthcheck
    return {"status": "ok"}


@app.post("/evaluate/start", response_model=StartEvalResponse)
def start_evaluation(req: StartEvalRequest):
    raw_question = req.question.strip()
    if not raw_question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    top_k = req.top_k or 5
    window_size = req.window_size or 0
    topic = (req.topic or "").strip() or None
    scenario_text = (req.scenario or "").strip()
    has_scenario = (
        bool(req.scenario_defined)
        if req.scenario_defined is not None
        else bool(scenario_text)
    )
    question = raw_question
    if scenario_text and scenario_text not in question:
        question = f"{raw_question}\n\nScenario: {scenario_text}"

    t0 = time.monotonic()
    log.info(
        "eval.start q_len=%d raw_q_len=%d top_k=%d window=%d topic=%s has_scenario=%s",
        len(question),
        len(raw_question),
        top_k,
        window_size,
        topic or "-",
        has_scenario,
    )

    # Expand NL question into BM25 terms/phrases and vector concepts
    expansion = expand_query(question)

    # Retrieve contexts (be resilient to backend hiccups)
    try:
        bm25_hits = bm25.search(question, top_k=top_k, expanded={
            "bm25_terms": expansion.get("bm25_terms") or [],
            "bm25_phrases": expansion.get("bm25_phrases") or [],
        })
    except Exception:
        bm25_hits = []
        log.exception("bm25.search failed q_len=%d", len(question))
    try:
        vector_hits = vector.search(
            question,
            top_k=top_k,
            window_size=window_size,
            concepts=expansion.get("vector_concepts") or None,
        )
    except Exception:
        vector_hits = []
        log.exception("weaviate.search failed q_len=%d", len(question))

    def _filter_hits(hits):
        out = []
        for h in hits:
            c = (h.get("content") or "").strip()
            if not c:
                continue
            out.append({
                "content": c,
                "document_id": h.get("document_id"),
                "chunk_index": h.get("chunk_index"),
                "source": h.get("source"),
            })
        return out

    bm25_hits_filtered = _filter_hits(bm25_hits)
    vector_hits_filtered = _filter_hits(vector_hits)
    log.info(
        "eval.retrieved bm25_hits=%d vector_hits=%d",
        len(bm25_hits_filtered), len(vector_hits_filtered),
    )

    bm25_context = "\n\n".join([h["content"] for h in bm25_hits_filtered])
    vector_context = "\n\n".join([h["content"] for h in vector_hits_filtered])

    if not bm25_context.strip():
        bm25_context = ""
    if not vector_context.strip():
        vector_context = ""

    # Keep generator constant
    try:
        answer_bm25 = generate_answer(question, bm25_context)
    except Exception as e:
        log.exception("gen.bm25 failed: %s", e)
        answer_bm25 = "Ik kan op basis van de aangeleverde context geen definitief antwoord geven."
    try:
        answer_vector = generate_answer(question, vector_context)
    except Exception as e:
        log.exception("gen.embeddings failed: %s", e)
        answer_vector = "Ik kan op basis van de aangeleverde context geen definitief antwoord geven."

    # Shuffle options
    options = [
        {"method": "bm25", "answer": answer_bm25, "sources": bm25_hits_filtered},
        {"method": "embeddings", "answer": answer_vector, "sources": vector_hits_filtered},
    ]
    random.shuffle(options)

    eval_id = str(uuid.uuid4())
    # Persist mapping and payload
    record = {
        "evaluation_id": eval_id,
        "created_at": datetime.utcnow().isoformat(),
        "question": question,
        "optionA": options[0],
        "optionB": options[1],
        "chosen_option": None,
        "chosen_method": None,
        "top_k": top_k,
        "window_size": window_size,
        "topic": topic,
        "scenario": scenario_text,
        "has_scenario": has_scenario,
    }
    store.create(record)
    log.info(
        "eval.ready id=%s bm25_len=%d vec_len=%d dur_ms=%d",
        eval_id, len(bm25_hits_filtered), len(vector_hits_filtered), int((time.monotonic() - t0) * 1000)
    )

    return StartEvalResponse(
        evaluation_id=eval_id,
        optionA={
            "method": options[0]["method"],
            "answer": options[0]["answer"],
            "sources": options[0]["sources"],
        },
        optionB={
            "method": options[1]["method"],
            "answer": options[1]["answer"],
            "sources": options[1]["sources"],
        },
    )


@app.post("/evaluate/submit", response_model=SubmitEvalResponse)
def submit_evaluation(req: SubmitEvalRequest):
    rec = store.get(req.evaluation_id)
    if not rec:
        raise HTTPException(status_code=404, detail="evaluation not found")
    if req.choice not in ("A", "B", "N"):
        raise HTTPException(status_code=400, detail="choice must be 'A', 'B' or 'N'")

    if req.choice == "N":
        chosen_method = "neutral"
    else:
        chosen_method = rec["optionA"]["method"] if req.choice == "A" else rec["optionB"]["method"]
    store.update_choice(req.evaluation_id, req.choice, chosen_method)

    return SubmitEvalResponse(
        evaluation_id=req.evaluation_id,
        choice=req.choice,
        chosen_method=chosen_method,
    )
