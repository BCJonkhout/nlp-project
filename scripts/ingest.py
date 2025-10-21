#!/usr/bin/env python3
import argparse
import os
import re
import time
import uuid as _uuid
from typing import List

from opensearchpy import OpenSearch, helpers
import weaviate


def simple_markdown_split(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunk = text[start:end]
        chunks.append(chunk)
        if end == n:
            break
        start = max(start + chunk_size - overlap, 0)
    return chunks


def ensure_opensearch_index(client: OpenSearch, index: str):
    if not client.indices.exists(index=index):
        body = {
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            },
            "mappings": {
                "properties": {
                    "content": {"type": "text"},
                    "document_id": {"type": "keyword"},
                    "chunk_index": {"type": "integer"},
                    "source": {"type": "keyword"}
                }
            }
        }
        client.indices.create(index=index, body=body)


def _weaviate_class_exists(wc: weaviate.WeaviateClient, class_name: str) -> bool:
    try:
        listed = wc.collections.list_all()
        if isinstance(listed, list):
            for c in listed:
                if (isinstance(c, str) and c == class_name) or (hasattr(c, 'name') and getattr(c, 'name', None) == class_name):
                    return True
        # Fallback: try to get the collection
        try:
            wc.collections.get(class_name)
            return True
        except Exception:
            return False
    except Exception:
        # Final fallback
        try:
            wc.collections.get(class_name)
            return True
        except Exception:
            return False


def ensure_weaviate_class(
    wc: weaviate.WeaviateClient,
    class_name: str,
    *,
    recreate: bool = False,
    project_id: str | None = None,
):
    exists = _weaviate_class_exists(wc, class_name)
    if recreate and exists:
        try:
            wc.collections.delete(class_name)
        except Exception:
            pass
        exists = False
    if exists:
        return
    # Prefer explicit vectorizer configuration if project id is available
    if project_id is None:
        project_id = os.getenv("GCP_PROJECT", "")
    try:
        if project_id:
            vc = weaviate.classes.config.Configure.Vectorizer.text2vec_google(project_id=project_id)
            wc.collections.create(
                name=class_name,
                vectorizer_config=vc,
                properties=[
                    weaviate.classes.config.Property(name="content", data_type=weaviate.classes.config.DataType.TEXT),
                    weaviate.classes.config.Property(name="document_id", data_type=weaviate.classes.config.DataType.TEXT),
                    weaviate.classes.config.Property(name="chunk_index", data_type=weaviate.classes.config.DataType.INT),
                    weaviate.classes.config.Property(name="source", data_type=weaviate.classes.config.DataType.TEXT),
                ]
            )
        else:
            # Rely on server DEFAULT_VECTORIZER_MODULE (text2vec-google)
            wc.collections.create(
                name=class_name,
                properties=[
                    weaviate.classes.config.Property(name="content", data_type=weaviate.classes.config.DataType.TEXT),
                    weaviate.classes.config.Property(name="document_id", data_type=weaviate.classes.config.DataType.TEXT),
                    weaviate.classes.config.Property(name="chunk_index", data_type=weaviate.classes.config.DataType.INT),
                    weaviate.classes.config.Property(name="source", data_type=weaviate.classes.config.DataType.TEXT),
                ]
            )
    except Exception as e:
        raise


def ingest_markdown(
    md_path: str,
    os_host: str,
    os_port: int,
    os_index: str,
    w_host: str,
    w_port: int,
    w_grpc_port: int,
    w_class: str,
    *,
    recreate_class: bool = False,
    gcp_project: str | None = None,
    w_rate_limit_per_minute: int = 50000,
):
    with open(md_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Derive document id from file name
    fname = os.path.basename(md_path)
    m = re.search(r'(BWBR\w+)', text)
    doc_id = m.group(1) if m else fname

    chunks = simple_markdown_split(text, chunk_size=1000, overlap=200)

    os_client = OpenSearch(hosts=[{"host": os_host, "port": os_port}], use_ssl=False, verify_certs=False)
    ensure_opensearch_index(os_client, os_index)

    actions = []
    for i, ch in enumerate(chunks):
        # Deterministic IDs to avoid duplicates on re-ingest
        oid = f"{doc_id}:{i}"
        actions.append({
            "_index": os_index,
            "_id": oid,
            "_source": {
                "content": ch,
                "document_id": doc_id,
                "chunk_index": i,
                "source": fname,
            }
        })
    if actions:
        helpers.bulk(os_client, actions)

    wc = None
    try:
        wc = weaviate.connect_to_custom(
            http_host=w_host,
            http_port=w_port,
            http_secure=False,
            grpc_host=w_host,
            grpc_port=w_grpc_port,
            grpc_secure=False,
            skip_init_checks=True,
        )
        ensure_weaviate_class(wc, w_class, recreate=recreate_class, project_id=gcp_project)
        coll = wc.collections.get(w_class)
        # Insert in batches with rate limit (max N chunks per minute)
        batch = []
        window_start = time.monotonic()
        sent_in_window = 0
        for i, ch in enumerate(chunks):
            # Deterministic UUID per chunk (same doc_id + chunk index)
            w_uuid = str(_uuid.uuid5(_uuid.NAMESPACE_URL, f"{doc_id}:{i}"))
            batch.append({
                "uuid": w_uuid,
                "properties": {
                    "content": ch,
                    "document_id": doc_id,
                    "chunk_index": i,
                    "source": fname,
                }
            })
            if len(batch) >= 64:
                # Rate limit enforcement
                if sent_in_window + len(batch) > w_rate_limit_per_minute:
                    elapsed = time.monotonic() - window_start
                    if elapsed < 60:
                        time.sleep(60 - elapsed)
                    window_start = time.monotonic()
                    sent_in_window = 0
                coll.data.insert_many(batch)
                sent_in_window += len(batch)
                batch.clear()
        if batch:
            if sent_in_window + len(batch) > w_rate_limit_per_minute:
                elapsed = time.monotonic() - window_start
                if elapsed < 60:
                    time.sleep(60 - elapsed)
                window_start = time.monotonic()
                sent_in_window = 0
            coll.data.insert_many(batch)
            sent_in_window += len(batch)

        print(f"Ingested {len(chunks)} chunks into OpenSearch index '{os_index}' and Weaviate class '{w_class}'.")
    finally:
        if wc is not None:
            try:
                wc.close()
            except Exception:
                pass


def main():
    ap = argparse.ArgumentParser(description="Ingest Markdown into OpenSearch (BM25) and Weaviate (embeddings)")
    ap.add_argument("markdown", help="Path to Markdown file to ingest")
    ap.add_argument("--os-host", default=os.getenv("OPENSEARCH_HOST", "localhost"))
    ap.add_argument("--os-port", type=int, default=int(os.getenv("OPENSEARCH_PORT", "9200")))
    ap.add_argument("--os-index", default=os.getenv("BM25_INDEX", "laws_bm25"))
    ap.add_argument("--w-host", default=os.getenv("WEAVIATE_HOST", "localhost"))
    ap.add_argument("--w-port", type=int, default=int(os.getenv("WEAVIATE_PORT", "8080")))
    ap.add_argument("--w-class", default=os.getenv("WEAVIATE_CLASS", "DocumentChunk"))
    ap.add_argument("--w-grpc-port", type=int, default=int(os.getenv("WEAVIATE_GRPC_PORT", "50051")))
    ap.add_argument("--recreate-class", action="store_true", help="Drop and recreate the Weaviate class with text2vec-google")
    ap.add_argument("--gcp-project", default=None, help="Explicit GCP project id for text2vec-google vectorizer")
    ap.add_argument("--w-rate-limit-per-minute", type=int, default=int(os.getenv("WEAVIATE_RATE_LIMIT_PER_MINUTE", "50000")), help="Max chunks inserted into Weaviate per minute")
    args = ap.parse_args()

    ingest_markdown(
        args.markdown,
        args.os_host,
        args.os_port,
        args.os_index,
        args.w_host,
        args.w_port,
        args.w_grpc_port,
        args.w_class,
        recreate_class=args.recreate_class,
        gcp_project=args.gcp_project,
        w_rate_limit_per_minute=args.w_rate_limit_per_minute,
    )


if __name__ == "__main__":
    main()
