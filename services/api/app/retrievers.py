from typing import List, Dict, Any
import logging
import time
import weaviate
from weaviate.classes.query import Filter
import os
from opensearchpy import OpenSearch
import httpx


class BM25Retriever:
    def __init__(self, host: str, port: int, index: str = "laws_bm25"):
        self.host = host
        self.port = port
        self.index = index
        self._client = None
        self._log = logging.getLogger(__name__ + ".BM25Retriever")

    def _client_ok(self):
        if self._client is None:
            user = os.getenv("OPENSEARCH_USER")
            password = os.getenv("OPENSEARCH_PASSWORD")
            http_auth = (user, password) if user and password else None
            self._client = OpenSearch(
                hosts=[{"host": self.host, "port": self.port}],
                use_ssl=False,
                verify_certs=False,
                ssl_show_warn=False,
                http_auth=http_auth,
            )
        return self._client

    def search(self, query: str, top_k: int = 5, expanded: dict | None = None) -> List[Dict[str, Any]]:
        t0 = time.monotonic()
        client = self._client_ok()
        body: Dict[str, Any]
        if expanded:
            terms = expanded.get("bm25_terms") or []
            phrases = expanded.get("bm25_phrases") or []
            should = []
            for p in phrases[:10]:
                should.append({
                    "match_phrase": {"content": {"query": p, "slop": 1, "boost": 3.0}}
                })
            for t in terms[:12]:
                should.append({
                    "match": {"content": {"query": t, "operator": "and", "boost": 1.5}}
                })
            # Also include the raw question with a small boost
            should.append({
                "match": {"content": {"query": query, "operator": "and", "boost": 1.0}}
            })
            body = {
                "size": top_k,
                "query": {"bool": {"should": should, "minimum_should_match": 1}},
                "_source": ["content", "document_id", "chunk_index", "source"],
            }
        else:
            body = {
                "size": top_k,
                "query": {
                    "match": {
                        "content": {
                            "query": query,
                            "operator": "and"
                        }
                    }
                },
                "_source": ["content", "document_id", "chunk_index", "source"]
            }
        try:
            resp = client.search(index=self.index, body=body)
            hits = []
            for h in resp.get("hits", {}).get("hits", []):
                src = h.get("_source", {})
                hits.append({
                    "content": (src.get("content") or ""),
                    "document_id": src.get("document_id"),
                    "chunk_index": src.get("chunk_index"),
                    "source": src.get("source"),
                    "score": h.get("_score"),
                })
            self._log.info(
                "bm25.search ok host=%s port=%s index=%s q_len=%d top_k=%d hits=%d dur_ms=%d expanded=%s",
                self.host, self.port, self.index, len(query), top_k, len(hits), int((time.monotonic() - t0) * 1000),
                bool(expanded),
            )
            return hits
        except Exception as e:
            self._log.exception(
                "bm25.search error host=%s port=%s index=%s top_k=%d: %s",
                self.host, self.port, self.index, top_k, e,
            )
            return []


class VectorRetriever:
    def __init__(self, host: str, port: int, class_name: str = "DocumentChunk"):
        self.host = host
        self.port = port
        self.class_name = class_name
        self._client = None
        self._log = logging.getLogger(__name__ + ".VectorRetriever")

    def _client_ok(self):
        if self._client is None:
            grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
            self._client = weaviate.connect_to_custom(
                http_host=self.host,
                http_port=self.port,
                http_secure=False,
                grpc_host=self.host,
                grpc_port=grpc_port,
                grpc_secure=False,
                skip_init_checks=True,
            )
        return self._client

    def search(self, query: str, top_k: int = 5, window_size: int = 0, concepts: list[str] | None = None) -> List[Dict[str, Any]]:
        wc = self._client_ok()
        collection = wc.collections.get(self.class_name)
        # Primary: HTTP GraphQL nearText using `properties { ... }` (do it right first)
        centers = []
        seen = set()
        results: List[Dict[str, Any]] = []

        def add_obj(props):
            key = (props.get("document_id"), props.get("chunk_index"))
            if key in seen:
                return
            seen.add(key)
            results.append({
                "content": (props.get("content") or ""),
                "document_id": props.get("document_id"),
                "chunk_index": props.get("chunk_index"),
                "source": props.get("source"),
            })

        url = f"http://{self.host}:{self.port}/v1/graphql"
        def _escape(s: str) -> str:
            return s.replace("\\", "\\\\").replace("\"", "\\\"")
        cons = concepts if concepts else [query]
        cons = [c for c in cons if isinstance(c, str) and c.strip()]
        cons = cons[:8] if cons else [query]
        cons_str = ", ".join([f'"{_escape(c)}"' for c in cons])
        query_props = (
            "{ Get { "
            f"{self.class_name}(nearText: {{ concepts: [{cons_str}] }}, limit: {int(top_k)}) "
            "{ properties { content document_id chunk_index source } } } }"
        )
        query_plain = (
            "{ Get { "
            f"{self.class_name}(nearText: {{ concepts: [{cons_str}] }}, limit: {int(top_k)}) "
            "{ content document_id chunk_index source } } }"
        )
        for attempt in range(1, 5 + 1):
            http_t0 = time.monotonic()
            try:
                with httpx.Client(timeout=20) as client:
                    r = client.post(url, json={"query": query_props})
                    status = r.status_code
                    if status == 429:
                        self._log.warning("weaviate.graphql nearText 429 attempt=%d", attempt)
                        time.sleep(min(2 ** attempt, 16))
                        continue
                    try:
                        r.raise_for_status()
                        data = r.json()
                        if data.get("errors"):
                            raise ValueError("GraphQL errors on properties shape")
                    except Exception:
                        r = client.post(url, json={"query": query_plain})
                        r.raise_for_status()
                        data = r.json()
                    objs = data.get("data", {}).get("Get", {}).get(self.class_name, [])
                    for o in objs:
                        props = o.get("properties") if isinstance(o, dict) and "properties" in o else o
                        if not isinstance(props, dict):
                            continue
                        centers.append(props)
                        add_obj(props)
                    self._log.info(
                        "weaviate.graphql nearText ok host=%s http=%s class=%s q_len=%d top_k=%d hits=%d dur_ms=%d",
                        self.host, self.port, self.class_name, len(query), top_k, len(results), int((time.monotonic() - http_t0) * 1000),
                    )
                    break
            except Exception as e:
                self._log.warning("weaviate.graphql nearText attempt=%d error: %s", attempt, e)
                time.sleep(min(2 ** attempt, 16))

        # Retry gRPC near_text only if GraphQL returned nothing (backup only)
        resp = None
        grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
        if False and not results:
            for attempt in range(1, 5 + 1):
                t0 = time.monotonic()
                try:
                    resp = collection.query.near_text(
                        query=query,
                        limit=top_k,
                        return_properties=["content", "document_id", "chunk_index", "source"],
                    )
                    self._log.info(
                        "weaviate.near_text ok host=%s http=%s grpc=%s class=%s q_len=%d top_k=%d dur_ms=%d",
                        self.host, self.port, grpc_port, self.class_name, len(query), top_k, int((time.monotonic() - t0) * 1000),
                    )
                    break
                except Exception as e:
                    self._log.warning(
                        "weaviate.near_text attempt=%d error: %s", attempt, e,
                    )
                    time.sleep(min(2 ** attempt, 8))
        if resp is not None:
            for o in resp.objects:
                props = o.properties
                centers.append(props)
                add_obj(props)

        if window_size and window_size > 0 and centers:
            for c in centers:
                doc_id = c.get("document_id")
                idx = c.get("chunk_index")
                if doc_id is None or idx is None:
                    continue
                neighbors = [idx + d for d in range(-window_size, window_size + 1) if d != 0]
                flt = Filter.all_of([
                    Filter.by_property("document_id").equal(doc_id),
                    Filter.any_of([Filter.by_property("chunk_index").equal(int(i)) for i in neighbors])
                ])
                try:
                    nb = collection.query.fetch_objects(
                        limit=len(neighbors),
                        filters=flt,
                        return_properties=["content", "document_id", "chunk_index", "source"],
                    )
                    for o in nb.objects:
                        add_obj(o.properties)
                except Exception as e:
                    self._log.exception("weaviate.fetch_neighbors error doc_id=%s neighbors=%s: %s", doc_id, neighbors, e)
        # HTTP GraphQL fallback (still Weaviate embeddings, not BM25)
        # Triggered if no centers were collected via gRPC near_text
        if not results:
            # HTTP GraphQL nearText fallback with retries (still embeddings)
            url = f"http://{self.host}:{self.port}/v1/graphql"
            # Try both GraphQL shapes: with `properties { ... }` (newer) and
            # without (older). We'll attempt `properties` first and fall back.
            query_props = (
                "{ Get { "
                f"{self.class_name}(nearText: {{ concepts: [\"{query}\"] }}, limit: {int(top_k)}) "
                "{ properties { content document_id chunk_index source } } } }"
            )
            query_plain = (
                "{ Get { "
                f"{self.class_name}(nearText: {{ concepts: [\"{query}\"] }}, limit: {int(top_k)}) "
                "{ content document_id chunk_index source } } }"
            )
            for attempt in range(1, 5 + 1):
                http_t0 = time.monotonic()
                try:
                    with httpx.Client(timeout=20) as client:
                        # First try the `properties { ... }` query
                        r = client.post(url, json={"query": query_props})
                        status = r.status_code
                        if status == 429:
                            # Backoff on rate limit
                            self._log.warning("weaviate.graphql nearText 429 attempt=%d", attempt)
                            time.sleep(min(2 ** attempt, 16))
                            continue
                        # If unsupported or GraphQL errors, fall back to plain fields
                        try:
                            r.raise_for_status()
                            data = r.json()
                            if data.get("errors"):
                                raise ValueError("GraphQL errors on properties shape")
                        except Exception:
                            r = client.post(url, json={"query": query_plain})
                            r.raise_for_status()
                            data = r.json()
                        objs = data.get("data", {}).get("Get", {}).get(self.class_name, [])
                        for o in objs:
                            # Support both shapes
                            props = o.get("properties") if isinstance(o, dict) and "properties" in o else o
                            if not isinstance(props, dict):
                                continue
                            add_obj({
                                "content": props.get("content"),
                                "document_id": props.get("document_id"),
                                "chunk_index": props.get("chunk_index"),
                                "source": props.get("source"),
                            })
                        self._log.info(
                            "weaviate.graphql nearText ok host=%s http=%s class=%s q_len=%d top_k=%d hits=%d dur_ms=%d",
                            self.host, self.port, self.class_name, len(query), top_k, len(results), int((time.monotonic() - http_t0) * 1000),
                        )
                        break
                except Exception as e:
                    self._log.warning("weaviate.graphql nearText attempt=%d error: %s", attempt, e)
                    time.sleep(min(2 ** attempt, 16))
        return results
