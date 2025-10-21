import os
import logging
from typing import Optional, Dict, List

import google.auth
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting, HarmCategory


_model_cache: Optional[GenerativeModel] = None
_log = logging.getLogger(__name__)


def _init_vertex_model() -> Optional[GenerativeModel]:
    global _model_cache
    if _model_cache is not None:
        return _model_cache

    # Require ADC to be available (service account mounted via GOOGLE_APPLICATION_CREDENTIALS)
    try:
        creds, project_id = google.auth.default()
    except Exception:
        project_id = os.getenv("GCP_PROJECT")
        creds = None

    # Project and location
    project = os.getenv("GCP_PROJECT", project_id or "")
    location = os.getenv("GCP_LOCATION", "europe-west4")

    if not project:
        return None

    try:
        vertexai.init(project=project, location=location, credentials=creds)
        model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        _model_cache = GenerativeModel(model_name)
        return _model_cache
    except Exception:
        return None


def generate_answer(question: str, context: str) -> str:
    """
    Keep G constant using Google Gemini (Vertex AI). If Vertex is not configured,
    fall back to a deterministic placeholder to keep flow testable.
    """
    system = (
        "Je bent een assistent die vragen over het Nederlandse recht beantwoordt. "
        "Gebruik uitsluitend de aangeleverde context (strikte eis). Als je het niet zeker weet, zeg dat je het niet weet. "
        "Antwoord altijd in het Nederlands, beknopt en feitelijk."
        "Indien je antwoord geeft op de vraag, is het belangrijk om je bron te citeren. "
        "Dit doe je door de relevante wetsnaam en/of artikelnummer tussen vierkante haken te vermelden en de bijbehorende uitspraak. "
        "Als je de vraag niet kan beantwoorden, graag dan wel een korte samenvatting de verschillende bronnen waar je wel toegang tot hebt."
    )
    user = f"Vraag: {question}\n\nContext:\n{context[:8000]}"

    model = _init_vertex_model()
    if model is None:
        return (
            "[Fallback]\n"
            "Gemini is niet geconfigureerd (ontbrekende GOOGLE_APPLICATION_CREDENTIALS/GCP_PROJECT). "
            "Mount het serviceaccount JSON en stel de omgevingsvariabelen in.\n\n"
            f"Contextfragment: {context[:500]}"
        )

    try:
        # Light safety settings (named args to match SDK)
        safety_settings = [
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
            ),
        ]
        _log.info("gen.start model=%s q_len=%d ctx_len=%d", getattr(model, "model_name", "gemini"), len(question), len(context))
        # Use simple string contents to avoid Part/type issues
        resp = model.generate_content(
            [system, user],
            generation_config={
                "temperature": float(os.getenv("GEMINI_TEMPERATURE", "0.2"))
            },
            safety_settings=safety_settings,
        )
        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            if not context.strip():
                return "Er is geen relevante context gevonden voor deze vraag. Probeer specifieker te vragen of voeg documenten toe."
            return "Ik kan op basis van de aangeleverde context geen definitief antwoord geven."
        _log.info("gen.done ok len=%d", len(text))
        return text
    except Exception as e:
        _log.exception("gen.error: %s", e)
        if not context.strip():
            return "Er is geen relevante context gevonden voor deze vraag. Probeer specifieker te vragen of voeg documenten toe."
        return "Ik kan op basis van de aangeleverde context geen definitief antwoord geven."


def _simple_keyword_expand(question: str) -> Dict[str, List[str]]:
    import re
    text = question.lower()
    text = re.sub(r"[\(\)\[\]\{\},.;:!?]", " ", text)
    tokens = [t for t in re.split(r"\s+", text) if t]
    stop = {
        "de", "het", "een", "en", "of", "voor", "van", "in", "op", "met", "zonder",
        "over", "hoe", "wat", "waar", "wanneer", "welk", "welke", "is", "zijn", "kan",
        "kunnen", "moet", "moeten", "mag", "mogen", "niet", "wel", "tot", "te", "bij",
        "dan", "als", "die", "dat", "dit", "daar", "er", "het", "een", "om", "naar",
    }
    terms = []
    for t in tokens:
        if t.isdigit():
            terms.append(t)
            continue
        if t in stop:
            continue
        if len(t) <= 2:
            continue
        terms.append(t)
    terms = list(dict.fromkeys(terms))[:10]
    return {
        "bm25_terms": terms,
        "bm25_phrases": [],
        "vector_concepts": terms[:6] or [question.strip()],
    }


def expand_query(question: str) -> Dict[str, List[str]]:
    """
    Use Gemini to extract BM25 terms/phrases and vector concepts.
    Falls back to a simple local keyword extractor when Gemini is unavailable.
    """
    model = _init_vertex_model()
    if model is None:
        return _simple_keyword_expand(question)

    sys = (
        "Je helpt een zoekmachine voor Nederlands recht. "
        "Zet de natuurlijke vraag om in kernzoektermen en korte zinsdelen. "
        "Geef beknopt en strikt JSON conform dit schema: "
        "{\"bm25_terms\": [..], \"bm25_phrases\": [..], \"vector_concepts\": [..]} . "
        "Gebruik maximaal 8 items per lijst. Geen uitleg."
    )
    user = (
        "Vraag: " + question + "\n\n"
        "Let op: \n"
        "- bm25_terms: losse woorden (bv. wetsnaam, artikelnummer, kernbegrippen).\n"
        "- bm25_phrases: korte zinnen (2-5 woorden) voor match_phrase.\n"
        "- vector_concepts: synoniemen/varianten om nearText te verrijken."
    )
    try:
        resp = model.generate_content([sys, user], generation_config={"temperature": 0.1})
        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            # Try to assemble text from candidate parts (Vertex SDK sometimes omits .text)
            fragments: List[str] = []
            for cand in getattr(resp, "candidates", []) or []:
                content = getattr(cand, "content", None)
                parts = getattr(content, "parts", None) if content else None
                if not parts and hasattr(cand, "content"):
                    parts = getattr(cand.content, "parts", None)
                if not parts:
                    continue
                for part in parts:
                    fragment = getattr(part, "text", None)
                    if fragment:
                        fragments.append(fragment)
            text = "".join(fragments).strip()
        if text.startswith("```"):
            # Handle fenced JSON (```json ... ```)
            segments = text.split("```")
            if len(segments) >= 3:
                text = segments[1]
                if text.lower().startswith("json"):
                    text = text[4:]
                text = (text or "").strip()
        import json as _json
        data = _json.loads(text)
        bm25_terms = [str(x) for x in data.get("bm25_terms", [])][:8]
        bm25_phrases = [str(x) for x in data.get("bm25_phrases", [])][:8]
        vector_concepts = [str(x) for x in data.get("vector_concepts", [])][:8]
        if not (bm25_terms or bm25_phrases or vector_concepts):
            return _simple_keyword_expand(question)
        return {
            "bm25_terms": bm25_terms,
            "bm25_phrases": bm25_phrases,
            "vector_concepts": vector_concepts or bm25_terms or [question.strip()],
        }
    except Exception as e:
        _log.warning("expand_query fallback due to error: %s", e)
        return _simple_keyword_expand(question)
