"""
api/gemini_models.py
====================
Canonical, future-proof Gemini model auto-discovery and task-aware selection
for every automated test, audit, optimizer, and API endpoint in They Own WHAT??.

Design Principles:
  - Never freeze a model version string as the sole choice: dynamically query
    Google's `/v1beta/models` catalog (with pagination) and rank general-purpose
    reasoning models across both traditional (`-pro`, `-flash`, `-flash-lite`)
    and Gemini 4+ / 5+ named tiers (`gemini-4-neon`, `gemini-4-hydrogen`,
    `gemini-4-carbon`, `gemini-4-argon`, etc.).
  - Select the most advanced model appropriate for the task within constraints:
      * `task="deep_audit"` / `"flagship"` -> newest flagship reasoning model
        (e.g., `gemini-4-neon`, fallback `gemini-4-hydrogen`, `gemini-3.8-flash`)
      * `task="fast"` / `"interactive"`    -> newest fast low-latency non-lite
        model (`gemini-4-argon` / `gemini-3.8-flash` / newest fast generation)
      * `task="lite"`                      -> newest `-lite` / `flash-lite` model
  - Caches live catalog discovery in-process (default 6h TTL) so interactive API
    endpoints incur zero per-request discovery latency while automatically
    upgrading whenever newer Gemini models roll out.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("gemini-models")

GEMINI_MODELS_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MODELS_URL = f"{GEMINI_MODELS_BASE_URL}?pageSize=200"

EXCLUDED_MODEL_SUBSTRINGS = (
    "embedding",
    "imagen",
    "image",
    "tts",
    "audio",
    "live",
    "robotics",
    "computer-use",
    "deep-research",
    "vision",
    "bisect",
    "gemma",
    "veo",
    "aqa",
    "customtools",
    "transcribe",
    "nano",
    "omni",
    "learnlm",
    "banana",
    "dialog",
    "lyria",
)

_CACHE_LOCK = threading.RLock()
_DISCOVERY_CACHE: Dict[Tuple[str, str], Tuple[float, List[str]]] = {}
DEFAULT_CACHE_TTL_SECONDS = 6 * 3600


def resolve_gemini_api_key() -> str:
    return (
        os.environ.get("GEMINI_KEY")
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or ""
    ).strip()


def score_gemini_model_entry(
    model_obj: Dict[str, Any],
    *,
    task: str = "deep_audit",
) -> Optional[Tuple[int, int, int, int, int, int, int, str]]:
    """Score a `/v1beta/models` entry for `task` (`deep_audit`, `fast`, `lite`, or `flash`).

    Returns `(major, minor, is_thinking, cap_rank, out_bucket, stability, recency_tie, model_id)`
    or `None` if ineligible. Higher tuple wins.
    """
    raw_name = str(model_obj.get("name") or "").strip()
    if not raw_name.startswith("models/"):
        return None
    model_id = raw_name.split("/", 1)[1].lower()
    if not model_id.startswith("gemini-"):
        return None

    methods = model_obj.get("supportedGenerationMethods") or []
    if "generateContent" not in methods:
        return None
    if any(bad in model_id for bad in EXCLUDED_MODEL_SUBSTRINGS):
        return None
    if model_id.endswith("-latest") and not re.search(r"^gemini-\d", model_id):
        return None

    ver_match = re.match(r"^gemini-(\d+)(?:\.(\d+))?-", model_id)
    if not ver_match:
        return None
    major = int(ver_match.group(1))
    minor = int(ver_match.group(2) or 0)

    is_lite = ("-lite" in model_id or "flash-lite" in model_id)
    is_flash = ("-flash" in model_id and not is_lite)
    is_pro = ("-pro" in model_id)
    is_thinking = 1 if model_obj.get("thinking") else 0
    out_limit = int(model_obj.get("outputTokenLimit") or 0)
    out_bucket = 2 if out_limit >= 65536 else (1 if out_limit >= 32768 else 0)

    task_l = (task or "deep_audit").strip().lower()
    if task_l in ("lite", "flash-lite"):
        if not is_lite:
            return None
        cap_rank = 1
    elif task_l == "flash":
        if not is_flash:
            return None
        cap_rank = 1
    elif task_l in ("fast", "interactive"):
        # Fast general-purpose: newest Flash OR compact/fast Gemini 4+ tier (excluding lite)
        if is_lite:
            return None
        if is_flash:
            cap_rank = 3
        elif major >= 4 and ("-argon" in model_id or "-carbon" in model_id or out_limit <= 32768):
            cap_rank = 4
        elif "-ultra" in model_id or "-neon" in model_id or is_pro:
            cap_rank = 2
        else:
            cap_rank = 3
    else:
        # "deep_audit" / "flagship" / "pro": newest highest-capability reasoning model
        if is_lite:
            return None
        if "-ultra" in model_id or "-neon" in model_id or is_pro:
            cap_rank = 4
        elif is_flash:
            cap_rank = 1
        elif out_limit >= 65536:
            cap_rank = 3
        else:
            cap_rank = 2

    if "exp" in model_id:
        stability = 1
    elif "preview" in model_id:
        stability = 2
    elif model_id.endswith("-latest"):
        stability = 3
    else:
        stability = 4

    date_nums = [int(x) for x in re.findall(r"-(\d{2,4})", model_id)]
    recency_tie = sum(date_nums) if date_nums else 0
    return (
        major,
        minor,
        is_thinking,
        cap_rank,
        out_bucket,
        stability,
        recency_tie,
        raw_name.split("/", 1)[1],
    )


def fetch_gemini_model_catalog(
    api_key: str,
    *,
    fetch_json: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
    timeout_s: int = 20,
) -> List[Dict[str, Any]]:
    if not api_key:
        return []

    def _default_fetch(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))

    fetcher = fetch_json or _default_fetch
    models: List[Dict[str, Any]] = []
    seen_tokens = set()
    page_token = ""
    for _ in range(6):
        url = GEMINI_MODELS_URL + (f"&pageToken={page_token}" if page_token else "")
        payload = fetcher(url, {"x-goog-api-key": api_key, "Accept": "application/json"})
        page_models = payload.get("models") or []
        models.extend(m for m in page_models if isinstance(m, dict))
        page_token = str(payload.get("nextPageToken") or "").strip()
        if not page_token or page_token in seen_tokens:
            break
        seen_tokens.add(page_token)
    return models


def discover_ranked_gemini_models(
    api_key: str,
    *,
    task: str = "deep_audit",
    limit: int = 4,
    fetch_json: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
    env_override_var: str = "GEMINI_OPTIMIZER_MODEL",
) -> List[str]:
    """Return a ranked fallback list of up to `limit` best-available Gemini model IDs."""
    override = (os.environ.get(env_override_var) or os.environ.get("GEMINI_MODEL_OVERRIDE") or "").strip()
    defaults = (
        ["gemini-4-neon", "gemini-4-hydrogen", "gemini-3.8-flash", "gemini-2.5-pro"]
        if task not in ("fast", "interactive", "flash", "lite", "flash-lite")
        else (
            ["gemini-3.5-flash-lite", "gemini-2.5-flash-lite"]
            if task in ("lite", "flash-lite")
            else ["gemini-4-argon", "gemini-3.8-flash", "gemini-4-neon", "gemini-2.5-flash"]
        )
    )
    ranked: List[str] = []
    if override:
        ranked.append(override)

    if api_key:
        try:
            catalog = fetch_gemini_model_catalog(api_key, fetch_json=fetch_json)
            scored = [
                s
                for m in catalog
                for s in [score_gemini_model_entry(m, task=task)]
                if s is not None
            ]
            scored.sort(reverse=True)
            for item in scored:
                mid = item[-1]
                if mid not in ranked:
                    ranked.append(mid)
                if len(ranked) >= limit:
                    break
        except Exception as exc:
            logger.warning("Dynamic Gemini discovery (%s) fell back to defaults: %s", task, exc)

    for d in defaults:
        if d not in ranked:
            ranked.append(d)
    return ranked[:limit]


def get_cached_gemini_model(
    api_key: Optional[str] = None,
    *,
    task: str = "fast",
    ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
) -> str:
    """Fast, thread-safe cached model selector for interactive FastAPI routes & batch jobs."""
    key = (api_key or resolve_gemini_api_key()).strip()
    cache_key = (task.lower(), key[:8] if key else "")
    now = time.time()
    with _CACHE_LOCK:
        hit = _DISCOVERY_CACHE.get(cache_key)
        if hit and (now - hit[0]) < ttl_seconds and hit[1]:
            return hit[1][0]

    ranked = discover_ranked_gemini_models(key, task=task, limit=4)
    with _CACHE_LOCK:
        _DISCOVERY_CACHE[cache_key] = (now, ranked)
    return ranked[0]
