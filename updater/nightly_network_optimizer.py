#!/usr/bin/env python3
"""
updater/nightly_network_optimizer.py
====================================
Nightly 5-Jurisdiction, Jurisdiction-by-Jurisdiction Recursive Network-Linking
& Pipeline Performance Optimization Engine for They Own WHAT??.

Design Principles (strictly following `docs/ASSURANCE_POLICY.md`):
  1. Jurisdiction-by-Jurisdiction (NEVER cross-jurisdiction parcel linking):
     Each night inspects a cohort of 5 jurisdictions ONE BY ONE across all 20
     live jurisdictions (full 20-jurisdiction rotation every 4 nights). All
     network audits, missed-connection discovery, false-positive checks, and
     rebuilds operate strictly WITHIN that single jurisdiction's records.
  2. Recursive Examine -> Adjust -> Gate -> Rebuild -> Re-Examine Loop:
     For each jurisdiction J (up to `--max-passes`, default 3):
       - Examines J's networks + raw parcel/corporate records for:
           * Missed ownership connections (falsely blacklisted cohesive HQ
             addresses in `oc_registered_agent_addresses`, benign `C/O` prefix
             false-exclusions, unlinked high-volume businesses missing OC match
             due to abbreviation drift, same-ZIP street-spelling HQ drift,
             unlinked corporate series siblings, split same-officer portfolios).
           * False positives & hygiene issues (assessor placeholder/redaction
             owner strings, over-dispersed multi-HQ hairballs, newly emerging
             3rd-party tax-agent / servicer / RA hubs).
           * Step-by-step pipeline performance & DB efficiency (table bloat,
             stale `ANALYZE` stats, missing read/join indexes, stage timings,
             peak RSS vs. historical baseline).
       - Invokes the **best-available Gemini model** (dynamically discovered
         via `/v1beta/models` on every run, defaulting to the newest Gemini Pro
         with automatic Flash fallback) in an **advisory** analytical pass over
         public property/corporate aggregates ONLY (never user PII).
       - Any candidate rule (from deterministic detectors OR verified Gemini
         hypotheses) MUST pass strict PostgreSQL public-record proof AND our
         deterministic unit regression suite (`test_third_party_exclusions.py`
         + `test_oc_linker_and_schema.py`) before rebuilding J, followed by
         post-rebuild precision invariants (automatic rollback on regression!).
       - When Pass k applies validated improvements, rebuilds ONLY J and
         re-examines J in Pass k+1 until convergence or `max_passes`.
  3. Spread Generalized Logic & Process Improvements Across Jurisdictions:
     Verified generalized rules (`biz_abbrev`, `placeholder_owner`,
     `third_party_hub_block`, `ra_allowlist`, `benign_care_of_token`,
     `officer_role_suffix`) persist in `network_linking_rules` +
     `docs/benchmarks/learned_linking_rules.json` so all 20 jurisdictions
     benefit automatically on their next rebuild (while never linking parcels
     across borders).
  4. Nightly Email Report to `salmun@storrsmagic.org`:
     Dispatches a structured HTML + plain-text summary of every jurisdiction
     examined, recursive passes, missed connections recovered, false positives
     pruned, Gemini advisory insights, rules propagated, and performance/DB
     optimizations applied.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import datetime as dt
import gc
import html
import json
import logging
import os
import re
import resource
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dotenv import load_dotenv

_ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(_ENV_PATH):
    load_dotenv(_ENV_PATH)

from api.gemini_models import (
    discover_ranked_gemini_models,
    score_gemini_model_entry,
)
from api.linking_rules import (
    DEFAULT_RULES_JSON_PATH,
    deactivate_learned_rule,
    ensure_linking_rules_tables,
    get_learned_ra_allowlist,
    reload_learned_rules,
    upsert_learned_rule,
)
from api.shared_utils import (
    clear_shared_utils_caches,
    extract_owner_family_key,
    extract_postal_street_zip_key,
    extract_series_stem_info,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    looks_like_business_name,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_owner_name,
)
from updater.send_audit_email import send_audit_email

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [nightly-optimizer] %(message)s",
)
logger = logging.getLogger("nightly-network-optimizer")

DEFAULT_REPORT_RECIPIENT = (
    os.environ.get("NIGHTLY_OPTIMIZER_EMAIL_TO") or "salmun@storrsmagic.org"
).strip()
STATE_PATH = os.path.join(
    REPO_ROOT, "docs", "benchmarks", "nightly-linking-optimizer-state.json"
)
REPORTS_DIR = os.path.join(
    REPO_ROOT, "docs", "benchmarks", "nightly_linking_reports"
)

GEMINI_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models?pageSize=200"
GEMINI_GEN_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
EXCLUDED_MODEL_SUBSTRINGS = (
    "embedding", "imagen", "image", "tts", "audio", "live", "robotics",
    "computer-use", "deep-research", "vision", "bisect", "gemma", "veo", "aqa",
    "customtools", "transcribe", "nano", "omni",
)

# All 20 live jurisdictions arranged into 4 nightly cohorts of 5 (full cycle every 4 nights).
# Each jurisdiction is audited and rebuilt strictly ONE BY ONE (zero cross-jurisdiction linking).
ALL_JURISDICTIONS: Tuple[str, ...] = (
    # Cohort 0: Largest flagship states
    "ct", "ma", "ny", "fl", "il",
    # Cohort 1: Major states & NJ
    "ca", "mn", "va", "nc", "nj",
    # Cohort 2: Mid-size statewide parcels
    "oh", "co", "wi", "ar", "mt",
    # Cohort 3: VT + 4 flagship standalone cities
    "vt", "dc", "baltimore", "detroit", "philadelphia",
)

JURISDICTION_META: Dict[str, Dict[str, Any]] = {
    "ct": {"label": "Connecticut (Statewide)", "kind": "ct", "state_code": "CT", "oc_jur": "us_ct"},
    "ma": {"label": "Massachusetts (351 Towns)", "kind": "ma", "state_code": "MA", "oc_jur": "us_ma"},
    "ny": {"label": "New York (62 Counties + NYC)", "kind": "state", "state_code": "NY", "oc_jur": "us_ny", "source_tag": "NY_UNIFIED_62_COUNTY_HPD_ORPTS"},
    "fl": {"label": "Florida (67 Counties)", "kind": "state", "state_code": "FL", "oc_jur": "us_fl", "source_tag": "FL_UNIFIED_67_COUNTY_PARCELS"},
    "il": {"label": "Illinois (Chicagoland)", "kind": "state", "state_code": "IL", "oc_jur": "us_il", "source_tag": "IL_COOK_COLLAR_CAMA"},
    "ca": {"label": "California (LA & SF + Business Lessors)", "kind": "state", "state_code": "CA", "oc_jur": "us_ca", "source_tag": "CA_ASSESSOR_ROLL_AND_BIZ_TAX"},
    "mn": {"label": "Minnesota (Twin Cities)", "kind": "state", "state_code": "MN", "oc_jur": "us_mn", "source_tag": "MN_TWIN_CITIES_CAMA_AND_RENTAL_LICENSES"},
    "va": {"label": "Virginia (Statewide)", "kind": "state", "state_code": "VA", "oc_jur": "us_va", "source_tag": "VA_STATEWIDE_PARCELS"},
    "nc": {"label": "North Carolina (Statewide)", "kind": "state", "state_code": "NC", "oc_jur": "us_nc", "source_tag": "NC_ONEMAP_STATEWIDE_PARCELS"},
    "nj": {"label": "New Jersey (DCA BHI)", "kind": "nj", "state_code": "NJ", "oc_jur": "us_nj"},
    "oh": {"label": "Ohio (Cuyahoga / Cleveland)", "kind": "state", "state_code": "OH", "oc_jur": "us_oh", "source_tag": "OH_CUYAHOGA_AUDITOR_CAMA"},
    "co": {"label": "Colorado (Statewide)", "kind": "state", "state_code": "CO", "oc_jur": "us_co", "source_tag": "CO_OIT_PUBLIC_PARCELS"},
    "wi": {"label": "Wisconsin (Statewide)", "kind": "state", "state_code": "WI", "oc_jur": "us_wi", "source_tag": "WI_SCO_DOA_STATEWIDE_PARCELS"},
    "ar": {"label": "Arkansas (Statewide)", "kind": "state", "state_code": "AR", "oc_jur": "us_ar", "source_tag": "ARKANSAS_GIS_PARCELS"},
    "mt": {"label": "Montana (56 Counties)", "kind": "state", "state_code": "MT", "oc_jur": "us_mt", "source_tag": "MT_MSDI_ORION_CADASTRAL"},
    "vt": {"label": "Vermont (Statewide)", "kind": "state", "state_code": "VT", "oc_jur": "us_vt", "source_tag": "VT_VCGI_STANDARDIZED_PARCELS"},
    "dc": {"label": "Washington, D.C.", "kind": "city", "state_code": "DC", "oc_jur": "us_dc"},
    "baltimore": {"label": "Baltimore, MD", "kind": "city", "state_code": "MD", "oc_jur": "us_md"},
    "detroit": {"label": "Detroit, MI", "kind": "city", "state_code": "MI", "oc_jur": "us_mi"},
    "philadelphia": {"label": "Philadelphia, PA", "kind": "city", "state_code": "PA", "oc_jur": "us_pa"},
}

_PLACEHOLDER_DETECT_RE = re.compile(
    r"(?:UNAVAILABLE\s+OWNER|OWNER\s+UNAVAILABLE|NAME\s+NOT\s+ON\s+FILE|"
    r"OWNER\s+NOT\s+ON\s+FILE|OWNER\s+OF\s+RECORD|CURRENT\s+OWNER\s+OF|"
    r"NOT\s+AVAILABLE|NOT\s+PROVIDED|NO\s+NAME\s+ON\s+FILE|NAME\s+WITHHELD|"
    r"CONFIDENTIAL|REDACTED|PRIVACY\s+ACT|PROTECTED\s+OWNER|WITHHELD\s+BY\s+LAW|"
    r"TAXPAYER\s+OF|UNKNOWN\s+OWNER|OWNER\s+UNKNOWN|DO\s+NOT\s+MAIL|"
    r"SEE\s+ASSESSOR|ADDRESS\s+UNKNOWN|NO\s+OWNER\s+LISTED)",
    re.IGNORECASE,
)

_BENIGN_CO_CANDIDATE_RE = re.compile(
    r"^\s*(?:C\s*/\s*O|CARE\s+OF)\s+([A-Z]{2,14})\s*[,;:\-]\s*(\d+\s+[A-Z0-9].{6,}\b\d{5}(?:-\d{4})?\b)",
    re.IGNORECASE,
)
_STREET_NUM_PREFIX_RE = re.compile(r"^(\d+[A-Z0-9-]*)\s+(.+)$")


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso_utc(val: Optional[dt.datetime] = None) -> str:
    d = val or _utc_now()
    return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _peak_rss_mb() -> float:
    # Linux ru_maxrss is in KB
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL") or "postgresql://user:password@127.0.0.1:5432/ctdata"
    if "@ctdata_db:" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db:", "@127.0.0.1:")
    return raw


def _resolve_gemini_key() -> str:
    return (
        os.environ.get("GEMINI_KEY")
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or ""
    ).strip()


# ---------------------------------------------------------------------------
# Best-Available Gemini Model Auto-Discovery & Advisory Review
# ---------------------------------------------------------------------------

def score_gemini_model_candidate(
    model_obj: Dict[str, Any], desired_tier: str = "best"
) -> Optional[Tuple[int, int, int, int, int, int, str]]:
    """Rank a Gemini `/v1beta/models` entry so nightly runs continuously upgrade
    to the newest, strongest general-purpose reasoning Gemini model (including
    Gemini 4+ named tiers like `gemini-4-neon` / `gemini-4-hydrogen` as well as
    traditional `-pro` / `-flash` generations and future Gemini 5+ releases)."""
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

    tier = desired_tier.lower()
    if tier == "flash":
        if not is_flash:
            return None
    elif tier == "pro":
        # Flagship reasoning tier: `-pro`, `-ultra`, or Gemini 4+ non-lite named flagships
        if is_lite or (major < 4 and not is_pro):
            return None
    else:
        # "best": any non-lite general reasoning model across all generations
        if is_lite:
            return None

    # Capability rank within the same (major, minor):
    #   4 = ultra / neon / pro flagship
    #   3 = other high-output named Gemini 4+ reasoning models (e.g. hydrogen)
    #   2 = mid-tier named Gemini 4+ models (e.g. carbon, argon)
    #   1 = flash
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
    return (major, minor, is_thinking, cap_rank, stability, recency_tie, raw_name.split("/", 1)[1])


def discover_best_gemini_models(
    api_key: str,
    fetch_json: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
) -> Tuple[str, str]:
    """Return `(best_primary_model, best_fallback_model)` dynamically from `/v1beta/models`.
    Ranks newest generations first (e.g. `gemini-4-neon` > `gemini-4-hydrogen` >
    `gemini-3.8-flash` > `gemini-3.1-pro-preview`), automatically upgrading as newer
    Gemini models ship. Honors `GEMINI_OPTIMIZER_MODEL` override when explicitly set."""
    override = (os.environ.get("GEMINI_OPTIMIZER_MODEL") or "").strip()
    default_primary, default_fallback = "gemini-4-neon", "gemini-3.8-flash"
    if not api_key:
        return (override or default_primary, default_fallback)

    def _default_fetch(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    fetcher = fetch_json or _default_fetch
    best_primary, best_fallback = default_primary, default_fallback
    try:
        payload = fetcher(
            GEMINI_MODELS_URL,
            {"x-goog-api-key": api_key, "Accept": "application/json"},
        )
        models = payload.get("models") or []
        all_scored = [
            s for m in models if isinstance(m, dict)
            for s in [score_gemini_model_candidate(m, "best")] if s is not None
        ]
        if all_scored:
            all_scored.sort(reverse=True)
            best_primary = all_scored[0][6]
            # Pick the highest-ranked distinct fallback model (preferring a different family/tier)
            fallbacks = [s[6] for s in all_scored[1:] if s[6] != best_primary]
            if fallbacks:
                best_fallback = fallbacks[0]
    except Exception as exc:
        logger.warning("Gemini model auto-discovery fell back to defaults (%s)", exc)

    return (override or best_primary, best_fallback)


def call_gemini_json(
    api_key: str,
    models: Sequence[str],
    system_instruction: str,
    user_payload: Dict[str, Any],
    *,
    timeout_s: int = 90,
) -> Tuple[Optional[Dict[str, Any]], str, Optional[str]]:
    """Invoke best-available Gemini model(s) in priority order expecting structured JSON.
    Returns `(parsed_json, model_used, error_or_none)`."""
    if not api_key:
        return None, "disabled", "No GEMINI_KEY/GEMINI_API_KEY configured"

    body_dict = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [
            {
                "role": "user",
                "parts": [{"text": json.dumps(user_payload, default=str)}],
            }
        ],
        "generationConfig": {
            "temperature": 0.15,
            "responseMimeType": "application/json",
        },
    }
    raw_body = json.dumps(body_dict).encode("utf-8")
    last_err = None

    for model_id in models:
        if not model_id:
            continue
        url = f"{GEMINI_GEN_BASE}/{model_id}:generateContent"
        req = urllib.request.Request(
            url,
            data=raw_body,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                resp_json = json.loads(resp.read().decode("utf-8"))
            candidates = resp_json.get("candidates") or []
            if not candidates:
                last_err = f"{model_id}: empty candidates"
                continue
            parts = (candidates[0].get("content") or {}).get("parts") or []
            text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
            full_text = "\n".join(text_chunks).strip()
            if full_text.startswith("```"):
                full_text = re.sub(r"^```(?:json)?\s*|\s*```$", "", full_text, flags=re.DOTALL).strip()
            parsed = json.loads(full_text)
            if isinstance(parsed, dict):
                return parsed, model_id, None
            last_err = f"{model_id}: non-object JSON"
        except urllib.error.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                pass
            last_err = f"{model_id}: HTTP {exc.code} {err_body}"
            logger.warning("Gemini call failed on %s (%s); trying fallback if available.", model_id, exc.code)
        except Exception as exc:
            last_err = f"{model_id}: {exc}"
            logger.warning("Gemini call failed on %s (%s); trying fallback if available.", model_id, exc)

    return None, (models[0] if models else "none"), last_err


# ---------------------------------------------------------------------------
# Rotation State Ledger (5 jurisdictions per night, jurisdiction-by-jurisdiction)
# ---------------------------------------------------------------------------

def load_optimizer_state(state_path: str = STATE_PATH) -> Dict[str, Any]:
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                data.setdefault("cursor_index", 0)
                data.setdefault("cycle_number", 1)
                data.setdefault("priority_queue", [])
                data.setdefault("history", [])
                data.setdefault("timings_baseline", {})
                return data
        except Exception:
            pass
    return {
        "version": 1,
        "cursor_index": 0,
        "cycle_number": 1,
        "priority_queue": [],
        "last_run_at": None,
        "timings_baseline": {},
        "history": [],
    }


def save_optimizer_state(state: Dict[str, Any], state_path: str = STATE_PATH) -> None:
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    # Cap history length cleanly
    if isinstance(state.get("history"), list) and len(state["history"]) > 90:
        state["history"] = state["history"][-90:]
    tmp = f"{state_path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)
        fh.write("\n")
    os.replace(tmp, state_path)


def select_tonight_cohort(
    state: Dict[str, Any],
    *,
    cohort_size: int = 5,
    explicit_jurisdictions: Optional[List[str]] = None,
) -> Tuple[List[str], int, int]:
    """Select tonight's 5 jurisdictions (or explicit list), returning
    `(selected_list, next_cursor_index, next_cycle_number)`."""
    if explicit_jurisdictions:
        chosen = [j.lower().strip() for j in explicit_jurisdictions if j.lower().strip() in JURISDICTION_META]
        return chosen, int(state.get("cursor_index", 0)), int(state.get("cycle_number", 1))

    n = len(ALL_JURISDICTIONS)
    cursor = int(state.get("cursor_index", 0)) % n
    cycle = int(state.get("cycle_number", 1))
    selected: List[str] = []

    # Pull up to 1 priority re-check jurisdiction if queued by an earlier rule propagation,
    # while still advancing the main 5-jurisdiction rotation cleanly so all 20 stay covered.
    pq = [j for j in (state.get("priority_queue") or []) if j in JURISDICTION_META]
    if pq and cohort_size >= 2:
        selected.append(pq.pop(0))

    while len(selected) < min(cohort_size, n):
        cand = ALL_JURISDICTIONS[cursor]
        cursor = (cursor + 1) % n
        if cursor == 0:
            cycle += 1
        if cand not in selected:
            selected.append(cand)

    state["priority_queue"] = pq
    return selected, cursor, cycle


# ---------------------------------------------------------------------------
# Jurisdiction Table Helpers & Metrics Snapshot
# ---------------------------------------------------------------------------

def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %s AND c.relkind IN ('r','p','m')
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def _table_columns(cur, table_name: str) -> Set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def _jur_tables(jur: str) -> Tuple[str, str]:
    if jur == "ct":
        return ("properties", "entity_networks")
    return (f"{jur}_properties", f"{jur}_networks")


def collect_jurisdiction_metrics(conn, jur: str) -> Dict[str, Any]:
    """Capture a crisp, bounded before/after quantitative snapshot for a single jurisdiction."""
    props_tbl, nets_tbl = _jur_tables(jur)
    metrics: Dict[str, Any] = {
        "jurisdiction": jur,
        "props_table": props_tbl,
        "nets_table": nets_tbl,
        "total_parcels": 0,
        "total_networks": 0,
        "multi_entity_networks": 0,
        "oc_matched_parcels": 0,
        "top_10_networks": [],
        "placeholder_network_count": 0,
        "hairball_candidates": [],
    }
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '45000'")
        if not _table_exists(cur, props_tbl) or not _table_exists(cur, nets_tbl):
            metrics["missing_tables"] = True
            return metrics

        p_cols = _table_columns(cur, props_tbl)
        n_cols = _table_columns(cur, nets_tbl)

        # Fast planner estimate fallback or exact count
        cur.execute(f"SELECT COUNT(*) FROM {props_tbl}")
        metrics["total_parcels"] = int(cur.fetchone()[0] or 0)

        if jur == "ct":
            cur.execute("SELECT COUNT(DISTINCT network_id) FROM entity_networks")
            metrics["total_networks"] = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT network_id FROM entity_networks
                    GROUP BY network_id HAVING COUNT(*) >= 2
                ) t
                """
            )
            metrics["multi_entity_networks"] = int(cur.fetchone()[0] or 0)
            if _table_exists(cur, "cached_insights"):
                cur.execute(
                    """
                    SELECT display_name, property_count, business_count
                    FROM cached_insights
                    ORDER BY property_count DESC NULLS LAST
                    LIMIT 10
                    """
                )
                metrics["top_10_networks"] = [
                    {"display_name": r[0], "building_count": int(r[1] or 0), "entity_count": int(r[2] or 0)}
                    for r in cur.fetchall()
                ]
        else:
            cur.execute(f"SELECT COUNT(*) FROM {nets_tbl}")
            metrics["total_networks"] = int(cur.fetchone()[0] or 0)

            if "member_names" in n_cols:
                ent_expr = "COALESCE(array_length(member_names, 1), 1)"
            elif "entity_count" in n_cols:
                ent_expr = "COALESCE(entity_count, 1)"
            elif "business_count" in n_cols:
                ent_expr = "COALESCE(business_count, 1)"
            else:
                ent_expr = "1"

            if ent_expr != "1":
                cur.execute(f"SELECT COUNT(*) FROM {nets_tbl} WHERE {ent_expr} >= 2")
                metrics["multi_entity_networks"] = int(cur.fetchone()[0] or 0)

            if "oc_company_number" in p_cols:
                cur.execute(f"SELECT COUNT(*) FROM {props_tbl} WHERE oc_company_number IS NOT NULL AND oc_company_number != ''")
                metrics["oc_matched_parcels"] = int(cur.fetchone()[0] or 0)

            b_col = "building_count" if "building_count" in n_cols else ("property_count" if "property_count" in n_cols else "1")
            cur.execute(
                f"""
                SELECT network_key, display_name, {b_col}::int AS bcnt, ({ent_expr})::int AS ecnt
                FROM {nets_tbl}
                ORDER BY {b_col} DESC NULLS LAST
                LIMIT 10
                """
            )
            metrics["top_10_networks"] = [
                {
                    "network_key": r[0],
                    "display_name": r[1],
                    "building_count": int(r[2] or 0),
                    "entity_count": int(r[3] or 0),
                }
                for r in cur.fetchall()
            ]
    return metrics


# ---------------------------------------------------------------------------
# Jurisdiction-by-Jurisdiction Meticulous Examination Detectors
# ---------------------------------------------------------------------------

def _detect_placeholder_networks(conn, jur: str) -> List[Dict[str, Any]]:
    """Find assessor redaction/placeholder owner strings forming fake networks inside J."""
    if jur == "ct":
        return []
    _, nets_tbl = _jur_tables(jur)
    found: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '30000'")
        if not _table_exists(cur, nets_tbl):
            return []
        n_cols = _table_columns(cur, nets_tbl)
        b_col = "building_count" if "building_count" in n_cols else "1"
        cur.execute(
            f"""
            SELECT network_key, display_name, {b_col}::int AS bcnt
            FROM {nets_tbl}
            WHERE display_name IS NOT NULL
              AND {b_col} >= 5
              AND display_name ~* '(UNAVAILABLE|NOT ON FILE|NOT AVAILABLE|CONFIDENTIAL|REDACTED|WITHHELD|OWNER OF RECORD|CURRENT OWNER|TAXPAYER OF|UNKNOWN OWNER|OWNER UNKNOWN|PRIVACY ACT|DO NOT MAIL|NOT PROVIDED)'
            ORDER BY {b_col} DESC
            LIMIT 40
            """
        )
        for nk, disp, bcnt in cur.fetchall():
            cleaned = re.sub(r"[*#_~-]+", " ", str(disp or "")).strip()
            norm = normalize_owner_name(cleaned)
            if not norm or looks_like_business_name(cleaned):
                continue
            if _PLACEHOLDER_DETECT_RE.search(cleaned):
                found.append(
                    {
                        "network_key": nk,
                        "display_name": disp,
                        "normalized": norm,
                        "building_count": int(bcnt or 0),
                    }
                )
    return found


def _detect_falsely_blacklisted_cohesive_hqs(conn, jur: str) -> List[Dict[str, Any]]:
    """Find cohesive single-operator landlord HQs in J that were accidentally blacklisted
    in `oc_registered_agent_addresses`, blocking real ownership connections."""
    if jur == "ct":
        return []
    props_tbl, _ = _jur_tables(jur)
    oc_jur = JURISDICTION_META[jur]["oc_jur"]
    allowlist = get_learned_ra_allowlist()
    candidates: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '45000'")
        if not _table_exists(cur, props_tbl) or not _table_exists(cur, "oc_registered_agent_addresses"):
            return []
        p_cols = _table_columns(cur, props_tbl)
        if "mailing_address" not in p_cols:
            return []

        # Sample top shared mailing addresses in J that have >= 5 parcels
        cur.execute(
            f"""
            SELECT mailing_address, COUNT(*)::int AS p_cnt,
                   COUNT(DISTINCT owner_name)::int AS distinct_owners,
                   ARRAY_AGG(DISTINCT owner_name) FILTER (WHERE owner_name IS NOT NULL) AS sample_owners
            FROM (
                SELECT mailing_address, owner_name
                FROM {props_tbl}
                WHERE mailing_address IS NOT NULL AND mailing_address != ''
                  AND owner_name IS NOT NULL AND owner_name != ''
                LIMIT 600000
            ) s
            GROUP BY mailing_address
            HAVING COUNT(*) >= 5 AND COUNT(DISTINCT owner_name) >= 2
            ORDER BY COUNT(*) DESC
            LIMIT 400
            """
        )
        coarse_to_stats: Dict[str, Dict[str, Any]] = {}
        for raw_addr, p_cnt, d_own, s_owners in cur.fetchall():
            if is_excluded_third_party_address(raw_addr):
                continue
            coarse = normalize_mailing_address_coarse(raw_addr)
            if not coarse or coarse in allowlist:
                continue
            st = coarse_to_stats.setdefault(
                coarse,
                {"parcels": 0, "owners": set(), "raw_sample": raw_addr},
            )
            st["parcels"] += int(p_cnt or 0)
            for o in (s_owners or [])[:40]:
                st["owners"].add(str(o))

        if not coarse_to_stats:
            return []

        # Intersect with oc_registered_agent_addresses
        cur.execute(
            """
            SELECT address_coarse, distinct_company_count, source
            FROM oc_registered_agent_addresses
            WHERE address_coarse = ANY(%s)
            """,
            (list(coarse_to_stats.keys()),),
        )
        ra_hits = cur.fetchall()
        if not ra_hits:
            return []

        for addr_coarse, oc_co_cnt, ra_src in ra_hits:
            # Never rescue known hard-coded commercial RAs / law hubs
            if is_excluded_third_party_address(addr_coarse):
                continue
            stats = coarse_to_stats[addr_coarse]
            # Check officer cohesion in oc_companies + oc_officers for this exact address
            cur.execute(
                """
                WITH cos AS (
                    SELECT jurisdiction_code, company_number
                    FROM oc_companies
                    WHERE jurisdiction_code = %s
                      AND (
                          agent_address_coarse = %s
                          OR registered_address_coarse = %s
                          OR mailing_address_coarse = %s
                      )
                    LIMIT 120
                ),
                offs AS (
                    SELECT c.company_number, o.name_norm
                    FROM cos c
                    JOIN oc_officers o
                      ON o.jurisdiction_code = c.jurisdiction_code
                     AND o.company_number = c.company_number
                    WHERE COALESCE(o.is_current, TRUE) = TRUE
                      AND o.role_class IN ('beneficial_principal', 'corporate_parent')
                      AND o.name_norm IS NOT NULL AND o.name_norm != ''
                )
                SELECT
                    (SELECT COUNT(DISTINCT company_number) FROM cos)::int AS co_cnt,
                    (SELECT COUNT(DISTINCT company_number) FROM offs)::int AS cos_with_off,
                    (SELECT name_norm FROM offs GROUP BY name_norm ORDER BY COUNT(DISTINCT company_number) DESC LIMIT 1) AS top_off,
                    (SELECT COUNT(DISTINCT company_number) FROM offs WHERE name_norm = (
                        SELECT name_norm FROM offs GROUP BY name_norm ORDER BY COUNT(DISTINCT company_number) DESC LIMIT 1
                    ))::int AS top_off_cos
                """,
                (oc_jur, addr_coarse, addr_coarse, addr_coarse),
            )
            row = cur.fetchone()
            if not row:
                continue
            co_cnt, cos_with_off, top_off, top_off_cos = row
            co_cnt = int(co_cnt or 0)
            cos_with_off = int(cos_with_off or 0)
            top_off_cos = int(top_off_cos or 0)
            off_share = (top_off_cos / cos_with_off) if cos_with_off >= 3 else 0.0

            # Also check parcel owner-family cohesion
            fams = Counter(
                extract_owner_family_key(o) or normalize_owner_name(o)
                for o in stats["owners"]
            )
            top_fam, top_fam_cnt = fams.most_common(1)[0] if fams else ("", 0)
            fam_share = top_fam_cnt / max(len(stats["owners"]), 1)

            # High-confidence cohesive landlord HQ:
            # Either >= 55% of local OC companies at this address share one beneficial principal
            # (with >= 5 companies), OR >= 65% of parcel owners share one corporate family stem
            # AND OC company count is modest (<= 400, not a CT Corp skyscraper).
            if (cos_with_off >= 5 and off_share >= 0.55) or (
                len(stats["owners"]) >= 4 and fam_share >= 0.65 and int(oc_co_cnt or 0) <= 400 and (cos_with_off < 5 or off_share >= 0.40)
            ):
                candidates.append(
                    {
                        "address_coarse": addr_coarse,
                        "sample_raw_address": stats["raw_sample"],
                        "parcels_in_jur": stats["parcels"],
                        "distinct_owners_in_jur": len(stats["owners"]),
                        "oc_company_count": int(oc_co_cnt or 0),
                        "local_oc_companies": co_cnt,
                        "top_officer": top_off or "",
                        "officer_concentration": round(off_share, 3),
                        "top_owner_family": top_fam,
                        "family_concentration": round(fam_share, 3),
                        "ra_source": ra_src,
                        "auto_fixable": True,
                    }
                )
    return candidates[:15]


def _detect_benign_care_of_false_exclusions(conn, jur: str) -> List[Dict[str, Any]]:
    """Find short C/O tokens (e.g., country codes / assessor prefixes) causing valid street
    mailing addresses inside J to be falsely rejected by `is_excluded_third_party_address`."""
    if jur == "ct":
        return []
    props_tbl, _ = _jur_tables(jur)
    token_hits: Counter = Counter()
    token_samples: Dict[str, List[str]] = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '30000'")
        if not _table_exists(cur, props_tbl):
            return []
        if "mailing_address" not in _table_columns(cur, props_tbl):
            return []
        cur.execute(
            f"""
            SELECT mailing_address, COUNT(*)::int AS cnt
            FROM {props_tbl}
            WHERE mailing_address ~* '^\\s*(C\\s*/\\s*O|CARE\\s+OF)\\s+'
            GROUP BY mailing_address
            ORDER BY COUNT(*) DESC
            LIMIT 1500
            """
        )
        for raw_addr, cnt in cur.fetchall():
            if not is_excluded_third_party_address(raw_addr):
                continue
            m = _BENIGN_CO_CANDIDATE_RE.match(str(raw_addr or ""))
            if not m:
                continue
            tok = " ".join(m.group(1).upper().split())
            rest = m.group(2).strip()
            # Ensure the underlying street address itself is NOT a 3rd-party hub
            if is_excluded_third_party_address(rest):
                continue
            # Never whitelist tokens that look like banks, law firms, CPAs, servicers, or tax agents
            if any(
                bad in tok
                for bad in ("LAW", "ATTY", "CPA", "TAX", "BANK", "MTG", "LOAN", "SERV", "AGENT", "ESQ", "CORP", "TRUST", "GOV", "DEPT")
            ):
                continue
            token_hits[tok] += int(cnt or 0)
            if len(token_samples[tok]) < 3:
                token_samples[tok].append(raw_addr)

    out: List[Dict[str, Any]] = []
    for tok, total_cnt in token_hits.most_common(10):
        # Require high volume (>= 50 parcels) + short country/assessor-code shape for auto-apply
        out.append(
            {
                "token": tok,
                "affected_parcels": total_cnt,
                "samples": token_samples[tok],
                "auto_fixable": total_cnt >= 50 and len(tok) <= 6 and tok.isalpha(),
            }
        )
    return out


def _detect_missed_oc_business_abbrevs(conn, jur: str) -> List[Dict[str, Any]]:
    """Inspect top unlinked business owners in J (`oc_company_number IS NULL`) to discover
    exact single-token abbreviation mismatches against active in-state `oc_companies`
    corroborated by matching 5-digit ZIP code."""
    if jur == "ct":
        return []
    props_tbl, _ = _jur_tables(jur)
    oc_jur = JURISDICTION_META[jur]["oc_jur"]

    abbrev_evidence: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    unlinked_samples: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '45000'")
        if not _table_exists(cur, props_tbl):
            return []
        p_cols = _table_columns(cur, props_tbl)
        if "oc_company_number" not in p_cols or "owner_name" not in p_cols:
            return []

        cur.execute(
            f"""
            SELECT owner_name,
                   MAX(mailing_address) AS sample_mail,
                   COUNT(*)::int AS p_cnt
            FROM {props_tbl}
            WHERE (oc_company_number IS NULL OR oc_company_number = '')
              AND owner_name IS NOT NULL AND owner_name != ''
            GROUP BY owner_name
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
            LIMIT 250
            """
        )
        rows = cur.fetchall()
        for raw_owner, sample_mail, p_cnt in rows:
            if not looks_like_business_name(raw_owner) or is_excluded_third_party_entity(raw_owner):
                continue
            norm = normalize_business_name(raw_owner)
            toks = norm.split()
            if len(toks) < 2:
                continue
            m_zip = re.search(r"\b(\d{5})\b", str(sample_mail or ""))
            zip5 = m_zip.group(1) if m_zip else ""
            if len(unlinked_samples) < 30:
                unlinked_samples.append(
                    {"owner_name": raw_owner, "norm": norm, "parcels": int(p_cnt), "sample_mail": sample_mail or ""}
                )
            if not zip5 or len(toks[0]) < 4 or not toks[0].isalpha():
                continue

            # Query in-state active OC companies sharing the exact first distinctive token + ZIP
            cur.execute(
                """
                SELECT name_norm, company_number, registered_address_coarse, mailing_address_coarse
                FROM oc_companies
                WHERE jurisdiction_code = %s
                  AND name_norm LIKE %s
                  AND COALESCE(is_active, TRUE) = TRUE
                  AND (
                      COALESCE(registered_address_raw, '') LIKE %s
                      OR COALESCE(mailing_address_raw, '') LIKE %s
                  )
                LIMIT 15
                """,
                (oc_jur, toks[0] + " %", f"%{zip5}%", f"%{zip5}%"),
            )
            for oc_norm, oc_num, reg_c, mail_c in cur.fetchall():
                oc_toks = str(oc_norm or "").split()
                if len(oc_toks) != len(toks):
                    continue
                diffs = [(a, b) for a, b in zip(toks, oc_toks) if a != b]
                if len(diffs) == 1:
                    short_tok, full_tok = diffs[0]
                    # Must look like a genuine abbreviation/truncation (short is prefix or subsequence of full)
                    if (
                        short_tok.isalpha()
                        and full_tok.isalpha()
                        and 2 <= len(short_tok) < len(full_tok)
                        and len(full_tok) >= 5
                        and short_tok[0] == full_tok[0]
                        and _is_subsequence(short_tok, full_tok)
                    ):
                        abbrev_evidence[(short_tok, full_tok)].append(
                            {
                                "owner_name": raw_owner,
                                "oc_name": oc_norm,
                                "oc_company_number": oc_num,
                                "zip5": zip5,
                                "parcels": int(p_cnt),
                            }
                        )

    findings: List[Dict[str, Any]] = []
    for (short_tok, full_tok), evs in sorted(
        abbrev_evidence.items(), key=lambda kv: (-len(kv[1]), -sum(x["parcels"] for x in kv[1]))
    ):
        distinct_entities = len({e["owner_name"] for e in evs})
        total_parcels = sum(e["parcels"] for e in evs)
        # Only auto-apply standard business vocabulary expansions corroborated across >= 3 distinct entities
        safe_target_words = {
            "PARTNERSHIP", "LIMITED", "CORPORATION", "INCORPORATED", "ASSOCIATION",
            "ASSOCIATES", "MANAGEMENT", "PROPERTIES", "DEVELOPMENT", "HOLDINGS",
            "INVESTMENTS", "REALTY", "ENTERPRISES", "APARTMENTS", "RESIDENTIAL",
            "COMMERCIAL", "COMMUNITIES", "VENTURES", "ACQUISITIONS", "HOUSING",
            "SERVICES", "BUILDERS", "BUILDING", "COMPANY", "GROUP", "CENTER",
        }
        findings.append(
            {
                "abbrev": short_tok,
                "expansion": full_tok,
                "distinct_entities": distinct_entities,
                "affected_parcels": total_parcels,
                "samples": evs[:4],
                "auto_fixable": distinct_entities >= 3 and full_tok in safe_target_words,
            }
        )
    return findings[:15]


def _is_subsequence(short: str, full: str) -> bool:
    it = iter(full)
    return all(ch in it for ch in short)


def _detect_missed_same_zip_hq_aliases(conn, jur: str) -> List[Dict[str, Any]]:
    """Find pairs of SEPARATE networks inside J whose mailing addresses share the exact
    same 5-digit ZIP + house number (e.g. `60 LEO BIRMINGHAM PKWY` vs `60 LEO M BIRMINGHAM PKWY`)
    AND share a verified OC officer or corporate family stem — meaning a minor street-name
    spelling variation split one landlord portfolio into two networks."""
    if jur == "ct":
        return []
    props_tbl, nets_tbl = _jur_tables(jur)
    findings: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '45000'")
        if not _table_exists(cur, props_tbl) or not _table_exists(cur, nets_tbl):
            return []
        p_cols = _table_columns(cur, props_tbl)
        n_cols = _table_columns(cur, nets_tbl)
        b_col = "building_count" if "building_count" in n_cols else "1"

        # Pull top 500 networks in J and their primary mailing addresses (via member_addresses or bbl_list join)
        rows_for_zip: List[Tuple[str, str, int, str, int, List[str]]] = []
        if "member_addresses" in n_cols and "member_names" in n_cols:
            cur.execute(
                f"""
                SELECT network_key, display_name, {b_col}::int AS bcnt, member_addresses, member_names
                FROM {nets_tbl}
                WHERE {b_col} >= 4
                ORDER BY {b_col} DESC
                LIMIT 500
                """
            )
            for nk, disp, bcnt, m_addrs, m_names in cur.fetchall():
                owners = [str(o) for o in (m_names or []) if o][:30]
                for addr in (m_addrs or [])[:15]:
                    if addr:
                        rows_for_zip.append((nk, disp, int(bcnt or 0), str(addr), int(bcnt or 1), owners))
        elif "bbl_list" in n_cols and "bbl" in p_cols and "mailing_address" in p_cols:
            cur.execute(
                f"""
                WITH top_nets AS (
                    SELECT network_key, display_name, {b_col}::int AS bcnt, bbl_list
                    FROM {nets_tbl}
                    WHERE {b_col} >= 4
                    ORDER BY {b_col} DESC
                    LIMIT 300
                )
                SELECT tn.network_key, tn.display_name, tn.bcnt,
                       p.mailing_address, COUNT(*)::int AS addr_cnt,
                       ARRAY_AGG(DISTINCT p.owner_name) FILTER (WHERE p.owner_name IS NOT NULL) AS owners
                FROM top_nets tn
                JOIN {props_tbl} p ON p.bbl = ANY(tn.bbl_list)
                WHERE p.mailing_address IS NOT NULL AND p.mailing_address != ''
                GROUP BY tn.network_key, tn.display_name, tn.bcnt, p.mailing_address
                HAVING COUNT(*) >= 2
                """
            )
            rows_for_zip = list(cur.fetchall())
        else:
            return []

        # Group by (zip5, street_number)
        by_zip_num: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        net_meta: Dict[str, Dict[str, Any]] = {}
        for nk, disp, bcnt, raw_addr, acnt, owners in rows_for_zip:
            if is_excluded_third_party_address(raw_addr):
                continue
            zip_key = extract_postal_street_zip_key(raw_addr)
            if not zip_key or "||" not in zip_key:
                continue
            street_part, zip5 = zip_key.split("||", 1)
            if street_part.startswith("PO BOX "):
                continue
            m_num = _STREET_NUM_PREFIX_RE.match(street_part)
            if not m_num:
                continue
            house_num, street_rest = m_num.group(1), m_num.group(2)
            net_meta[nk] = {"display_name": disp, "building_count": int(bcnt or 0)}
            fams = {extract_owner_family_key(o) for o in (owners or [])[:25] if extract_owner_family_key(o)}
            by_zip_num[(zip5, house_num)].append(
                {
                    "network_key": nk,
                    "zip_key": zip_key,
                    "street_part": street_part,
                    "street_tokens": set(street_rest.split()),
                    "addr_cnt": int(acnt or 0),
                    "families": fams,
                    "raw_addr": raw_addr,
                }
            )

        seen_pairs: Set[Tuple[str, str]] = set()
        for (zip5, house_num), items in by_zip_num.items():
            # Distinct zip_keys across distinct networks at same (zip5, house_num)
            distinct_keys = {it["zip_key"] for it in items}
            distinct_nets = {it["network_key"] for it in items}
            if len(distinct_keys) < 2 or len(distinct_nets) < 2:
                continue
            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    a, b = items[i], items[j]
                    if a["network_key"] == b["network_key"] or a["zip_key"] == b["zip_key"]:
                        continue
                    pair_key = tuple(sorted((a["zip_key"], b["zip_key"])))
                    if pair_key in seen_pairs:
                        continue
                    # Street tokens must strongly overlap (e.g. only 1 extra middle initial or suffix difference)
                    inter = a["street_tokens"] & b["street_tokens"]
                    union = a["street_tokens"] | b["street_tokens"]
                    if not union or len(inter) < 2 or (len(inter) / len(union)) < 0.65:
                        continue
                    # Corroborate via shared non-generic corporate family stem OR identical surname/display token
                    shared_fams = {f for f in (a["families"] & b["families"]) if len(f) >= 6}
                    disp_a = normalize_owner_name(net_meta[a["network_key"]]["display_name"])
                    disp_b = normalize_owner_name(net_meta[b["network_key"]]["display_name"])
                    same_disp = bool(disp_a and disp_a == disp_b)
                    if not shared_fams and not same_disp:
                        continue

                    seen_pairs.add(pair_key)
                    # Canonical = the higher-count zip_key
                    canon, alias = (
                        (a["zip_key"], b["zip_key"])
                        if a["addr_cnt"] >= b["addr_cnt"]
                        else (b["zip_key"], a["zip_key"])
                    )
                    findings.append(
                        {
                            "zip5": zip5,
                            "alias_zip_key": alias,
                            "canonical_zip_key": canon,
                            "net_a": {
                                "network_key": a["network_key"],
                                "display_name": net_meta[a["network_key"]]["display_name"],
                                "buildings": net_meta[a["network_key"]]["building_count"],
                            },
                            "net_b": {
                                "network_key": b["network_key"],
                                "display_name": net_meta[b["network_key"]]["display_name"],
                                "buildings": net_meta[b["network_key"]]["building_count"],
                            },
                            "shared_families": sorted(shared_fams)[:5],
                            "auto_fixable": bool(shared_fams or same_disp),
                        }
                    )
    return findings[:15]


def _detect_unlinked_series_siblings(conn, jur: str) -> List[Dict[str, Any]]:
    """Find pairs of separate networks inside J sharing an identical non-generic corporate
    series stem (advisory missed-connection leads for human/Gemini inspection)."""
    if jur == "ct":
        return []
    _, nets_tbl = _jur_tables(jur)
    leads: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '30000'")
        if not _table_exists(cur, nets_tbl):
            return []
        n_cols = _table_columns(cur, nets_tbl)
        b_col = "building_count" if "building_count" in n_cols else "1"
        cur.execute(
            f"""
            SELECT network_key, display_name, {b_col}::int AS bcnt
            FROM {nets_tbl}
            WHERE {b_col} >= 3 AND display_name IS NOT NULL
            ORDER BY {b_col} DESC
            LIMIT 1500
            """
        )
        by_stem: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for nk, disp, bcnt in cur.fetchall():
            stem, has_series, is_generic = extract_series_stem_info(disp)
            if not stem or is_generic or len(stem.split()) < 2:
                continue
            by_stem[stem].append({"network_key": nk, "display_name": disp, "buildings": int(bcnt or 0), "has_series": has_series})

        for stem, rows in sorted(by_stem.items(), key=lambda kv: -sum(r["buildings"] for r in kv[1])):
            if len(rows) < 2 or not any(r["has_series"] for r in rows):
                continue
            leads.append(
                {
                    "series_stem": stem,
                    "network_count": len(rows),
                    "combined_buildings": sum(r["buildings"] for r in rows),
                    "networks": rows[:6],
                }
            )
            if len(leads) >= 15:
                break
    return leads


def _detect_dispersed_hairballs_and_new_hubs(conn, jur: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Audit large networks inside J for over-dispersed multi-HQ hairballs AND identify
    newly emerging 3rd-party tax/RA/servicer mailing hubs (zero artificial quotas: uses
    strict dispersion + disjoint external-HQ proof)."""
    if jur == "ct":
        return ([], [])
    props_tbl, nets_tbl = _jur_tables(jur)
    hairballs: List[Dict[str, Any]] = []
    new_hubs: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '45000'")
        if not _table_exists(cur, props_tbl) or not _table_exists(cur, nets_tbl):
            return ([], [])
        p_cols = _table_columns(cur, props_tbl)
        n_cols = _table_columns(cur, nets_tbl)
        if "mailing_address" not in p_cols or "bbl" not in p_cols or "bbl_list" not in n_cols:
            return ([], [])
        b_col = "building_count" if "building_count" in n_cols else "1"

        cur.execute(
            f"""
            SELECT network_key, display_name, {b_col}::int AS bcnt, bbl_list
            FROM {nets_tbl}
            WHERE {b_col} >= 80
            ORDER BY {b_col} DESC
            LIMIT 40
            """
        )
        big_nets = cur.fetchall()
        for nk, disp, bcnt, bbl_list in big_nets:
            sample_bbls = list(bbl_list or [])[:2500]
            if not sample_bbls:
                continue
            cur.execute(
                f"""
                SELECT mailing_address, owner_name
                FROM {props_tbl}
                WHERE bbl = ANY(%s)
                """,
                (sample_bbls,),
            )
            hq_counts: Counter = Counter()
            fam_counts: Counter = Counter()
            for m_addr, own in cur.fetchall():
                c_addr = normalize_mailing_address_coarse(m_addr) if m_addr else ""
                if c_addr:
                    hq_counts[c_addr] += 1
                fam = extract_owner_family_key(own) or normalize_owner_name(own)
                if fam:
                    fam_counts[fam] += 1

            total_with_hq = sum(hq_counts.values())
            if len(hq_counts) >= 40 and total_with_hq >= 80:
                top3_share = sum(c for _, c in hq_counts.most_common(3)) / total_with_hq
                top3_fam_share = sum(c for _, c in fam_counts.most_common(3)) / max(sum(fam_counts.values()), 1)
                if top3_share < 0.25 and top3_fam_share < 0.25:
                    hairballs.append(
                        {
                            "network_key": nk,
                            "display_name": disp,
                            "building_count": int(bcnt or 0),
                            "distinct_mailing_hqs": len(hq_counts),
                            "top3_hq_share": round(top3_share, 3),
                            "distinct_owner_families": len(fam_counts),
                            "top3_family_share": round(top3_fam_share, 3),
                            "top_hqs": [{"address": a, "count": c} for a, c in hq_counts.most_common(3)],
                        }
                    )

        # Check for newly emerging 3rd-party hubs: mailing address shared by >= 8 unrelated
        # owner families whose own OC registered/mailing HQs are >= 6 disjoint external addresses.
        if "oc_company_number" in p_cols:
            cur.execute(
                f"""
                SELECT mailing_address,
                       COUNT(*)::int AS p_cnt,
                       COUNT(DISTINCT owner_name)::int AS d_own,
                       ARRAY_AGG(DISTINCT oc_company_number) FILTER (WHERE oc_company_number IS NOT NULL AND oc_company_number != '') AS oc_nums,
                       ARRAY_AGG(DISTINCT owner_name) FILTER (WHERE owner_name IS NOT NULL) AS owners
                FROM (
                    SELECT mailing_address, owner_name, oc_company_number
                    FROM {props_tbl}
                    WHERE mailing_address IS NOT NULL AND mailing_address != ''
                    LIMIT 500000
                ) s
                GROUP BY mailing_address
                HAVING COUNT(DISTINCT owner_name) >= 10
                ORDER BY COUNT(DISTINCT owner_name) DESC
                LIMIT 100
                """
            )
            oc_jur = JURISDICTION_META[jur]["oc_jur"]
            for raw_addr, p_cnt, d_own, oc_nums, owners in cur.fetchall():
                if is_excluded_third_party_address(raw_addr):
                    continue
                coarse = normalize_mailing_address_coarse(raw_addr)
                if not coarse or coarse in get_learned_ra_allowlist():
                    continue
                fams = Counter(extract_owner_family_key(o) or normalize_owner_name(o) for o in (owners or [])[:120])
                if len(fams) < 8:
                    continue
                top_fam_share = fams.most_common(1)[0][1] / max(sum(fams.values()), 1)
                if top_fam_share >= 0.25:
                    continue
                oc_sample = [x for x in (oc_nums or []) if x][:50]
                if len(oc_sample) < 6:
                    continue
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT COALESCE(NULLIF(mailing_address_coarse, ''), registered_address_coarse))::int
                    FROM oc_companies
                    WHERE jurisdiction_code = %s
                      AND company_number = ANY(%s)
                      AND COALESCE(NULLIF(mailing_address_coarse, ''), registered_address_coarse) IS NOT NULL
                      AND COALESCE(NULLIF(mailing_address_coarse, ''), registered_address_coarse) != %s
                    """,
                    (oc_jur, oc_sample, coarse),
                )
                ext_hqs = int((cur.fetchone() or [0])[0] or 0)
                if ext_hqs >= 6:
                    new_hubs.append(
                        {
                            "address_coarse": coarse,
                            "sample_raw_address": raw_addr,
                            "parcels": int(p_cnt or 0),
                            "distinct_owner_families": len(fams),
                            "top_family_share": round(top_fam_share, 3),
                            "disjoint_external_oc_hqs": ext_hqs,
                            "auto_fixable": ext_hqs >= 8 and top_fam_share < 0.20,
                        }
                    )
    return (hairballs[:10], new_hubs[:10])


# ---------------------------------------------------------------------------
# Pipeline Performance & Database Efficiency Auditor + Safe Auto-Maintenance
# ---------------------------------------------------------------------------

def audit_and_optimize_db_performance(conn, jur: str, *, apply_maintenance: bool = True) -> Dict[str, Any]:
    """Inspect table bloat, planner stats freshness, and critical join/lookup indexes
    for J's tables, applying safe `ANALYZE` and missing read/join indexes automatically."""
    props_tbl, nets_tbl = _jur_tables(jur)
    perf: Dict[str, Any] = {
        "tables": {},
        "actions_taken": [],
        "recommendations": [],
    }
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60000'")
        for tbl in (props_tbl, nets_tbl):
            if not _table_exists(cur, tbl):
                continue
            cur.execute(
                """
                SELECT
                    pg_total_relation_size(c.oid) / (1024.0 * 1024.0) AS total_mb,
                    COALESCE(s.n_live_tup, 0)::bigint,
                    COALESCE(s.n_dead_tup, 0)::bigint,
                    COALESCE(s.seq_scan, 0)::bigint,
                    COALESCE(s.idx_scan, 0)::bigint,
                    s.last_analyze,
                    s.last_autoanalyze
                FROM pg_class c
                LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                WHERE c.relname = %s
                LIMIT 1
                """,
                (tbl,),
            )
            row = cur.fetchone()
            if not row:
                continue
            total_mb, live_tup, dead_tup, seq_scan, idx_scan, last_an, last_auto = row
            dead_ratio = dead_tup / max(live_tup, 1)
            latest_an = max([d for d in (last_an, last_auto) if d is not None], default=None)
            stale_days = ((_utc_now() - latest_an).total_seconds() / 86400.0) if latest_an else 999.0

            perf["tables"][tbl] = {
                "total_mb": round(float(total_mb or 0.0), 1),
                "live_tuples": int(live_tup or 0),
                "dead_tuples": int(dead_tup or 0),
                "dead_ratio": round(dead_ratio, 3),
                "seq_scans": int(seq_scan or 0),
                "idx_scans": int(idx_scan or 0),
                "analyze_age_days": round(stale_days, 1) if latest_an else None,
            }

            if apply_maintenance and (dead_ratio > 0.12 or stale_days > 7.0):
                t0 = time.time()
                cur.execute(f"ANALYZE {tbl}")
                conn.commit()
                perf["actions_taken"].append(
                    f"ANALYZE {tbl} (dead_ratio={dead_ratio:.2%}, elapsed={time.time() - t0:.2f}s)"
                )

            if dead_ratio > 0.30 and total_mb > 100:
                perf["recommendations"].append(
                    f"{tbl} has {dead_ratio:.0%} dead tuples ({total_mb:.0f} MB) — schedule off-peak VACUUM (ANALYZE) {tbl}."
                )

        # Ensure high-value read/join indexes exist on non-CT tables >= 50k rows
        if jur != "ct" and _table_exists(cur, props_tbl) and _table_exists(cur, nets_tbl):
            p_cols = _table_columns(cur, props_tbl)
            n_cols = _table_columns(cur, nets_tbl)
            cur.execute(
                """
                SELECT indexname, indexdef FROM pg_indexes
                WHERE schemaname = 'public' AND tablename IN (%s, %s)
                """,
                (props_tbl, nets_tbl),
            )
            idx_defs = [r[1].lower() for r in cur.fetchall()]

            desired_indexes: List[Tuple[str, str, str]] = []
            if "network_key" in p_cols and not any(f"on public.{props_tbl}" in d and "(network_key)" in d for d in idx_defs):
                desired_indexes.append((f"idx_{jur}_props_netkey_opt", props_tbl, "(network_key)"))
            if "oc_company_number" in p_cols and not any(f"on public.{props_tbl}" in d and "oc_company_number" in d for d in idx_defs):
                desired_indexes.append((f"idx_{jur}_props_ocnum_opt", props_tbl, "(oc_company_number) WHERE oc_company_number IS NOT NULL"))
            if "building_count" in n_cols and not any(f"on public.{nets_tbl}" in d and "building_count" in d for d in idx_defs):
                desired_indexes.append((f"idx_{jur}_nets_bldg_desc_opt", nets_tbl, "(building_count DESC)"))

            for idx_name, tbl, cols_expr in desired_indexes:
                if apply_maintenance:
                    t0 = time.time()
                    try:
                        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {tbl} {cols_expr}")
                        conn.commit()
                        perf["actions_taken"].append(
                            f"Created missing index {idx_name} ON {tbl} {cols_expr} ({time.time() - t0:.2f}s)"
                        )
                    except Exception as idx_exc:
                        conn.rollback()
                        perf["recommendations"].append(
                            f"Index {idx_name} ON {tbl} {cols_expr} deferred: {idx_exc}"
                        )
                else:
                    perf["recommendations"].append(
                        f"Missing recommended index: CREATE INDEX IF NOT EXISTS {idx_name} ON {tbl} {cols_expr}"
                    )
    return perf


# ---------------------------------------------------------------------------
# Deterministic Regression Gate + Single-Jurisdiction Profiled Rebuild
# ---------------------------------------------------------------------------

def run_deterministic_unit_gate() -> Tuple[bool, str]:
    """Run our core linking/exclusion unit test suites before keeping any newly learned rule."""
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT
    env["DATABASE_URL"] = _resolve_db_url()
    cmd = [
        sys.executable, "-m", "unittest",
        "tests/test_third_party_exclusions.py",
        "tests/test_oc_linker_and_schema.py",
    ]
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=180
    )
    ok = proc.returncode == 0
    tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-8:]
    return ok, "\n".join(tail)


def rebuild_single_jurisdiction_profiled(conn, jur: str) -> Dict[str, Any]:
    """Rebuild ONLY jurisdiction `jur` (strictly single-jurisdiction!) while recording
    wall-clock step timings and peak RSS."""
    meta = JURISDICTION_META[jur]
    kind = meta["kind"]
    t_start = time.time()
    timings: Dict[str, Any] = {"jurisdiction": jur, "kind": kind}

    # Ensure newly applied learned rules are active in-process before rebuilding
    reload_learned_rules(conn=conn)
    clear_shared_utils_caches()

    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT
    env["DATABASE_URL"] = _resolve_db_url()

    if kind == "ct":
        t0 = time.time()
        proc = subprocess.run(
            [sys.executable, "api/safe_network_refresh.py", "--skip-linking"],
            cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=3600,
        )
        timings["ct_safe_refresh_s"] = round(time.time() - t0, 2)
        if proc.returncode != 0:
            raise RuntimeError(f"CT safe_network_refresh failed (exit={proc.returncode}): {proc.stderr[-400:]}")
    elif kind == "nj":
        t0 = time.time()
        proc = subprocess.run(
            [sys.executable, "-m", "nj.build_nj_networks"],
            cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=1800,
        )
        timings["nj_rebuild_s"] = round(time.time() - t0, 2)
        if proc.returncode != 0:
            raise RuntimeError(f"NJ rebuild failed (exit={proc.returncode}): {proc.stderr[-400:]}")
    elif kind == "ma":
        from ma.ingest_massgis import build_and_save_ma_networks
        t0 = time.time()
        p_cnt, n_cnt = build_and_save_ma_networks(conn)
        timings["ma_rebuild_s"] = round(time.time() - t0, 2)
        timings["parcels"] = p_cnt
        timings["networks"] = n_cnt
    elif kind == "state":
        from states.ingest_statewide_parcels import build_and_save_state_networks
        t0 = time.time()
        p_cnt, n_cnt = build_and_save_state_networks(conn, jur, meta["state_code"], meta["source_tag"])
        timings["state_rebuild_s"] = round(time.time() - t0, 2)
        timings["parcels"] = p_cnt
        timings["networks"] = n_cnt
    elif kind == "city":
        from api.populate_cities import rebuild_city_networks_from_db
        t0 = time.time()
        rebuild_city_networks_from_db(conn, jur)
        timings["city_rebuild_s"] = round(time.time() - t0, 2)
    else:
        raise ValueError(f"Unknown jurisdiction kind: {kind}")

    # Invalidate stale per-jurisdiction top-networks kv_cache entry
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM kv_cache WHERE key LIKE %s", (f"city_top_nets:%:{jur}:%",))
        conn.commit()
    except Exception:
        conn.rollback()

    gc.collect()
    timings["total_s"] = round(time.time() - t_start, 2)
    timings["peak_rss_mb"] = _peak_rss_mb()
    return timings


# ---------------------------------------------------------------------------
# Gemini Advisory Analytical Review (Per Jurisdiction, Strictly Source-Backed)
# ---------------------------------------------------------------------------

GEMINI_SYSTEM_PROMPT = """You are a senior data-engineering and public-records entity-resolution auditor for They Own WHAT?? (a public-interest property ownership explorer).

GOVERNING EPISTEMIC HONESTY RULES:
- You are auditing ONE SINGLE JURISDICTION at a time. NEVER suggest linking parcels across different states or jurisdictions.
- Base every observation strictly on the provided public parcel/business registry evidence. Never fabricate owner names, dates, or corporate relationships.
- Do NOT suggest hard artificial entity-count caps (like max 10 or 75 LLCs per address). Distinguish legitimate cohesive large landlords from 3rd-party registered agents/lawyers/servicers using structural officer/HQ cohesion vs. dispersion.
- Return ONLY valid JSON matching this schema:
{
  "jurisdiction_assessment": "2-3 sentence honest summary of linking accuracy & notable gaps",
  "missed_connection_hypotheses": [
    {"title": "...", "evidence": "...", "recommended_logic_change": "...", "confidence": "high|medium|low"}
  ],
  "false_positive_risks": [
    {"network_or_hub": "...", "evidence": "...", "recommended_fix": "...", "confidence": "high|medium|low"}
  ],
  "pipeline_performance_suggestions": [
    {"step": "...", "bottleneck": "...", "optimization": "..."}
  ],
  "cross_jurisdiction_process_lessons": [
    "Generalizable linking logic or data-hygiene improvement (NOT cross-border merging) applicable to other jurisdictions"
  ]
}"""


def run_gemini_advisory_review(
    api_key: str,
    models: Sequence[str],
    jur: str,
    metrics: Dict[str, Any],
    deterministic_findings: Dict[str, Any],
    perf_audit: Dict[str, Any],
) -> Dict[str, Any]:
    """Run an advisory Gemini Pro analysis over a single jurisdiction's public aggregates."""
    payload = {
        "jurisdiction": jur,
        "label": JURISDICTION_META[jur]["label"],
        "metrics_summary": {
            "total_parcels": metrics.get("total_parcels"),
            "total_networks": metrics.get("total_networks"),
            "multi_entity_networks": metrics.get("multi_entity_networks"),
            "oc_matched_parcels": metrics.get("oc_matched_parcels"),
            "top_10_networks": metrics.get("top_10_networks", [])[:10],
        },
        "deterministic_findings": {
            "falsely_blacklisted_cohesive_hqs": deterministic_findings.get("cohesive_hqs", [])[:6],
            "benign_care_of_exclusions": deterministic_findings.get("benign_care_of", [])[:6],
            "missed_oc_business_abbrevs": deterministic_findings.get("missed_biz_abbrevs", [])[:8],
            "missed_same_zip_hq_aliases": deterministic_findings.get("same_zip_aliases", [])[:8],
            "unlinked_series_siblings": deterministic_findings.get("series_siblings", [])[:8],
            "placeholder_networks": deterministic_findings.get("placeholders", [])[:8],
            "dispersed_hairballs": deterministic_findings.get("hairballs", [])[:6],
            "emerging_third_party_hubs": deterministic_findings.get("new_hubs", [])[:6],
        },
        "db_performance": perf_audit,
    }
    parsed, model_used, err = call_gemini_json(
        api_key, models, GEMINI_SYSTEM_PROMPT, payload
    )
    return {
        "model_used": model_used,
        "ok": parsed is not None,
        "error": err,
        "advisory": parsed or {},
    }


# ---------------------------------------------------------------------------
# Recursive Single-Jurisdiction Optimization Loop (max_passes = 3)
# ---------------------------------------------------------------------------

def _collect_auto_fix_rules(jur: str, findings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert verified deterministic findings into concrete `network_linking_rules` specs."""
    specs: List[Dict[str, Any]] = []

    for hq in findings.get("cohesive_hqs") or []:
        if hq.get("auto_fixable"):
            specs.append(
                {
                    "rule_type": "ra_allowlist",
                    "rule_key": hq["address_coarse"],
                    "rule_value": hq.get("top_officer") or hq.get("top_owner_family") or "COHESIVE_HQ",
                    "scope": "global",
                    "evidence": {
                        "jur": jur,
                        "parcels": hq["parcels_in_jur"],
                        "officer_concentration": hq["officer_concentration"],
                        "family_concentration": hq["family_concentration"],
                        "top_officer": hq["top_officer"],
                    },
                }
            )

    for co in findings.get("benign_care_of") or []:
        if co.get("auto_fixable"):
            specs.append(
                {
                    "rule_type": "benign_care_of_token",
                    "rule_key": co["token"],
                    "rule_value": "BENIGN_PREFIX",
                    "scope": "global",
                    "evidence": {"jur": jur, "affected_parcels": co["affected_parcels"], "samples": co["samples"]},
                }
            )

    for ab in findings.get("missed_biz_abbrevs") or []:
        if ab.get("auto_fixable"):
            specs.append(
                {
                    "rule_type": "biz_abbrev",
                    "rule_key": ab["abbrev"],
                    "rule_value": ab["expansion"],
                    "scope": "global",
                    "evidence": {
                        "jur": jur,
                        "distinct_entities": ab["distinct_entities"],
                        "affected_parcels": ab["affected_parcels"],
                    },
                }
            )

    for al in findings.get("same_zip_aliases") or []:
        if al.get("auto_fixable"):
            specs.append(
                {
                    "rule_type": "address_alias",
                    "rule_key": al["alias_zip_key"],
                    "rule_value": al["canonical_zip_key"],
                    "scope": jur,
                    "evidence": {
                        "jur": jur,
                        "shared_families": al["shared_families"],
                        "net_a": al["net_a"],
                        "net_b": al["net_b"],
                    },
                }
            )

    for ph in findings.get("placeholders") or []:
        norm = ph.get("normalized") or ""
        if norm and len(norm) >= 5:
            specs.append(
                {
                    "rule_type": "placeholder_owner",
                    "rule_key": norm,
                    "rule_value": "EXCLUDE",
                    "scope": "global",
                    "evidence": {"jur": jur, "display_name": ph["display_name"], "buildings": ph["building_count"]},
                }
            )

    for hub in findings.get("new_hubs") or []:
        if hub.get("auto_fixable"):
            specs.append(
                {
                    "rule_type": "third_party_hub_block",
                    "rule_key": hub["address_coarse"],
                    "rule_value": "BLOCK_HUB",
                    "scope": "global",
                    "evidence": {
                        "jur": jur,
                        "distinct_owner_families": hub["distinct_owner_families"],
                        "disjoint_external_oc_hqs": hub["disjoint_external_oc_hqs"],
                    },
                }
            )
    return specs


def _record_optimizer_run(
    conn,
    run_id: str,
    jur: str,
    pass_num: int,
    status: str,
    metrics_before: Dict[str, Any],
    metrics_after: Dict[str, Any],
    findings: Dict[str, Any],
    applied_rules: List[Dict[str, Any]],
    timings: Dict[str, Any],
) -> None:
    try:
        ensure_linking_rules_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO network_optimizer_runs
                    (run_id, jurisdiction, pass_number, finished_at, status,
                     metrics_before, metrics_after, findings, applied_rules, timings, peak_rss_mb)
                VALUES (%s, %s, %s, NOW(), %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    run_id,
                    jur,
                    pass_num,
                    status,
                    json.dumps(metrics_before, default=str),
                    json.dumps(metrics_after, default=str),
                    json.dumps(findings, default=str),
                    json.dumps(applied_rules, default=str),
                    json.dumps(timings, default=str),
                    _peak_rss_mb(),
                ),
            )
        conn.commit()
    except Exception as exc:
        logger.warning("Could not persist network_optimizer_runs row for %s: %s", jur, exc)
        try:
            conn.rollback()
        except Exception:
            pass


def optimize_single_jurisdiction(
    conn,
    run_id: str,
    jur: str,
    *,
    max_passes: int = 3,
    dry_run: bool = False,
    allow_rebuild: bool = True,
    gemini_key: str = "",
    gemini_models: Sequence[str] = (),
    use_gemini: bool = True,
) -> Dict[str, Any]:
    """Comprehensively examine, adjust, rebuild, and recursively re-examine ONE jurisdiction."""
    logger.info("=" * 72)
    logger.info("▶ Optimizing jurisdiction [%s] — %s (max_passes=%d, dry_run=%s)", jur.upper(), JURISDICTION_META[jur]["label"], max_passes, dry_run)
    logger.info("=" * 72)

    t_jur_start = time.time()
    ensure_linking_rules_tables(conn)
    reload_learned_rules(conn=conn)
    clear_shared_utils_caches()

    initial_metrics = collect_jurisdiction_metrics(conn, jur)
    if initial_metrics.get("missing_tables"):
        return {
            "jurisdiction": jur,
            "label": JURISDICTION_META[jur]["label"],
            "status": "skipped_missing_tables",
            "passes": [],
            "initial_metrics": initial_metrics,
            "final_metrics": initial_metrics,
            "applied_rules": [],
            "rebuilt": False,
            "elapsed_s": round(time.time() - t_jur_start, 2),
        }

    perf_audit = audit_and_optimize_db_performance(conn, jur, apply_maintenance=not dry_run)
    passes_log: List[Dict[str, Any]] = []
    all_applied_rules: List[Dict[str, Any]] = []
    seen_rule_sigs: Set[Tuple[str, str, str]] = set()
    any_rebuilt = False
    current_metrics = initial_metrics
    gemini_review: Optional[Dict[str, Any]] = None

    for pass_num in range(1, max(1, max_passes) + 1):
        t_pass = time.time()
        logger.info("[%s] Pass %d/%d — examining missed connections & false positives...", jur.upper(), pass_num, max_passes)

        findings = {
            "cohesive_hqs": _detect_falsely_blacklisted_cohesive_hqs(conn, jur),
            "benign_care_of": _detect_benign_care_of_false_exclusions(conn, jur),
            "missed_biz_abbrevs": _detect_missed_oc_business_abbrevs(conn, jur),
            "same_zip_aliases": _detect_missed_same_zip_hq_aliases(conn, jur),
            "series_siblings": _detect_unlinked_series_siblings(conn, jur),
            "placeholders": _detect_placeholder_networks(conn, jur),
        }
        hairballs, new_hubs = _detect_dispersed_hairballs_and_new_hubs(conn, jur)
        findings["hairballs"] = hairballs
        findings["new_hubs"] = new_hubs
        examine_s = round(time.time() - t_pass, 2)

        # On Pass 1 (and after final rebuild if rebuilt), run advisory Gemini Pro analysis
        if use_gemini and gemini_key and pass_num == 1:
            t_gem = time.time()
            gemini_review = run_gemini_advisory_review(
                gemini_key, gemini_models, jur, current_metrics, findings, perf_audit
            )
            gemini_review["elapsed_s"] = round(time.time() - t_gem, 2)
            logger.info(
                "[%s] Gemini advisory (%s): ok=%s (%.1fs)",
                jur.upper(),
                gemini_review.get("model_used"),
                gemini_review.get("ok"),
                gemini_review["elapsed_s"],
            )

        candidate_rules = [
            spec for spec in _collect_auto_fix_rules(jur, findings)
            if (spec["rule_type"], spec["rule_key"], spec["scope"]) not in seen_rule_sigs
        ]

        pass_record: Dict[str, Any] = {
            "pass_number": pass_num,
            "examine_s": examine_s,
            "findings_summary": {
                "cohesive_hqs": len(findings["cohesive_hqs"]),
                "benign_care_of": len(findings["benign_care_of"]),
                "missed_biz_abbrevs": len(findings["missed_biz_abbrevs"]),
                "same_zip_aliases": len(findings["same_zip_aliases"]),
                "series_siblings": len(findings["series_siblings"]),
                "placeholders": len(findings["placeholders"]),
                "hairballs": len(findings["hairballs"]),
                "new_hubs": len(findings["new_hubs"]),
            },
            "candidate_rule_count": len(candidate_rules),
            "applied_rules": [],
            "rebuild_timings": {},
            "status": "converged",
        }

        if not candidate_rules:
            logger.info("[%s] Pass %d converged — 0 new auto-fixable rules.", jur.upper(), pass_num)
            passes_log.append(pass_record)
            _record_optimizer_run(
                conn, run_id, jur, pass_num, "converged",
                current_metrics, current_metrics, findings, [], {"examine_s": examine_s}
            )
            break

        if dry_run:
            pass_record["status"] = "dry_run_candidates_found"
            pass_record["candidate_rules"] = candidate_rules
            passes_log.append(pass_record)
            _record_optimizer_run(
                conn, run_id, jur, pass_num, "dry_run",
                current_metrics, current_metrics, findings, candidate_rules, {"examine_s": examine_s}
            )
            break

        # Apply validated rules + prune rescued RA rows cleanly
        applied_this_pass: List[Dict[str, Any]] = []
        for spec in candidate_rules:
            rec = upsert_learned_rule(
                conn,
                spec["rule_type"],
                spec["rule_key"],
                spec["rule_value"],
                scope=spec["scope"],
                discovered_in=jur,
                evidence=spec.get("evidence"),
            )
            if spec["rule_type"] == "ra_allowlist":
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM oc_registered_agent_addresses WHERE address_coarse = %s",
                        (rec["rule_key"],),
                    )
                conn.commit()
            applied_this_pass.append(rec)
            seen_rule_sigs.add((spec["rule_type"], spec["rule_key"], spec["scope"]))

        clear_shared_utils_caches()

        # Mandatory deterministic unit regression gate before rebuild
        gate_ok, gate_out = run_deterministic_unit_gate()
        if not gate_ok:
            logger.error("[%s] Unit regression gate FAILED! Rolling back %d rules.", jur.upper(), len(applied_this_pass))
            for rec in applied_this_pass:
                deactivate_learned_rule(conn, rec["rule_type"], rec["rule_key"], scope=rec["scope"])
            clear_shared_utils_caches()
            pass_record["status"] = "rolled_back_unit_gate"
            pass_record["gate_output"] = gate_out
            passes_log.append(pass_record)
            _record_optimizer_run(
                conn, run_id, jur, pass_num, "rolled_back_unit_gate",
                current_metrics, current_metrics, findings, applied_this_pass, {"examine_s": examine_s}
            )
            break

        # Rebuild ONLY this single jurisdiction (if rebuild enabled)
        if allow_rebuild:
            prev_metrics = current_metrics
            prev_hairballs = len(findings["hairballs"])
            rebuild_timings = rebuild_single_jurisdiction_profiled(conn, jur)
            post_metrics = collect_jurisdiction_metrics(conn, jur)
            post_hairballs, _ = _detect_dispersed_hairballs_and_new_hubs(conn, jur)

            # Precision safety invariant: never allow hairball explosion OR >35% catastrophic collapse
            net_before = max(int(prev_metrics.get("total_networks") or 0), 1)
            net_after = int(post_metrics.get("total_networks") or 0)
            collapse_ratio = (net_before - net_after) / net_before
            if len(post_hairballs) > prev_hairballs + 2 or collapse_ratio > 0.35:
                logger.error(
                    "[%s] Post-rebuild invariant violated (hairballs %d->%d, collapse=%.1f%%). Rolling back & restoring!",
                    jur.upper(), prev_hairballs, len(post_hairballs), collapse_ratio * 100.0,
                )
                for rec in applied_this_pass:
                    deactivate_learned_rule(conn, rec["rule_type"], rec["rule_key"], scope=rec["scope"])
                clear_shared_utils_caches()
                rebuild_single_jurisdiction_profiled(conn, jur)
                current_metrics = collect_jurisdiction_metrics(conn, jur)
                pass_record["status"] = "rolled_back_invariant"
                pass_record["rebuild_timings"] = rebuild_timings
                passes_log.append(pass_record)
                _record_optimizer_run(
                    conn, run_id, jur, pass_num, "rolled_back_invariant",
                    prev_metrics, current_metrics, findings, applied_this_pass, rebuild_timings
                )
                break

            any_rebuilt = True
            current_metrics = post_metrics
            all_applied_rules.extend(applied_this_pass)
            pass_record["applied_rules"] = applied_this_pass
            pass_record["rebuild_timings"] = rebuild_timings
            pass_record["status"] = "applied_and_rebuilt"
            passes_log.append(pass_record)
            _record_optimizer_run(
                conn, run_id, jur, pass_num, "applied_and_rebuilt",
                prev_metrics, current_metrics, findings, applied_this_pass,
                {"examine_s": examine_s, **rebuild_timings},
            )
            logger.info(
                "[%s] Pass %d applied %d rule(s) & rebuilt in %.1fs (networks: %s -> %s, multi-entity: %s -> %s). Recursing...",
                jur.upper(), pass_num, len(applied_this_pass), rebuild_timings.get("total_s", 0.0),
                f"{prev_metrics.get('total_networks', 0):,}", f"{current_metrics.get('total_networks', 0):,}",
                f"{prev_metrics.get('multi_entity_networks', 0):,}", f"{current_metrics.get('multi_entity_networks', 0):,}",
            )
        else:
            all_applied_rules.extend(applied_this_pass)
            pass_record["applied_rules"] = applied_this_pass
            pass_record["status"] = "rules_applied_no_rebuild"
            passes_log.append(pass_record)
            _record_optimizer_run(
                conn, run_id, jur, pass_num, "rules_applied_no_rebuild",
                current_metrics, current_metrics, findings, applied_this_pass, {"examine_s": examine_s}
            )
            break

    # Final post-rebuild Gemini check if rules were applied & rebuilt
    if use_gemini and gemini_key and any_rebuilt:
        final_findings = {
            "cohesive_hqs": [],
            "benign_care_of": [],
            "missed_biz_abbrevs": [],
            "same_zip_aliases": [],
            "series_siblings": _detect_unlinked_series_siblings(conn, jur),
            "placeholders": _detect_placeholder_networks(conn, jur),
            "hairballs": _detect_dispersed_hairballs_and_new_hubs(conn, jur)[0],
            "new_hubs": [],
        }
        gemini_review = run_gemini_advisory_review(
            gemini_key, gemini_models, jur, current_metrics, final_findings, perf_audit
        )

    gc.collect()
    return {
        "jurisdiction": jur,
        "label": JURISDICTION_META[jur]["label"],
        "status": "optimized" if any_rebuilt else ("dry_run" if dry_run else "clean_verified"),
        "passes": passes_log,
        "initial_metrics": initial_metrics,
        "final_metrics": current_metrics,
        "applied_rules": all_applied_rules,
        "perf_audit": perf_audit,
        "gemini_review": gemini_review,
        "rebuilt": any_rebuilt,
        "elapsed_s": round(time.time() - t_jur_start, 2),
        "peak_rss_mb": _peak_rss_mb(),
    }


def detect_rule_propagation_targets(
    conn,
    applied_global_rules: List[Dict[str, Any]],
    already_run_jurs: Set[str],
) -> List[str]:
    """Identify other jurisdictions (strictly for spreading generalized logic/rules,
    NEVER cross-border linking) whose raw tables match newly learned global rules."""
    if not applied_global_rules:
        return []
    placeholder_keys = [r["rule_key"] for r in applied_global_rules if r.get("rule_type") == "placeholder_owner"]
    hub_keys = [r["rule_key"] for r in applied_global_rules if r.get("rule_type") == "third_party_hub_block"]
    if not placeholder_keys and not hub_keys:
        return []

    impacted: List[str] = []
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '15000'")
        for jur in ALL_JURISDICTIONS:
            if jur in already_run_jurs or jur == "ct":
                continue
            _, nets_tbl = _jur_tables(jur)
            if not _table_exists(cur, nets_tbl):
                continue
            if placeholder_keys:
                cur.execute(
                    f"SELECT 1 FROM {nets_tbl} WHERE UPPER(TRIM(display_name)) = ANY(%s) LIMIT 1",
                    (placeholder_keys,),
                )
                if cur.fetchone():
                    impacted.append(jur)
                    continue
    return impacted


# ---------------------------------------------------------------------------
# Report Rendering (Markdown + Plain Text + Rich HTML Email)
# ---------------------------------------------------------------------------

def render_nightly_report(summary: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Return `(subject, plain_text, html_body, markdown_doc)`."""
    date_str = summary["started_at"][:10]
    jurs_upper = [r["jurisdiction"].upper() for r in summary["results"]]
    total_rules = sum(len(r.get("applied_rules") or []) for r in summary["results"])
    total_rebuilt = sum(1 for r in summary["results"] if r.get("rebuilt"))
    total_passes = sum(len(r.get("passes") or []) for r in summary["results"])
    gemini_model = summary.get("gemini_primary_model") or "disabled"

    subject = (
        f"[They Own WHAT?] Nightly 5-Jurisdiction Linking Optimizer ({', '.join(jurs_upper)}) — "
        f"{total_rules} rule(s), {total_rebuilt} rebuilt ({date_str})"
    )

    md_lines = [
        f"# Nightly 5-Jurisdiction Network-Linking Optimization Report ({date_str})",
        "",
        f"- **Run ID:** `{summary['run_id']}`",
        f"- **Mode:** `{'DRY-RUN' if summary.get('dry_run') else 'LIVE RECURSIVE OPTIMIZATION'}` (strictly jurisdiction-by-jurisdiction; zero cross-jurisdiction parcel linking)",
        f"- **Jurisdictions Examined ({len(jurs_upper)}):** {', '.join(jurs_upper)}",
        f"- **Rotation Cycle:** Cycle {summary.get('cycle_number')} (next cursor index: {summary.get('next_cursor_index')}/20)",
        f"- **Gemini Advisory Model (Auto-Discovered Best Available):** `{gemini_model}` (fallback: `{summary.get('gemini_fallback_model', 'none')}`)",
        f"- **Totals:** {total_passes} recursive pass(es) | {total_rules} verified rule(s) applied | {total_rebuilt} jurisdiction(s) rebuilt | {summary.get('total_elapsed_s', 0):.1f}s elapsed | Peak RSS {summary.get('peak_rss_mb', 0):.0f} MB",
        "",
    ]

    if summary.get("propagated_rules"):
        md_lines.append("## Generalized Logic & Rules Spread Across Shared Engine")
        for r in summary["propagated_rules"]:
            md_lines.append(
                f"- `{r['rule_type']}` ({r.get('scope', 'global')}): `{r['rule_key']}`"
                + (f" -> `{r['rule_value']}`" if r.get("rule_value") else "")
                + f" *(discovered in {str(r.get('discovered_in', '')).upper()})*"
            )
        if summary.get("propagation_queued_jurisdictions"):
            md_lines.append(
                f"- **Queued for priority re-check in upcoming cycles:** {', '.join(j.upper() for j in summary['propagation_queued_jurisdictions'])}"
            )
        md_lines.append("")

    for res in summary["results"]:
        j = res["jurisdiction"].upper()
        init_m = res.get("initial_metrics") or {}
        fin_m = res.get("final_metrics") or {}
        md_lines.append(f"## {j} — {res.get('label', j)} (`{res.get('status')}`)")
        md_lines.append(
            f"- **Parcels:** {init_m.get('total_parcels', 0):,} | "
            f"**Networks:** {init_m.get('total_networks', 0):,} -> {fin_m.get('total_networks', 0):,} | "
            f"**Multi-Entity:** {init_m.get('multi_entity_networks', 0):,} -> {fin_m.get('multi_entity_networks', 0):,} | "
            f"**Elapsed:** {res.get('elapsed_s', 0):.1f}s"
        )
        for p in res.get("passes") or []:
            fs = p.get("findings_summary") or {}
            md_lines.append(
                f"  - **Pass {p['pass_number']}** (`{p['status']}`, examine={p.get('examine_s', 0)}s): "
                f"cohesive_HQs={fs.get('cohesive_hqs', 0)}, benign_C/O={fs.get('benign_care_of', 0)}, "
                f"missed_abbrevs={fs.get('missed_biz_abbrevs', 0)}, zip_aliases={fs.get('same_zip_aliases', 0)}, "
                f"series_leads={fs.get('series_siblings', 0)}, placeholders={fs.get('placeholders', 0)}, "
                f"hairballs={fs.get('hairballs', 0)}, new_3p_hubs={fs.get('new_hubs', 0)}"
            )
            if p.get("rebuild_timings"):
                md_lines.append(f"    - Rebuild timings: `{json.dumps(p['rebuild_timings'])}`")
        if res.get("applied_rules"):
            md_lines.append("- **Verified Fixes Applied:**")
            for r in res["applied_rules"]:
                md_lines.append(f"  - `{r['rule_type']}`: `{r['rule_key']}` -> `{r.get('rule_value', '')}`")
        perf = res.get("perf_audit") or {}
        if perf.get("actions_taken"):
            md_lines.append("- **DB / Pipeline Efficiency Actions:**")
            for act in perf["actions_taken"]:
                md_lines.append(f"  - {act}")
        if perf.get("recommendations"):
            md_lines.append("- **DB / Pipeline Recommendations:**")
            for rec in perf["recommendations"]:
                md_lines.append(f"  - {rec}")
        gem = res.get("gemini_review") or {}
        adv = gem.get("advisory") or {}
        if gem.get("ok") and adv:
            md_lines.append(f"- **Gemini Advisory (`{gem.get('model_used')}`, non-binding):** {adv.get('jurisdiction_assessment', '')}")
            for h in (adv.get("missed_connection_hypotheses") or [])[:4]:
                md_lines.append(f"  - *Missed connection [{h.get('confidence', '?')}]:* **{h.get('title', '')}** — {h.get('recommended_logic_change', '')}")
            for fp in (adv.get("false_positive_risks") or [])[:4]:
                md_lines.append(f"  - *False-positive watch [{fp.get('confidence', '?')}]:* **{fp.get('network_or_hub', '')}** — {fp.get('recommended_fix', '')}")
            for ps in (adv.get("pipeline_performance_suggestions") or [])[:3]:
                md_lines.append(f"  - *Performance ({ps.get('step', '')}):* {ps.get('optimization', '')}")
        elif gem.get("error"):
            md_lines.append(f"- **Gemini Advisory:** skipped/error (`{gem.get('error')}`)")
        md_lines.append("")

    markdown_doc = "\n".join(md_lines)
    plain_text = markdown_doc

    # Clean HTML rendering
    html_parts = [
        "<html><body style='font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; color:#1e293b; max-width:860px; margin:0 auto; padding:20px;'>",
        f"<h2 style='color:#1e1b4b; margin-bottom:4px;'>They Own WHAT?? — Nightly 5-Jurisdiction Linking Optimizer</h2>",
        f"<p style='color:#475569; margin-top:0;'>Date: <b>{html.escape(date_str)}</b> &middot; Jurisdiction-by-Jurisdiction Cohort: <b>{html.escape(', '.join(jurs_upper))}</b> &middot; Gemini Model: <code>{html.escape(gemini_model)}</code></p>",
        "<table style='border-collapse:collapse; width:100%; margin:12px 0 20px 0; font-size:13px;'>"
        "<tr style='background:#f1f5f9; text-align:left;'>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Jurisdiction</th>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Status</th>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Passes</th>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Networks (Before &rarr; After)</th>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Rules Applied</th>"
        "<th style='padding:8px; border:1px solid #cbd5e1;'>Elapsed</th>"
        "</tr>",
    ]
    for res in summary["results"]:
        init_m = res.get("initial_metrics") or {}
        fin_m = res.get("final_metrics") or {}
        html_parts.append(
            "<tr>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'><b>{html.escape(res['jurisdiction'].upper())}</b></td>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'>{html.escape(str(res.get('status', '')))}</td>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'>{len(res.get('passes') or [])}</td>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'>{init_m.get('total_networks', 0):,} &rarr; {fin_m.get('total_networks', 0):,}</td>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'>{len(res.get('applied_rules') or [])}</td>"
            f"<td style='padding:8px; border:1px solid #cbd5e1;'>{res.get('elapsed_s', 0):.1f}s</td>"
            "</tr>"
        )
    html_parts.append("</table>")

    for res in summary["results"]:
        j = res["jurisdiction"].upper()
        html_parts.append(
            f"<div style='border:1px solid #e2e8f0; border-radius:8px; padding:14px 16px; margin-bottom:14px;'>"
            f"<h3 style='margin:0 0 8px 0; color:#312e81;'>{html.escape(j)} — {html.escape(res.get('label', j))}</h3>"
        )
        if res.get("applied_rules"):
            html_parts.append("<p style='margin:6px 0;'><b>Verified Rules Applied:</b></p><ul style='margin:4px 0 8px 20px;'>")
            for r in res["applied_rules"]:
                html_parts.append(
                    f"<li><code>{html.escape(r['rule_type'])}</code>: <code>{html.escape(r['rule_key'])}</code>"
                    + (f" &rarr; <code>{html.escape(r['rule_value'])}</code>" if r.get("rule_value") else "")
                    + "</li>"
                )
            html_parts.append("</ul>")

        perf = res.get("perf_audit") or {}
        if perf.get("actions_taken"):
            html_parts.append("<p style='margin:6px 0;'><b>Performance &amp; DB Maintenance:</b></p><ul style='margin:4px 0 8px 20px;'>")
            for act in perf["actions_taken"]:
                html_parts.append(f"<li>{html.escape(act)}</li>")
            html_parts.append("</ul>")

        gem = res.get("gemini_review") or {}
        adv = gem.get("advisory") or {}
        if gem.get("ok") and adv:
            html_parts.append(
                f"<p style='margin:6px 0;'><b>Gemini Advisory (<code>{html.escape(str(gem.get('model_used', '')))}</code>):</b> "
                f"{html.escape(str(adv.get('jurisdiction_assessment', '')))}</p>"
            )
            hyps = adv.get("missed_connection_hypotheses") or []
            if hyps:
                html_parts.append("<p style='margin:4px 0;'><i>Missed-Connection Leads:</i></p><ul style='margin:2px 0 8px 20px;'>")
                for h in hyps[:4]:
                    html_parts.append(
                        f"<li><b>{html.escape(str(h.get('title', '')))}</b> [{html.escape(str(h.get('confidence', '')))}]: "
                        f"{html.escape(str(h.get('recommended_logic_change', '')))}</li>"
                    )
                html_parts.append("</ul>")
            fps = adv.get("false_positive_risks") or []
            if fps:
                html_parts.append("<p style='margin:4px 0;'><i>False-Positive Watch:</i></p><ul style='margin:2px 0 8px 20px;'>")
                for fp in fps[:4]:
                    html_parts.append(
                        f"<li><b>{html.escape(str(fp.get('network_or_hub', '')))}</b> [{html.escape(str(fp.get('confidence', '')))}]: "
                        f"{html.escape(str(fp.get('recommended_fix', '')))}</li>"
                    )
                html_parts.append("</ul>")
        html_parts.append("</div>")

    html_parts.append(
        "<p style='font-size:12px; color:#64748b;'>Generated automatically by <code>updater/nightly_network_optimizer.py</code>. "
        "All auto-applied rules passed deterministic PostgreSQL public-record proof, unit regression gates, and post-rebuild precision invariants. "
        "Gemini insights are advisory only.</p></body></html>"
    )
    return subject, plain_text, "\n".join(html_parts), markdown_doc


# ---------------------------------------------------------------------------
# Main Orchestrator & CLI
# ---------------------------------------------------------------------------

def run_nightly_optimization(
    *,
    cohort_size: int = 5,
    max_passes: int = 3,
    explicit_jurisdictions: Optional[List[str]] = None,
    dry_run: bool = False,
    allow_rebuild: bool = True,
    use_gemini: bool = True,
    send_email: bool = False,
    email_to: Optional[str] = None,
    state_path: str = STATE_PATH,
) -> Dict[str, Any]:
    t0 = time.time()
    started_at = _iso_utc()
    run_id = f"nlo-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    state = load_optimizer_state(state_path)
    selected_jurs, next_cursor, next_cycle = select_tonight_cohort(
        state, cohort_size=cohort_size, explicit_jurisdictions=explicit_jurisdictions
    )

    gemini_key = _resolve_gemini_key() if use_gemini else ""
    best_pro, best_flash = discover_best_gemini_models(gemini_key) if gemini_key else ("disabled", "disabled")
    gemini_models = list(dict.fromkeys(m for m in (best_pro, best_flash, "gemini-2.5-pro", "gemini-2.5-flash") if m and m != "disabled")) if gemini_key else []
    logger.info(
        "Starting Nightly 5-Jurisdiction Optimizer run_id=%s cohort=%s gemini=(%s, fallback=%s)",
        run_id, [j.upper() for j in selected_jurs], best_pro, best_flash,
    )

    dsn = _resolve_db_url()
    results: List[Dict[str, Any]] = []
    global_rules_applied: List[Dict[str, Any]] = []
    any_rebuilt = False

    # Process each jurisdiction strictly ONE BY ONE (isolated DB connection + gc.collect())
    for jur in selected_jurs:
        conn = psycopg2.connect(dsn)
        try:
            res = optimize_single_jurisdiction(
                conn,
                run_id,
                jur,
                max_passes=max_passes,
                dry_run=dry_run,
                allow_rebuild=allow_rebuild,
                gemini_key=gemini_key,
                gemini_models=gemini_models,
                use_gemini=use_gemini,
            )
            results.append(res)
            if res.get("rebuilt"):
                any_rebuilt = True
            for r in res.get("applied_rules") or []:
                if r.get("scope") == "global":
                    global_rules_applied.append(r)
        except Exception as exc:
            logger.exception("Optimizer crashed on jurisdiction %s: %s", jur.upper(), exc)
            results.append(
                {
                    "jurisdiction": jur,
                    "label": JURISDICTION_META[jur]["label"],
                    "status": "error",
                    "error": str(exc),
                    "passes": [],
                    "applied_rules": [],
                    "rebuilt": False,
                    "elapsed_s": 0.0,
                }
            )
        finally:
            conn.close()
        gc.collect()

    # Check which other jurisdictions benefit from newly learned global rules
    propagation_targets: List[str] = []
    if global_rules_applied and not dry_run:
        conn = psycopg2.connect(dsn)
        try:
            propagation_targets = detect_rule_propagation_targets(
                conn, global_rules_applied, set(selected_jurs)
            )
        finally:
            conn.close()

    # Refresh landing_snapshot:v1 if any jurisdiction was rebuilt
    landing_refresh_s = None
    if any_rebuilt and not dry_run:
        try:
            os.environ["DATABASE_URL"] = dsn
            from api.landing_cache import build_and_store_landing_snapshot
            tl = time.time()
            build_and_store_landing_snapshot()
            landing_refresh_s = round(time.time() - tl, 2)
            logger.info("✓ Refreshed landing_snapshot:v1 in %.1fs after nightly rebuilds.", landing_refresh_s)
        except Exception as exc:
            logger.warning("⚠ Landing snapshot refresh after nightly optimization failed: %s", exc)

    # Update state cursor & history (only advance rotation cursor on non-explicit runs)
    if not explicit_jurisdictions:
        state["cursor_index"] = next_cursor
        state["cycle_number"] = next_cycle
    existing_pq = list(state.get("priority_queue") or [])
    for pt in propagation_targets:
        if pt not in existing_pq:
            existing_pq.append(pt)
    state["priority_queue"] = existing_pq[:10]
    state["last_run_at"] = _iso_utc()

    for res in results:
        for p in res.get("passes") or []:
            rt = p.get("rebuild_timings") or {}
            if rt.get("total_s"):
                state.setdefault("timings_baseline", {})[res["jurisdiction"]] = {
                    "total_s": rt["total_s"],
                    "peak_rss_mb": rt.get("peak_rss_mb"),
                    "recorded_at": _iso_utc(),
                }

    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": _iso_utc(),
        "dry_run": dry_run,
        "cycle_number": state["cycle_number"],
        "next_cursor_index": state["cursor_index"],
        "gemini_primary_model": best_pro,
        "gemini_fallback_model": best_flash,
        "jurisdictions": selected_jurs,
        "results": results,
        "propagated_rules": global_rules_applied,
        "propagation_queued_jurisdictions": propagation_targets,
        "landing_refresh_s": landing_refresh_s,
        "total_elapsed_s": round(time.time() - t0, 2),
        "peak_rss_mb": _peak_rss_mb(),
    }

    state["history"].append(
        {
            "run_id": run_id,
            "started_at": started_at,
            "jurisdictions": selected_jurs,
            "rules_applied": sum(len(r.get("applied_rules") or []) for r in results),
            "rebuilt": [r["jurisdiction"] for r in results if r.get("rebuilt")],
            "gemini_model": best_pro,
            "elapsed_s": summary["total_elapsed_s"],
        }
    )
    if not dry_run or not os.path.exists(state_path):
        save_optimizer_state(state, state_path)

    # Persist Markdown & JSON report archive
    os.makedirs(REPORTS_DIR, exist_ok=True)
    subject, plain_text, html_body, md_doc = render_nightly_report(summary)
    date_tag = started_at[:10]
    md_path = os.path.join(REPORTS_DIR, f"{date_tag}-{run_id}.md")
    json_path = os.path.join(REPORTS_DIR, f"{date_tag}-{run_id}.json")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(md_doc)
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    summary["report_md_path"] = md_path
    summary["report_json_path"] = json_path

    if send_email:
        recipients = [r.strip() for r in (email_to or DEFAULT_REPORT_RECIPIENT).split(",") if r.strip()]
        sent = send_audit_email(subject, plain_text, body_html=html_body, recipients=recipients)
        summary["email_sent"] = sent
        summary["email_recipients"] = recipients

    logger.info("✓ Nightly optimization finished: %s (report: %s)", subject, md_path)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Nightly 5-jurisdiction jurisdiction-by-jurisdiction recursive network-linking optimizer."
    )
    parser.add_argument("--jurisdictions", nargs="*", help="Explicit jurisdictions to inspect one by one (overrides 5-jurisdiction rotation).")
    parser.add_argument("--cohort-size", type=int, default=5, help="Jurisdictions per nightly cohort (default: 5).")
    parser.add_argument("--max-passes", type=int, default=3, help="Max recursive examine->rebuild passes per jurisdiction (default: 3).")
    parser.add_argument("--dry-run", action="store_true", help="Inspect & report without modifying rules or rebuilding.")
    parser.add_argument("--no-rebuild", action="store_true", help="Record verified rules without triggering immediate rebuild.")
    parser.add_argument("--no-gemini", action="store_true", help="Skip advisory Gemini Pro review.")
    parser.add_argument("--send-email", action="store_true", help="Send nightly HTML+text report via SMTP.")
    parser.add_argument("--to", default=DEFAULT_REPORT_RECIPIENT, help="Email recipient(s), comma-separated (default: salmun@storrsmagic.org).")
    args = parser.parse_args(argv)

    run_nightly_optimization(
        cohort_size=args.cohort_size,
        max_passes=args.max_passes,
        explicit_jurisdictions=args.jurisdictions,
        dry_run=args.dry_run,
        allow_rebuild=not args.no_rebuild,
        use_gemini=not args.no_gemini,
        send_email=args.send_email,
        email_to=args.to,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
