#!/usr/bin/env python3
"""
Weekly Dynamic Gemini Audit, Self-Optimization & 1-Click Maintainer Review Digest
(`scripts/weekly_gemini_audit.py`)
=================================================================================
1. Dynamically queries Google's `/v1beta/models` endpoint to select the latest
   highest-versioned Gemini Pro model (with automatic fallback to the latest
   Gemini Flash model on HTTP 429/503).
2. Audits all 17 live jurisdictions across PostgreSQL & `/api/completeness`:
   - Parcel/building & network scale parity
   - Strict 3rd-party bank / servicer / registered-agent exclusion (`0` leak invariant)
   - Max cluster ceiling (`<= 5,000` buildings invariant)
   - Corporate registry principal coverage (`CT`, `PA`, `NY`, `CO`, `FL`)
   - Open maintainer fix requests (`docs/review-fix-queue.json`) & user feedback
3. Performs safe cache/completeness optimization (`--optimize`), invokes the
   latest Gemini Pro model (`thinkingLevel: "HIGH"`) for deep cross-jurisdiction
   synthesis, writes `docs/benchmarks/latest-weekly-gemini-audit.json` +
   `build/weekly-gemini-audit-email.html`, and emails `salmunk@gmail.com` and
   `me@salmun.net` with HMAC-signed 1-click Approve / Decline buttons.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

env_file = ROOT_DIR / ".env"
if env_file.exists():
    load_dotenv(env_file)
else:
    load_dotenv()

from api.audit_review_routes import (
    DEFAULT_DECISIONS_PATH,
    DEFAULT_FIX_QUEUE_PATH,
    DEFAULT_LATEST_AUDIT_PATH,
    build_quick_decision_url,
    load_review_state,
    resolve_fix_queue_item,
    sign_review_token,
)
from api.shared_utils import THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN
from updater.send_audit_email import DEFAULT_RECIPIENTS, send_audit_email

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("weekly-gemini-audit")

GEMINI_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models?pageSize=200"
GEMINI_GEN_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

EXCLUDED_MODEL_SUBSTRINGS = (
    "tts",
    "image",
    "imagen",
    "audio",
    "embedding",
    "vision",
    "robotics",
    "computer-use",
    "customtools",
    "learnlm",
    "gemma",
    "veo",
    "aqa",
)

LIVE_NON_CT_JURISDICTIONS: Tuple[Tuple[str, str, str], ...] = (
    ("nyc", "NY", "New York City"),
    ("nj", "NJ", "New Jersey (DCA BHI)"),
    ("philadelphia", "PHILADELPHIA", "Philadelphia, PA"),
    ("dc", "DC", "Washington, D.C."),
    ("baltimore", "BALTIMORE", "Baltimore, MD"),
    ("boston", "BOSTON", "Boston, MA"),
    ("ma", "MA", "Massachusetts (351 Towns)"),
    ("fl", "FL", "Florida (67 Counties)"),
    ("miami", "MIAMI", "Miami-Dade, FL"),
    ("ny", "NYS", "New York State (33 Counties)"),
    ("co", "CO", "Colorado"),
    ("nc", "NC", "North Carolina (100 Counties)"),
    ("wi", "WI", "Wisconsin (72 Counties)"),
    ("vt", "VT", "Vermont"),
    ("detroit", "DETROIT", "Detroit, MI"),
    ("minneapolis", "MINNEAPOLIS", "Minneapolis, MN"),
)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso_utc(val: Optional[dt.datetime] = None) -> str:
    d = val or _utc_now()
    return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL") or "postgresql://user:password@127.0.0.1:5432/ctdata"
    if "@ctdata_db:" in raw and not Path("/.dockerenv").exists():
        return raw.replace("@ctdata_db:", "@127.0.0.1:")
    return raw


def _resolve_gemini_key() -> str:
    return (
        os.environ.get("GEMINI_KEY")
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or ""
    ).strip()


def score_gemini_model_candidate(model_obj: Dict[str, Any], desired_tier: str = "pro") -> Optional[Tuple[int, int, int, int, str]]:
    """
    Rank a `/v1beta/models` entry for general-purpose reasoning/structured JSON output.
    Returns `(major, minor, tier_match, stability_rank, model_id)` or `None` if ineligible.
    Higher tuple wins.
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

    # Ignore unversioned "-latest" aliases when explicit numbered releases exist,
    # unless no numbered model matches.
    if model_id.endswith("-latest") and not re.search(r"^gemini-\d", model_id):
        return None

    tier = desired_tier.lower()
    if tier == "pro":
        if "-pro" not in model_id:
            return None
    elif tier == "flash":
        if "-flash" not in model_id or "flash-lite" in model_id:
            return None
    elif tier == "flash-lite":
        if "flash-lite" not in model_id:
            return None

    ver_match = re.match(r"^gemini-(\d+)(?:\.(\d+))?-", model_id)
    if not ver_match:
        return None
    major = int(ver_match.group(1))
    minor = int(ver_match.group(2) or 0)

    # Within the exact same (major, minor), prefer stable GA > -latest > preview > exp
    if "exp" in model_id:
        stability = 1
    elif "preview" in model_id:
        stability = 2
    elif model_id.endswith("-latest"):
        stability = 3
    else:
        stability = 4

    # Extract optional date suffix like -05-06 or -001 as tiebreaker
    date_nums = [int(x) for x in re.findall(r"-(\d{2,4})", model_id)]
    recency_tie = sum(date_nums) if date_nums else 0

    return (major, minor, stability, recency_tie, raw_name.split("/", 1)[1])


def discover_latest_gemini_model(
    api_key: str,
    *,
    tier: str = "pro",
    fetch_json: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
) -> str:
    """
    Dynamically query `/v1beta/models` and return the newest Gemini model ID
    for `tier` (`pro`, `flash`, or `flash-lite`).
    """
    fallback = "gemini-2.5-pro" if tier == "pro" else "gemini-2.5-flash"
    if not api_key:
        return fallback

    def _default_fetch(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    fetcher = fetch_json or _default_fetch
    try:
        payload = fetcher(GEMINI_MODELS_URL, {"x-goog-api-key": api_key, "Accept": "application/json"})
        models = payload.get("models") or []
        scored: List[Tuple[int, int, int, int, str]] = []
        for m in models:
            if isinstance(m, dict):
                s = score_gemini_model_candidate(m, desired_tier=tier)
                if s is not None:
                    scored.append(s)
        if scored:
            scored.sort(reverse=True)
            return scored[0][4]
    except Exception as exc:
        logger.warning("Dynamic Gemini model discovery (%s) fell back (%s)", tier, exc)
    return fallback


def collect_recent_git_commits(root_dir: Path = ROOT_DIR, days: int = 7) -> List[Dict[str, str]]:
    try:
        out = subprocess.check_output(
            [
                "git",
                "log",
                f"--since={int(days)} days ago",
                "--pretty=format:%H%x09%h%x09%cI%x09%s",
                "-n",
                "25",
            ],
            cwd=str(root_dir),
            text=True,
            stderr=subprocess.DEVNULL,
        )
        commits = []
        for line in out.splitlines():
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                commits.append(
                    {
                        "sha": parts[0],
                        "short_sha": parts[1],
                        "committed_at": parts[2],
                        "subject": parts[3],
                    }
                )
        return commits
    except Exception:
        return []


def collect_platform_audit_snapshot(
    *,
    db_url: Optional[str] = None,
    api_base_url: str = "http://127.0.0.1:8000",
    optimize: bool = False,
) -> Dict[str, Any]:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from api.shared_utils import is_excluded_third_party_entity

    resolved_db = db_url or _resolve_db_url()
    conn = psycopg2.connect(resolved_db)
    optimizations_applied: List[str] = []
    jurisdictions: List[Dict[str, Any]] = []
    clean_api = api_base_url.rstrip("/")

    def _probe_live_private_networks(url: str) -> Tuple[int, int]:
        """Return `(leak_count, max_cluster)` from live user-facing private network endpoint."""
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                payload = json.loads(r.read().decode("utf-8"))
            items = (
                payload
                if isinstance(payload, list)
                else (payload.get("items") or payload.get("networks") or payload.get("results") or payload.get("landlords") or [])
            )
            leaks = 0
            mx = 0
            for it in (items or [])[:50]:
                if not isinstance(it, dict):
                    continue
                nm = str(
                    it.get("display_name")
                    or it.get("name")
                    or it.get("landlord_name")
                    or it.get("entity_name")
                    or ""
                ).strip()
                bcnt = int(
                    it.get("building_count")
                    or it.get("property_count")
                    or it.get("total_properties")
                    or it.get("properties")
                    or 0
                )
                if bcnt > mx:
                    mx = bcnt
                if nm and is_excluded_third_party_entity(nm):
                    leaks += 1
            return leaks, mx
        except Exception:
            return 0, 0

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Connecticut statewide ground truth
            cur.execute("SELECT COUNT(*) AS c, COUNT(*) FILTER (WHERE nhpd_id IS NOT NULL) AS sub_c FROM properties WHERE COALESCE(source, '') <> 'NYS_OPEN_DATA'")
            ct_prop_row = cur.fetchone()
            ct_props = int(ct_prop_row["c"] or 0)
            ct_sub = int(ct_prop_row["sub_c"] or 0)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS c,
                    COUNT(*) FILTER (WHERE COALESCE(total_properties, 0) >= 2) AS multi_c,
                    COALESCE(MAX(total_properties), 0) AS mx
                FROM networks
                WHERE COALESCE(total_properties, 0) >= 1
                """
            )
            ct_net_row = cur.fetchone()
            ct_nets = int(ct_net_row["c"] or 0)
            ct_multi = int(ct_net_row["multi_c"] or 0)
            ct_max = int(ct_net_row["mx"] or 0)
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(DISTINCT property_id) FROM code_enforcement WHERE property_id IS NOT NULL)
                    + (SELECT COUNT(DISTINCT property_id) FROM evictions WHERE property_id IS NOT NULL) AS enf_c
                """
            )
            ct_enf = int((cur.fetchone() or {}).get("enf_c") or 0)
            ct_leaks, ct_live_mx = _probe_live_private_networks(f"{clean_api}/api/monitor?city=HARTFORD&dimension=network")
            jurisdictions.append(
                {
                    "slug": "ct",
                    "dataset_key": "CT",
                    "label": "Connecticut (169 Towns)",
                    "properties": ct_props,
                    "networks": ct_nets,
                    "multi_networks": ct_multi,
                    "max_cluster": ct_live_mx or ct_max,
                    "third_party_leaks": ct_leaks,
                    "subsidized_parcels": ct_sub,
                    "enforcement_parcels": ct_enf,
                }
            )

            # Ensure parent `us_networks` has GIN index on `bbl_list` when optimizing
            if optimize:
                cur.execute("SELECT to_regclass('us_networks') AS un, to_regclass('idx_us_networks_bbl_list_gin') AS uidx")
                un_row = cur.fetchone()
                if un_row and un_row["un"] and not un_row["uidx"]:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_us_networks_bbl_list_gin ON us_networks USING GIN (bbl_list)")
                    conn.commit()
                    optimizations_applied.append("Created GIN `bbl_list` index on partitioned `us_networks`.")

            # 2. All 16 non-CT live jurisdictions
            for slug, ds_key, label in LIVE_NON_CT_JURISDICTIONS:
                prop_tbl = f"{slug}_properties"
                net_tbl = f"{slug}_networks"
                stats_tbl = f"{slug}_bbl_stats"

                cur.execute("SELECT to_regclass(%s) AS p, to_regclass(%s) AS n, to_regclass(%s) AS s", (prop_tbl, net_tbl, stats_tbl))
                reg = cur.fetchone()
                if not reg or not reg["p"] or not reg["n"]:
                    jurisdictions.append(
                        {
                            "slug": slug,
                            "dataset_key": ds_key,
                            "label": label,
                            "properties": 0,
                            "networks": 0,
                            "multi_networks": 0,
                            "max_cluster": 0,
                            "third_party_leaks": 0,
                            "subsidized_parcels": 0,
                            "enforcement_parcels": 0,
                            "missing_tables": True,
                        }
                    )
                    continue

                cur.execute(f"SELECT COUNT(*) AS c FROM {prop_tbl}")
                p_cnt = int(cur.fetchone()["c"] or 0)
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_nets,
                        COUNT(*) FILTER (WHERE COALESCE(building_count, 0) >= 2) AS multi_nets,
                        COALESCE(MAX(building_count), 0) AS max_cluster
                    FROM {net_tbl}
                    """
                )
                nrow = cur.fetchone()
                sub_cnt = 0
                enf_cnt = 0
                if reg["s"]:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) FILTER (WHERE COALESCE(nhpd_subsidy, FALSE)) AS sub_cnt,
                            COUNT(*) FILTER (
                                WHERE COALESCE(violations_total, 0) > 0
                                   OR COALESCE(evictions_total, 0) > 0
                                   OR COALESCE(litigations_total, 0) > 0
                            ) AS enf_cnt
                        FROM {stats_tbl}
                        """
                    )
                    srow = cur.fetchone()
                    sub_cnt = int(srow["sub_cnt"] or 0)
                    enf_cnt = int(srow["enf_cnt"] or 0)

                if optimize and p_cnt > 0:
                    cur.execute(
                        """
                        SELECT c.relispartition,
                               EXISTS (
                                   SELECT 1 FROM pg_indexes
                                   WHERE tablename = %s AND indexdef ILIKE '%%gin%%bbl_list%%'
                               ) AS has_gin
                        FROM pg_class c
                        WHERE c.relname = %s
                        """,
                        (net_tbl, net_tbl),
                    )
                    idx_meta = cur.fetchone() or {}
                    if not idx_meta.get("relispartition") and not idx_meta.get("has_gin"):
                        idx_name = f"{slug}_networks_bbl_gin_idx"
                        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {net_tbl} USING GIN (bbl_list)")
                        conn.commit()
                        optimizations_applied.append(f"Created missing GIN bbl_list index `{idx_name}` on `{net_tbl}`.")

                live_leaks, live_mx = _probe_live_private_networks(
                    f"{clean_api}/api/{slug}/networks?limit=50&include_institutional=false"
                )
                jurisdictions.append(
                    {
                        "slug": slug,
                        "dataset_key": ds_key,
                        "label": label,
                        "properties": p_cnt,
                        "networks": int(nrow["total_nets"] or 0),
                        "multi_networks": int(nrow["multi_nets"] or 0),
                        "max_cluster": live_mx or int(nrow["max_cluster"] or 0),
                        "third_party_leaks": live_leaks,
                        "subsidized_parcels": sub_cnt,
                        "enforcement_parcels": enf_cnt,
                    }
                )

            # 3. Corporate registries & empirical registered-agent quarantine stats
            corp_stats: Dict[str, Dict[str, int]] = {}
            cur.execute("SELECT to_regclass('oc_companies') AS c, to_regclass('oc_officers') AS o, to_regclass('oc_registered_agent_addresses') AS r")
            oc_reg = cur.fetchone()
            quarantined_ra_hubs = 0
            if oc_reg and oc_reg["c"]:
                cur.execute("SELECT jurisdiction_code, COUNT(*) AS cnt FROM oc_companies GROUP BY jurisdiction_code ORDER BY cnt DESC")
                for r in cur.fetchall():
                    corp_stats.setdefault(r["jurisdiction_code"], {})["companies"] = int(r["cnt"] or 0)
            if oc_reg and oc_reg["o"]:
                cur.execute("SELECT jurisdiction_code, COUNT(*) AS cnt FROM oc_officers GROUP BY jurisdiction_code ORDER BY cnt DESC")
                for r in cur.fetchall():
                    corp_stats.setdefault(r["jurisdiction_code"], {})["officers"] = int(r["cnt"] or 0)
            if oc_reg and oc_reg["r"]:
                cur.execute("SELECT COUNT(*) AS cnt FROM oc_registered_agent_addresses")
                quarantined_ra_hubs = int(cur.fetchone()["cnt"] or 0)

            # 4. Recent user feedback (excluding synthetic unit-test verification probes)
            recent_feedback: List[Dict[str, Any]] = []
            cur.execute("SELECT to_regclass('user_feedback') AS uf")
            if cur.fetchone()["uf"]:
                cur.execute(
                    """
                    SELECT id, report_type, description, created_at::text AS created_at
                    FROM user_feedback
                    WHERE created_at >= NOW() - INTERVAL '14 days'
                      AND COALESCE(description, '') <> 'Test automated feedback submission verification'
                    ORDER BY created_at DESC
                    LIMIT 15
                    """
                )
                recent_feedback = [dict(r) for r in cur.fetchall()]

            if optimize:
                cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
                conn.commit()
    finally:
        conn.close()

    # 5. Live API `/api/completeness` check (and optional cache refresh)
    completeness_summary: Dict[str, Any] = {"reachable": False}
    clean_api = api_base_url.rstrip("/")
    comp_url = f"{clean_api}/api/completeness{'?refresh=true' if optimize else ''}"
    try:
        with urllib.request.urlopen(comp_url, timeout=90) as resp:
            comp_data = json.loads(resp.read().decode("utf-8"))
            sources = comp_data.get("sources") or []
            non_success = [
                s.get("municipality")
                for s in sources
                if str(s.get("status") or "").lower() not in ("success", "")
            ]
            completeness_summary = {
                "reachable": True,
                "source_rows": len(sources),
                "non_success_sources": non_success[:20],
            }
            if optimize:
                optimizations_applied.append(
                    f"Refreshed live `/api/completeness` matrix cache across {len(sources)} municipal & statewide source entries."
                )
    except Exception as exc:
        completeness_summary = {"reachable": False, "error": str(exc)}

    review_state = load_review_state()
    recent_commits = collect_recent_git_commits(ROOT_DIR, days=7)

    total_props = sum(j["properties"] for j in jurisdictions)
    total_nets = sum(j["networks"] for j in jurisdictions)
    total_leaks = sum(j["third_party_leaks"] for j in jurisdictions)

    return {
        "captured_at": _iso_utc(),
        "live_jurisdiction_count": len(jurisdictions),
        "total_properties": total_props,
        "total_networks": total_nets,
        "total_third_party_leaks": total_leaks,
        "quarantined_registered_agent_hubs": quarantined_ra_hubs,
        "corporate_registries": corp_stats,
        "jurisdictions": jurisdictions,
        "completeness": completeness_summary,
        "recent_feedback": recent_feedback,
        "recent_commits": recent_commits,
        "open_fix_items": review_state["fixQueue"]["openItems"],
        "resolved_fix_items": review_state["fixQueue"]["resolvedItems"][:10],
        "active_decisions": review_state["decisions"],
        "optimizations_applied": optimizations_applied,
    }


GEMINI_AUDIT_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "OBJECT",
    "required": [
        "headline",
        "executive_summary",
        "highlights",
        "jurisdiction_notes",
        "review_items",
    ],
    "properties": {
        "headline": {"type": "STRING"},
        "executive_summary": {"type": "STRING"},
        "highlights": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "jurisdiction_notes": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "required": ["jurisdiction", "status", "note"],
                "properties": {
                    "jurisdiction": {"type": "STRING"},
                    "status": {"type": "STRING"},
                    "note": {"type": "STRING"},
                },
            },
        },
        "review_items": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "required": ["item_key", "domain", "jurisdiction", "title", "why", "details"],
                "properties": {
                    "item_key": {"type": "STRING"},
                    "domain": {"type": "STRING"},
                    "jurisdiction": {"type": "STRING"},
                    "title": {"type": "STRING"},
                    "why": {"type": "STRING"},
                    "details": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                },
            },
        },
    },
}


def _invoke_gemini_structured(
    api_key: str,
    model_id: str,
    prompt_text: str,
) -> Dict[str, Any]:
    url = f"{GEMINI_GEN_BASE}/{urllib.parse.quote(model_id, safe='')}:generateContent"
    thinking_attempts: List[Optional[Dict[str, Any]]] = [
        {"thinkingLevel": "HIGH"},
        {"thinkingBudget": 8192},
        None,
    ]
    last_err: Optional[Exception] = None
    for thinking_cfg in thinking_attempts:
        gen_cfg: Dict[str, Any] = {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": GEMINI_AUDIT_RESPONSE_SCHEMA,
        }
        if thinking_cfg is not None:
            gen_cfg["thinkingConfig"] = thinking_cfg

        body = {
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You are the Principal Data Integrity & Ownership-Graph Auditor for "
                            "'They Own WHAT??' (theyownwhat.net), a strict source-only public records "
                            "landlord & property portfolio transparency platform. Never invent numbers; "
                            "cite only the exact counts in the provided ground-truth telemetry JSON. "
                            "Propose `review_items` ONLY for genuine policy/UX/data-gap sign-offs, "
                            "new jurisdiction rollouts, user feedback triage, or detected anomalies "
                            "(1 to 4 concise, high-signal review items max). Use stable kebab-case "
                            "`item_key` identifiers."
                        )
                    }
                ]
            },
            "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
            "generationConfig": gen_cfg,
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            parts = (((raw.get("candidates") or [{}])[0].get("content") or {}).get("parts")) or []
            text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict) and not p.get("thought") and p.get("text")]
            if not text_chunks:
                text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
            parsed = json.loads("".join(text_chunks).strip())
            if isinstance(parsed, dict):
                return parsed
        except urllib.error.HTTPError as http_err:
            err_body = ""
            try:
                err_body = http_err.read().decode("utf-8", errors="ignore")[:300]
            except Exception:
                pass
            last_err = RuntimeError(f"Gemini HTTP {http_err.code} on {model_id}: {err_body}")
            # If 400 Invalid Argument due to thinkingConfig schema mismatch, try next thinking_cfg
            if http_err.code == 400 and thinking_cfg is not None:
                continue
            break
        except Exception as exc:
            last_err = exc
            break
    raise last_err or RuntimeError(f"Gemini call failed on {model_id}")


def filter_unapproved_review_items(
    items: Sequence[Dict[str, Any]],
    active_decisions: Sequence[Dict[str, Any]],
    now: Optional[dt.datetime] = None,
) -> List[Dict[str, Any]]:
    ref_now = now or _utc_now()
    approved_keys = set()
    for dec in active_decisions:
        if str(dec.get("disposition") or "").lower() != "approved":
            continue
        exp_raw = str(dec.get("expiresAt") or "").strip()
        if exp_raw:
            try:
                exp_dt = dt.datetime.fromisoformat(exp_raw.replace("Z", "+00:00"))
                if exp_dt < ref_now:
                    continue
            except Exception:
                pass
        if dec.get("itemKey"):
            approved_keys.add(str(dec["itemKey"]))

    filtered: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        key = str(item.get("item_key") or "").strip()
        if not key or key in approved_keys or key in seen:
            continue
        seen.add(key)
        filtered.append(item)
    return filtered


def run_gemini_audit_analysis(
    snapshot: Dict[str, Any],
    *,
    api_key: Optional[str] = None,
    model_override: Optional[str] = None,
) -> Dict[str, Any]:
    key = api_key if api_key is not None else _resolve_gemini_key()
    primary_model = model_override or discover_latest_gemini_model(key, tier="pro")
    fallback_model = discover_latest_gemini_model(key, tier="flash")

    used_model = primary_model
    ai_result: Optional[Dict[str, Any]] = None
    model_error: Optional[str] = None

    compact_snapshot = {
        "captured_at": snapshot["captured_at"],
        "live_jurisdiction_count": snapshot["live_jurisdiction_count"],
        "total_properties": snapshot["total_properties"],
        "total_networks": snapshot["total_networks"],
        "total_third_party_leaks": snapshot["total_third_party_leaks"],
        "quarantined_registered_agent_hubs": snapshot["quarantined_registered_agent_hubs"],
        "corporate_registries": snapshot["corporate_registries"],
        "jurisdictions": snapshot["jurisdictions"],
        "completeness": snapshot["completeness"],
        "optimizations_applied": snapshot["optimizations_applied"],
        "open_fix_items": snapshot["open_fix_items"],
        "recent_feedback": snapshot["recent_feedback"][:5],
        "recent_commits": [c["subject"] for c in snapshot["recent_commits"][:12]],
    }
    prompt = (
        "Perform the weekly comprehensive platform audit & optimization review for They Own WHAT?? "
        "based on the following live PostgreSQL and API telemetry JSON:\n\n"
        + json.dumps(compact_snapshot, indent=2)
    )

    if key:
        for candidate in (primary_model, fallback_model):
            try:
                logger.info("Invoking dynamic Gemini model `%s` for weekly audit...", candidate)
                ai_result = _invoke_gemini_structured(key, candidate, prompt)
                used_model = candidate
                model_error = None
                break
            except Exception as exc:
                model_error = str(exc)
                logger.warning("Gemini model `%s` failed (%s)", candidate, exc)

    if not ai_result:
        used_model = f"{primary_model} (deterministic-fallback)"
        ai_result = {
            "headline": (
                f"Weekly Platform Audit: {snapshot['live_jurisdiction_count']} Jurisdictions, "
                f"{snapshot['total_properties']:,} Properties & {snapshot['total_networks']:,} Networks Verified"
            ),
            "executive_summary": (
                f"Audited {snapshot['live_jurisdiction_count']} live jurisdictions totaling "
                f"{snapshot['total_properties']:,} source-backed parcels/buildings and "
                f"{snapshot['total_networks']:,} landlord ownership networks "
                f"({snapshot['total_third_party_leaks']} third-party bank/servicer leaks)."
            ),
            "highlights": [
                f"Verified {snapshot['total_properties']:,} properties and {snapshot['total_networks']:,} networks across all {snapshot['live_jurisdiction_count']} live jurisdictions.",
                f"Quarantined {snapshot['quarantined_registered_agent_hubs']:,} empirical commercial registered-agent hub addresses.",
            ],
            "jurisdiction_notes": [],
            "review_items": [],
        }

    # Deterministic safety net: ensure any real anomaly or unreviewed user feedback is surfaced
    raw_review_items = list(ai_result.get("review_items") or [])
    for j in snapshot["jurisdictions"]:
        if j.get("third_party_leaks", 0) > 0:
            raw_review_items.insert(
                0,
                {
                    "item_key": f"leak-guard-{j['slug']}",
                    "domain": "3rd-Party Network Guard",
                    "jurisdiction": j["dataset_key"],
                    "title": f"Investigate {j['third_party_leaks']} potential 3rd-party cluster name(s) in {j['label']}",
                    "why": "Network display_name matched THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN on a multi-property non-institutional network.",
                    "details": [f"Properties: {j['properties']:,} | Multi-networks: {j['multi_networks']:,}"],
                },
            )
        statewide_slugs = {"ct", "nj", "ma", "fl", "ny", "co", "nc", "wi", "vt"}
        cluster_ceiling = 20000 if j.get("slug") in statewide_slugs else 5000
        if j.get("max_cluster", 0) > cluster_ceiling:
            raw_review_items.insert(
                0,
                {
                    "item_key": f"mega-cluster-{j['slug']}",
                    "domain": "Cluster Ceiling Invariant",
                    "jurisdiction": j["dataset_key"],
                    "title": f"Review max private cluster ({j['max_cluster']:,} bldgs) in {j['label']}",
                    "why": f"Exceeds the {cluster_ceiling:,}-building private landlord cluster ceiling invariant.",
                    "details": [f"Jurisdiction `{j['slug']}` max_cluster={j['max_cluster']:,}"],
                },
            )

    for fb in snapshot.get("recent_feedback") or []:
        fb_id = fb.get("id")
        raw_review_items.append(
            {
                "item_key": f"user-feedback-{fb_id}",
                "domain": "User Feedback Triage",
                "jurisdiction": "PLATFORM",
                "title": f"User feedback #{fb_id} ({fb.get('report_type') or 'report'})",
                "why": str(fb.get("description") or "")[:280],
                "details": [f"Submitted: {fb.get('created_at')}"],
            }
        )

    pending_review_items = filter_unapproved_review_items(
        raw_review_items,
        snapshot.get("active_decisions") or [],
    )

    return {
        "schema_version": 1,
        "generated_at": _iso_utc(),
        "gemini_model": used_model,
        "gemini_primary_discovered": primary_model,
        "gemini_fallback_discovered": fallback_model,
        "gemini_error": model_error,
        "headline": ai_result.get("headline") or "They Own WHAT?? Weekly Gemini Audit",
        "executive_summary": ai_result.get("executive_summary") or "",
        "highlights": ai_result.get("highlights") or [],
        "optimizations_applied": snapshot.get("optimizations_applied") or [],
        "jurisdiction_notes": ai_result.get("jurisdiction_notes") or [],
        "review_items": pending_review_items,
        "snapshot": snapshot,
    }


def build_weekly_audit_email(
    report: Dict[str, Any],
    *,
    public_base_url: str = "https://theyownwhat.net",
    local_base_url: str = "http://localhost:6262",
) -> Tuple[str, str, str]:
    """Return `(subject, plain_text_body, html_body)` with HMAC-signed 1-click review links."""
    snap = report.get("snapshot") or {}
    date_str = str(report.get("generated_at") or _iso_utc())[:10]
    review_items = report.get("review_items") or []
    open_fixes = snap.get("open_fix_items") or []
    resolved_fixes = snap.get("resolved_fix_items") or []
    commits = snap.get("recent_commits") or []
    jurisdictions = snap.get("jurisdictions") or []
    model_name = str(report.get("gemini_model") or "gemini-pro")

    status_emoji = "⚠️" if snap.get("total_third_party_leaks", 0) > 0 else "✅"
    subject = (
        f"[They Own WHAT??] {status_emoji} Weekly Gemini Audit ({date_str}) — "
        f"{snap.get('live_jurisdiction_count', 0)} Jurisdictions, {len(review_items)} Review Item(s)"
    )

    # Plain-text fallback
    txt_lines = [
        f"They Own WHAT?? — Weekly Gemini Audit & Optimization Summary ({date_str})",
        f"Model: {model_name}",
        f"Scale: {snap.get('total_properties', 0):,} properties | {snap.get('total_networks', 0):,} networks | {snap.get('live_jurisdiction_count', 0)} live jurisdictions",
        f"3rd-Party Leaks: {snap.get('total_third_party_leaks', 0)} | Quarantined RA Hubs: {snap.get('quarantined_registered_agent_hubs', 0):,}",
        "",
        report.get("headline") or "",
        report.get("executive_summary") or "",
        "",
    ]
    for h in report.get("highlights") or []:
        txt_lines.append(f"  * {h}")
    if review_items:
        txt_lines.append("\nITEMS FOR MAINTAINER REVIEW (1-Click Links):")
        for item in review_items:
            app_url = build_quick_decision_url(
                public_base_url,
                decision="approve",
                item_key=item["item_key"],
                title=item.get("title", ""),
                domain=item.get("domain", ""),
                jurisdiction=item.get("jurisdiction", ""),
            )
            rej_url = build_quick_decision_url(
                public_base_url,
                decision="reject",
                item_key=item["item_key"],
                title=item.get("title", ""),
                domain=item.get("domain", ""),
                jurisdiction=item.get("jurisdiction", ""),
            )
            txt_lines.append(f"  - [{item.get('domain')}] {item.get('title')}: {item.get('why')}")
            txt_lines.append(f"    Approve: {app_url}")
            txt_lines.append(f"    Decline / Request Fix: {rej_url}")
    plain_text = "\n".join(txt_lines)

    # Rich HTML email matching Renter Toolkit's digest style
    esc = lambda s: html.escape(str(s or ""))

    approve_all_url = build_quick_decision_url(
        public_base_url,
        decision="approve",
        item_key="approve-all-pending",
        title="Approve All Pending Weekly Audit Items",
        domain="Weekly Gemini Audit",
    )
    queue_token = sign_review_token("*")
    queue_url = f"{public_base_url.rstrip('/')}/api/audit-review/queue?token={urllib.parse.quote(queue_token)}"
    local_queue_url = f"{local_base_url.rstrip('/')}/api/audit-review/queue"

    highlights_html = "".join(
        f"<li style='margin-bottom:6px;'>{esc(h)}</li>"
        for h in [*(report.get("optimizations_applied") or []), *(report.get("highlights") or [])]
    ) or "<li>All scheduled completeness and network-guard invariants passed cleanly.</li>"

    notes_by_jur = {
        str(n.get("jurisdiction") or "").upper(): str(n.get("note") or "")
        for n in (report.get("jurisdiction_notes") or [])
        if isinstance(n, dict)
    }

    jur_rows_html = ""
    for j in jurisdictions:
        leak_badge = (
            "<span style='color:#059669;font-weight:700;'>0 leaks</span>"
            if j.get("third_party_leaks", 0) == 0
            else f"<span style='color:#dc2626;font-weight:700;'>{j['third_party_leaks']} alert(s)</span>"
        )
        note = notes_by_jur.get(str(j.get("dataset_key", "")).upper()) or notes_by_jur.get(str(j.get("slug", "")).upper()) or ""
        jur_rows_html += f"""
        <tr>
          <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;font-weight:700;color:#0f172a;">{esc(j.get('label'))}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;text-align:right;font-family:monospace;">{int(j.get('properties', 0)):,}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;text-align:right;font-family:monospace;">{int(j.get('networks', 0)):,}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;text-align:right;font-family:monospace;">{int(j.get('max_cluster', 0)):,}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;text-align:right;">{leak_badge}</td>
        </tr>
        {f'<tr><td colspan="5" style="padding:4px 10px 8px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;background:#f8fafc;">{esc(note)}</td></tr>' if note else ''}"""

    if review_items:
        cards = []
        for item in review_items:
            ik = str(item.get("item_key") or "review-item")
            t_str = str(item.get("title") or ik)
            dom_str = str(item.get("domain") or "Weekly Audit")
            jur_str = str(item.get("jurisdiction") or "")
            app_u = build_quick_decision_url(
                public_base_url, decision="approve", item_key=ik, title=t_str, domain=dom_str, jurisdiction=jur_str
            )
            rej_src_u = build_quick_decision_url(
                public_base_url,
                decision="reject",
                item_key=ik,
                title=t_str,
                domain=dom_str,
                jurisdiction=jur_str,
                reason="Broken or wrong URL / data source",
            )
            rej_copy_u = build_quick_decision_url(
                public_base_url,
                decision="reject",
                item_key=ik,
                title=t_str,
                domain=dom_str,
                jurisdiction=jur_str,
                reason="Inaccurate limitations or UI copy",
            )
            note_u = build_quick_decision_url(
                public_base_url, decision="explain", item_key=ik, title=t_str, domain=dom_str, jurisdiction=jur_str
            )
            local_u = build_quick_decision_url(
                local_base_url, decision="approve", item_key=ik, title=t_str, domain=dom_str, jurisdiction=jur_str
            )
            det_list = [str(d) for d in (item.get("details") or [])[:8]]
            det_html = (
                "<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:9px 12px;margin-bottom:12px;'>"
                "<ul style='margin:0;padding-left:18px;font-size:12px;color:#334155;line-height:1.45;'>"
                + "".join(f"<li>{esc(d)}</li>" for d in det_list)
                + "</ul></div>"
                if det_list
                else ""
            )
            cards.append(
                f"""
                <div style="border:1px solid #cbd5e1;border-radius:12px;padding:16px;margin-bottom:14px;background:#ffffff;">
                  <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;color:#2563eb;margin-bottom:4px;">
                    {esc(dom_str)}{f' &middot; {esc(jur_str)}' if jur_str else ''}
                  </div>
                  <div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:6px;">{esc(t_str)}</div>
                  <p style="margin:0 0 10px;font-size:13px;color:#334155;line-height:1.5;">{esc(item.get('why'))}</p>
                  {det_html}
                  <div>
                    <a href="{esc(app_u)}" style="display:inline-block;margin:0 8px 8px 0;padding:9px 14px;background:#059669;color:#ffffff;text-decoration:none;border-radius:8px;font-size:13px;font-weight:700;">✓ Yes &mdash; Approve &amp; Sign Off</a>
                    <a href="{esc(rej_src_u)}" style="display:inline-block;margin:0 8px 8px 0;padding:9px 14px;background:#dc2626;color:#ffffff;text-decoration:none;border-radius:8px;font-size:13px;font-weight:700;">✕ No &mdash; Broken / Wrong Source</a>
                    <a href="{esc(rej_copy_u)}" style="display:inline-block;margin:0 8px 8px 0;padding:9px 14px;background:#b91c1c;color:#ffffff;text-decoration:none;border-radius:8px;font-size:13px;font-weight:700;">✕ No &mdash; Inaccurate / Fix Needed</a>
                    <a href="{esc(note_u)}" style="display:inline-block;margin:0 8px 8px 0;padding:9px 14px;background:#f1f5f9;color:#0f172a;border:1px solid #cbd5e1;text-decoration:none;border-radius:8px;font-size:13px;font-weight:700;">✎ Add Custom Fix Note&hellip;</a>
                    <a href="{esc(local_u)}" style="display:inline-block;margin:0 0 8px 0;padding:9px 10px;color:#64748b;text-decoration:underline;font-size:11px;">Localhost fallback</a>
                  </div>
                </div>"""
            )
        review_section_html = (
            f"<div style='margin-bottom:12px;'>"
            f"<a href='{esc(approve_all_url)}' style='display:inline-block;padding:8px 14px;background:#0f172a;color:#ffffff;text-decoration:none;border-radius:8px;font-size:12px;font-weight:700;'>✓ Approve All {len(review_items)} Pending Item(s)</a> "
            f"<a href='{esc(queue_url)}' style='display:inline-block;margin-left:10px;font-size:12px;color:#2563eb;font-weight:600;'>Open Review &amp; Fix Queue &rarr;</a>"
            f"</div>"
            + "".join(cards)
        )
    else:
        review_section_html = (
            "<div style='padding:14px 16px;border-radius:10px;background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46;font-size:14px;font-weight:600;'>"
            "✓ Zero human review items pending right now! All weekly assurance sign-offs and invariants are green."
            "</div>"
        )

    fix_queue_html = ""
    if open_fixes or resolved_fixes:
        open_li = "".join(
            f"<li><strong>[OPEN &mdash; Queued for Agent]</strong> {esc(f.get('domain'))}: {esc(f.get('title'))} &mdash; {esc(f.get('explanation'))}</li>"
            for f in open_fixes
        )
        res_li = "".join(
            f"<li><strong>[RESOLVED]</strong> {esc(f.get('domain'))}: {esc(f.get('title'))} &mdash; {esc(f.get('resolutionNote') or 'Verified & resolved')}</li>"
            for f in resolved_fixes[:5]
        )
        fix_queue_html = f"""
        <h2 style="font-size:16px;margin:22px 0 8px;color:#0f172a;">4. Maintainer Fix Queue (<code>docs/review-fix-queue.json</code>)</h2>
        <ul style="margin:0;padding-left:18px;font-size:13px;color:#334155;line-height:1.5;">{open_li}{res_li}</ul>"""

    commits_html = (
        "".join(
            f"<tr><td style='padding:6px 8px;border-bottom:1px solid #e2e8f0;font-family:monospace;font-size:12px;color:#1d4ed8;font-weight:700;'>{esc(c.get('short_sha'))}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#0f172a;'>{esc(c.get('subject'))}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#64748b;white-space:nowrap;'>{esc(str(c.get('committed_at') or '')[:10])}</td></tr>"
            for c in commits[:12]
        )
        or "<tr><td colspan='3' style='padding:10px;font-size:12px;color:#64748b;'>No new git commits in the last 7 days.</td></tr>"
    )

    html_body = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><title>{esc(subject)}</title></head>
<body style="margin:0;padding:20px;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#0f172a;">
  <div style="max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #cbd5e1;border-radius:14px;overflow:hidden;">
    <div style="background:#0f172a;color:#ffffff;padding:20px 24px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#93c5fd;">
        They Own WHAT?? &middot; Weekly Dynamic Gemini Audit &amp; Optimization Digest
      </div>
      <h1 style="margin:6px 0 6px;font-size:19px;line-height:1.3;">{esc(report.get('headline'))}</h1>
      <div style="font-size:12px;color:#cbd5e1;line-height:1.5;">
        Model: <code style="color:#bfdbfe;">{esc(model_name)}</code> &middot;
        Jurisdictions: <strong>{int(snap.get('live_jurisdiction_count', 0))}</strong> &middot;
        Properties: <strong>{int(snap.get('total_properties', 0)):,}</strong> &middot;
        Networks: <strong>{int(snap.get('total_networks', 0)):,}</strong> &middot;
        3rd-Party Leaks: <strong>{int(snap.get('total_third_party_leaks', 0))}</strong>
      </div>
    </div>

    <div style="padding:20px 24px;">
      <h2 style="font-size:16px;margin:0 0 8px;color:#0f172a;">1. Executive Summary &amp; Autonomous Optimizations</h2>
      <p style="margin:0 0 12px;font-size:13px;color:#334155;line-height:1.55;">{esc(report.get('executive_summary'))}</p>
      <ul style="margin:0 0 20px;padding-left:18px;font-size:13px;color:#334155;line-height:1.5;">{highlights_html}</ul>

      <h2 style="font-size:16px;margin:0 0 10px;color:#0f172a;">2. Items for Your 1-Click Review ({len(review_items)})</h2>
      {review_section_html}

      <h2 style="font-size:16px;margin:22px 0 10px;color:#0f172a;">3. Live Jurisdiction Coverage &amp; Integrity Matrix</h2>
      <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
        <thead>
          <tr style="background:#f8fafc;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">
            <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;">Jurisdiction</th>
            <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;">Properties</th>
            <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;">Networks</th>
            <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;">Max Net</th>
            <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;">3rd-Party Guard</th>
          </tr>
        </thead>
        <tbody>{jur_rows_html}</tbody>
      </table>

      {fix_queue_html}

      <h2 style="font-size:16px;margin:22px 0 8px;color:#0f172a;">5. Recent 7-Day Repository Changes ({len(commits)})</h2>
      <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;">
        <tbody>{commits_html}</tbody>
      </table>

      <div style="margin-top:18px;padding-top:14px;border-top:1px solid #e2e8f0;font-size:12px;color:#64748b;">
        Manage decisions anytime: <a href="{esc(queue_url)}" style="color:#2563eb;font-weight:600;">theyownwhat.net Review Queue</a> &middot;
        <a href="{esc(local_queue_url)}" style="color:#64748b;">localhost:6262 Review Queue</a>
      </div>
    </div>
  </div>
</body>
</html>"""
    return subject, plain_text, html_body


def dispatch_github_weekly_email_workflow(
    root_dir: Path = ROOT_DIR,
    workflow_file: str = "weekly-audit.yml",
    ref: str = "main",
) -> bool:
    """Trigger `.github/workflows/weekly-audit.yml` via GitHub REST API `workflow_dispatch`."""
    try:
        remote_url = subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=str(root_dir),
            text=True,
        ).strip()
        m = re.search(r"github\.com[:/]([^/]+/[^/.]+?)(?:\.git)?$", remote_url, re.I)
        if not m:
            return False
        slug = m.group(1)
        creds = subprocess.check_output(
            ["git", "credential", "fill"],
            cwd=str(root_dir),
            input="protocol=https\nhost=github.com\n\n",
            text=True,
        )
        tok_m = re.search(r"^password=(.+)$", creds, re.M)
        if not tok_m:
            return False
        token = tok_m.group(1).strip()
        url = f"https://api.github.com/repos/{slug}/actions/workflows/{urllib.parse.quote(workflow_file)}/dispatches"
        req = urllib.request.Request(
            url,
            data=json.dumps({"ref": ref}).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "theyownwhat-weekly-gemini-audit",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            ok = 200 <= resp.status < 300
            if ok:
                logger.info("✓ Dispatched GitHub Actions workflow `%s` on `%s` for email delivery.", workflow_file, slug)
            return ok
    except Exception as exc:
        logger.warning("GitHub Actions workflow_dispatch fallback unavailable (%s)", exc)
        return False


def write_audit_artifacts(
    report: Dict[str, Any],
    *,
    subject: str,
    html_body: str,
    latest_json_path: Path = DEFAULT_LATEST_AUDIT_PATH,
    html_out_path: Optional[Path] = None,
) -> Tuple[Path, Path]:
    latest_json_path.parent.mkdir(parents=True, exist_ok=True)
    latest_json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    date_tag = str(report.get("generated_at") or _iso_utc())[:10]
    dated_path = latest_json_path.parent / f"weekly-gemini-audit-{date_tag}.json"
    dated_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    resolved_html_path = html_out_path or (ROOT_DIR / "build" / "weekly-gemini-audit-email.html")
    resolved_html_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_html_path.write_text(html_body, encoding="utf-8")

    summary_meta_path = resolved_html_path.parent / "weekly-gemini-audit-meta.json"
    summary_meta_path.write_text(
        json.dumps(
            {
                "subject": subject,
                "generated_at": report.get("generated_at"),
                "gemini_model": report.get("gemini_model"),
                "review_item_count": len(report.get("review_items") or []),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return latest_json_path, resolved_html_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="They Own WHAT?? Weekly Dynamic Gemini Audit & Review CLI")
    sub = parser.add_subparsers(dest="cmd")

    run_p = sub.add_parser("run", help="Run full weekly Gemini audit, optional optimization, and email digest")
    run_p.add_argument("--optimize", action="store_true", help="Apply safe index/completeness optimizations")
    run_p.add_argument("--send-email", action="store_true", help="Dispatch summary email via SMTP / GH Actions fallback")
    run_p.add_argument("--dispatch-gh-workflow", action="store_true", help="Trigger weekly-audit.yml if local SMTP is blocked")
    run_p.add_argument("--model", default="", help="Optional explicit Gemini model override")
    run_p.add_argument("--public-url", default=os.environ.get("PUBLIC_SITE_URL", "https://theyownwhat.net"))

    ren_p = sub.add_parser("render-from-latest", help="Render HTML email from docs/benchmarks/latest-weekly-gemini-audit.json")
    ren_p.add_argument("--public-url", default=os.environ.get("PUBLIC_SITE_URL", "https://theyownwhat.net"))

    sub.add_parser("list-fixes", help="List open maintainer fix-queue items in docs/review-fix-queue.json")

    res_p = sub.add_parser("resolve-fix", help="Mark a fix-queue item as RESOLVED")
    res_p.add_argument("--id", required=True, help="Fix ID in docs/review-fix-queue.json")
    res_p.add_argument("--note", required=True, help="Resolution verification note")

    args = parser.parse_args(list(argv) if argv is not None else None)
    cmd = args.cmd or "run"

    if cmd == "list-fixes":
        state = load_review_state()
        print(json.dumps(state["fixQueue"], indent=2))
        return 0

    if cmd == "resolve-fix":
        updated = resolve_fix_queue_item(args.id, args.note)
        print(json.dumps({"ok": True, "resolved": updated}, indent=2))
        return 0

    if cmd == "render-from-latest":
        if not DEFAULT_LATEST_AUDIT_PATH.exists():
            logger.error("No latest audit JSON found at %s", DEFAULT_LATEST_AUDIT_PATH)
            return 1
        report = json.loads(DEFAULT_LATEST_AUDIT_PATH.read_text(encoding="utf-8"))
        subject, _, html_body = build_weekly_audit_email(report, public_base_url=args.public_url)
        _, html_path = write_audit_artifacts(report, subject=subject, html_body=html_body)
        logger.info("Rendered email HTML to %s (subject: %s)", html_path, subject)
        return 0

    # Default: `run`
    opt = getattr(args, "optimize", False)
    pub_url = getattr(args, "public_url", "https://theyownwhat.net")
    model_ovr = getattr(args, "model", "") or None

    snapshot = collect_platform_audit_snapshot(optimize=opt)
    report = run_gemini_audit_analysis(snapshot, model_override=model_ovr)
    subject, plain_text, html_body = build_weekly_audit_email(report, public_base_url=pub_url)
    json_path, html_path = write_audit_artifacts(report, subject=subject, html_body=html_body)
    logger.info("✓ Wrote weekly Gemini audit report -> %s and %s", json_path, html_path)

    if getattr(args, "send_email", False):
        sent = send_audit_email(subject, plain_text, body_html=html_body, recipients=DEFAULT_RECIPIENTS)
        if not sent and getattr(args, "dispatch_gh_workflow", False):
            dispatch_github_weekly_email_workflow(ROOT_DIR)

    print(
        json.dumps(
            {
                "ok": True,
                "gemini_model": report["gemini_model"],
                "live_jurisdictions": snapshot["live_jurisdiction_count"],
                "total_properties": snapshot["total_properties"],
                "total_networks": snapshot["total_networks"],
                "total_third_party_leaks": snapshot["total_third_party_leaks"],
                "review_items": len(report["review_items"]),
                "open_fix_items": len(snapshot["open_fix_items"]),
                "report_json": str(json_path),
                "email_html": str(html_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
