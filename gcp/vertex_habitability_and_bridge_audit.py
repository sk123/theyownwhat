#!/usr/bin/env python3
"""
gcp/vertex_habitability_and_bridge_audit.py
===========================================
One-time Vertex AI (`renter-help`, `--configuration=storrsmagic`) batch enrichment
and adversarial quality-audit runner for `theyownwhat` using expiring GCP credits.

Assurance Policy Guardrails (`docs/ASSURANCE_POLICY.md`):
  - Processes ONLY public government/assessor/code-enforcement/corporate records.
    NEVER sends private user documents, notes, or personal data.
  - All model outputs are persisted as clearly labeled taxonomy mappings or
    advisory triage queues (`review_status = 'ADVISORY_REVIEW_REQUIRED'`),
    preserving original verbatim public text alongside every classification.
  - Hard request cap (`--max-requests`) prevents unbounded spend.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
PROJECT = "renter-help"
CONFIG = "storrsmagic"

HABITABILITY_CATEGORIES = [
    "heat_hot_water",
    "mold_moisture_leaks",
    "pests_rodents_vermin",
    "lead_paint_hazards",
    "fire_egress_safety",
    "structural_roof_exterior",
    "plumbing_sewage",
    "electrical_gas",
    "unlicensed_illegal_occupancy",
    "trash_sanitation",
    "administrative_or_other",
]

NATIONWIDE_NETWORK_TABLES = [
    ("ny", "ny_networks"),
    ("nyc", "nyc_networks"),
    ("nj", "nj_networks"),
    ("ma", "ma_networks"),
    ("boston", "boston_networks"),
    ("fl", "fl_networks"),
    ("miami", "miami_networks"),
    ("il", "il_networks"),
    ("chicago", "chicago_networks"),
    ("ca", "ca_networks"),
    ("pa_philly", "philadelphia_networks"),
    ("dc", "dc_networks"),
    ("baltimore", "baltimore_networks"),
    ("detroit", "detroit_networks"),
    ("minneapolis", "minneapolis_networks"),
    ("mn", "mn_networks"),
    ("oh", "oh_networks"),
    ("co", "co_networks"),
    ("nc", "nc_networks"),
    ("va", "va_networks"),
    ("wi", "wi_networks"),
    ("vt", "vt_networks"),
    ("ar", "ar_networks"),
    ("mt", "mt_networks"),
]


class _TokenCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._token = ""
        self._fetched_at = 0.0

    def get(self) -> str:
        with self._lock:
            now = time.time()
            if not self._token or (now - self._fetched_at) > 2400:
                cmd = [
                    "docker", "run", "--rm",
                    "-e", "CLOUDSDK_METRICS_REQUEST_ATTRIBUTION=agent-skills/1.0 (skill:gcloud)",
                    "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
                    GCLOUD_IMAGE,
                    "gcloud", "auth", "print-access-token", f"--configuration={CONFIG}", "--quiet",
                ]
                self._token = subprocess.check_output(cmd, text=True).strip()
                self._fetched_at = now
            return self._token


TOKEN_CACHE = _TokenCache()


def call_vertex_json(
    model: str,
    system_instruction: str,
    user_payload: str,
    max_output_tokens: int = 8192,
) -> Tuple[Any, Dict[str, Any]]:
    url = f"https://aiplatform.googleapis.com/v1/projects/{PROJECT}/locations/global/publishers/google/models/{model}:generateContent"
    body = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": user_payload}]}],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": max_output_tokens,
            "responseMimeType": "application/json",
        },
    }
    raw_body = json.dumps(body).encode("utf-8")
    last_err = None
    for attempt in range(4):
        try:
            tok = TOKEN_CACHE.get()
            req = urllib.request.Request(
                url,
                data=raw_body,
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            parts = (((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
            txt = "".join(p.get("text", "") for p in parts).strip()
            usage = data.get("usageMetadata") or {}
            return json.loads(txt), usage
        except Exception as exc:
            last_err = exc
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"Vertex call failed after 4 attempts: {last_err}")


def _ensure_habitability_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS habitability_violation_taxonomy (
                source_system     TEXT NOT NULL,
                raw_text_key      TEXT NOT NULL,
                raw_text_sample   TEXT NOT NULL,
                primary_category  TEXT NOT NULL,
                secondary_tags    TEXT[] DEFAULT ARRAY[]::TEXT[],
                severity_tier     TEXT NOT NULL,
                advocate_summary  TEXT,
                model_id          TEXT NOT NULL,
                review_status     TEXT NOT NULL DEFAULT 'ADVISORY_AUTO_TAGGED',
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (source_system, raw_text_key)
            );
            CREATE INDEX IF NOT EXISTS idx_hab_tax_cat ON habitability_violation_taxonomy (primary_category);
            """
        )
    conn.commit()


def run_habitability_tagging(model: str, workers: int, max_requests: int) -> Dict[str, Any]:
    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    _ensure_habitability_table(conn)

    items: List[Tuple[str, str, str]] = []  # (source_system, key, sample_text)
    with conn.cursor() as cur:
        # 1. Local Hartford / CT code_enforcement distinct descriptions
        cur.execute(
            """
            SELECT DISTINCT
                TRIM(COALESCE(record_type, '') || ' | ' || COALESCE(record_name, '') || ' | ' || COALESCE(inspection_type, '')) AS txt
            FROM code_enforcement
            WHERE TRIM(COALESCE(record_type, '') || COALESCE(record_name, '') || COALESCE(inspection_type, '')) != ''
            """
        )
        for (txt,) in cur.fetchall():
            k = " ".join(txt.upper().split())[:240]
            if k:
                items.append(("ct_code_enforcement", k, txt[:400]))

        # 2. Local Chicago DOB building violations distinct descriptions
        cur.execute(
            """
            SELECT DISTINCT COALESCE(violation_code, '') || ' | ' || COALESCE(violation_description, '') AS txt
            FROM chicago_building_violations
            WHERE COALESCE(violation_description, '') != ''
            """
        )
        for (txt,) in cur.fetchall():
            k = " ".join(txt.upper().split())[:240]
            if k:
                items.append(("chicago_dob", k, txt[:400]))

    # 3. Also pull top official public violation code descriptions from NYC HPD (wvxf-dwi5) & Boston (8sq6-p7et)
    #    via fast O(1) Socrata GROUP BY so advocate habitability tags cover NYC & Boston too!
    public_catalog_queries = [
        (
            "nyc_hpd",
            "https://data.cityofnewyork.us/resource/wvxf-dwi5.json?$select=novdescription,class,count(violationid)%20as%20cnt&$where=novdescription%20IS%20NOT%20NULL&$group=novdescription,class&$order=cnt%20DESC&$limit=1500",
            lambda r: f"Class {r.get('class','')} | {r.get('novdescription','')}",
        ),
        (
            "chicago_dob_full",
            "https://data.cityofchicago.org/resource/22u3-xenr.json?$select=violation_code,violation_description,count(id)%20as%20cnt&$where=violation_description%20IS%20NOT%20NULL&$group=violation_code,violation_description&$order=cnt%20DESC&$limit=1000",
            lambda r: f"{r.get('violation_code','')} | {r.get('violation_description','')}",
        ),
    ]
    for src_sys, qurl, fmt_fn in public_catalog_queries:
        try:
            req = urllib.request.Request(qurl, headers={"User-Agent": "TheyOwnWhatPublicRecords/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                rows = json.loads(resp.read().decode("utf-8"))
            added = 0
            for r in rows:
                txt = fmt_fn(r).strip()
                k = " ".join(txt.upper().split())[:240]
                if k and len(k) >= 6:
                    items.append((src_sys, k, txt[:400]))
                    added += 1
            print(f"  ✓ Fetched {added:,} distinct public violation wordings from {src_sys}", flush=True)
        except Exception as exc:
            print(f"  ⚠ Skipping optional remote catalog {src_sys}: {exc}", flush=True)

    # Deduplicate against already-tagged entries in Postgres
    with conn.cursor() as cur:
        cur.execute("SELECT source_system, raw_text_key FROM habitability_violation_taxonomy")
        already = set(cur.fetchall())

    dedup: Dict[Tuple[str, str], str] = {}
    for src_sys, k, sample in items:
        if (src_sys, k) not in already and (src_sys, k) not in dedup:
            dedup[(src_sys, k)] = sample
    todo = [(s, k, v) for (s, k), v in dedup.items()]
    print(f"▶ [Habitability Tagging] {len(todo):,} new unique public violation descriptions to tag (already cached: {len(already):,})", flush=True)

    sys_prompt = (
        "You are a housing-habitability code classifier for legal-aid attorneys. "
        "Classify each official public municipal code-violation wording strictly based on the exact text provided. "
        "Never invent facts. Return a JSON array of objects, one per input item, with keys: "
        "idx (int), primary_category (one of: " + ", ".join(HABITABILITY_CATEGORIES) + "), "
        "secondary_tags (array of 0-2 strings from the same category list), "
        "severity_tier (one of: emergency_life_safety, major_habitability, minor_or_admin), "
        "advocate_summary (short <=12-word plain-English label faithful to the source wording)."
    )

    batch_size = 40
    batches = [todo[i : i + batch_size] for i in range(0, len(todo), batch_size)][:max_requests]
    print(f"  - Dispatching {len(batches)} Vertex AI batches ({model}, workers={workers})...", flush=True)

    inserted = 0
    total_prompt_tok = 0
    total_cand_tok = 0
    all_rows_to_insert: List[Tuple[Any, ...]] = []

    def _run_batch(b_idx: int, batch: List[Tuple[str, str, str]]) -> Tuple[int, List[Tuple[Any, ...]], Dict[str, Any]]:
        payload = json.dumps([{"idx": i, "text": txt} for i, (_, _, txt) in enumerate(batch)])
        parsed, usage = call_vertex_json(model, sys_prompt, payload, max_output_tokens=8192)
        rows_out = []
        if isinstance(parsed, list):
            by_idx = {int(x.get("idx", -1)): x for x in parsed if isinstance(x, dict)}
            for i, (src_sys, key, sample) in enumerate(batch):
                res = by_idx.get(i)
                if not res:
                    continue
                cat = res.get("primary_category", "administrative_or_other")
                if cat not in HABITABILITY_CATEGORIES:
                    cat = "administrative_or_other"
                sec = [t for t in (res.get("secondary_tags") or []) if t in HABITABILITY_CATEGORIES and t != cat][:2]
                sev = res.get("severity_tier", "minor_or_admin")
                if sev not in ("emergency_life_safety", "major_habitability", "minor_or_admin"):
                    sev = "minor_or_admin"
                summ = str(res.get("advocate_summary") or "")[:160]
                rows_out.append((src_sys, key, sample, cat, sec, sev, summ, model))
        return b_idx, rows_out, usage

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_run_batch, i, b) for i, b in enumerate(batches)]
        done = 0
        for fut in as_completed(futs):
            b_idx, rows_out, usage = fut.result()
            all_rows_to_insert.extend(rows_out)
            total_prompt_tok += int(usage.get("promptTokenCount") or 0)
            total_cand_tok += int(usage.get("candidatesTokenCount") or 0)
            done += 1
            if done % 15 == 0 or done == len(batches):
                print(f"    [{done}/{len(batches)}] batches tagged ({len(all_rows_to_insert):,} rows)", flush=True)

    if all_rows_to_insert:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO habitability_violation_taxonomy (
                    source_system, raw_text_key, raw_text_sample, primary_category,
                    secondary_tags, severity_tier, advocate_summary, model_id
                ) VALUES %s
                ON CONFLICT (source_system, raw_text_key) DO UPDATE SET
                    primary_category = EXCLUDED.primary_category,
                    secondary_tags = EXCLUDED.secondary_tags,
                    severity_tier = EXCLUDED.severity_tier,
                    advocate_summary = EXCLUDED.advocate_summary,
                    model_id = EXCLUDED.model_id
                """,
                all_rows_to_insert,
            )
            inserted = len(all_rows_to_insert)
        conn.commit()

    # Export permanent JSON snapshot too
    out_path = os.path.join(REPO_ROOT, "docs", "benchmarks", "habitability_violation_taxonomy_summary.json")
    with conn.cursor() as cur:
        cur.execute("SELECT primary_category, COUNT(*) FROM habitability_violation_taxonomy GROUP BY 1 ORDER BY 2 DESC")
        by_cat = {r[0]: int(r[1]) for r in cur.fetchall()}
    summary = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "model": model,
        "newly_tagged": inserted,
        "total_in_db": sum(by_cat.values()),
        "by_category": by_cat,
        "prompt_tokens": total_prompt_tok,
        "output_tokens": total_cand_tok,
        "elapsed_sec": round(time.time() - t0, 1),
    }
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    conn.close()
    print(f"✅ Habitability taxonomy saved ({summary['total_in_db']:,} total rows, {summary['elapsed_sec']}s): {by_cat}", flush=True)
    return summary


def run_false_bridge_audit(model: str, top_per_jur: int, workers: int, max_requests: int) -> Dict[str, Any]:
    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    candidates: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        # 1. CT Top networks
        cur.execute(
            """
            SELECT n.id, n.primary_name, n.total_properties, n.business_count, n.principal_count,
                   ARRAY(SELECT entity_name FROM entity_networks en WHERE en.network_id = n.id AND en.entity_type = 'business' LIMIT 15) AS bizs,
                   ARRAY(SELECT entity_name FROM entity_networks en WHERE en.network_id = n.id AND en.entity_type = 'principal' LIMIT 12) AS ppls
            FROM networks n
            WHERE n.business_count >= 3 OR n.total_properties >= 15
            ORDER BY n.total_properties DESC
            LIMIT %s
            """,
            (top_per_jur * 2,),
        )
        for nid, pname, props, bcnt, pcnt, bizs, ppls in cur.fetchall():
            candidates.append(
                {
                    "jur": "ct",
                    "network_id": str(nid),
                    "display_name": pname,
                    "buildings": int(props or 0),
                    "entity_count": int(bcnt or 0),
                    "principal_count": int(pcnt or 0),
                    "sample_entities": (bizs or [])[:15],
                    "sample_people": (ppls or [])[:12],
                }
            )

        # 2. All 24 nationwide tables
        for jur_lbl, tbl in NATIONWIDE_NETWORK_TABLES:
            try:
                cur.execute(
                    f"""
                    SELECT network_key, display_name, building_count,
                           COALESCE(array_length(member_names, 1), 0) AS mcnt,
                           member_names[1:15], member_addresses[1:5]
                    FROM {tbl}
                    WHERE COALESCE(array_length(member_names, 1), 0) >= 3 OR building_count >= 15
                    ORDER BY building_count DESC
                    LIMIT %s
                    """,
                    (top_per_jur,),
                )
                for nkey, disp, bldgs, mcnt, mems, addrs in cur.fetchall():
                    candidates.append(
                        {
                            "jur": jur_lbl,
                            "network_id": str(nkey),
                            "display_name": disp,
                            "buildings": int(bldgs or 0),
                            "entity_count": int(mcnt or 0),
                            "sample_entities": (mems or [])[:15],
                            "sample_addresses": (addrs or [])[:5],
                        }
                    )
            except Exception as exc:
                conn.rollback()
                print(f"  ⚠ Skipping {tbl}: {exc}", flush=True)

    conn.close()
    print(f"▶ [False-Bridge Audit] Collected {len(candidates):,} top multi-entity networks across 25 jurisdictions.", flush=True)

    sys_prompt = (
        "You are an adversarial data-quality auditor reviewing public landlord ownership clusters for housing legal-aid attorneys. "
        "For each cluster, inspect ONLY the provided public entity names, addresses, and people. "
        "Flag ONLY clusters with concrete evidence of a false merge or non-landlord leakage: "
        "1) mixed_unrelated_individuals (many unrelated human owners/surnames merged via a CMRA/agent/homonym), "
        "2) undetected_cmra_or_agent_hub (virtual mailbox, registered agent, law firm, CPA, or tax servicer address), "
        "3) non_landlord_institution (bank/servicer/REO, government, utility, church, school, condo/HOA, resident co-op, commercial retail chain), "
        "4) charity_board_crossover (nonprofit/charity merged with unrelated for-profit private landlords), "
        "5) clean (looks like a legitimate cohesive landlord/operator/REIT portfolio or legitimate standalone nonprofit housing operator). "
        "Return a JSON array containing ONLY flagged items (omit 'clean' items to save tokens!) with keys: "
        "idx (int), issue_type (one of the 4 issue types), severity (high|medium), reason (concise <=25-word explanation citing exact entity/address evidence), suggested_general_rule (concise <=20-word system-wide fix)."
    )

    batch_size = 15
    batches = [candidates[i : i + batch_size] for i in range(0, len(candidates), batch_size)][:max_requests]
    print(f"  - Auditing {len(batches)} batches ({len(batches) * batch_size:,} networks max) with {model} (workers={workers})...", flush=True)

    flagged: List[Dict[str, Any]] = []
    total_prompt_tok = 0
    total_cand_tok = 0

    def _audit_batch(b_idx: int, batch: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        payload = json.dumps([{"idx": i, **item} for i, item in enumerate(batch)])
        parsed, usage = call_vertex_json(model, sys_prompt, payload, max_output_tokens=8192)
        out = []
        if isinstance(parsed, list):
            for hit in parsed:
                if not isinstance(hit, dict):
                    continue
                itype = hit.get("issue_type", "")
                if itype == "clean" or not itype:
                    continue
                idx = hit.get("idx")
                if isinstance(idx, int) and 0 <= idx < len(batch):
                    src = batch[idx]
                    out.append(
                        {
                            "jur": src["jur"],
                            "network_id": src["network_id"],
                            "display_name": src["display_name"],
                            "buildings": src["buildings"],
                            "entity_count": src["entity_count"],
                            "issue_type": itype,
                            "severity": hit.get("severity", "medium"),
                            "reason": hit.get("reason", ""),
                            "suggested_general_rule": hit.get("suggested_general_rule", ""),
                            "sample_entities": src.get("sample_entities", [])[:8],
                            "sample_addresses": src.get("sample_addresses", [])[:3],
                            "review_status": "ADVISORY_REVIEW_REQUIRED",
                        }
                    )
        return out, usage

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_audit_batch, i, b) for i, b in enumerate(batches)]
        done = 0
        for fut in as_completed(futs):
            hits, usage = fut.result()
            flagged.extend(hits)
            total_prompt_tok += int(usage.get("promptTokenCount") or 0)
            total_cand_tok += int(usage.get("candidatesTokenCount") or 0)
            done += 1
            if done % 20 == 0 or done == len(batches):
                print(f"    [{done}/{len(batches)}] audit batches done (flagged so far: {len(flagged):,})", flush=True)

    flagged.sort(key=lambda r: (0 if r["severity"] == "high" else 1, -int(r["buildings"] or 0)))
    by_jur: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    for f in flagged:
        by_jur[f["jur"]] = by_jur.get(f["jur"], 0) + 1
        by_type[f["issue_type"]] = by_type.get(f["issue_type"], 0) + 1

    report = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "model": model,
        "advisory_only": True,
        "networks_audited": min(len(candidates), len(batches) * batch_size),
        "flagged_count": len(flagged),
        "high_severity_count": sum(1 for f in flagged if f["severity"] == "high"),
        "by_issue_type": by_type,
        "by_jurisdiction": by_jur,
        "prompt_tokens": total_prompt_tok,
        "output_tokens": total_cand_tok,
        "elapsed_sec": round(time.time() - t0, 1),
        "findings": flagged,
    }
    out_path = os.path.join(REPO_ROOT, "docs", "benchmarks", "vertex_false_bridge_audit_20261120.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(
        f"✅ False-bridge audit saved: audited={report['networks_audited']:,}, "
        f"flagged={report['flagged_count']:,} (high={report['high_severity_count']:,}) in {report['elapsed_sec']}s -> {out_path}",
        flush=True,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Vertex AI habitability tagging & 25-jurisdiction adversarial false-bridge audit.")
    parser.add_argument("--tasks", nargs="+", default=["habitability", "bridge_audit"], choices=["habitability", "bridge_audit"])
    parser.add_argument("--habitability-model", default="gemini-3.5-flash-lite")
    parser.add_argument("--audit-model", default="gemini-3.6-flash")
    parser.add_argument("--top-per-jur", type=int, default=150, help="Top N multi-entity networks per jurisdiction to audit (default 150 -> ~3,750 networks)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-requests", type=int, default=300)
    args = parser.parse_args()

    if "habitability" in args.tasks:
        run_habitability_tagging(args.habitability_model, workers=args.workers, max_requests=args.max_requests)
    if "bridge_audit" in args.tasks:
        run_false_bridge_audit(args.audit_model, top_per_jur=args.top_per_jur, workers=args.workers, max_requests=args.max_requests)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
