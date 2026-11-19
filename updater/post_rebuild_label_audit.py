"""
updater/post_rebuild_label_audit.py
===================================
Post-rebuild & scheduled representative-label audit for ownership networks.

Runs automatically after every jurisdiction network rebuild (and on a daily
schedule via `updater/scheduled_runner.py`) to ensure the people & entities
named on "Notable Portfolios" / top cards accurately represent the whole
ownership network rather than an arbitrary single-property SPV or minor signer.

Two-tier epistemically honest architecture (`docs/ASSURANCE_POLICY.md`):
  Tier 1 — Deterministic Registry Pass (auto-applied to `network_representative_profiles`):
    Uses `api.network_profile.fetch_batch_network_oc_profiles` +
    `derive_network_business_profile` over loaded public corporate/assessor filings
    (`oc_companies`, `oc_officers`, `member_names`, `member_addresses`) to cache
    verified operator names, key officers, HQ addresses, and provenance basis for
    top networks (`building_count`, `open_violations`, `evictions_total`).

  Tier 2 — External Public-Source Verification Pass (optional `--with-web-audit`):
    Uses Gemini with live Google Search grounding on public business/entity/property
    identifiers ONLY (never personal/user data) to inspect ambiguous/SPV-labeled top
    networks (e.g., matching an operator's public portfolio website or SEC/registry
    filing that lists the properties in the network).
    CRITICAL ASSURANCE RULE: External web findings are ALWAYS persisted as
    `status = 'pending_review'` in `network_label_audit_candidates` with required
    public source URLs and evidence snippets — NEVER silently auto-applied without
    maintainer verification (`--approve <id>`). Approved citations are merged into
    `network_representative_profiles` with their public `external_source_url` shown
    transparently in the UI.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html as html_mod
import json
import logging
import os
import re
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import RealDictCursor

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from api.network_profile import (
    CITY_TO_OC_JUR,
    derive_network_business_profile,
    fetch_batch_network_oc_profiles,
)

logger = logging.getLogger("post-rebuild-label-audit")

ALL_CITY_JURISDICTIONS: Tuple[str, ...] = (
    "ma", "ny", "nyc", "ca", "fl", "il", "mn", "nj", "dc",
    "baltimore", "detroit", "philadelphia",
    "co", "mt", "nc", "oh", "va", "vt", "wi", "ar",
)


def ensure_representative_profile_tables(conn) -> None:
    """Create `network_representative_profiles` and `network_label_audit_candidates` if absent."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_representative_profiles (
                jurisdiction text NOT NULL,
                network_key text NOT NULL,
                operator_name text,
                operator_source text,
                operator_basis text,
                operating_entities jsonb NOT NULL DEFAULT '[]'::jsonb,
                primary_hq_address text,
                hq_entity_count integer,
                business_count integer,
                representative_human_name text,
                key_officers jsonb NOT NULL DEFAULT '[]'::jsonb,
                officer_count integer NOT NULL DEFAULT 0,
                sample_entities jsonb NOT NULL DEFAULT '[]'::jsonb,
                external_source_url text,
                external_source_label text,
                is_manual_override boolean NOT NULL DEFAULT FALSE,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY (jurisdiction, network_key)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_net_rep_profiles_jur_op_lower
                ON network_representative_profiles (jurisdiction, LOWER(operator_name))
                WHERE operator_name IS NOT NULL
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_label_audit_candidates (
                id bigserial PRIMARY KEY,
                jurisdiction text NOT NULL,
                network_key text NOT NULL,
                current_display_name text,
                current_operator_name text,
                suggested_operator_name text,
                suggested_representative_person text,
                suggested_hq_address text,
                rationale text,
                evidence_urls jsonb NOT NULL DEFAULT '[]'::jsonb,
                evidence_snippets jsonb NOT NULL DEFAULT '[]'::jsonb,
                model_used text,
                status text NOT NULL DEFAULT 'pending_review',
                reviewed_at timestamptz,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                UNIQUE (jurisdiction, network_key, suggested_operator_name)
            )
            """
        )
    conn.commit()


def _table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s
            LIMIT 1
            """,
            (table_name,),
        )
        return cur.fetchone() is not None


def _select_top_networks_for_audit(conn, city: str, limit: int = 120) -> List[Dict[str, Any]]:
    """Select the union of top networks by portfolio size, code violations, and evictions."""
    net_table = f"{city}_networks"
    stats_table = f"{city}_bbl_stats"
    if not _table_exists(conn, net_table):
        return []

    has_stats = _table_exists(conn, stats_table)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check if oc_company_ids column exists on this table
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (net_table,),
        )
        cols = {r["column_name"] for r in cur.fetchall()}
        oc_col = "n.oc_company_ids" if "oc_company_ids" in cols else "NULL::text[] AS oc_company_ids"

        cur.execute(
            f"""
            SELECT
                n.network_key,
                n.display_name,
                n.building_count,
                n.member_names,
                n.member_addresses,
                n.connection_signals,
                {oc_col}
            FROM {net_table} n
            WHERE COALESCE(n.building_count, 0) >= 2
            ORDER BY n.building_count DESC NULLS LAST, n.display_name ASC
            LIMIT %s
            """,
            (limit,),
        )
        by_key: Dict[str, Dict[str, Any]] = {}
        for r in cur.fetchall():
            d = dict(r)
            if isinstance(d.get("connection_signals"), str):
                try:
                    d["connection_signals"] = json.loads(d["connection_signals"])
                except Exception:
                    d["connection_signals"] = {}
            by_key[d["network_key"]] = d

        # Also include top 25 networks by code violations / evictions if stats table exists
        if has_stats and limit >= 25:
            try:
                cur.execute(
                    f"""
                    WITH viol_hits AS (
                        SELECT n.network_key, SUM(COALESCE(s.violations_open, 0) + COALESCE(s.evictions_total, 0) * 5) AS score
                        FROM {stats_table} s
                        JOIN {net_table} n ON ARRAY[s.bbl]::text[] <@ n.bbl_list
                        WHERE COALESCE(s.violations_open, 0) > 0 OR COALESCE(s.evictions_total, 0) > 0
                        GROUP BY n.network_key
                        ORDER BY score DESC
                        LIMIT 25
                    )
                    SELECT
                        n.network_key,
                        n.display_name,
                        n.building_count,
                        n.member_names,
                        n.member_addresses,
                        n.connection_signals,
                        {oc_col}
                    FROM viol_hits vh
                    JOIN {net_table} n ON n.network_key = vh.network_key
                    """
                )
                for r in cur.fetchall():
                    d = dict(r)
                    if d["network_key"] in by_key:
                        continue
                    if isinstance(d.get("connection_signals"), str):
                        try:
                            d["connection_signals"] = json.loads(d["connection_signals"])
                        except Exception:
                            d["connection_signals"] = {}
                    by_key[d["network_key"]] = d
            except Exception as exc:
                logger.debug("Optional stats union skipped for %s: %s", city, exc)

    return sorted(by_key.values(), key=lambda x: -(int(x.get("building_count") or 0)))


def _upsert_deterministic_profiles(
    conn,
    city: str,
    rows: Sequence[Dict[str, Any]],
    profiles_by_key: Dict[str, Dict[str, Any]],
) -> int:
    """Persist deterministic profiles while preserving any maintainer-approved overrides."""
    updated = 0
    with conn.cursor() as cur:
        for row in rows:
            nk = row["network_key"]
            prof = profiles_by_key.get(nk)
            if not prof:
                continue
            cur.execute(
                """
                INSERT INTO network_representative_profiles (
                    jurisdiction, network_key,
                    operator_name, operator_source, operator_basis,
                    operating_entities, primary_hq_address, hq_entity_count,
                    business_count, representative_human_name,
                    key_officers, officer_count, sample_entities,
                    updated_at
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s,
                    %s::jsonb, %s, %s,
                    %s, %s,
                    %s::jsonb, %s, %s::jsonb,
                    NOW()
                )
                ON CONFLICT (jurisdiction, network_key) DO UPDATE SET
                    operator_name = CASE
                        WHEN network_representative_profiles.is_manual_override THEN network_representative_profiles.operator_name
                        ELSE EXCLUDED.operator_name
                    END,
                    operator_source = CASE
                        WHEN network_representative_profiles.is_manual_override THEN network_representative_profiles.operator_source
                        ELSE EXCLUDED.operator_source
                    END,
                    operator_basis = CASE
                        WHEN network_representative_profiles.is_manual_override THEN network_representative_profiles.operator_basis
                        ELSE EXCLUDED.operator_basis
                    END,
                    operating_entities = EXCLUDED.operating_entities,
                    primary_hq_address = COALESCE(
                        CASE WHEN network_representative_profiles.is_manual_override THEN network_representative_profiles.primary_hq_address ELSE NULL END,
                        EXCLUDED.primary_hq_address
                    ),
                    hq_entity_count = EXCLUDED.hq_entity_count,
                    business_count = EXCLUDED.business_count,
                    representative_human_name = CASE
                        WHEN network_representative_profiles.is_manual_override AND network_representative_profiles.representative_human_name IS NOT NULL
                        THEN network_representative_profiles.representative_human_name
                        ELSE EXCLUDED.representative_human_name
                    END,
                    key_officers = EXCLUDED.key_officers,
                    officer_count = EXCLUDED.officer_count,
                    sample_entities = EXCLUDED.sample_entities,
                    updated_at = NOW()
                """,
                (
                    city,
                    nk,
                    prof.get("operator_name"),
                    prof.get("operator_source"),
                    prof.get("operator_basis"),
                    json.dumps(prof.get("operating_entities") or []),
                    prof.get("primary_hq_address"),
                    prof.get("hq_entity_count"),
                    prof.get("business_count"),
                    prof.get("representative_human_name"),
                    json.dumps(prof.get("key_officers") or []),
                    int(prof.get("officer_count") or 0),
                    json.dumps(prof.get("sample_entities") or []),
                ),
            )
            updated += 1
    conn.commit()
    return updated


def _invalidate_city_network_caches(conn, city: str) -> None:
    """Clear cached `/api/{city}/networks` entries in `kv_cache` so fresh labels serve immediately."""
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM kv_cache WHERE key LIKE %s", (f"city_top_nets:%:{city}:%",))
        conn.commit()
    except Exception as exc:
        logger.debug("kv_cache clear skipped for %s: %s", city, exc)


def _needs_external_web_verification(row: Dict[str, Any], prof: Dict[str, Any]) -> bool:
    """Return True when a major top portfolio still has an SPV/ambiguous operator name worth web-verifying."""
    if int(row.get("building_count") or 0) < 25:
        return False
    op_src = prof.get("operator_source") or ""
    op_name = (prof.get("operator_name") or "").upper()
    if not op_name or op_src in ("deed_owner", ""):
        return True
    # Also review top-15 portfolios where operator_name still looks like a generic holding/LP/SPV
    if re.search(r"\b(BORROWER|PROPCO|SUB|LLC\s+MANAGER|HOLDINGS\s+[A-Z0-9]+|ML\s+\d+|LJS|FKH|HPA|BMF)\b", op_name):
        return True
    return False


def _run_grounded_web_audit_for_network(
    city: str,
    row: Dict[str, Any],
    prof: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Use Gemini with Google Search grounding on PUBLIC corporate/property identifiers ONLY
    to locate official corporate portfolio pages or reputable public sources confirming
    the parent operator/management brand for an ownership network.

    NEVER sends private/user data. ALWAYS requires verifiable public URLs.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return None

    try:
        import requests
        from api.gemini_models import resolve_latest_gemini_model

        model_name = resolve_latest_gemini_model(api_key)
    except Exception as exc:
        logger.warning("Gemini model resolution unavailable for web audit: %s", exc)
        return None

    # Only include strictly public corporate/business identifiers & aggregate property counts
    sample_members = [
        m for m in (row.get("member_names") or [])[:15]
        if m and not re.search(r"@", str(m))
    ]
    payload_summary = {
        "jurisdiction": city.upper(),
        "property_count": int(row.get("building_count") or 0),
        "current_display_name": row.get("display_name"),
        "current_operator_name": prof.get("operator_name"),
        "current_operator_basis": prof.get("operator_basis"),
        "primary_hq_address": prof.get("primary_hq_address"),
        "operating_entities": prof.get("operating_entities") or [],
        "representative_officers": prof.get("key_officers") or [],
        "sample_member_entities": sample_members,
    }

    prompt = (
        "You are auditing a public-records real-estate ownership network label for epistemic accuracy.\n"
        "Use Google Search to check public official sources (such as the operating company's own "
        "portfolio website, SEC filings, state business registries, or municipal/nonprofit housing "
        "databases) to verify whether these linked public LLCs/LPs and headquarters belong to a "
        "well-known parent operating/management company (e.g., WinnCompanies, The Hamilton Company, "
        "City Realty Group, Tricon Residential, FirstKey Homes, Realty Income, etc.).\n\n"
        "STRICT RULES:\n"
        "1. NEVER invent or guess. If public sources do not clearly confirm a parent operator, "
        "return `\"confident\": false`.\n"
        "2. Every claim MUST be backed by at least one real public URL found in search.\n"
        "3. Return ONLY valid JSON with keys:\n"
        "   - confident (boolean)\n"
        "   - suggested_operator_name (string or null)\n"
        "   - suggested_representative_person (string or null)\n"
        "   - suggested_hq_address (string or null)\n"
        "   - rationale (concise 1-2 sentence explanation citing specific matched entities/properties)\n"
        "   - evidence_urls (array of exact public URLs)\n"
        "   - evidence_snippets (array of short factual quotes/facts from those pages)\n\n"
        f"Public network record:\n{json.dumps(payload_summary, indent=2)}"
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.1},
    }
    try:
        resp = requests.post(url, json=body, timeout=45)
        if not resp.ok:
            logger.warning("Gemini grounded web audit HTTP %s for %s/%s", resp.status_code, city, row.get("network_key"))
            return None
        data = resp.json()
        candidates = data.get("candidates") or []
        if not candidates:
            return None
        cand0 = candidates[0]
        parts = (cand0.get("content") or {}).get("parts") or []
        raw_text = "\n".join(p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text"))
        # Extract grounding chunk URLs directly from Gemini metadata so URLs are never hallucinated
        grounding_urls: List[str] = []
        gm = cand0.get("groundingMetadata") or {}
        for chunk in (gm.get("groundingChunks") or []):
            web = chunk.get("web") if isinstance(chunk, dict) else None
            if isinstance(web, dict) and web.get("uri"):
                grounding_urls.append(str(web["uri"]))

        m = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if not m:
            return None
        parsed = json.loads(m.group(0))
        if not parsed.get("confident") or not parsed.get("suggested_operator_name"):
            return None
        urls = list(dict.fromkeys([u for u in (parsed.get("evidence_urls") or []) + grounding_urls if isinstance(u, str) and u.startswith("http")]))
        if not urls:
            # Strict epistemic policy: reject any external label candidate lacking verifiable public URLs
            return None
        return {
            "suggested_operator_name": str(parsed["suggested_operator_name"]).strip()[:140],
            "suggested_representative_person": (
                str(parsed["suggested_representative_person"]).strip()[:120]
                if parsed.get("suggested_representative_person")
                else None
            ),
            "suggested_hq_address": (
                str(parsed["suggested_hq_address"]).strip()[:200]
                if parsed.get("suggested_hq_address")
                else None
            ),
            "rationale": str(parsed.get("rationale") or "").strip()[:600],
            "evidence_urls": urls[:8],
            "evidence_snippets": [str(s)[:300] for s in (parsed.get("evidence_snippets") or [])[:5]],
            "model_used": model_name,
        }
    except Exception as exc:
        logger.warning("Gemini grounded web audit failed for %s/%s: %s", city, row.get("network_key"), exc)
        return None


def audit_jurisdiction_labels(
    conn,
    city: str,
    limit: int = 120,
    with_web_audit: bool = False,
    web_audit_max: int = 12,
) -> Dict[str, Any]:
    """
    Run the post-rebuild representative-label pass for one jurisdiction:
      1. Deterministic registry pass -> upserts `network_representative_profiles` + clears `kv_cache`.
      2. Optional grounded web verification -> queues cited candidates in `network_label_audit_candidates`
         (`status = 'pending_review'`, never auto-applied without verification).
    """
    t0 = time.time()
    city_l = city.lower().strip()
    ensure_representative_profile_tables(conn)

    rows = _select_top_networks_for_audit(conn, city_l, limit=limit)
    if not rows:
        return {"jurisdiction": city_l, "audited": 0, "updated": 0, "web_candidates": 0, "elapsed_s": 0.0}

    def _query(sql: str, params=()):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    # Batch in chunks of 40 so SQL queries stay fast & bounded in memory
    profiles_by_key: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(rows), 40):
        batch = rows[start : start + 40]
        oc_enr = fetch_batch_network_oc_profiles(_query, city_l, batch)
        for r in batch:
            nk = r["network_key"]
            profiles_by_key[nk] = derive_network_business_profile(
                r.get("display_name"),
                r.get("member_names") or [],
                r.get("member_addresses") or [],
                r.get("connection_signals") or {},
                oc_enr.get(nk),
            )

    updated = _upsert_deterministic_profiles(conn, city_l, rows, profiles_by_key)
    _invalidate_city_network_caches(conn, city_l)

    web_queued = 0
    if with_web_audit and os.environ.get("GEMINI_API_KEY"):
        checked = 0
        for idx, r in enumerate(rows):
            if checked >= web_audit_max:
                break
            nk = r["network_key"]
            prof = profiles_by_key.get(nk) or {}
            # Audit either top-10 flagship cards or ambiguous/SPV top cards
            if idx < 10 or _needs_external_web_verification(r, prof):
                checked += 1
                cand = _run_grounded_web_audit_for_network(city_l, r, prof)
                if not cand:
                    continue
                # Skip queuing if external name already matches deterministic operator_name
                if (prof.get("operator_name") or "").strip().upper() == cand["suggested_operator_name"].upper():
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO network_label_audit_candidates (
                            jurisdiction, network_key,
                            current_display_name, current_operator_name,
                            suggested_operator_name, suggested_representative_person,
                            suggested_hq_address, rationale,
                            evidence_urls, evidence_snippets, model_used, status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, 'pending_review')
                        ON CONFLICT (jurisdiction, network_key, suggested_operator_name) DO UPDATE SET
                            rationale = EXCLUDED.rationale,
                            evidence_urls = EXCLUDED.evidence_urls,
                            evidence_snippets = EXCLUDED.evidence_snippets,
                            model_used = EXCLUDED.model_used,
                            updated_at = NOW()
                        """,
                        (
                            city_l,
                            nk,
                            r.get("display_name"),
                            prof.get("operator_name"),
                            cand["suggested_operator_name"],
                            cand.get("suggested_representative_person"),
                            cand.get("suggested_hq_address"),
                            cand["rationale"],
                            json.dumps(cand["evidence_urls"]),
                            json.dumps(cand["evidence_snippets"]),
                            cand["model_used"],
                        ),
                    )
                conn.commit()
                web_queued += 1

    elapsed = round(time.time() - t0, 2)
    summary = {
        "jurisdiction": city_l,
        "audited": len(rows),
        "updated": updated,
        "web_candidates_queued": web_queued,
        "elapsed_s": elapsed,
        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    logger.info(
        "[POST-REBUILD-AUDIT] %s: updated=%d/%d profiles, web_candidates=%d (%.2fs)",
        city_l.upper(), updated, len(rows), web_queued, elapsed,
    )
    return summary


def approve_candidate(conn, candidate_id: int) -> Dict[str, Any]:
    """
    Approve a reviewed external-source candidate and apply its cited label to
    `network_representative_profiles`. Returns the candidate plus `prior_profile`
    so 1-click Undo can restore the previous deterministic row losslessly.
    """
    ensure_representative_profile_tables(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM network_label_audit_candidates WHERE id = %s",
            (candidate_id,),
        )
        cand = cur.fetchone()
        if not cand:
            raise ValueError(f"Candidate id={candidate_id} not found")

        cur.execute(
            """
            SELECT operator_name, operator_source, operator_basis,
                   primary_hq_address, representative_human_name,
                   external_source_url, external_source_label, is_manual_override
            FROM network_representative_profiles
            WHERE jurisdiction = %s AND network_key = %s
            """,
            (cand["jurisdiction"], cand["network_key"]),
        )
        prior_row = cur.fetchone()
        prior_profile = dict(prior_row) if prior_row else None

        urls = cand.get("evidence_urls") or []
        primary_url = urls[0] if isinstance(urls, list) and urls else None
        source_label = None
        if primary_url:
            m = re.match(r"https?://(?:www\.)?([^/]+)", primary_url)
            source_label = m.group(1) if m else primary_url

        cur.execute(
            """
            INSERT INTO network_representative_profiles (
                jurisdiction, network_key,
                operator_name, operator_source, operator_basis,
                primary_hq_address, representative_human_name,
                external_source_url, external_source_label,
                is_manual_override, updated_at
            )
            VALUES (%s, %s, %s, 'verified_external_source', %s, %s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (jurisdiction, network_key) DO UPDATE SET
                operator_name = EXCLUDED.operator_name,
                operator_source = 'verified_external_source',
                operator_basis = EXCLUDED.operator_basis,
                primary_hq_address = COALESCE(EXCLUDED.primary_hq_address, network_representative_profiles.primary_hq_address),
                representative_human_name = COALESCE(EXCLUDED.representative_human_name, network_representative_profiles.representative_human_name),
                external_source_url = EXCLUDED.external_source_url,
                external_source_label = EXCLUDED.external_source_label,
                is_manual_override = TRUE,
                updated_at = NOW()
            """,
            (
                cand["jurisdiction"],
                cand["network_key"],
                cand["suggested_operator_name"],
                cand.get("rationale") or f"Verified via {source_label or 'public source'}",
                cand.get("suggested_hq_address"),
                cand.get("suggested_representative_person"),
                primary_url,
                source_label,
            ),
        )
        cur.execute(
            "UPDATE network_label_audit_candidates SET status = 'approved', reviewed_at = NOW() WHERE id = %s",
            (candidate_id,),
        )
    conn.commit()
    _invalidate_city_network_caches(conn, cand["jurisdiction"])
    out = dict(cand)
    out["prior_profile"] = prior_profile
    return out


def reject_candidate(conn, candidate_id: int) -> Dict[str, Any]:
    """Mark a candidate as rejected so future rebuild audits do not re-propose it."""
    ensure_representative_profile_tables(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM network_label_audit_candidates WHERE id = %s", (candidate_id,))
        cand = cur.fetchone()
        if not cand:
            raise ValueError(f"Candidate id={candidate_id} not found")
        cur.execute(
            "UPDATE network_label_audit_candidates SET status = 'rejected', reviewed_at = NOW() WHERE id = %s",
            (candidate_id,),
        )
    conn.commit()
    return dict(cand)


def revert_candidate(conn, candidate_id: int, prior_profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Revert an approved/rejected candidate back to `pending_review` and restore prior profile state."""
    ensure_representative_profile_tables(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM network_label_audit_candidates WHERE id = %s", (candidate_id,))
        cand = cur.fetchone()
        if not cand:
            raise ValueError(f"Candidate id={candidate_id} not found")
        cur.execute(
            "UPDATE network_label_audit_candidates SET status = 'pending_review', reviewed_at = NULL WHERE id = %s",
            (candidate_id,),
        )
        if prior_profile is not None:
            cur.execute(
                """
                UPDATE network_representative_profiles
                SET operator_name = %s,
                    operator_source = %s,
                    operator_basis = %s,
                    primary_hq_address = %s,
                    representative_human_name = %s,
                    external_source_url = %s,
                    external_source_label = %s,
                    is_manual_override = %s,
                    updated_at = NOW()
                WHERE jurisdiction = %s AND network_key = %s
                """,
                (
                    prior_profile.get("operator_name"),
                    prior_profile.get("operator_source"),
                    prior_profile.get("operator_basis"),
                    prior_profile.get("primary_hq_address"),
                    prior_profile.get("representative_human_name"),
                    prior_profile.get("external_source_url"),
                    prior_profile.get("external_source_label"),
                    bool(prior_profile.get("is_manual_override")),
                    cand["jurisdiction"],
                    cand["network_key"],
                ),
            )
    conn.commit()
    _invalidate_city_network_caches(conn, cand["jurisdiction"])
    return dict(cand)


def fetch_pending_candidates(conn, limit: int = 40) -> List[Dict[str, Any]]:
    """Fetch pending external-source label candidates enriched with current parcel counts."""
    ensure_representative_profile_tables(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                c.id,
                c.jurisdiction,
                c.network_key,
                c.current_display_name,
                c.current_operator_name,
                c.suggested_operator_name,
                c.suggested_representative_person,
                c.suggested_hq_address,
                c.rationale,
                c.evidence_urls,
                c.evidence_snippets,
                c.model_used,
                c.created_at,
                rp.primary_hq_address AS current_hq_address,
                rp.representative_human_name AS current_representative_person,
                rp.business_count AS entity_count
            FROM network_label_audit_candidates c
            LEFT JOIN network_representative_profiles rp
              ON rp.jurisdiction = c.jurisdiction
             AND rp.network_key = c.network_key
            WHERE c.status = 'pending_review'
            ORDER BY c.created_at DESC, c.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    # Best-effort parcel count lookup per jurisdiction
    by_jur: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_jur.setdefault(r["jurisdiction"], []).append(r)
    for jur, jrows in by_jur.items():
        if not _table_exists(conn, f"{jur}_networks"):
            continue
        keys = [r["network_key"] for r in jrows]
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"SELECT network_key, building_count FROM {jur}_networks WHERE network_key = ANY(%s)",
                    (keys,),
                )
                counts = {x["network_key"]: int(x["building_count"] or 0) for x in cur.fetchall()}
                for r in jrows:
                    r["building_count"] = counts.get(r["network_key"])
        except Exception:
            pass
    return rows


def build_label_audit_email_html(
    candidates: Sequence[Dict[str, Any]],
    audit_results: Sequence[Dict[str, Any]] = (),
    *,
    base_url: str = "https://theyownwhat.net",
    reviewer: str = "salmunk@gmail.com",
) -> Tuple[str, str, str]:
    """
    Build a detailed HTML + plain-text human-approval email matching the Tenant Toolkit
    signed 1-click Approve / Decline confirmation flow.
    Returns `(subject, html_body, text_body)`.
    """
    from api.audit_review_routes import build_quick_decision_url, sign_review_token

    clean_base = str(base_url or "https://theyownwhat.net").strip().rstrip("/")
    n_cands = len(candidates)
    n_jurs = len(audit_results)
    total_profiles = sum(int(r.get("updated") or 0) for r in audit_results)
    date_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    subject = (
        f"[They Own WHAT??] {n_cands} Representative Network Label{'s' if n_cands != 1 else ''} "
        f"Ready for Human Approval ({date_str})"
    )

    queue_token = sign_review_token("*", reviewer=reviewer)
    queue_url = f"{clean_base}/api/audit-review/queue?{urlencode({'token': queue_token})}"
    approve_all_url = build_quick_decision_url(
        clean_base,
        decision="approve",
        item_key="label-candidate:approve-all",
        title=f"Approve All {n_cands} Pending Representative Network Labels",
        domain="Representative Label Audit",
        reviewer=reviewer,
    )

    cards_html = []
    text_lines = [
        f"They Own WHAT?? — Representative Network Label Audit ({date_str})",
        f"Jurisdictions audited: {n_jurs} | Deterministic profiles refreshed: {total_profiles} | Pending approval: {n_cands}",
        "",
    ]

    for cand in candidates:
        cid = cand["id"]
        jur_up = str(cand.get("jurisdiction") or "").upper()
        cur_op = cand.get("current_operator_name") or cand.get("current_display_name") or "Unlabeled"
        cur_disp = cand.get("current_display_name") or ""
        prop_op = cand.get("suggested_operator_name") or ""
        prop_person = cand.get("suggested_representative_person") or ""
        cur_person = cand.get("current_representative_person") or ""
        prop_hq = cand.get("suggested_hq_address") or ""
        cur_hq = cand.get("current_hq_address") or ""
        rationale = cand.get("rationale") or ""
        urls = cand.get("evidence_urls") if isinstance(cand.get("evidence_urls"), list) else []
        snippets = cand.get("evidence_snippets") if isinstance(cand.get("evidence_snippets"), list) else []
        bldg_cnt = cand.get("building_count")
        ent_cnt = cand.get("entity_count")
        item_key = f"label-candidate:{cid}"
        card_title = f"[{jur_up}] {cur_op} -> {prop_op}"

        approve_url = build_quick_decision_url(
            clean_base,
            decision="approve",
            item_key=item_key,
            title=card_title,
            domain="Representative Label Audit",
            jurisdiction=jur_up,
            reviewer=reviewer,
        )
        decline_url = build_quick_decision_url(
            clean_base,
            decision="reject",
            item_key=item_key,
            title=card_title,
            domain="Representative Label Audit",
            jurisdiction=jur_up,
            reviewer=reviewer,
        )

        scale_bits = []
        if bldg_cnt:
            scale_bits.append(f"{int(bldg_cnt):,} parcels")
        if ent_cnt:
            scale_bits.append(f"{int(ent_cnt):,} entities")
        scale_str = " · ".join(scale_bits)

        urls_html = "".join(
            f'<li style="margin:0 0 4px;"><a href="{html_mod.escape(u)}" style="color:#1d4ed8;text-decoration:underline;word-break:break-all;">{html_mod.escape(u)}</a></li>'
            for u in urls[:5]
        )
        snips_html = "".join(
            f'<li style="margin:0 0 4px;color:#475569;">&ldquo;{html_mod.escape(str(s))}&rdquo;</li>'
            for s in snippets[:3]
        )

        cards_html.append(f"""
        <div style="border:1px solid #cbd5e1;border-radius:14px;background:#ffffff;padding:18px 20px;margin:0 0 16px;">
          <div style="margin:0 0 10px;">
            <span style="display:inline-block;background:#eef2ff;color:#3730a3;border:1px solid #c7d2fe;border-radius:6px;padding:2px 8px;font-size:11px;font-weight:800;letter-spacing:0.04em;">{html_mod.escape(jur_up)}</span>
            {f'<span style="display:inline-block;margin-left:8px;color:#64748b;font-size:12px;font-weight:600;">{html_mod.escape(scale_str)}</span>' if scale_str else ''}
            <span style="display:inline-block;margin-left:8px;color:#94a3b8;font-size:11px;">Candidate #{cid}</span>
          </div>

          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:8px 0;margin:0 -8px 12px;">
            <tr>
              <td width="48%" valign="top" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:10px 12px;">
                <div style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;margin-bottom:4px;">Current Card Label</div>
                <div style="font-size:14px;font-weight:800;color:#334155;">{html_mod.escape(cur_op)}</div>
                {f'<div style="font-size:11px;color:#64748b;margin-top:3px;">Deed anchor: {html_mod.escape(cur_disp)}</div>' if cur_disp and cur_disp.upper() != cur_op.upper() else ''}
                {f'<div style="font-size:11px;color:#64748b;margin-top:2px;">Principal: {html_mod.escape(cur_person)}</div>' if cur_person else ''}
                {f'<div style="font-size:11px;color:#64748b;margin-top:2px;">HQ: {html_mod.escape(cur_hq)}</div>' if cur_hq else ''}
              </td>
              <td width="52%" valign="top" style="background:#ecfdf5;border:1px solid #a7f3d0;border-radius:10px;padding:10px 12px;">
                <div style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:0.06em;color:#047857;margin-bottom:4px;">Proposed Verified Label</div>
                <div style="font-size:15px;font-weight:900;color:#065f46;">{html_mod.escape(prop_op)}</div>
                {f'<div style="font-size:11px;color:#065f46;margin-top:3px;"><strong>Principal:</strong> {html_mod.escape(prop_person)}</div>' if prop_person else ''}
                {f'<div style="font-size:11px;color:#065f46;margin-top:2px;"><strong>HQ:</strong> {html_mod.escape(prop_hq)}</div>' if prop_hq else ''}
              </td>
            </tr>
          </table>

          {f'<p style="margin:0 0 10px;font-size:13px;line-height:1.5;color:#1e293b;"><strong>Evidence summary:</strong> {html_mod.escape(rationale)}</p>' if rationale else ''}

          {f'<div style="margin:0 0 10px;"><div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:0.05em;color:#475569;margin-bottom:4px;">Public Source Citations</div><ul style="margin:0;padding-left:18px;font-size:12px;">{urls_html}</ul></div>' if urls_html else ''}

          {f'<div style="margin:0 0 12px;"><div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:0.05em;color:#475569;margin-bottom:4px;">Quoted Evidence</div><ul style="margin:0;padding-left:18px;font-size:12px;">{snips_html}</ul></div>' if snips_html else ''}

          <div style="padding-top:10px;border-top:1px solid #e2e8f0;">
            <a href="{html_mod.escape(approve_url)}" style="display:inline-block;background:#059669;color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;padding:10px 18px;border-radius:9px;margin-right:8px;">&#10003; Approve &amp; Apply Label</a>
            <a href="{html_mod.escape(decline_url)}" style="display:inline-block;background:#ffffff;color:#b91c1c;border:1px solid #fca5a5;text-decoration:none;font-weight:700;font-size:13px;padding:9px 16px;border-radius:9px;">&#10005; Decline</a>
          </div>
        </div>
        """)

        text_lines.extend([
            f"[{jur_up}] Candidate #{cid}: {cur_op} -> {prop_op} ({scale_str})",
            f"  Rationale: {rationale}",
            f"  Sources: {', '.join(urls[:3])}",
            f"  Approve: {approve_url}",
            f"  Decline: {decline_url}",
            "",
        ])

    approve_all_banner = ""
    if n_cands >= 2:
        approve_all_banner = f"""
        <div style="background:#f0fdf4;border:1px solid #86efac;border-radius:12px;padding:14px 16px;margin:0 0 18px;">
          <div style="font-size:13px;font-weight:700;color:#14532d;margin-bottom:8px;">Reviewed all {n_cands} cited candidates below and ready to publish?</div>
          <a href="{html_mod.escape(approve_all_url)}" style="display:inline-block;background:#059669;color:#ffffff;text-decoration:none;font-weight:800;font-size:13px;padding:10px 18px;border-radius:9px;">&#10003; Approve All {n_cands} Pending Labels</a>
          <a href="{html_mod.escape(queue_url)}" style="display:inline-block;margin-left:10px;color:#1d4ed8;font-size:12px;font-weight:700;text-decoration:underline;">Open Full Review Queue &rarr;</a>
        </div>
        """

    html_body = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /></head>
<body style="margin:0;padding:20px;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#0f172a;">
  <div style="max-width:680px;margin:0 auto;">
    <div style="background:#0f172a;color:#ffffff;border-radius:16px;padding:22px 24px;margin-bottom:16px;">
      <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:0.08em;color:#93c5fd;">They Own WHAT?? &middot; Post-Rebuild Label Assurance</div>
      <h1 style="margin:6px 0 6px;font-size:20px;line-height:1.3;">Representative Network Label Review ({date_str})</h1>
      <p style="margin:0;font-size:13px;color:#cbd5e1;line-height:1.5;">
        After network rebuilds complete, our audit refreshes deterministic registry labels automatically and queues external public-source candidates here for your sign-off. <strong>Nothing below is published until you click Approve and confirm.</strong>
      </p>
      <div style="margin-top:12px;font-size:12px;color:#e2e8f0;">
        <strong>{n_jurs or len({c.get('jurisdiction') for c in candidates})}</strong> jurisdiction(s) &middot;
        <strong>{total_profiles:,}</strong> registry profiles refreshed &middot;
        <strong>{n_cands}</strong> external-source candidate(s) awaiting approval
      </div>
    </div>

    {approve_all_banner}
    {"".join(cards_html)}

    <div style="font-size:11px;color:#64748b;line-height:1.5;padding:12px 4px;">
      Links are HMAC-SHA256 signed (30-day TTL) and prefetcher-safe (clicking opens a confirmation page before modifying PostgreSQL or caches). Every external suggestion is advisory only and must be backed by the cited public URLs.
      &nbsp;&middot;&nbsp;<a href="{html_mod.escape(queue_url)}" style="color:#2563eb;">View Review Queue</a>
    </div>
  </div>
</body>
</html>"""

    return subject, html_body, "\n".join(text_lines)


def send_label_audit_email(
    candidates: Sequence[Dict[str, Any]],
    audit_results: Sequence[Dict[str, Any]] = (),
    *,
    recipient: Optional[str] = None,
    base_url: Optional[str] = None,
    send_smtp: bool = False,
) -> Dict[str, Any]:
    """Render, save local preview (`docs/benchmarks/latest-label-audit-email.html`), and optionally SMTP-send the approval email."""
    to_addr = (
        recipient
        or os.environ.get("NIGHTLY_OPTIMIZER_EMAIL_TO")
        or os.environ.get("ALERT_EMAIL_TO")
        or "salmunk@gmail.com"
    ).strip()
    site_base = (base_url or os.environ.get("PUBLIC_SITE_URL") or "https://theyownwhat.net").strip()

    subject, html_body, text_body = build_label_audit_email_html(
        candidates,
        audit_results,
        base_url=site_base,
        reviewer=to_addr,
    )

    preview_path = BASE_DIR / "docs" / "benchmarks" / "latest-label-audit-email.html"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(html_body, encoding="utf-8")

    result: Dict[str, Any] = {
        "candidate_count": len(candidates),
        "recipient": to_addr,
        "subject": subject,
        "preview_path": str(preview_path),
        "sent": False,
        "error": None,
    }
    if not send_smtp or not candidates:
        return result

    smtp_host = (os.environ.get("SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = (os.environ.get("ALERT_EMAIL_FROM") or os.environ.get("MAIL_USERNAME") or "").strip()
    smtp_pass = "".join((os.environ.get("ALERT_EMAIL_PASS") or os.environ.get("MAIL_PASSWORD") or "").split())

    if not smtp_user or not smtp_pass:
        result["error"] = "SMTP credentials (ALERT_EMAIL_FROM / ALERT_EMAIL_PASS) not configured."
        logger.warning("Label audit email skipped: %s", result["error"])
        return result

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=25) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [to_addr], msg.as_string())
        result["sent"] = True
        logger.info("✓ Sent representative-label approval email (%d candidates) to %s", len(candidates), to_addr)
    except Exception as exc:
        result["error"] = str(exc)
        logger.warning("⚠ Label audit email delivery failed: %s (HTML preview saved at %s)", exc, preview_path)
    return result


def run_post_rebuild_hook(city: str, limit: int = 120, with_web_audit: bool = False) -> Optional[Dict[str, Any]]:
    """Safe entry point called automatically at the end of network rebuild scripts."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        try:
            return audit_jurisdiction_labels(conn, city, limit=limit, with_web_audit=with_web_audit)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Post-rebuild representative label audit skipped for %s: %s", city, exc)
        return None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Post-rebuild representative network label audit")
    parser.add_argument("--jurisdiction", "-j", help="Single jurisdiction (e.g. ma, ca, ny, fl) or 'all'", default="all")
    parser.add_argument("--limit", type=int, default=120, help="Top N networks per jurisdiction to audit")
    parser.add_argument("--with-web-audit", action="store_true", help="Also run Google-Search-grounded Gemini verification (queued as pending_review)")
    parser.add_argument("--web-max", type=int, default=10, help="Max networks per jurisdiction for external web audit")
    parser.add_argument("--send-email", action="store_true", help="Send signed 1-click approval HTML email for pending candidates")
    parser.add_argument("--to", help="Recipient email address for approval digest")
    parser.add_argument("--base-url", default=os.environ.get("PUBLIC_SITE_URL", "https://theyownwhat.net"), help="Public base URL for signed approval buttons")
    parser.add_argument("--approve", type=int, help="Approve a pending external-source candidate ID")
    parser.add_argument("--reject", type=int, help="Reject a pending external-source candidate ID")
    parser.add_argument("--list-pending", action="store_true", help="List pending external-source label candidates")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL environment variable is required")

    conn = psycopg2.connect(db_url)
    try:
        ensure_representative_profile_tables(conn)
        if args.approve is not None:
            res = approve_candidate(conn, args.approve)
            print(json.dumps({"approved": res["id"], "jurisdiction": res["jurisdiction"], "operator_name": res["suggested_operator_name"]}, indent=2))
            return
        if args.reject is not None:
            res = reject_candidate(conn, args.reject)
            print(json.dumps({"rejected": res["id"], "jurisdiction": res["jurisdiction"], "operator_name": res["suggested_operator_name"]}, indent=2))
            return
        if args.list_pending:
            pending = fetch_pending_candidates(conn, limit=50)
            print(json.dumps(pending, indent=2, default=str))
            return

        targets = list(ALL_CITY_JURISDICTIONS) if args.jurisdiction.lower() == "all" else [args.jurisdiction.lower()]
        results = []
        for jur in targets:
            if not _table_exists(conn, f"{jur}_networks"):
                continue
            results.append(
                audit_jurisdiction_labels(
                    conn,
                    jur,
                    limit=args.limit,
                    with_web_audit=args.with_web_audit,
                    web_audit_max=args.web_max,
                )
            )
        email_status = None
        pending = fetch_pending_candidates(conn, limit=40)
        if pending and (args.send_email or args.with_web_audit):
            email_status = send_label_audit_email(
                pending,
                results,
                recipient=args.to,
                base_url=args.base_url,
                send_smtp=args.send_email,
            )
        print(json.dumps({"audited_jurisdictions": len(results), "results": results, "email": email_status}, indent=2, default=str))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
