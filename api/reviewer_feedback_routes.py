"""
api/reviewer_feedback_routes.py
===============================
Trusted-reviewer network error reporting & HMAC-signed 1-click revert endpoints.

Design & Epistemic Honesty Rules (`docs/ASSURANCE_POLICY.md`):
  - Invited human reviewers using `/review` (or `review.theyownwhat.net`) can flag
    erroneously linked principals, entities, bridge addresses, properties, or
    network merges directly inside the network UI.
  - Submissions NEVER directly edit or suppress live landlord networks (preventing
    weaponization as a removal tool by a disgruntled landlord).
  - Instead, reports are queued as `pending` in `network_error_reports` and
    evaluated once per night by `updater/evaluate_reviewer_reports.py` against
    loaded public records. Verified root causes generalize into global rules in
    `network_linking_rules` + `docs/benchmarks/learned_linking_rules.json` and
    rebuild through shadow/audit gates.
  - Every automated change reported in the daily digest includes a prefetch-safe
    HMAC-signed Revert link (`GET /api/review-reports/revert` -> `POST`).
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import subprocess
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from api.audit_review_routes import (
    CSP_HEADER,
    _authorize_request,
    sign_review_token,
)
from api.feedback import spawn_feedback_notification_email
from api.linking_rules import deactivate_learned_rule, ensure_linking_rules_tables

logger = logging.getLogger("reviewer-feedback")

router = APIRouter()

VALID_JURISDICTIONS = frozenset({
    "ct", "ny", "nyc", "dc", "baltimore", "detroit", "philadelphia",
    "nj", "ma", "boston", "fl", "miami", "il", "chicago", "mn", "minneapolis",
    "ca", "oh", "mt", "co", "vt", "wi", "nc", "ar", "va",
})

JURISDICTION_ALIASES = {
    "nyc": "ny",
    "nys": "ny",
    "boston": "ma",
    "miami": "fl",
    "chicago": "il",
    "minneapolis": "mn",
    "philly": "philadelphia",
    "washington": "dc",
}

VALID_REPORT_KINDS = frozenset({
    "false_positive",
    "missed_connection",
})

VALID_TARGET_TYPES = frozenset({
    "principal",
    "entity",
    "address",
    "property",
    "network_merge",
    "network",
    "label",
})

VALID_REASON_CODES = frozenset({
    "wrong_person",
    "wrong_entity",
    "third_party_intermediary",
    "shared_office_or_maildrop",
    "same_name_different_person",
    "non_landlord_entity",
    "nominee_manager_factory",
    "stale_or_sold_property",
    "wrong_label",
    "missed_shared_address",
    "missed_overblocked_hq",
    "missed_shared_principal",
    "missed_corporate_affiliate",
    "missed_shared_email",
    "missed_false_exclusion",
    "missed_connection_other",
    "other",
})

REASON_LABELS: Dict[str, str] = {
    "wrong_person": "Person does not belong in this network",
    "wrong_entity": "Entity / LLC does not belong in this network",
    "third_party_intermediary": "Third-party attorney, CPA, registered agent, or property manager",
    "shared_office_or_maildrop": "Shared office tower, UPS/PMB maildrop, or unrelated suite address",
    "same_name_different_person": "Different person with the same name (homonym collision)",
    "non_landlord_entity": "Non-landlord entity (co-op, HOA, bank, utility, charity, school, govt)",
    "nominee_manager_factory": "Commercial nominee manager / corporate formation factory",
    "stale_or_sold_property": "Stale or sold property / outdated officer record",
    "wrong_label": "Misleading representative network label",
    "missed_shared_address": "Missed link: share same office/mailing address (spelling, ZIP, or C/O variant)",
    "missed_overblocked_hq": "Missed link: cohesive landlord HQ appears over-blocked as a registered agent",
    "missed_shared_principal": "Missed link: share same principal/officer (name or title suffix mismatch)",
    "missed_corporate_affiliate": "Missed link: same corporate group / abbreviation / affiliate mismatch",
    "missed_shared_email": "Missed link: share same business email or custom domain",
    "missed_false_exclusion": "Missed link: genuine private landlord LLC falsely excluded as institutional",
    "missed_connection_other": "Missed link: other real ownership connection",
    "other": "Other linkage error",
}

# In-memory per-IP sliding window rate limiter (generous for active invited reviewers)
_RATE_WINDOW_SEC = 3600
_RATE_MAX_PER_HOUR = 80
_RATE_BUCKETS: Dict[str, Deque[float]] = defaultdict(deque)


def _get_db_url() -> str:
    return os.environ.get("DATABASE_URL") or "postgresql://user:password@127.0.0.1:5432/ctdata"


def _connect():
    return psycopg2.connect(_get_db_url(), connect_timeout=5)


def normalize_jurisdiction(raw: str) -> str:
    j = str(raw or "ct").strip().lower()
    j = JURISDICTION_ALIASES.get(j, j)
    return j if j in VALID_JURISDICTIONS else "ct"


def ensure_reviewer_feedback_tables(conn) -> None:
    """Create `network_error_reports` and `network_error_report_reverts` tables idempotently."""
    ensure_linking_rules_tables(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_error_reports (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'pending',
                report_kind TEXT NOT NULL DEFAULT 'false_positive',
                jurisdiction TEXT NOT NULL,
                network_id TEXT NOT NULL DEFAULT '',
                network_key TEXT NOT NULL DEFAULT '',
                network_name TEXT NOT NULL DEFAULT '',
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                target_id TEXT NOT NULL DEFAULT '',
                related_target_type TEXT NOT NULL DEFAULT '',
                related_target_value TEXT NOT NULL DEFAULT '',
                related_target_id TEXT NOT NULL DEFAULT '',
                bridge_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                reason_code TEXT NOT NULL DEFAULT 'other',
                explanation TEXT NOT NULL DEFAULT '',
                evidence_url TEXT NOT NULL DEFAULT '',
                reviewer_label TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                client_ip_hash TEXT NOT NULL DEFAULT '',
                evaluation_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                applied_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
                affected_jurisdictions JSONB NOT NULL DEFAULT '[]'::jsonb,
                rebuild_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                evaluated_at TIMESTAMPTZ,
                reverted_at TIMESTAMPTZ,
                reverted_by TEXT NOT NULL DEFAULT '',
                revert_reason TEXT NOT NULL DEFAULT '',
                revert_rebuild_status TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_error_reports_status_created
            ON network_error_reports (status, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_error_reports_jur_status
            ON network_error_reports (jurisdiction, status)
            """
        )
        # Idempotent schema upgrade for existing live databases
        for col_def in (
            "report_kind TEXT NOT NULL DEFAULT 'false_positive'",
            "related_target_type TEXT NOT NULL DEFAULT ''",
            "related_target_value TEXT NOT NULL DEFAULT ''",
            "related_target_id TEXT NOT NULL DEFAULT ''",
        ):
            cur.execute(f"ALTER TABLE network_error_reports ADD COLUMN IF NOT EXISTS {col_def}")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_error_report_reverts (
                id BIGSERIAL PRIMARY KEY,
                report_id BIGINT NOT NULL REFERENCES network_error_reports(id) ON DELETE CASCADE,
                reverted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                reverted_by TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                deactivated_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
                rebuild_jurisdictions JSONB NOT NULL DEFAULT '[]'::jsonb,
                rebuild_status TEXT NOT NULL DEFAULT 'queued'
            )
            """
        )
    conn.commit()


def _client_ip_hash(request: Request) -> str:
    ip = (
        request.headers.get("cf-connecting-ip")
        or (request.headers.get("x-forwarded-for") or "").split(",", 1)[0].strip()
        or (request.client.host if request.client else "127.0.0.1")
    )
    salt = os.environ.get("AUDIT_REVIEW_SECRET") or "theyownwhat-review-salt"
    return hashlib.sha256(f"{salt}:{ip}".encode("utf-8")).hexdigest()[:24]


def _check_rate_limit(ip_hash: str) -> None:
    now = time.time()
    q = _RATE_BUCKETS[ip_hash]
    while q and now - q[0] > _RATE_WINDOW_SEC:
        q.popleft()
    if len(q) >= _RATE_MAX_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded for reviewer reports. Please wait before submitting more.",
        )
    q.append(now)


def build_revert_report_url(
    base_url: str,
    report_id: int,
    *,
    reviewer: str = "salmunk@gmail.com",
    ttl_days: int = 30,
    secret: Optional[bytes] = None,
) -> str:
    """Build an HMAC-signed, prefetch-safe 1-click revert confirmation URL for the daily digest."""
    clean_base = str(base_url or "https://theyownwhat.net").strip().rstrip("/")
    item_key = f"review-report:{int(report_id)}"
    token = sign_review_token(
        item_key,
        reviewer=reviewer,
        ttl_days=ttl_days,
        secret=secret,
    )
    params = urlencode({"report_id": str(int(report_id)), "token": token})
    return f"{clean_base}/api/review-reports/revert?{params}"


class ReviewReportCreate(BaseModel):
    report_kind: str = Field(default="false_positive", max_length=32)
    jurisdiction: str = Field(..., min_length=2, max_length=32)
    network_id: str = Field(default="", max_length=120)
    network_key: str = Field(default="", max_length=160)
    network_name: str = Field(default="", max_length=240)
    target_type: str = Field(..., min_length=3, max_length=32)
    target_value: str = Field(..., min_length=1, max_length=300)
    target_id: str = Field(default="", max_length=160)
    related_target_type: str = Field(default="", max_length=32)
    related_target_value: str = Field(default="", max_length=300)
    related_target_id: str = Field(default="", max_length=160)
    bridge_context: Dict[str, Any] = Field(default_factory=dict)
    reason_code: str = Field(default="other", max_length=64)
    explanation: str = Field(default="", max_length=2000)
    evidence_url: str = Field(default="", max_length=600)
    reviewer_label: str = Field(default="", max_length=120)
    source_path: str = Field(default="", max_length=300)


class RevertReportBody(BaseModel):
    report_id: int
    token: str = ""
    reason: str = Field(default="", max_length=1000)
    skip_rebuild: bool = False


def _sanitize_bridge_context(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Keep bridge_context bounded and JSON-safe without bloating DB rows."""
    if not isinstance(ctx, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in list(ctx.items())[:25]:
        ks = str(k)[:60]
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[ks] = v[:300] if isinstance(v, str) else v
        elif isinstance(v, list):
            out[ks] = [str(item)[:200] for item in v[:30]]
        elif isinstance(v, dict):
            out[ks] = {str(ik)[:40]: str(iv)[:200] for ik, iv in list(v.items())[:15]}
    return out


def format_review_report_notification(
    *,
    report_id: int,
    report_kind: str,
    jurisdiction: str,
    network_name: str,
    network_key: str,
    network_id: str,
    target_type: str,
    target_value: str,
    target_id: str,
    related_target_type: str,
    related_target_value: str,
    related_target_id: str,
    reason_code: str,
    explanation: str,
    evidence_url: str,
    reviewer_label: str,
    source_path: str,
    bridge_context: Dict[str, Any],
    deduplicated: bool = False,
) -> Tuple[str, str, str]:
    """Build (subject, text_body, html_body) for /review feedback events."""
    submitted_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    jur_u = (jurisdiction or "ct").upper()
    kind_label = "Missed Connection" if report_kind == "missed_connection" else "False Positive"
    reason_label = REASON_LABELS.get(reason_code, reason_code)
    event_heading = (
        f"Additional note on open /review report #{int(report_id)}"
        if deduplicated
        else f"New /review {kind_label} report #{int(report_id)}"
    )
    subject = (
        f"[They Own WHAT?] Note added to /review report #{int(report_id)} ({jur_u}): {target_value[:60]}"
        if deduplicated
        else f"[They Own WHAT?] New /review {kind_label} #{int(report_id)} ({jur_u}): {target_value[:60]}"
    )

    source_url = ""
    if source_path:
        clean_p = source_path if source_path.startswith("/") else f"/{source_path}"
        source_url = f"https://theyownwhat.net{clean_p}"

    detail_rows: List[Tuple[str, str]] = [
        ("Report ID", f"#{int(report_id)}"),
        ("Event", "Appended Note (deduplicated within 24h)" if deduplicated else "New Reviewer Report"),
        ("Kind", f"{kind_label} ({report_kind})"),
        ("Jurisdiction", jur_u),
        ("Submitted", submitted_utc),
        ("Reviewer", reviewer_label or "(anonymous)"),
        ("Reason", f"{reason_label} [{reason_code}]"),
        ("Network", network_name or "(none)"),
        ("Network Key / ID", " / ".join(x for x in (network_key, network_id) if x) or "(none)"),
        ("Target", f"[{target_type}] {target_value}" + (f" (ID: {target_id})" if target_id else "")),
    ]
    if related_target_value:
        detail_rows.append((
            "Related Target (Side B)",
            f"[{related_target_type or 'entity'}] {related_target_value}"
            + (f" (ID: {related_target_id})" if related_target_id else ""),
        ))
    if evidence_url:
        detail_rows.append(("Evidence URL", evidence_url))
    if source_url:
        detail_rows.append(("Page URL", source_url))

    text_lines = [event_heading, ""]
    for label, val in detail_rows:
        text_lines.append(f"{label:24s} {val}")
    text_lines.append("")
    text_lines.append("Explanation:")
    text_lines.append(explanation or "(no explanation entered)")
    if bridge_context:
        text_lines.append("")
        text_lines.append("Bridge / UI Context:")
        text_lines.append(json.dumps(bridge_context, indent=2, sort_keys=True))
    text_lines.append("")
    text_lines.append("---")
    text_lines.append("Status: queued as `pending` for tonight's public-record verification pass.")
    text_body = "\n".join(text_lines)

    tr_html = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#475569;vertical-align:top;white-space:nowrap;'><strong>{html.escape(label)}</strong></td>"
        f"<td style='padding:4px 0;color:#0f172a;'>"
        + (
            f"<a href='{html.escape(val)}'>{html.escape(val)}</a>"
            if label in ("Evidence URL", "Page URL") and val.startswith(("http://", "https://"))
            else html.escape(val)
        )
        + "</td></tr>"
        for label, val in detail_rows
    )
    bridge_html = ""
    if bridge_context:
        bridge_html = (
            "<h3 style='margin:14px 0 6px;font-size:15px;'>Bridge / UI Context</h3>"
            f"<pre style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:10px 12px;font-size:12px;overflow-x:auto;'>"
            f"{html.escape(json.dumps(bridge_context, indent=2, sort_keys=True))}</pre>"
        )
    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #0f172a; max-width: 720px;">
      <h2 style="margin: 0 0 12px;">{html.escape(event_heading)} ({html.escape(jur_u)})</h2>
      <table style="border-collapse: collapse; width: 100%; margin-bottom: 14px; font-size: 14px;">
        {tr_html}
      </table>
      <h3 style="margin: 12px 0 6px; font-size: 15px;">Explanation</h3>
      <div style="white-space: pre-wrap; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 10px 12px; font-size: 14px;">{html.escape(explanation or '(no explanation entered)')}</div>
      {bridge_html}
      <p style="margin-top: 16px; font-size: 12px; color: #64748b;">
        Queued as <code>pending</code> in <code>network_error_reports</code> for tonight's public-record verification pass.
      </p>
    </div>
    """
    return subject, text_body, html_body


def format_revert_notification(result: Dict[str, Any], *, reverted_by: str, reason: str) -> Tuple[str, str, str]:
    """Build (subject, text_body, html_body) for reviewer report revert events."""
    rid = result.get("report_id")
    submitted_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[They Own WHAT?] Reverted /review report #{rid}"
    deactivated = result.get("deactivated_rules") or []
    jurs = result.get("rebuild_jurisdictions") or []
    rebuild_status = result.get("rebuild_status") or ""
    text_body = (
        f"Reviewer report #{rid} was reverted.\n\n"
        f"Reverted By:         {reverted_by or '(unknown)'}\n"
        f"Timestamp:           {submitted_utc}\n"
        f"Reason:              {reason or '(none provided)'}\n"
        f"Deactivated Rules:   {json.dumps(deactivated)}\n"
        f"Rebuild Jurisdictions: {', '.join(str(j).upper() for j in jurs) or '(none)'}\n"
        f"Rebuild Status:      {rebuild_status}\n"
    )
    html_body = (
        f"<div style='font-family:sans-serif;color:#0f172a;max-width:680px;'>"
        f"<h2>Reverted /review Report #{html.escape(str(rid))}</h2>"
        f"<p><strong>Reverted By:</strong> {html.escape(str(reverted_by or '(unknown)'))}<br>"
        f"<strong>Timestamp:</strong> {html.escape(submitted_utc)}<br>"
        f"<strong>Reason:</strong> {html.escape(str(reason or '(none provided)'))}<br>"
        f"<strong>Deactivated Rules:</strong> <code>{html.escape(json.dumps(deactivated))}</code><br>"
        f"<strong>Rebuild Jurisdictions:</strong> {html.escape(', '.join(str(j).upper() for j in jurs) or '(none)')}<br>"
        f"<strong>Rebuild Status:</strong> {html.escape(str(rebuild_status))}</p></div>"
    )
    return subject, text_body, html_body


@router.post("/api/review-reports")
def submit_review_report(payload: ReviewReportCreate, request: Request):
    """Queue a trusted-reviewer report for nightly public-record verification.

    Never mutates live networks directly — every report must pass evidence checks
    during the nightly evaluation pass before any rule is persisted.
    """
    ip_hash = _client_ip_hash(request)
    _check_rate_limit(ip_hash)

    jur = normalize_jurisdiction(payload.jurisdiction)
    r_kind = str(payload.report_kind or "false_positive").strip().lower()
    if r_kind not in VALID_REPORT_KINDS:
        raise HTTPException(status_code=400, detail=f"Unsupported report_kind: {r_kind}")

    t_type = str(payload.target_type or "").strip().lower()
    if t_type not in VALID_TARGET_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported target_type: {t_type}")

    r_code = str(payload.reason_code or ("missed_connection_other" if r_kind == "missed_connection" else "other")).strip().lower()
    if r_code not in VALID_REASON_CODES:
        r_code = "missed_connection_other" if r_kind == "missed_connection" else "other"

    target_val = " ".join(str(payload.target_value or "").strip().split())
    if not target_val:
        raise HTTPException(status_code=400, detail="target_value is required")

    rel_type = str(payload.related_target_type or "").strip().lower()
    rel_val = " ".join(str(payload.related_target_value or "").strip().split())
    if r_kind == "missed_connection":
        if not rel_val:
            raise HTTPException(status_code=400, detail="related_target_value is required for missed_connection reports")
        if rel_type not in VALID_TARGET_TYPES:
            rel_type = "entity"
    elif rel_type and rel_type not in VALID_TARGET_TYPES:
        rel_type = ""

    ev_url = str(payload.evidence_url or "").strip()
    if ev_url and not (ev_url.startswith("https://") or ev_url.startswith("http://")):
        raise HTTPException(status_code=400, detail="evidence_url must start with http:// or https://")

    safe_ctx = _sanitize_bridge_context(payload.bridge_context)

    conn = _connect()
    try:
        ensure_reviewer_feedback_tables(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Deduplicate identical pending reports within 24 hours
            cur.execute(
                """
                SELECT id, status, created_at
                FROM network_error_reports
                WHERE status = 'pending'
                  AND report_kind = %s
                  AND jurisdiction = %s
                  AND network_id = %s
                  AND target_type = %s
                  AND UPPER(target_value) = UPPER(%s)
                  AND UPPER(related_target_value) = UPPER(%s)
                  AND created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY id DESC
                LIMIT 1
                """,
                (r_kind, jur, payload.network_id.strip(), t_type, target_val, rel_val),
            )
            existing = cur.fetchone()
            if existing:
                # Merge any additional explanation/evidence into the pending report
                if payload.explanation.strip():
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET explanation = CASE
                                WHEN explanation = '' THEN %s
                                WHEN position(%s in explanation) > 0 THEN explanation
                                ELSE explanation || E'\n---\n' || %s
                            END,
                            evidence_url = CASE
                                WHEN evidence_url = '' AND %s <> '' THEN %s
                                ELSE evidence_url
                            END,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            payload.explanation.strip(),
                            payload.explanation.strip()[:120],
                            payload.explanation.strip(),
                            ev_url,
                            ev_url,
                            existing["id"],
                        ),
                    )
                    conn.commit()
                subj, txt_body, html_body = format_review_report_notification(
                    report_id=int(existing["id"]),
                    report_kind=r_kind,
                    jurisdiction=jur,
                    network_name=payload.network_name.strip(),
                    network_key=payload.network_key.strip(),
                    network_id=payload.network_id.strip(),
                    target_type=t_type,
                    target_value=target_val,
                    target_id=payload.target_id.strip(),
                    related_target_type=rel_type,
                    related_target_value=rel_val,
                    related_target_id=payload.related_target_id.strip(),
                    reason_code=r_code,
                    explanation=payload.explanation.strip(),
                    evidence_url=ev_url,
                    reviewer_label=payload.reviewer_label.strip(),
                    source_path=payload.source_path.strip(),
                    bridge_context=safe_ctx,
                    deduplicated=True,
                )
                spawn_feedback_notification_email(subj, txt_body, html_body=html_body)
                return {
                    "ok": True,
                    "report_id": int(existing["id"]),
                    "status": "pending",
                    "deduplicated": True,
                    "message": (
                        "Added your note to an open report for this item. "
                        "It will be checked against public records in tonight's verification pass."
                    ),
                }

            cur.execute(
                """
                INSERT INTO network_error_reports (
                    status, report_kind, jurisdiction, network_id, network_key, network_name,
                    target_type, target_value, target_id,
                    related_target_type, related_target_value, related_target_id,
                    bridge_context,
                    reason_code, explanation, evidence_url, reviewer_label,
                    source_path, client_ip_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                RETURNING id, status, created_at
                """,
                (
                    "pending",
                    r_kind,
                    jur,
                    payload.network_id.strip(),
                    payload.network_key.strip(),
                    payload.network_name.strip(),
                    t_type,
                    target_val,
                    payload.target_id.strip(),
                    rel_type,
                    rel_val,
                    payload.related_target_id.strip(),
                    json.dumps(safe_ctx),
                    r_code,
                    payload.explanation.strip(),
                    ev_url,
                    payload.reviewer_label.strip(),
                    payload.source_path.strip(),
                    ip_hash,
                ),
            )
            row = cur.fetchone()
        conn.commit()
        subj, txt_body, html_body = format_review_report_notification(
            report_id=int(row["id"]),
            report_kind=r_kind,
            jurisdiction=jur,
            network_name=payload.network_name.strip(),
            network_key=payload.network_key.strip(),
            network_id=payload.network_id.strip(),
            target_type=t_type,
            target_value=target_val,
            target_id=payload.target_id.strip(),
            related_target_type=rel_type,
            related_target_value=rel_val,
            related_target_id=payload.related_target_id.strip(),
            reason_code=r_code,
            explanation=payload.explanation.strip(),
            evidence_url=ev_url,
            reviewer_label=payload.reviewer_label.strip(),
            source_path=payload.source_path.strip(),
            bridge_context=safe_ctx,
            deduplicated=False,
        )
        spawn_feedback_notification_email(subj, txt_body, html_body=html_body)
        return {
            "ok": True,
            "report_id": int(row["id"]),
            "report_kind": r_kind,
            "status": str(row["status"]),
            "deduplicated": False,
            "message": (
                "Missed connection recorded. Tonight's verification job will inspect both sides in public records, "
                "look for an organic builder tweak (e.g. address/abbreviation/role normalization or over-blocked HQ allowlist) "
                "that makes the link naturally across jurisdictions, rebuild affected networks, and summarize in the daily digest."
                if r_kind == "missed_connection"
                else "Report recorded. Tonight's verification job will check the underlying public filings, "
                "generalize any confirmed logic fix across jurisdictions, rebuild affected networks, "
                "and summarize the outcome in the daily digest."
            ),
        }
    finally:
        conn.close()


def _spawn_revert_rebuild(jurisdictions: List[str], report_id: int) -> str:
    """Spawn a detached rebuild for affected jurisdictions after reverting a learned rule."""
    clean_jurs = [normalize_jurisdiction(j) for j in jurisdictions if j]
    clean_jurs = sorted(set(clean_jurs))
    if not clean_jurs:
        return "skipped"
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script_path = os.path.join(repo_root, "updater", "evaluate_reviewer_reports.py")
    if not os.path.exists(script_path):
        return "script_missing"
    try:
        cmd = [
            sys.executable,
            script_path,
            "--rebuild-only",
            "--jurisdictions",
            ",".join(clean_jurs),
            "--revert-report-id",
            str(int(report_id)),
        ]
        subprocess.Popen(
            cmd,
            cwd=repo_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return "running"
    except Exception as exc:
        logger.error("Failed to spawn revert rebuild for report %s: %s", report_id, exc)
        return f"spawn_failed: {exc}"


def revert_report_in_db(
    conn,
    report_id: int,
    *,
    reverted_by: str,
    reason: str = "",
    trigger_rebuild: bool = True,
    json_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministically deactivate all learned rules applied for `report_id` and record audit trail."""
    ensure_reviewer_feedback_tables(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM network_error_reports WHERE id = %s", (int(report_id),))
        report = cur.fetchone()
    if not report:
        raise HTTPException(status_code=404, detail=f"Review report #{report_id} not found.")

    if str(report.get("status") or "").lower() == "reverted":
        return {
            "ok": True,
            "already_reverted": True,
            "report_id": int(report_id),
            "status": "reverted",
            "deactivated_rules": report.get("applied_rules") or [],
            "rebuild_status": report.get("revert_rebuild_status") or "completed",
            "message": f"Report #{report_id} was already reverted on {report.get('reverted_at')}.",
        }

    applied_rules = report.get("applied_rules") if isinstance(report.get("applied_rules"), list) else []
    affected_jurs = report.get("affected_jurisdictions") if isinstance(report.get("affected_jurisdictions"), list) else []
    if not affected_jurs and report.get("jurisdiction"):
        affected_jurs = [str(report["jurisdiction"])]

    deactivated: List[Dict[str, Any]] = []
    for rule in applied_rules:
        if not isinstance(rule, dict):
            continue
        rt = str(rule.get("rule_type") or "").strip()
        rk = str(rule.get("rule_key") or "").strip()
        sc = str(rule.get("scope") or "global").strip()
        if not rt or not rk:
            continue
        kwargs: Dict[str, Any] = {"scope": sc}
        if json_path:
            kwargs["json_path"] = json_path
        deactivate_learned_rule(conn, rt, rk, **kwargs)
        deactivated.append({"rule_type": rt, "rule_key": rk, "scope": sc})

    rebuild_status = "skipped"
    if trigger_rebuild and deactivated:
        rebuild_status = _spawn_revert_rebuild(affected_jurs, int(report_id))

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE network_error_reports
            SET status = 'reverted',
                reverted_at = NOW(),
                reverted_by = %s,
                revert_reason = %s,
                revert_rebuild_status = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (reverted_by, reason.strip(), rebuild_status, int(report_id)),
        )
        cur.execute(
            """
            INSERT INTO network_error_report_reverts
                (report_id, reverted_by, reason, deactivated_rules, rebuild_jurisdictions, rebuild_status)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s)
            """,
            (
                int(report_id),
                reverted_by,
                reason.strip(),
                json.dumps(deactivated),
                json.dumps(affected_jurs),
                rebuild_status,
            ),
        )
    conn.commit()

    return {
        "ok": True,
        "already_reverted": False,
        "report_id": int(report_id),
        "status": "reverted",
        "deactivated_rules": deactivated,
        "affected_jurisdictions": affected_jurs,
        "rebuild_status": rebuild_status,
        "message": (
            f"Deactivated {len(deactivated)} learned rule(s) for report #{report_id}"
            + (
                f" and queued network rebuild for {', '.join(j.upper() for j in affected_jurs)}."
                if rebuild_status == "running"
                else "."
            )
        ),
    }


REVERT_PAGE_JS = r"""
(function () {
  const form = document.getElementById('revert-form');
  const statusBox = document.getElementById('status-box');
  const submitBtn = document.getElementById('revert-submit-btn');
  if (!form || !statusBox) return;

  function setStatus(kind, text) {
    statusBox.className = 'status-box ' + kind;
    statusBox.textContent = text;
    statusBox.hidden = false;
  }

  form.addEventListener('submit', async function (event) {
    event.preventDefault();
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = 'Reverting rule & queueing rebuild…';
    }
    setStatus('warn', 'Deactivating learned rule(s) and restoring prior network logic…');
    try {
      const payload = {
        report_id: Number(form.dataset.reportId || 0),
        token: form.dataset.token || '',
        reason: (document.getElementById('revert-reason')?.value || '').trim(),
      };
      const res = await fetch('/api/review-reports/revert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.detail || ('HTTP ' + res.status));
      }
      setStatus('ok', data.message || 'Change reverted cleanly.');
      if (submitBtn) {
        submitBtn.textContent = 'Reverted';
      }
    } catch (err) {
      setStatus('err', 'Revert failed: ' + (err && err.message ? err.message : String(err)));
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Confirm Revert & Rebuild Network';
      }
    }
  });
})();
""".strip()


@router.get("/api/review-reports/revert.js")
def serve_revert_js():
    return Response(
        content=REVERT_PAGE_JS,
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-store", "Content-Security-Policy": CSP_HEADER},
    )


@router.get("/api/review-reports/revert", response_class=HTMLResponse)
def get_revert_confirmation_page(
    request: Request,
    report_id: int = Query(..., ge=1),
    token: str = Query(""),
):
    """Prefetch-safe GET confirmation page for 1-click revert links in the daily digest.
    Zero state mutation occurs on GET so email link scanners cannot trigger rollbacks."""
    expected_key = f"review-report:{int(report_id)}"
    ok, reviewer, err = _authorize_request(request, token, expected_item_key=expected_key)
    if not ok:
        return HTMLResponse(
            content=f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="robots" content="noindex,nofollow">
            <title>Revert Link Invalid — They Own WHAT??</title></head>
            <body style="font-family:system-ui,sans-serif;padding:2rem;background:#0f172a;color:#f8fafc;">
            <main style="max-width:560px;margin:auto;background:#1e293b;padding:1.5rem;border-radius:12px;border:1px solid #334155;">
            <h1 style="margin-top:0;font-size:1.25rem;">Unable to Authorize Revert Link</h1>
            <p style="color:#fca5a5;">{html.escape(err)}</p>
            </main></body></html>""",
            status_code=403,
            headers={"Content-Security-Policy": CSP_HEADER, "X-Robots-Tag": "noindex, nofollow"},
        )

    conn = _connect()
    try:
        ensure_reviewer_feedback_tables(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM network_error_reports WHERE id = %s", (int(report_id),))
            report = cur.fetchone()
    finally:
        conn.close()

    if not report:
        raise HTTPException(status_code=404, detail=f"Review report #{report_id} not found.")

    status = str(report.get("status") or "pending")
    is_reverted = status == "reverted"
    applied_rules = report.get("applied_rules") if isinstance(report.get("applied_rules"), list) else []
    eval_sum = report.get("evaluation_summary") if isinstance(report.get("evaluation_summary"), dict) else {}
    rebuild_sum = report.get("rebuild_summary") if isinstance(report.get("rebuild_summary"), dict) else {}
    affected_jurs = report.get("affected_jurisdictions") if isinstance(report.get("affected_jurisdictions"), list) else []

    rules_lis = "".join(
        f"<li><code>{html.escape(str(r.get('rule_type') or ''))}</code>: "
        f"<strong>{html.escape(str(r.get('rule_key') or ''))}</strong> "
        f"(scope: {html.escape(str(r.get('scope') or 'global'))})</li>"
        for r in applied_rules
        if isinstance(r, dict)
    ) or "<li>No persistent linking rules were attached to this report.</li>"

    verdict_reason = str(eval_sum.get("rationale") or eval_sum.get("summary") or "No evaluation summary recorded.")
    jur_label = str(report.get("jurisdiction") or "").upper()
    reason_human = REASON_LABELS.get(str(report.get("reason_code") or ""), str(report.get("reason_code") or ""))

    before_after = ""
    if rebuild_sum:
        before_after = f"<p><strong>Rebuild outcome:</strong> {html.escape(json.dumps(rebuild_sum, default=str)[:450])}</p>"

    action_block = (
        f'<div id="status-box" class="status-box ok">This report was already reverted on {html.escape(str(report.get("reverted_at") or ""))} by {html.escape(str(report.get("reverted_by") or "maintainer"))}.</div>'
        if is_reverted
        else f"""
        <div id="status-box" class="status-box" hidden></div>
        <form id="revert-form" data-report-id="{int(report_id)}" data-token="{html.escape(token)}">
          <label for="revert-reason" class="block-label">Optional note (why you disagreed with this automated change):</label>
          <textarea id="revert-reason" name="reason" placeholder="e.g., These two entities really are co-owned by the same operator..."></textarea>
          <div class="actions">
            <button id="revert-submit-btn" type="submit" class="btn-danger">Confirm Revert &amp; Rebuild Network</button>
          </div>
        </form>
        """
    )

    page_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow" />
  <title>Revert Automated Rule Change #{int(report_id)} — They Own WHAT??</title>
  <style>
    :root {{ color-scheme: dark light; --bg:#0f172a; --card:#1e293b; --surface:#0b1220; --border:#334155; --text:#f8fafc; --muted:#94a3b8; }}
    @media (prefers-color-scheme: light) {{ :root {{ --bg:#f1f5f9; --card:#ffffff; --surface:#f8fafc; --border:#cbd5e1; --text:#0f172a; --muted:#475569; }} }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; padding:1.25rem; font-family:system-ui,-apple-system,sans-serif; background:var(--bg); color:var(--text); line-height:1.5; }}
    main {{ width:100%; max-width:680px; background:var(--card); border:1px solid var(--border); border-radius:16px; padding:1.5rem; box-shadow:0 18px 40px rgba(0,0,0,0.22); }}
    .eyebrow {{ font-size:0.78rem; font-weight:700; letter-spacing:0.06em; text-transform:uppercase; color:var(--muted); margin:0 0 0.35rem; }}
    h1 {{ font-size:1.3rem; margin:0 0 0.75rem; }}
    .context-card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:1rem; font-size:0.93rem; margin-bottom:1rem; }}
    .block-label {{ display:block; font-weight:700; font-size:0.92rem; margin:0.75rem 0 0.35rem; }}
    textarea {{ width:100%; min-height:92px; padding:0.7rem 0.8rem; border-radius:10px; border:1px solid var(--border); background:var(--surface); color:var(--text); font:inherit; }}
    .actions {{ margin-top:1rem; display:flex; gap:0.75rem; flex-wrap:wrap; }}
    .btn-danger {{ background:#dc2626; color:#fff; border:none; border-radius:10px; padding:0.75rem 1.25rem; font-weight:700; font-size:0.96rem; cursor:pointer; }}
    .btn-danger:hover {{ background:#b91c1c; }}
    .btn-danger:disabled {{ opacity:0.6; cursor:not-allowed; }}
    .status-box {{ padding:0.9rem 1rem; border-radius:12px; border:2px solid var(--border); margin:0 0 1rem; font-weight:600; }}
    .status-box.ok {{ background:rgba(16,185,129,0.16); border-color:#10b981; color:#6ee7b7; }}
    .status-box.warn {{ background:rgba(245,158,11,0.16); border-color:#f59e0b; color:#fde68a; }}
    .status-box.err {{ background:rgba(239,68,68,0.16); border-color:#ef4444; color:#fca5a5; }}
    code {{ background:rgba(148,163,184,0.18); padding:0.12rem 0.35rem; border-radius:4px; font-size:0.88em; }}
  </style>
</head>
<body>
  <main>
    <p class="eyebrow">They Own WHAT?? &middot; Reviewer Feedback Governance</p>
    <h1>Revert Automated Logic Change (Report #{int(report_id)})</h1>
    <section class="context-card">
      <p style="margin:0 0 0.4rem;"><strong>Jurisdiction:</strong> {html.escape(jur_label)} &nbsp;&middot;&nbsp; <strong>Type:</strong> <code>{html.escape(str(report.get('report_kind') or 'false_positive'))}</code> &nbsp;&middot;&nbsp; <strong>Status:</strong> <code>{html.escape(status)}</code></p>
      <p style="margin:0 0 0.4rem;"><strong>Network:</strong> {html.escape(str(report.get('network_name') or report.get('network_id') or '—'))}</p>
      {f'<p style="margin:0 0 0.4rem;"><strong>Reported missed link:</strong> {html.escape(str(report.get("target_value") or "—"))} &harr; {html.escape(str(report.get("related_target_value") or "—"))}</p>' if str(report.get('report_kind') or '') == 'missed_connection' else f'<p style="margin:0 0 0.4rem;"><strong>Flagged {html.escape(str(report.get("target_type") or "item"))}:</strong> {html.escape(str(report.get("target_value") or "—"))}</p>'}
      <p style="margin:0 0 0.4rem;"><strong>Reporter reason:</strong> {html.escape(reason_human)} {f'({html.escape(str(report.get("reviewer_label")))})' if report.get("reviewer_label") else ''}</p>
      {f'<p style="margin:0 0 0.4rem;"><strong>Reporter note:</strong> {html.escape(str(report.get("explanation")))}</p>' if report.get("explanation") else ''}
      <hr style="border:none;border-top:1px solid var(--border);margin:0.75rem 0;" />
      <p style="margin:0 0 0.4rem;"><strong>Nightly verification findings:</strong> {html.escape(verdict_reason)}</p>
      <p style="margin:0.5rem 0 0.25rem;"><strong>Generalized rule(s) to deactivate:</strong></p>
      <ul style="margin:0 0 0.5rem;padding-left:1.25rem;">{rules_lis}</ul>
      <p style="margin:0 0 0.25rem;"><strong>Jurisdictions affected on revert:</strong> {html.escape(', '.join(str(j).upper() for j in affected_jurs) or jur_label)}</p>
      {before_after}
    </section>
    {action_block}
    <p style="font-size:0.82rem;color:var(--muted);margin-top:1rem;">Authorized for {html.escape(reviewer)}. Opening this page makes no changes until you click Confirm.</p>
  </main>
  <script src="/api/review-reports/revert.js" defer></script>
</body>
</html>"""
    return HTMLResponse(
        content=page_html,
        headers={"Content-Security-Policy": CSP_HEADER, "X-Robots-Tag": "noindex, nofollow"},
    )


@router.post("/api/review-reports/revert")
def post_revert_report(body: RevertReportBody, request: Request):
    """Execute an HMAC-authorized revert: deactivate applied rules & trigger rebuild."""
    expected_key = f"review-report:{int(body.report_id)}"
    ok, reviewer, err = _authorize_request(request, body.token, expected_item_key=expected_key)
    if not ok:
        raise HTTPException(status_code=403, detail=err)

    conn = _connect()
    try:
        result = revert_report_in_db(
            conn,
            int(body.report_id),
            reverted_by=reviewer,
            reason=body.reason,
            trigger_rebuild=not body.skip_rebuild,
        )
        subj, txt_body, html_body = format_revert_notification(
            result, reverted_by=reviewer, reason=body.reason
        )
        spawn_feedback_notification_email(subj, txt_body, html_body=html_body)
        return JSONResponse(content=result)
    finally:
        conn.close()


@router.get("/api/review-reports/recent")
def list_recent_review_reports(
    request: Request,
    token: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
):
    """Maintainer-only endpoint returning recent reviewer feedback reports and evaluation outcomes."""
    ok, _, err = _authorize_request(request, token, expected_item_key="*")
    if not ok:
        raise HTTPException(status_code=403, detail=err)

    conn = _connect()
    try:
        ensure_reviewer_feedback_tables(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, created_at, updated_at, status, jurisdiction,
                       network_id, network_key, network_name,
                       target_type, target_value, target_id,
                       reason_code, explanation, evidence_url, reviewer_label,
                       evaluation_summary, applied_rules, affected_jurisdictions,
                       rebuild_summary, evaluated_at, reverted_at, reverted_by,
                       revert_reason, revert_rebuild_status
                FROM network_error_reports
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall()
        return {"ok": True, "count": len(rows), "reports": rows}
    finally:
        conn.close()
