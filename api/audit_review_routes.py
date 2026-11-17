"""
1-Click Email Review & Automated Fix-Queue Endpoints (`api/audit_review_routes.py`)
===================================================================================
Mirrors the Renter Toolkit (`admin.renter.help/quick-decision`) maintainer review
and self-healing fix-queue workflow for They Own WHAT??:

- `GET  /api/audit-review/quick-decision`     -> Safe against email link prefetchers
  (zero state mutation on HTTP GET); renders an accessible, CSP-strict confirmation &
  reason-entry form pre-populated from the HMAC-signed email link.
- `GET  /api/audit-review/quick-decision.js`  -> External script (`defer`, no inline JS).
- `POST /api/audit-review/act`                -> Executes `approve`, `reject` (adds an
  `OPEN` item to `docs/review-fix-queue.json`), or `undo`, persisting to both JSON
  review ledgers in `docs/` and PostgreSQL `audit_review_decisions`.
- `GET  /api/audit-review/queue`              -> Displays all open/resolved fix-queue
  items and active weekly review sign-offs (HTML or JSON).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

logger = logging.getLogger("they-own-what.audit-review")

router = APIRouter(prefix="/api/audit-review", tags=["audit-review"])

_FILE_LOCK = threading.Lock()

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DECISIONS_PATH = ROOT_DIR / "docs" / "weekly-review-decisions.json"
DEFAULT_FIX_QUEUE_PATH = ROOT_DIR / "docs" / "review-fix-queue.json"
DEFAULT_LATEST_AUDIT_PATH = ROOT_DIR / "docs" / "benchmarks" / "latest-weekly-gemini-audit.json"

CSP_HEADER = (
    "default-src 'self'; "
    "img-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; "
    "script-src 'self'; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)

PRESET_REJECTION_REASONS: Tuple[str, ...] = (
    "Broken or wrong URL / data source",
    "Third-party / false-positive entity merge",
    "Inaccurate limitations or UI copy",
    "Confirmed code or query bug — fix & add regression test",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: Optional[datetime] = None) -> str:
    target = dt or _utc_now()
    return target.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_review_signing_secret(env: Optional[Dict[str, str]] = None) -> bytes:
    """
    Derive a deterministic HMAC-SHA256 signing key from AUDIT_REVIEW_SECRET or
    ALERT_EMAIL_PASS / MAIL_PASSWORD so host CLI scripts, ctdata_api containers,
    and GitHub Actions `render-from-latest` runs sign and verify the exact same
    1-click email links without requiring duplicate secret setup.
    """
    source_env = env if env is not None else os.environ
    explicit = (source_env.get("AUDIT_REVIEW_SECRET") or "").strip()
    if explicit:
        return hashlib.sha256(f"tow-audit-review-v1:{explicit}".encode("utf-8")).digest()

    mail_secret = "".join(
        (source_env.get("ALERT_EMAIL_PASS") or source_env.get("MAIL_PASSWORD") or "").split()
    )
    if mail_secret:
        return hashlib.sha256(f"tow-audit-review-mail-v1:{mail_secret}".encode("utf-8")).digest()

    gemini_secret = (source_env.get("GEMINI_KEY") or source_env.get("GEMINI_API_KEY") or "").strip()
    if gemini_secret:
        return hashlib.sha256(f"tow-audit-review-gemini-v1:{gemini_secret}".encode("utf-8")).digest()

    return hashlib.sha256(b"tow-audit-review-derived-v1:theyownwhat-local-dev-only-secret").digest()


def sign_review_token(
    item_key: str,
    *,
    reviewer: str = "salmunk@gmail.com",
    ttl_days: int = 30,
    now_ts: Optional[int] = None,
    secret: Optional[bytes] = None,
) -> str:
    """Create a URL-safe HMAC-SHA256 token bound to `item_key` (or `*` for dashboard/all)."""
    issued = int(now_ts if now_ts is not None else time.time())
    exp = issued + max(1, int(ttl_days)) * 86400
    payload_obj = {
        "k": str(item_key or "").strip(),
        "r": str(reviewer or "salmunk@gmail.com").strip(),
        "exp": exp,
    }
    payload_bytes = json.dumps(payload_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("ascii").rstrip("=")
    key = secret if secret is not None else get_review_signing_secret()
    sig = hmac.new(key, payload_b64.encode("ascii"), hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).decode("ascii").rstrip("=")
    return f"{payload_b64}.{sig_b64}"


def verify_review_token(
    token: str,
    *,
    expected_item_key: Optional[str] = None,
    now_ts: Optional[int] = None,
    secret: Optional[bytes] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Verify an HMAC-signed email review token."""
    clean = str(token or "").strip()
    if not clean or "." not in clean:
        return False, None, "Missing or malformed review signature token."
    payload_b64, sig_b64 = clean.split(".", 1)
    if not payload_b64 or not sig_b64:
        return False, None, "Malformed review token segments."

    key = secret if secret is not None else get_review_signing_secret()
    expected_sig = hmac.new(key, payload_b64.encode("ascii"), hashlib.sha256).digest()
    expected_sig_b64 = base64.urlsafe_b64encode(expected_sig).decode("ascii").rstrip("=")
    if not hmac.compare_digest(sig_b64, expected_sig_b64):
        return False, None, "Invalid review token signature."

    try:
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception:
        return False, None, "Invalid review token payload encoding."

    now_val = int(now_ts if now_ts is not None else time.time())
    exp = int(payload.get("exp") or 0)
    if exp < now_val:
        return False, None, "This email review link has expired. Open the latest weekly audit email."

    tok_key = str(payload.get("k") or "").strip()
    if expected_item_key and tok_key not in ("*", expected_item_key):
        return False, None, "Token item_key does not match the requested review item."

    return True, payload, ""


def build_quick_decision_url(
    base_url: str,
    *,
    decision: str,
    item_key: str,
    title: str = "",
    domain: str = "",
    jurisdiction: str = "",
    reason: str = "",
    reviewer: str = "salmunk@gmail.com",
    ttl_days: int = 30,
    secret: Optional[bytes] = None,
) -> str:
    clean_base = str(base_url or "https://theyownwhat.net").strip().rstrip("/")
    token = sign_review_token(
        item_key,
        reviewer=reviewer,
        ttl_days=ttl_days,
        secret=secret,
    )
    params: Dict[str, str] = {
        "decision": decision,
        "item_key": item_key,
        "token": token,
    }
    if title:
        params["title"] = title[:180]
    if domain:
        params["domain"] = domain[:80]
    if jurisdiction:
        params["jurisdiction"] = jurisdiction[:40]
    if reason:
        params["reason"] = reason[:180]
    return f"{clean_base}/api/audit-review/quick-decision?{urlencode(params)}"


def _is_direct_local_request(request: Request) -> bool:
    host_hdr = (request.headers.get("host") or "").split(":", 1)[0].strip().lower()
    if host_hdr not in ("localhost", "127.0.0.1", "[::1]"):
        return False
    forwarded = (request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for") or "").strip()
    if forwarded and forwarded.split(",", 1)[0].strip() not in ("127.0.0.1", "::1"):
        return False
    return True


def _authorize_request(
    request: Request,
    token: str,
    expected_item_key: Optional[str] = None,
) -> Tuple[bool, str, str]:
    if token:
        ok, payload, err = verify_review_token(token, expected_item_key=expected_item_key)
        if ok and payload:
            return True, str(payload.get("r") or "salmunk@gmail.com"), ""
        return False, "", err
    if _is_direct_local_request(request):
        return True, "local-maintainer", ""
    return False, "", "Missing signed email review token."


def _read_json_file(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)


def _write_json_file(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def load_review_state(
    *,
    decisions_path: Path = DEFAULT_DECISIONS_PATH,
    fix_queue_path: Path = DEFAULT_FIX_QUEUE_PATH,
    latest_audit_path: Path = DEFAULT_LATEST_AUDIT_PATH,
) -> Dict[str, Any]:
    decisions_doc = _read_json_file(decisions_path, {"schemaVersion": 1, "decisions": []})
    fix_doc = _read_json_file(fix_queue_path, {"schemaVersion": 1, "items": []})
    audit_doc = _read_json_file(latest_audit_path, {})

    decisions = [d for d in (decisions_doc.get("decisions") or []) if isinstance(d, dict)]
    fix_items = [i for i in (fix_doc.get("items") or []) if isinstance(i, dict)]
    open_fixes = [i for i in fix_items if str(i.get("status", "")).upper() == "OPEN"]
    resolved_fixes = [i for i in fix_items if str(i.get("status", "")).upper() == "RESOLVED"]
    review_items = [r for r in (audit_doc.get("review_items") or []) if isinstance(r, dict)]

    return {
        "decisions": decisions,
        "fixQueue": {
            "allItems": fix_items,
            "openItems": open_fixes,
            "resolvedItems": resolved_fixes,
        },
        "latestAudit": {
            "generatedAt": audit_doc.get("generated_at"),
            "model": audit_doc.get("gemini_model"),
            "headline": audit_doc.get("headline"),
            "reviewItems": review_items,
        },
    }


def _persist_db_decision_best_effort(
    *,
    item_key: str,
    decision: str,
    domain: str,
    jurisdiction: str,
    title: str,
    note: str,
    reviewer: str,
    fix_id: Optional[str] = None,
) -> None:
    try:
        import psycopg2

        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            return
        conn = psycopg2.connect(db_url, connect_timeout=3)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS audit_review_decisions (
                            id SERIAL PRIMARY KEY,
                            item_key TEXT NOT NULL,
                            decision TEXT NOT NULL,
                            domain TEXT,
                            jurisdiction TEXT,
                            title TEXT,
                            note TEXT,
                            reviewer TEXT,
                            fix_id TEXT,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                        """
                    )
                    cur.execute(
                        """
                        INSERT INTO audit_review_decisions
                            (item_key, decision, domain, jurisdiction, title, note, reviewer, fix_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (item_key, decision, domain, jurisdiction, title, note, reviewer, fix_id),
                    )
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Optional audit_review_decisions DB log skipped: %s", exc)


def record_review_decision(
    *,
    decision: str,
    item_key: str,
    title: str = "",
    domain: str = "Weekly Audit",
    jurisdiction: str = "",
    preset_reasons: Optional[List[str]] = None,
    explanation: str = "",
    reviewer: str = "salmunk@gmail.com",
    undo_descriptor: Optional[Dict[str, Any]] = None,
    max_age_days: int = 90,
    decisions_path: Path = DEFAULT_DECISIONS_PATH,
    fix_queue_path: Path = DEFAULT_FIX_QUEUE_PATH,
    latest_audit_path: Path = DEFAULT_LATEST_AUDIT_PATH,
) -> Dict[str, Any]:
    """
    Execute `approve`, `reject`, or `undo` atomically against
    `docs/weekly-review-decisions.json` and `docs/review-fix-queue.json`.
    """
    clean_decision = str(decision or "").strip().lower()
    clean_key = str(item_key or "").strip()
    now_iso = _iso_utc()
    expires_iso = _iso_utc(_utc_now() + timedelta(days=max(1, int(max_age_days))))

    with _FILE_LOCK:
        decisions_doc = _read_json_file(decisions_path, {"schemaVersion": 1, "decisions": []})
        fix_doc = _read_json_file(fix_queue_path, {"schemaVersion": 1, "items": []})
        audit_doc = _read_json_file(latest_audit_path, {})

        decisions_list: List[Dict[str, Any]] = [
            d for d in (decisions_doc.get("decisions") or []) if isinstance(d, dict)
        ]
        fix_items: List[Dict[str, Any]] = [
            i for i in (fix_doc.get("items") or []) if isinstance(i, dict)
        ]
        audit_items_by_key = {
            str(r.get("item_key") or ""): r
            for r in (audit_doc.get("review_items") or [])
            if isinstance(r, dict) and r.get("item_key")
        }

        matched_item = audit_items_by_key.get(clean_key, {})
        resolved_title = (title or matched_item.get("title") or clean_key or "Weekly audit review item").strip()
        resolved_domain = (domain or matched_item.get("domain") or "Weekly Audit").strip()
        resolved_jur = (jurisdiction or matched_item.get("jurisdiction") or "").strip()

        if clean_decision == "approve":
            if clean_key == "approve-all-pending":
                keys_to_approve = list(audit_items_by_key.keys()) or ["approve-all-pending"]
            else:
                keys_to_approve = [clean_key]

            decisions_list = [d for d in decisions_list if d.get("itemKey") not in set(keys_to_approve)]
            for k in keys_to_approve:
                m_item = audit_items_by_key.get(k, {})
                decisions_list.append(
                    {
                        "itemKey": k,
                        "disposition": "approved",
                        "title": m_item.get("title") or resolved_title,
                        "domain": m_item.get("domain") or resolved_domain,
                        "jurisdiction": m_item.get("jurisdiction") or resolved_jur,
                        "reviewer": reviewer,
                        "reviewedAt": now_iso,
                        "expiresAt": expires_iso,
                        "note": explanation.strip() or "Approved via 1-click weekly audit email review.",
                    }
                )
            decisions_doc["schemaVersion"] = 1
            decisions_doc["updatedAt"] = now_iso
            decisions_doc["decisions"] = decisions_list
            _write_json_file(decisions_path, decisions_doc)

            _persist_db_decision_best_effort(
                item_key=",".join(keys_to_approve),
                decision="approve",
                domain=resolved_domain,
                jurisdiction=resolved_jur,
                title=resolved_title,
                note=explanation.strip() or "Approved via 1-click email review.",
                reviewer=reviewer,
            )
            return {
                "ok": True,
                "decision": "approve",
                "summary": (
                    f"Approved all {len(keys_to_approve)} pending weekly review item(s) ({max_age_days}-day sign-off window)."
                    if clean_key == "approve-all-pending"
                    else f'Approved "{resolved_title}" ({max_age_days}-day sign-off window).'
                ),
                "undoDescriptor": {"type": "decisions", "itemKeys": keys_to_approve},
            }

        if clean_decision == "reject":
            reasons_clean = [str(r).strip() for r in (preset_reasons or []) if str(r).strip()]
            note_parts = []
            if reasons_clean:
                note_parts.append(f"[{'; '.join(reasons_clean)}]")
            if explanation.strip():
                note_parts.append(explanation.strip())
            final_explanation = (
                " ".join(note_parts).strip()
                or "Declined via 1-click weekly audit email — investigate and remediate in automated optimization run."
            )

            fix_id = f"fix-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
            new_fix = {
                "id": fix_id,
                "status": "OPEN",
                "itemKey": clean_key or f"manual:{fix_id}",
                "domain": resolved_domain,
                "jurisdiction": resolved_jur,
                "title": resolved_title,
                "presetReasons": reasons_clean,
                "explanation": final_explanation,
                "reviewer": reviewer,
                "createdAt": now_iso,
                "resolvedAt": None,
                "resolutionNote": None,
            }
            # Remove any stale approval for the same itemKey so it isn't treated as approved
            if clean_key:
                decisions_list = [d for d in decisions_list if d.get("itemKey") != clean_key]
                decisions_doc["updatedAt"] = now_iso
                decisions_doc["decisions"] = decisions_list
                _write_json_file(decisions_path, decisions_doc)

            fix_items.insert(0, new_fix)
            fix_doc["schemaVersion"] = 1
            fix_doc["updatedAt"] = now_iso
            fix_doc["items"] = fix_items
            _write_json_file(fix_queue_path, fix_doc)

            _persist_db_decision_best_effort(
                item_key=clean_key or fix_id,
                decision="reject",
                domain=resolved_domain,
                jurisdiction=resolved_jur,
                title=resolved_title,
                note=final_explanation,
                reviewer=reviewer,
                fix_id=fix_id,
            )
            return {
                "ok": True,
                "decision": "reject",
                "fixRecord": new_fix,
                "summary": f'Declined "{resolved_title}" and queued {fix_id} in docs/review-fix-queue.json for automated remediation.',
                "undoDescriptor": {"type": "fix", "id": fix_id},
            }

        if clean_decision == "undo":
            desc = undo_descriptor or {}
            dtype = desc.get("type")
            if dtype == "decisions":
                keys = set(str(k) for k in (desc.get("itemKeys") or []))
                decisions_list = [d for d in decisions_list if d.get("itemKey") not in keys]
                decisions_doc["updatedAt"] = now_iso
                decisions_doc["decisions"] = decisions_list
                _write_json_file(decisions_path, decisions_doc)
            elif dtype == "fix" and desc.get("id"):
                target_id = str(desc.get("id"))
                fix_items = [i for i in fix_items if i.get("id") != target_id]
                fix_doc["updatedAt"] = now_iso
                fix_doc["items"] = fix_items
                _write_json_file(fix_queue_path, fix_doc)
            else:
                raise HTTPException(status_code=400, detail="Missing or unsupported undoDescriptor.")

            _persist_db_decision_best_effort(
                item_key=clean_key or "undo",
                decision="undo",
                domain=resolved_domain,
                jurisdiction=resolved_jur,
                title=resolved_title,
                note="Reverted 1-click email decision.",
                reviewer=reviewer,
            )
            return {
                "ok": True,
                "decision": "undo",
                "summary": "Reverted your review decision — this item is open for review again.",
                "undoDescriptor": None,
            }

    raise HTTPException(status_code=400, detail=f"Unsupported decision '{clean_decision}'. Expected approve, reject, or undo.")


def resolve_fix_queue_item(
    fix_id: str,
    resolution_note: str,
    *,
    fix_queue_path: Path = DEFAULT_FIX_QUEUE_PATH,
) -> Dict[str, Any]:
    """Mark an OPEN item in `docs/review-fix-queue.json` as RESOLVED after automated remediation."""
    clean_id = str(fix_id or "").strip()
    if not clean_id:
        raise ValueError("fix_id is required")
    now_iso = _iso_utc()
    with _FILE_LOCK:
        fix_doc = _read_json_file(fix_queue_path, {"schemaVersion": 1, "items": []})
        items = [i for i in (fix_doc.get("items") or []) if isinstance(i, dict)]
        matched = None
        for item in items:
            if item.get("id") == clean_id:
                item["status"] = "RESOLVED"
                item["resolvedAt"] = now_iso
                item["resolutionNote"] = resolution_note.strip() or "Verified & resolved by automated agent."
                matched = item
                break
        if not matched:
            raise KeyError(f"Fix queue item '{clean_id}' not found")
        fix_doc["updatedAt"] = now_iso
        fix_doc["items"] = items
        _write_json_file(fix_queue_path, fix_doc)
        return matched


def _with_security_headers(resp: Response) -> Response:
    resp.headers["Content-Security-Policy"] = CSP_HEADER
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Cache-Control"] = "no-store"
    return resp


QUICK_DECISION_JS = """(() => {
  const form = document.getElementById('qd-form');
  if (!form) return;

  const statusBox = document.getElementById('qd-status');
  const submitBtn = document.getElementById('qd-submit-btn');
  const undoBtn = document.getElementById('qd-undo-btn');
  const rejectDetails = document.getElementById('qd-reject-details');
  const decisionRadios = form.querySelectorAll('input[name="decision"]');

  let lastUndoDescriptor = null;

  function syncDecisionVisibility() {
    let selected = 'approve';
    decisionRadios.forEach((r) => {
      if (r.checked) selected = r.value;
    });
    if (rejectDetails) {
      rejectDetails.hidden = selected !== 'reject';
    }
    if (submitBtn) {
      submitBtn.textContent = selected === 'approve'
        ? '✓ Confirm Approval'
        : '✕ Queue Fix for Automated Agent';
      submitBtn.className = selected === 'approve' ? 'btn btn-approve' : 'btn btn-reject';
    }
  }

  decisionRadios.forEach((radio) => {
    radio.addEventListener('change', syncDecisionVisibility);
  });
  syncDecisionVisibility();

  function showStatus(kind, text) {
    if (!statusBox) return;
    statusBox.hidden = false;
    statusBox.className = 'status-box ' + (kind === 'ok' ? 'is-ok' : kind === 'err' ? 'is-err' : 'is-warn');
    statusBox.textContent = text;
  }

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!submitBtn || submitBtn.disabled) return;

    const fd = new FormData(form);
    const decision = String(fd.get('decision') || 'approve');
    const presetReasons = fd.getAll('preset_reasons').map((v) => String(v));
    const payload = {
      token: String(fd.get('token') || ''),
      item_key: String(fd.get('item_key') || ''),
      title: String(fd.get('title') || ''),
      domain: String(fd.get('domain') || ''),
      jurisdiction: String(fd.get('jurisdiction') || ''),
      decision,
      preset_reasons: presetReasons,
      explanation: String(fd.get('explanation') || ''),
    };

    // Disable after valid submit click to prevent double-posts
    submitBtn.disabled = true;
    showStatus('warn', 'Saving your review decision…');

    try {
      const res = await fetch('/api/audit-review/act', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        submitBtn.disabled = false;
        showStatus('err', data.detail || data.error || ('Could not save decision (HTTP ' + res.status + ').'));
        return;
      }
      lastUndoDescriptor = data.undoDescriptor || null;
      if (undoBtn) {
        undoBtn.hidden = !lastUndoDescriptor;
      }
      showStatus(decision === 'approve' ? 'ok' : 'warn', data.summary || 'Decision saved.');
    } catch (err) {
      submitBtn.disabled = false;
      showStatus('err', err && err.message ? err.message : 'Network error while saving decision.');
    }
  });

  if (undoBtn) {
    undoBtn.addEventListener('click', async () => {
      if (!lastUndoDescriptor) return;
      undoBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch('/api/audit-review/act', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          body: JSON.stringify({
            token: String(fd.get('token') || ''),
            item_key: String(fd.get('item_key') || ''),
            title: String(fd.get('title') || ''),
            domain: String(fd.get('domain') || ''),
            jurisdiction: String(fd.get('jurisdiction') || ''),
            decision: 'undo',
            undo_descriptor: lastUndoDescriptor,
          }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          undoBtn.disabled = false;
          showStatus('err', data.detail || 'Could not revert decision.');
          return;
        }
        lastUndoDescriptor = null;
        undoBtn.hidden = true;
        undoBtn.disabled = false;
        if (submitBtn) submitBtn.disabled = false;
        showStatus('ok', data.summary || 'Decision reverted.');
      } catch (err) {
        undoBtn.disabled = false;
        showStatus('err', 'Network error while reverting decision.');
      }
    });
  }
})();
"""


@router.get("/quick-decision.js")
def serve_quick_decision_js() -> Response:
    resp = Response(content=QUICK_DECISION_JS, media_type="application/javascript; charset=utf-8")
    return _with_security_headers(resp)


@router.get("/quick-decision")
def render_quick_decision_page(
    request: Request,
    decision: str = "approve",
    item_key: str = "",
    token: str = "",
    title: str = "",
    domain: str = "Weekly Gemini Audit",
    jurisdiction: str = "",
    reason: str = "",
) -> Response:
    """
    Safe GET confirmation view for 1-click email links (never mutates state on GET,
    protecting against email scanner prefetchers). Submitting sends HTTP POST.
    """
    authorized, reviewer, err = _authorize_request(request, token, expected_item_key=item_key or None)
    if not authorized:
        err_html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Review Link Authorization Needed — They Own WHAT??</title></head>
<body style="font-family:system-ui,sans-serif;background:#0f172a;color:#f8fafc;padding:2rem;">
<main style="max-width:560px;margin:2rem auto;background:#1e293b;border:1px solid #ef4444;border-radius:14px;padding:1.5rem;">
<h1 style="margin-top:0;font-size:1.25rem;">Review Link Authorization Needed</h1>
<p style="color:#fca5a5;">{html.escape(err)}</p>
<p style="color:#94a3b8;font-size:0.95rem;">Use the signed link from your latest They Own WHAT?? Weekly Gemini Audit email.</p>
</main></body></html>"""
        return _with_security_headers(HTMLResponse(content=err_html, status_code=403))

    state = load_review_state()
    audit_items = {
        str(r.get("item_key") or ""): r
        for r in state["latestAudit"]["reviewItems"]
        if r.get("item_key")
    }
    matched = audit_items.get(item_key, {})
    resolved_title = title or matched.get("title") or (
        "Approve All Pending Weekly Audit Items" if item_key == "approve-all-pending" else item_key or "Weekly Audit Review Item"
    )
    resolved_domain = domain or matched.get("domain") or "Weekly Gemini Audit"
    resolved_jur = jurisdiction or matched.get("jurisdiction") or ""
    why_text = matched.get("why") or matched.get("summary") or ""
    details_list = matched.get("details") if isinstance(matched.get("details"), list) else []

    initial_decision = "reject" if decision.strip().lower() in ("reject", "decline", "explain") else "approve"
    approve_checked = "checked" if initial_decision == "approve" else ""
    reject_checked = "checked" if initial_decision == "reject" else ""
    reject_hidden = "" if initial_decision == "reject" else "hidden"

    details_html = ""
    if why_text or details_list:
        bullets = "".join(f"<li>{html.escape(str(d))}</li>" for d in details_list[:10])
        details_html = f"""
        <section class="context-card" aria-label="Review context">
          {f'<p style="margin:0 0 0.5rem;">{html.escape(str(why_text))}</p>' if why_text else ''}
          {f'<ul style="margin:0;padding-left:1.2rem;">{bullets}</ul>' if bullets else ''}
        </section>"""

    reason_checks_html = ""
    for idx, preset in enumerate(PRESET_REJECTION_REASONS):
        is_checked = "checked" if reason and reason.strip().lower() == preset.lower() else ""
        reason_checks_html += f"""
        <label class="choice-row" for="preset-reason-{idx}">
          <input id="preset-reason-{idx}" type="checkbox" name="preset_reasons" value="{html.escape(preset)}" {is_checked} />
          <span>{html.escape(preset)}</span>
        </label>"""

    prefill_note = reason if reason and reason not in PRESET_REJECTION_REASONS else ""
    queue_token = sign_review_token("*", reviewer=reviewer) if token else ""
    queue_href = f"/api/audit-review/queue?{urlencode({'token': queue_token})}" if queue_token else "/api/audit-review/queue"

    page_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>1-Click Weekly Audit Review — They Own WHAT??</title>
  <style>
    :root {{
      color-scheme: dark light;
      --bg: #0f172a;
      --card: #1e293b;
      --surface: #0b1220;
      --border: #334155;
      --text: #f8fafc;
      --muted: #94a3b8;
      --approve: #059669;
      --approve-hover: #047857;
      --reject: #dc2626;
      --reject-hover: #b91c1c;
      --ok-bg: rgba(16, 185, 129, 0.16);
      --ok-border: #10b981;
      --ok-text: #6ee7b7;
      --warn-bg: rgba(245, 158, 11, 0.16);
      --warn-border: #f59e0b;
      --warn-text: #fde68a;
      --err-bg: rgba(239, 68, 68, 0.16);
      --err-border: #ef4444;
      --err-text: #fca5a5;
    }}
    @media (prefers-color-scheme: light) {{
      :root {{
        --bg: #f1f5f9;
        --card: #ffffff;
        --surface: #f8fafc;
        --border: #cbd5e1;
        --text: #0f172a;
        --muted: #475569;
        --ok-bg: #ecfdf5;
        --ok-border: #059669;
        --ok-text: #065f46;
        --warn-bg: #fffbeb;
        --warn-border: #d97706;
        --warn-text: #92400e;
        --err-bg: #fef2f2;
        --err-border: #dc2626;
        --err-text: #991b1b;
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1.25rem;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }}
    main {{
      width: 100%;
      max-width: 640px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 1.5rem;
      box-shadow: 0 18px 40px rgba(0, 0, 0, 0.22);
    }}
    .eyebrow {{
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
      margin: 0 0 0.35rem;
    }}
    h1 {{
      font-size: 1.3rem;
      margin: 0 0 0.65rem;
      line-height: 1.3;
    }}
    .context-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 0.9rem 1rem;
      font-size: 0.92rem;
      margin-bottom: 1rem;
    }}
    fieldset {{
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 0.85rem 1rem;
      margin: 0 0 1rem;
    }}
    legend {{
      padding: 0 0.4rem;
      font-weight: 700;
      font-size: 0.92rem;
    }}
    .choice-row {{
      display: flex;
      align-items: center;
      gap: 0.65rem;
      min-height: 48px;
      padding: 0.35rem 0.25rem;
      cursor: pointer;
      font-size: 0.96rem;
    }}
    .choice-row input {{
      width: 20px;
      height: 20px;
      margin: 0;
      flex: none;
    }}
    label.block-label {{
      display: block;
      font-weight: 700;
      font-size: 0.92rem;
      margin: 0.75rem 0 0.35rem;
    }}
    textarea {{
      width: 100%;
      min-height: 104px;
      padding: 0.7rem 0.8rem;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--surface);
      color: var(--text);
      font: inherit;
      font-size: max(1rem, 1em);
    }}
    .status-box {{
      padding: 0.9rem 1rem;
      border-radius: 12px;
      border: 2px solid var(--border);
      margin: 0 0 1rem;
      font-weight: 600;
    }}
    .status-box.is-ok {{ background: var(--ok-bg); border-color: var(--ok-border); color: var(--ok-text); }}
    .status-box.is-warn {{ background: var(--warn-bg); border-color: var(--warn-border); color: var(--warn-text); }}
    .status-box.is-err {{ background: var(--err-bg); border-color: var(--err-border); color: var(--err-text); }}
    .btn-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.65rem;
      margin-top: 1rem;
    }}
    .btn {{
      min-height: 48px;
      padding: 0.65rem 1.15rem;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: transparent;
      color: var(--text);
      font: inherit;
      font-size: max(1rem, 1em);
      font-weight: 700;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
    }}
    .btn:disabled {{
      opacity: 0.6;
      cursor: not-allowed;
    }}
    .btn-approve {{ background: var(--approve); border-color: var(--approve); color: #ffffff; }}
    .btn-approve:hover:not(:disabled) {{ background: var(--approve-hover); }}
    .btn-reject {{ background: var(--reject); border-color: var(--reject); color: #ffffff; }}
    .btn-reject:hover:not(:disabled) {{ background: var(--reject-hover); }}
  </style>
  <script src="/api/audit-review/quick-decision.js" defer></script>
</head>
<body>
  <main>
    <p class="eyebrow">They Own WHAT?? &middot; {html.escape(resolved_domain)}{f' &middot; {html.escape(resolved_jur)}' if resolved_jur else ''}</p>
    <h1>{html.escape(resolved_title)}</h1>
    {details_html}
    <div id="qd-status" class="status-box" role="status" aria-live="polite" hidden></div>

    <form id="qd-form" method="POST" action="/api/audit-review/act">
      <input type="hidden" name="token" value="{html.escape(token)}" />
      <input type="hidden" name="item_key" value="{html.escape(item_key)}" />
      <input type="hidden" name="title" value="{html.escape(resolved_title)}" />
      <input type="hidden" name="domain" value="{html.escape(resolved_domain)}" />
      <input type="hidden" name="jurisdiction" value="{html.escape(resolved_jur)}" />

      <fieldset>
        <legend>Your Maintainer Decision</legend>
        <label class="choice-row" for="decision-approve">
          <input id="decision-approve" type="radio" name="decision" value="approve" {approve_checked} />
          <span><strong>Approve &amp; Sign Off</strong> &mdash; record 90-day sign-off</span>
        </label>
        <label class="choice-row" for="decision-reject">
          <input id="decision-reject" type="radio" name="decision" value="reject" {reject_checked} />
          <span><strong>Decline / Request Fix</strong> &mdash; queue for automated agent remediation</span>
        </label>
      </fieldset>

      <div id="qd-reject-details" {reject_hidden}>
        <fieldset>
          <legend>What needs fixing? (optional tags)</legend>
          {reason_checks_html}
        </fieldset>
        <label class="block-label" for="qd-note">Instructions for the optimization agent (optional)</label>
        <textarea id="qd-note" name="explanation" placeholder="Describe what looks wrong or how you want this handled…">{html.escape(prefill_note)}</textarea>
      </div>

      <div class="btn-row">
        <button id="qd-submit-btn" type="submit" class="btn {'btn-approve' if initial_decision == 'approve' else 'btn-reject'}">
          {'✓ Confirm Approval' if initial_decision == 'approve' else '✕ Queue Fix for Automated Agent'}
        </button>
        <button id="qd-undo-btn" type="button" class="btn" hidden>↺ Undo This Decision</button>
        <a href="{html.escape(queue_href)}" class="btn">View Review &amp; Fix Queue &rarr;</a>
      </div>
    </form>
  </main>
</body>
</html>"""
    return _with_security_headers(HTMLResponse(content=page_html, status_code=200))


@router.post("/act")
async def act_on_review_decision(request: Request) -> Response:
    content_type = (request.headers.get("content-type") or "").lower()
    wants_json = "application/json" in (request.headers.get("accept") or "").lower() or "application/json" in content_type

    if "application/json" in content_type:
        body = await request.json()
        token = str(body.get("token") or "")
        item_key = str(body.get("item_key") or body.get("itemKey") or "")
        decision = str(body.get("decision") or "approve")
        title = str(body.get("title") or "")
        domain = str(body.get("domain") or "Weekly Gemini Audit")
        jurisdiction = str(body.get("jurisdiction") or "")
        raw_reasons = body.get("preset_reasons") if "preset_reasons" in body else body.get("presetReasons")
        preset_reasons = raw_reasons if isinstance(raw_reasons, list) else []
        explanation = str(body.get("explanation") or "")
        raw_undo = body.get("undo_descriptor") if "undo_descriptor" in body else body.get("undoDescriptor")
        undo_descriptor = raw_undo if isinstance(raw_undo, dict) else None
    else:
        form = await request.form()
        token = str(form.get("token") or "")
        item_key = str(form.get("item_key") or "")
        decision = str(form.get("decision") or "approve")
        title = str(form.get("title") or "")
        domain = str(form.get("domain") or "Weekly Gemini Audit")
        jurisdiction = str(form.get("jurisdiction") or "")
        preset_reasons = [str(v) for v in form.getlist("preset_reasons")] if hasattr(form, "getlist") else []
        explanation = str(form.get("explanation") or "")
        undo_descriptor = None

    authorized, reviewer, err = _authorize_request(request, token, expected_item_key=item_key or None)
    if not authorized:
        if wants_json:
            return _with_security_headers(JSONResponse(status_code=403, content={"ok": False, "detail": err}))
        return _with_security_headers(HTMLResponse(status_code=403, content=f"<h1>403 Forbidden</h1><p>{html.escape(err)}</p>"))

    result = record_review_decision(
        decision=decision,
        item_key=item_key,
        title=title,
        domain=domain,
        jurisdiction=jurisdiction,
        preset_reasons=preset_reasons,
        explanation=explanation,
        reviewer=reviewer,
        undo_descriptor=undo_descriptor,
    )

    if wants_json:
        return _with_security_headers(JSONResponse(status_code=200, content=result))

    summary_esc = html.escape(str(result.get("summary") or "Decision recorded."))
    done_html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Review Decision Saved — They Own WHAT??</title></head>
<body style="font-family:system-ui,sans-serif;background:#0f172a;color:#f8fafc;padding:2rem;">
<main style="max-width:560px;margin:2rem auto;background:#1e293b;border:1px solid #10b981;border-radius:14px;padding:1.5rem;">
<h1 style="margin-top:0;font-size:1.25rem;">✓ Review Decision Saved</h1>
<p>{summary_esc}</p>
<p><a href="https://theyownwhat.net" style="color:#60a5fa;">Return to They Own WHAT?? &rarr;</a></p>
</main></body></html>"""
    return _with_security_headers(HTMLResponse(status_code=200, content=done_html))


@router.get("/queue")
def get_review_queue(request: Request, token: str = "") -> Response:
    authorized, _, err = _authorize_request(request, token, expected_item_key=None)
    if not authorized:
        return _with_security_headers(JSONResponse(status_code=403, content={"ok": False, "detail": err}))

    state = load_review_state()
    accept = (request.headers.get("accept") or "").lower()
    if "text/html" not in accept:
        return _with_security_headers(JSONResponse(status_code=200, content={"ok": True, **state}))

    open_rows = "".join(
        f"<li><strong>[{html.escape(str(i.get('id')))}]</strong> {html.escape(str(i.get('domain') or ''))}: "
        f"{html.escape(str(i.get('title') or ''))} &mdash; {html.escape(str(i.get('explanation') or ''))}</li>"
        for i in state["fixQueue"]["openItems"]
    ) or "<li>No open fix requests — queue is clear!</li>"

    dec_rows = "".join(
        f"<li><strong>{html.escape(str(d.get('disposition', '')).upper())}</strong>: "
        f"{html.escape(str(d.get('title') or d.get('itemKey') or ''))} "
        f"(reviewed {html.escape(str(d.get('reviewedAt') or '')[:10])})</li>"
        for d in state["decisions"][:25]
    ) or "<li>No recorded sign-offs yet.</li>"

    html_out = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Audit Review &amp; Fix Queue — They Own WHAT??</title></head>
<body style="font-family:system-ui,sans-serif;background:#0f172a;color:#f8fafc;padding:1.5rem;line-height:1.5;">
<main style="max-width:760px;margin:0 auto;background:#1e293b;border:1px solid #334155;border-radius:16px;padding:1.5rem;">
<p style="font-size:0.78rem;text-transform:uppercase;letter-spacing:0.06em;color:#94a3b8;margin:0;">They Own WHAT?? &middot; Maintainer Assurance</p>
<h1 style="margin:0.35rem 0 1rem;font-size:1.35rem;">Weekly Gemini Audit &amp; Fix Queue</h1>
<h2 style="font-size:1.05rem;">Open Fix Requests (<code>docs/review-fix-queue.json</code>)</h2>
<ul>{open_rows}</ul>
<h2 style="font-size:1.05rem;margin-top:1.25rem;">Recorded Sign-Offs (<code>docs/weekly-review-decisions.json</code>)</h2>
<ul>{dec_rows}</ul>
</main></body></html>"""
    return _with_security_headers(HTMLResponse(status_code=200, content=html_out))
