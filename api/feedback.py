from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import html as html_lib
import json
import logging
import os
import re
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from api.db import get_db_connection
from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv

# Load .env file if available
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Email config (set in .env)
# ---------------------------------------------------------------------------
REQUIRED_FEEDBACK_RECIPIENTS = (
    "salmun@storrsmagic.org",
    "salmunk@gmail.com",
    "me@salmun.net",
)
FEEDBACK_AUTO_REPLY_MESSAGE = (
    "All feedback will be reviewed once daily, please check back tomorrow "
    "to see if the issue is resolved or further tuning is necessary."
)
_EMAIL_EXTRACT_RE = re.compile(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")
_INTERNAL_ALERT_EMAILS = frozenset(addr.lower() for addr in REQUIRED_FEEDBACK_RECIPIENTS)

ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO", ", ".join(REQUIRED_FEEDBACK_RECIPIENTS))
ALERT_EMAIL_FROM = (os.environ.get("ALERT_EMAIL_FROM", "salmunk@gmail.com") or "").strip()
ALERT_EMAIL_PASS = "".join((os.environ.get("ALERT_EMAIL_PASS", "") or "").split())   # Gmail App Password


def extract_valid_email(*candidates: Optional[str], allow_internal: bool = False) -> str:
    """Extract the first syntactically valid, header-safe email address from candidate strings."""
    for raw in candidates:
        text = str(raw or "").strip()
        if not text:
            continue
        for match in _EMAIL_EXTRACT_RE.finditer(text):
            addr = match.group(1).strip()
            if "\r" in addr or "\n" in addr or len(addr) > 200:
                continue
            if not allow_internal and addr.lower() in _INTERNAL_ALERT_EMAILS:
                continue
            return addr
    return ""


def get_feedback_recipients(extra_recipients: Optional[List[str]] = None) -> List[str]:
    """Return deduplicated feedback notification recipients, always including salmun@storrsmagic.org."""
    raw_to = os.environ.get("ALERT_EMAIL_TO", ALERT_EMAIL_TO) or ""
    candidates: List[str] = []
    for addr in raw_to.split(","):
        cleaned = addr.strip()
        if cleaned:
            candidates.append(cleaned)
    candidates.extend(REQUIRED_FEEDBACK_RECIPIENTS)
    if extra_recipients:
        for addr in extra_recipients:
            cleaned = (addr or "").strip()
            if cleaned:
                candidates.append(cleaned)

    deduped: List[str] = []
    seen = set()
    for addr in candidates:
        key = addr.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(addr)
    return deduped


def _smtp_dispatch(
    subject: str,
    text_body: str,
    to_list: List[str],
    *,
    html_body: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> bool:
    """Low-level SMTP sender shared by admin alerts and reviewer auto-replies."""
    if str(os.environ.get("DISABLE_FEEDBACK_EMAILS", "")).strip().lower() in ("1", "true", "yes"):
        return False

    smtp_host = (os.environ.get("SMTP_HOST", "smtp.gmail.com") or "smtp.gmail.com").strip()
    smtp_from = (os.environ.get("ALERT_EMAIL_FROM", ALERT_EMAIL_FROM) or "").strip()
    smtp_pass = "".join((os.environ.get("ALERT_EMAIL_PASS", ALERT_EMAIL_PASS) or "").split())
    if not smtp_from or not smtp_pass:
        logger.warning("Email skipped: ALERT_EMAIL_FROM / ALERT_EMAIL_PASS not configured.")
        return False
    if not to_list:
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject.replace("\r", " ").replace("\n", " ")
        msg["From"] = smtp_from
        msg["To"] = ", ".join(to_list)
        if reply_to and "\r" not in reply_to and "\n" not in reply_to:
            msg["Reply-To"] = reply_to
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        if html_body:
            msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            with smtplib.SMTP(smtp_host, 587, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_from, smtp_pass)
                server.sendmail(smtp_from, to_list, msg.as_string())
        except smtplib.SMTPAuthenticationError:
            raise
        except Exception:
            with smtplib.SMTP_SSL(smtp_host, 465, timeout=15) as server:
                server.ehlo()
                server.login(smtp_from, smtp_pass)
                server.sendmail(smtp_from, to_list, msg.as_string())
        return True
    except Exception as exc:
        # Never let email failure surface to the user
        logger.warning("Email send failed (%s): %s", subject, exc)
        return False


def send_feedback_notification_email(
    subject: str,
    text_body: str,
    *,
    html_body: Optional[str] = None,
    recipients: Optional[List[str]] = None,
    reply_to: Optional[str] = None,
) -> bool:
    """Send an admin feedback notification email via configured SMTP. Returns True if sent."""
    to_list = get_feedback_recipients(recipients)
    return _smtp_dispatch(subject, text_body, to_list, html_body=html_body, reply_to=reply_to)


def spawn_feedback_notification_email(
    subject: str,
    text_body: str,
    *,
    html_body: Optional[str] = None,
    recipients: Optional[List[str]] = None,
    reply_to: Optional[str] = None,
) -> None:
    """Fire-and-forget background email dispatch so HTTP responses are never blocked."""
    threading.Thread(
        target=send_feedback_notification_email,
        args=(subject, text_body),
        kwargs={"html_body": html_body, "recipients": recipients, "reply_to": reply_to},
        daemon=True,
    ).start()


def format_reviewer_auto_reply_email(
    *,
    reference_id: int,
    event_label: str = "Feedback",
    summary: str = "",
) -> tuple[str, str, str]:
    """Build (subject, text_body, html_body) for the reviewer auto-reply confirmation."""
    subject = f"[They Own WHAT?] Feedback received (#{int(reference_id)})"
    clean_summary = " ".join(str(summary or "").strip().split())[:240]
    summary_line_txt = f"\nReference: #{int(reference_id)} ({event_label})" + (
        f" — {clean_summary}" if clean_summary else ""
    )
    text_body = (
        f"{FEEDBACK_AUTO_REPLY_MESSAGE}\n"
        f"{summary_line_txt}\n\n"
        "Thank you for helping improve rental housing ownership transparency.\n"
        "https://theyownwhat.net/review\n"
    )
    summary_html = (
        f"<p style='font-size:13px;color:#475569;margin:12px 0 0;'>"
        f"<strong>Reference:</strong> #{int(reference_id)} ({html_lib.escape(event_label)})"
        + (f" &mdash; {html_lib.escape(clean_summary)}" if clean_summary else "")
        + "</p>"
    )
    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #0f172a; max-width: 600px;">
      <p style="font-size: 15px; line-height: 1.55; margin: 0 0 12px;">
        {html_lib.escape(FEEDBACK_AUTO_REPLY_MESSAGE)}
      </p>
      {summary_html}
      <p style="font-size: 12px; color: #64748b; margin-top: 18px;">
        Thank you for helping improve rental housing ownership transparency.<br>
        <a href="https://theyownwhat.net/review">https://theyownwhat.net/review</a>
      </p>
    </div>
    """
    return subject, text_body, html_body


def send_reviewer_auto_reply_email(
    to_email: str,
    *,
    reference_id: int,
    event_label: str = "Feedback",
    summary: str = "",
) -> bool:
    """Send the daily-review auto-reply strictly to the submitting reviewer's email."""
    clean_to = extract_valid_email(to_email)
    if not clean_to:
        return False
    subject, text_body, html_body = format_reviewer_auto_reply_email(
        reference_id=reference_id,
        event_label=event_label,
        summary=summary,
    )
    return _smtp_dispatch(
        subject,
        text_body,
        [clean_to],
        html_body=html_body,
        reply_to="salmun@storrsmagic.org",
    )


def spawn_reviewer_auto_reply_email(
    to_email: str,
    *,
    reference_id: int,
    event_label: str = "Feedback",
    summary: str = "",
) -> None:
    """Fire-and-forget auto-reply email to a reviewer who left an email address."""
    clean_to = extract_valid_email(to_email)
    if not clean_to:
        return
    threading.Thread(
        target=send_reviewer_auto_reply_email,
        args=(clean_to,),
        kwargs={
            "reference_id": reference_id,
            "event_label": event_label,
            "summary": summary,
        },
        daemon=True,
    ).start()


def format_general_feedback_email(
    feedback_id: int,
    report_type: str,
    description: str,
    entities: list,
    reviewer_email: str = "",
) -> tuple[str, str, str]:
    """Build (subject, text_body, html_body) for general /api/feedback submissions."""
    submitted_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[They Own WHAT?] New feedback #{feedback_id}: {report_type}"

    entity_text_lines: List[str] = []
    entity_html_items: List[str] = []
    for e in (entities or []):
        if not isinstance(e, dict):
            val = str(e)
            entity_text_lines.append(f"  - {val}")
            entity_html_items.append(f"<li>{html_lib.escape(val)}</li>")
            continue
        name = e.get("name") or e.get("label") or e.get("title") or "?"
        etype = e.get("type") or "?"
        eid = e.get("id") or e.get("network_key") or e.get("bbl") or "N/A"
        extras = []
        for extra_k in ("jurisdiction", "state", "city", "address", "network_key", "bbl", "context"):
            if e.get(extra_k) and str(e.get(extra_k)) != str(eid):
                extras.append(f"{extra_k}={e.get(extra_k)}")
        extra_str = f" ({', '.join(extras)})" if extras else ""
        line = f"{name} [{etype}] (ID: {eid}){extra_str}"
        entity_text_lines.append(f"  - {line}")
        entity_html_items.append(f"<li><strong>{html_lib.escape(str(name))}</strong> [{html_lib.escape(str(etype))}] (ID: <code>{html_lib.escape(str(eid))}</code>){html_lib.escape(extra_str)}</li>")

    entities_text_block = (
        "\n\nRelated entities:\n" + "\n".join(entity_text_lines)
        if entity_text_lines
        else "\n\nRelated entities: (none attached)"
    )

    email_txt = f"Reviewer:    {reviewer_email}\n" if reviewer_email else ""
    text_body = (
        "New feedback submitted on They Own WHAT?\n\n"
        f"ID:          #{feedback_id}\n"
        f"Event:       General Feedback (/api/feedback)\n"
        f"Type:        {report_type}\n"
        f"{email_txt}"
        f"Submitted:   {submitted_utc}\n\n"
        f"Description:\n{description}{entities_text_block}\n\n"
        "---\n"
        "All feedback: https://theyownwhat.net/api/feedback\n"
    )

    entities_html = (
        f"<ul>{''.join(entity_html_items)}</ul>"
        if entity_html_items
        else "<p style='color:#64748b'>(none attached)</p>"
    )
    email_tr = (
        f"<tr><td style='padding: 4px 10px 4px 0; color: #475569;'><strong>Reviewer Email</strong></td><td>{html_lib.escape(reviewer_email)}</td></tr>"
        if reviewer_email
        else ""
    )
    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #0f172a; max-width: 680px;">
      <h2 style="margin: 0 0 12px;">New Feedback #{int(feedback_id)}: {html_lib.escape(str(report_type))}</h2>
      <table style="border-collapse: collapse; width: 100%; margin-bottom: 14px; font-size: 14px;">
        <tr><td style="padding: 4px 10px 4px 0; color: #475569;"><strong>Event</strong></td><td>General Feedback (<code>/api/feedback</code>)</td></tr>
        <tr><td style="padding: 4px 10px 4px 0; color: #475569;"><strong>Type</strong></td><td>{html_lib.escape(str(report_type))}</td></tr>
        {email_tr}
        <tr><td style="padding: 4px 10px 4px 0; color: #475569;"><strong>Submitted</strong></td><td>{html_lib.escape(submitted_utc)}</td></tr>
      </table>
      <h3 style="margin: 12px 0 6px; font-size: 15px;">Description</h3>
      <div style="white-space: pre-wrap; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 10px 12px; font-size: 14px;">{html_lib.escape(str(description or ''))}</div>
      <h3 style="margin: 14px 0 6px; font-size: 15px;">Related Entities</h3>
      {entities_html}
      <p style="margin-top: 16px; font-size: 12px; color: #64748b;">
        View recent submissions: <a href="https://theyownwhat.net/api/feedback">https://theyownwhat.net/api/feedback</a>
      </p>
    </div>
    """
    return subject, text_body, html_body


def _send_email_async(
    feedback_id: int,
    report_type: str,
    description: str,
    entities: list,
    reviewer_email: str = "",
):
    """Fire-and-forget email notification for general feedback submissions."""
    subject, text_body, html_body = format_general_feedback_email(
        feedback_id, report_type, description, entities, reviewer_email=reviewer_email
    )
    send_feedback_notification_email(
        subject, text_body, html_body=html_body, reply_to=reviewer_email or None
    )


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class FeedbackSubmit(BaseModel):
    report_type: str = Field(..., description="Type of issue: 'missing', 'mismatch', 'outdated', 'other'")
    description: str = Field(..., description="User provided details")
    related_entities: List[Dict[str, Any]] = Field(default=[], description="List of related entities (properties, businesses, networks)")
    reviewer_email: str = Field(default="", max_length=200, description="Optional email address for daily-review auto-reply")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.post("/api/feedback")
def submit_feedback(feedback: FeedbackSubmit, conn=Depends(get_db_connection)):
    """
    Submit user feedback about data quality issues.
    Triggers a background admin notification email and an optional reviewer auto-reply.
    """
    raw_email = (feedback.reviewer_email or "").strip()
    if raw_email and not extract_valid_email(raw_email, allow_internal=True):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    reviewer_email = extract_valid_email(raw_email, feedback.description)

    try:
        with conn.cursor() as cursor:
            cursor.execute("ALTER TABLE user_feedback ADD COLUMN IF NOT EXISTS reviewer_email TEXT NOT NULL DEFAULT ''")
            cursor.execute("ALTER TABLE user_feedback ADD COLUMN IF NOT EXISTS auto_reply_sent_at TIMESTAMPTZ")
            cursor.execute(
                """
                INSERT INTO user_feedback (
                    report_type, description, related_entities, reviewer_email, auto_reply_sent_at, created_at
                )
                VALUES (%s, %s, %s, %s, CASE WHEN %s <> '' THEN NOW() ELSE NULL END, NOW())
                RETURNING id
                """,
                (
                    feedback.report_type,
                    feedback.description,
                    json.dumps(feedback.related_entities),
                    reviewer_email,
                    reviewer_email,
                ),
            )
            conn.commit()
            new_id = cursor.fetchone()[0]

        # Fire admin alert in background — never blocks the API response
        threading.Thread(
            target=_send_email_async,
            args=(new_id, feedback.report_type, feedback.description, feedback.related_entities, reviewer_email),
            daemon=True,
        ).start()

        # Fire auto-reply to the submitting reviewer if they included an email address
        if reviewer_email:
            spawn_reviewer_auto_reply_email(
                reviewer_email,
                reference_id=int(new_id),
                event_label=f"Feedback ({feedback.report_type})",
                summary=feedback.description,
            )

        return {
            "id": new_id,
            "status": "submitted",
            "auto_reply_sent": bool(reviewer_email),
            "message": FEEDBACK_AUTO_REPLY_MESSAGE,
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/feedback")
def get_feedback(limit: int = 50, conn=Depends(get_db_connection)):
    """
    Get list of user feedback reports.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, report_type, description, related_entities, created_at 
                FROM user_feedback 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (limit,))
            feedbacks = cursor.fetchall()
            return feedbacks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
