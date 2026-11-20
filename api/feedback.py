from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import html as html_lib
import json
import logging
import os
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
ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO", ", ".join(REQUIRED_FEEDBACK_RECIPIENTS))
ALERT_EMAIL_FROM = (os.environ.get("ALERT_EMAIL_FROM", "salmunk@gmail.com") or "").strip()
ALERT_EMAIL_PASS = "".join((os.environ.get("ALERT_EMAIL_PASS", "") or "").split())   # Gmail App Password


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


def send_feedback_notification_email(
    subject: str,
    text_body: str,
    *,
    html_body: Optional[str] = None,
    recipients: Optional[List[str]] = None,
) -> bool:
    """Send a feedback notification email via configured Gmail SMTP. Returns True if sent."""
    if str(os.environ.get("DISABLE_FEEDBACK_EMAILS", "")).strip().lower() in ("1", "true", "yes"):
        return False

    smtp_host = (os.environ.get("SMTP_HOST", "smtp.gmail.com") or "smtp.gmail.com").strip()
    smtp_from = (os.environ.get("ALERT_EMAIL_FROM", ALERT_EMAIL_FROM) or "").strip()
    smtp_pass = "".join((os.environ.get("ALERT_EMAIL_PASS", ALERT_EMAIL_PASS) or "").split())
    if not smtp_from or not smtp_pass:
        logger.warning("Feedback email skipped: ALERT_EMAIL_FROM / ALERT_EMAIL_PASS not configured.")
        return False

    to_list = get_feedback_recipients(recipients)
    if not to_list:
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_from
        msg["To"] = ", ".join(to_list)
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
        logger.warning("Feedback email failed (%s): %s", subject, exc)
        return False


def spawn_feedback_notification_email(
    subject: str,
    text_body: str,
    *,
    html_body: Optional[str] = None,
    recipients: Optional[List[str]] = None,
) -> None:
    """Fire-and-forget background email dispatch so HTTP responses are never blocked."""
    threading.Thread(
        target=send_feedback_notification_email,
        args=(subject, text_body),
        kwargs={"html_body": html_body, "recipients": recipients},
        daemon=True,
    ).start()


def format_general_feedback_email(
    feedback_id: int,
    report_type: str,
    description: str,
    entities: list,
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

    text_body = (
        "New feedback submitted on They Own WHAT?\n\n"
        f"ID:          #{feedback_id}\n"
        f"Event:       General Feedback (/api/feedback)\n"
        f"Type:        {report_type}\n"
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
    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #0f172a; max-width: 680px;">
      <h2 style="margin: 0 0 12px;">New Feedback #{int(feedback_id)}: {html_lib.escape(str(report_type))}</h2>
      <table style="border-collapse: collapse; width: 100%; margin-bottom: 14px; font-size: 14px;">
        <tr><td style="padding: 4px 10px 4px 0; color: #475569;"><strong>Event</strong></td><td>General Feedback (<code>/api/feedback</code>)</td></tr>
        <tr><td style="padding: 4px 10px 4px 0; color: #475569;"><strong>Type</strong></td><td>{html_lib.escape(str(report_type))}</td></tr>
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


def _send_email_async(feedback_id: int, report_type: str, description: str, entities: list):
    """Fire-and-forget email notification for general feedback submissions."""
    subject, text_body, html_body = format_general_feedback_email(
        feedback_id, report_type, description, entities
    )
    send_feedback_notification_email(subject, text_body, html_body=html_body)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class FeedbackSubmit(BaseModel):
    report_type: str = Field(..., description="Type of issue: 'missing', 'mismatch', 'outdated', 'other'")
    description: str = Field(..., description="User provided details")
    related_entities: List[Dict[str, Any]] = Field(default=[], description="List of related entities (properties, businesses, networks)")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.post("/api/feedback")
def submit_feedback(feedback: FeedbackSubmit, conn=Depends(get_db_connection)):
    """
    Submit user feedback about data quality issues.
    Triggers a background email notification to the configured alert address.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO user_feedback (report_type, description, related_entities, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING id
            """, (feedback.report_type, feedback.description, json.dumps(feedback.related_entities)))
            conn.commit()
            new_id = cursor.fetchone()[0]

        # Fire email in background — never blocks the API response
        threading.Thread(
            target=_send_email_async,
            args=(new_id, feedback.report_type, feedback.description, feedback.related_entities),
            daemon=True,
        ).start()

        return {"id": new_id, "status": "submitted"}
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
