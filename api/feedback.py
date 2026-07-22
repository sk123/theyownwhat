from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
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

router = APIRouter()

# ---------------------------------------------------------------------------
# Email config (set in .env)
# ---------------------------------------------------------------------------
ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO",   "salmunk@gmail.com")
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "salmunk@gmail.com")
ALERT_EMAIL_PASS = os.environ.get("ALERT_EMAIL_PASS", "")   # Gmail App Password


def _send_email_async(feedback_id: int, report_type: str, description: str, entities: list):
    """Fire-and-forget email notification — runs in background thread."""
    if not ALERT_EMAIL_PASS:
        return  # silently skip if not configured

    try:
        subject = f"[They Own WHAT?] New feedback: {report_type}"

        entity_lines = ""
        if entities:
            entity_lines = "\n\nRelated entities:\n" + "\n".join(
                f"  • {e.get('name', e.get('display_name', '?'))} [{e.get('type', '?')}]"
                for e in entities
            )

        body = f"""New feedback submitted on They Own WHAT??

ID:          #{feedback_id}
Type:        {report_type}
Submitted:   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

Description:
{description}{entity_lines}

---
View all feedback: https://theyownwhat.net/admin (or GET /api/feedback)
"""

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = ALERT_EMAIL_FROM
        msg["To"]      = ALERT_EMAIL_TO
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(ALERT_EMAIL_FROM, ALERT_EMAIL_PASS)
            server.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO, msg.as_string())

    except Exception as e:
        # Never let email failure surface to the user
        import logging
        logging.getLogger(__name__).warning(f"Feedback email failed: {e}")


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
