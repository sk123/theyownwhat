#!/usr/bin/env python3
"""
updater/send_audit_email.py
============================
Utility script for sending weekly audit reports and feature branch summaries
to salmunk@gmail.com via SMTP.
"""

import os
import sys
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

# Load .env file if available
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("send-audit-email")

ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO",   "salmunk@gmail.com")
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "salmunk@gmail.com")
ALERT_EMAIL_PASS = os.environ.get("ALERT_EMAIL_PASS", "")

def send_audit_email(subject: str, body_text: str, body_html: str = None) -> bool:
    """Send audit status email to salmunk@gmail.com."""
    logger.info(f"Preparing audit email to {ALERT_EMAIL_TO}: {subject}")
    
    if not ALERT_EMAIL_PASS:
        logger.warning("ALERT_EMAIL_PASS environment variable not set. Email body logged to console instead of SMTP dispatch.")
        logger.info("\n--- EMAIL SUBJECT ---\n" + subject)
        logger.info("\n--- EMAIL BODY ---\n" + body_text)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = ALERT_EMAIL_FROM
        msg["To"]      = ALERT_EMAIL_TO
        
        msg.attach(MIMEText(body_text, "plain"))
        if body_html:
            msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(ALERT_EMAIL_FROM, ALERT_EMAIL_PASS)
            server.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO, msg.as_string())
        
        logger.info("✓ Audit email sent successfully to " + ALERT_EMAIL_TO)
        return True
    except Exception as e:
        logger.error(f"Failed to send audit email: {e}")
        return False

if __name__ == "__main__":
    test_subject = f"[They Own WHAT?] Weekly Audit & System Status - {datetime.utcnow().strftime('%Y-%m-%d')}"
    test_body = "Weekly audit script executed successfully. All systems operational."
    send_audit_email(test_subject, test_body)
