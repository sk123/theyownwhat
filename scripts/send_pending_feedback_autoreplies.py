#!/usr/bin/env python3
"""
scripts/send_pending_feedback_autoreplies.py
============================================
Idempotently sends the daily-review auto-reply email to any recent feedback or
`/review` submissions that included a reviewer email address and have not yet
received an auto-reply (`auto_reply_sent_at IS NULL`).

Usage:
  python3 scripts/send_pending_feedback_autoreplies.py --dry-run
  python3 scripts/send_pending_feedback_autoreplies.py --since-hours 72
  python3 scripts/send_pending_feedback_autoreplies.py --emails a@example.org b@example.org
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List

import psycopg2
from psycopg2.extras import RealDictCursor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.feedback import (
    FEEDBACK_AUTO_REPLY_MESSAGE,
    extract_valid_email,
    send_reviewer_auto_reply_email,
)


def _db_url() -> str:
    return os.environ.get("DATABASE_URL") or "postgresql://user:password@127.0.0.1:5432/ctdata"


def send_for_explicit_emails(emails: List[str], *, dry_run: bool = False) -> int:
    sent = 0
    seen = set()
    for raw in emails:
        addr = extract_valid_email(raw)
        if not addr or addr.lower() in seen:
            continue
        seen.add(addr.lower())
        if dry_run:
            print(f"[DRY-RUN] Would send auto-reply to {addr}: {FEEDBACK_AUTO_REPLY_MESSAGE}")
            sent += 1
            continue
        ok = send_reviewer_auto_reply_email(
            addr,
            reference_id=0,
            event_label="Feedback",
            summary="Follow-up confirmation",
        )
        print(f"{'✓ Sent' if ok else '✗ Failed'} auto-reply -> {addr}")
        if ok:
            sent += 1
    return sent


def backfill_db_autoreplies(*, since_hours: int = 72, dry_run: bool = False) -> int:
    conn = psycopg2.connect(_db_url())
    sent = 0
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("ALTER TABLE network_error_reports ADD COLUMN IF NOT EXISTS reviewer_email TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE network_error_reports ADD COLUMN IF NOT EXISTS auto_reply_sent_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE user_feedback ADD COLUMN IF NOT EXISTS reviewer_email TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE user_feedback ADD COLUMN IF NOT EXISTS auto_reply_sent_at TIMESTAMPTZ")
        conn.commit()

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, jurisdiction, target_value, related_target_value,
                       reviewer_label, reviewer_email, explanation, created_at
                FROM network_error_reports
                WHERE auto_reply_sent_at IS NULL
                  AND created_at >= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY id ASC
                """,
                (int(since_hours),),
            )
            ner_rows = cur.fetchall()
            for r in ner_rows:
                email = extract_valid_email(r.get("reviewer_email"), r.get("reviewer_label"), r.get("explanation"))
                if not email:
                    print(f"[SKIP] network_error_reports #{r['id']} ({r['jurisdiction']}): no email address on file (label={r.get('reviewer_label')!r})")
                    continue
                summary = str(r.get("target_value") or "")
                if r.get("related_target_value"):
                    summary += f" -> {r['related_target_value']}"
                if dry_run:
                    print(f"[DRY-RUN] Would send /review auto-reply for #{r['id']} to {email}")
                    sent += 1
                    continue
                ok = send_reviewer_auto_reply_email(
                    email,
                    reference_id=int(r["id"]),
                    event_label=f"/review ({str(r['jurisdiction']).upper()})",
                    summary=summary,
                )
                if ok:
                    cur.execute(
                        "UPDATE network_error_reports SET reviewer_email = %s, auto_reply_sent_at = NOW() WHERE id = %s",
                        (email, int(r["id"])),
                    )
                    conn.commit()
                    sent += 1
                    print(f"✓ Sent /review auto-reply for #{r['id']} -> {email}")

            cur.execute(
                """
                SELECT id, report_type, description, reviewer_email, created_at
                FROM user_feedback
                WHERE auto_reply_sent_at IS NULL
                  AND created_at >= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY id ASC
                """,
                (int(since_hours),),
            )
            uf_rows = cur.fetchall()
            for r in uf_rows:
                email = extract_valid_email(r.get("reviewer_email"), r.get("description"))
                if not email:
                    print(f"[SKIP] user_feedback #{r['id']}: no email address on file")
                    continue
                if dry_run:
                    print(f"[DRY-RUN] Would send /api/feedback auto-reply for #{r['id']} to {email}")
                    sent += 1
                    continue
                ok = send_reviewer_auto_reply_email(
                    email,
                    reference_id=int(r["id"]),
                    event_label=f"Feedback ({r['report_type']})",
                    summary=str(r.get("description") or ""),
                )
                if ok:
                    cur.execute(
                        "UPDATE user_feedback SET reviewer_email = %s, auto_reply_sent_at = NOW() WHERE id = %s",
                        (email, int(r["id"])),
                    )
                    conn.commit()
                    sent += 1
                    print(f"✓ Sent /api/feedback auto-reply for #{r['id']} -> {email}")
    finally:
        conn.close()
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description="Send daily-review auto-reply to recent feedback reviewers.")
    parser.add_argument("--since-hours", type=int, default=72, help="Lookback window for DB records (default: 72h).")
    parser.add_argument("--emails", nargs="*", default=[], help="Optional explicit reviewer emails to send auto-reply to.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    total = 0
    if args.emails:
        total += send_for_explicit_emails(args.emails, dry_run=args.dry_run)
    total += backfill_db_autoreplies(since_hours=args.since_hours, dry_run=args.dry_run)
    print(f"Done. Auto-replies {'planned' if args.dry_run else 'sent'}: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
