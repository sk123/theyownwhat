#!/usr/bin/env python3
"""
chicago/ingest_chicago.py
=========================
Automated ingestion pipeline for Chicago & Cook County building code violations,
property owner records, and code enforcement data from the City of Chicago Socrata API.

Data Source: City of Chicago Open Data Portal (data.cityofchicago.org/resource/22u3-xenr.json)
"""

import os
import sys
import json
import logging
import requests
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_batch, RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import normalize_business_name, normalize_person_name, normalize_mailing_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("chicago-ingest")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
CHICAGO_SOCRATA_ENDPOINT = "https://data.cityofchicago.org/resource/22u3-xenr.json"

def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception:
        return psycopg2.connect("postgresql://user:password@localhost:5432/ctdata")

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chicago_building_violations (
                id SERIAL PRIMARY KEY,
                violation_id VARCHAR(100) UNIQUE,
                violation_date TIMESTAMP,
                violation_code VARCHAR(100),
                violation_description TEXT,
                violation_status VARCHAR(50),
                address VARCHAR(255),
                city VARCHAR(100) DEFAULT 'CHICAGO',
                state VARCHAR(10) DEFAULT 'IL',
                zip VARCHAR(20),
                inspector_comments TEXT,
                property_group VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            );
            ALTER TABLE chicago_building_violations ADD COLUMN IF NOT EXISTS property_group VARCHAR(100);
            CREATE INDEX IF NOT EXISTS idx_chicago_address ON chicago_building_violations(address);
            CREATE INDEX IF NOT EXISTS idx_chicago_group ON chicago_building_violations(property_group);
        """)
        conn.commit()

def run_chicago_ingest(limit: int = 2000):
    logger.info(f"Starting Chicago Open Data Building Violations Ingestion (limit={limit})...")
    conn = get_db_connection()
    init_db(conn)

    params = {
        "$limit": limit,
        "$order": "violation_date DESC"
    }

    try:
        res = requests.get(CHICAGO_SOCRATA_ENDPOINT, params=params, timeout=20)
        res.raise_for_status()
        records = res.json()
        logger.info(f"Fetched {len(records)} building violation records from Chicago Socrata API.")
    except Exception as e:
        logger.error(f"Failed to fetch data from Chicago Socrata API: {e}")
        return False

    records_to_insert = []
    for r in records:
        v_id = r.get("id") or r.get("inspection_number")
        if not v_id:
            continue
        
        raw_date = r.get("violation_date")
        v_date = None
        if raw_date:
            try:
                v_date = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            except Exception:
                pass

        v_code = r.get("violation_code", "")
        v_desc = r.get("violation_description", "")
        v_status = r.get("violation_status", "OPEN")
        addr = r.get("address", "")
        v_zip = r.get("zip", "")
        comments = r.get("violation_inspector_comments", "")
        prop_group = r.get("property_group", "")

        records_to_insert.append((
            str(v_id), v_date, v_code, v_desc, v_status, addr, v_zip, comments, prop_group
        ))

    if records_to_insert:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO chicago_building_violations (
                    violation_id, violation_date, violation_code, violation_description,
                    violation_status, address, zip, inspector_comments, property_group
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (violation_id) DO UPDATE SET
                    violation_status = EXCLUDED.violation_status,
                    inspector_comments = EXCLUDED.inspector_comments;
            """, records_to_insert)
            
            # Record dataset freshness in data_source_status
            cur.execute("""
                INSERT INTO data_source_status (source_name, source_type, last_refreshed_at, refresh_status)
                VALUES ('CHICAGO', 'api', NOW(), 'success')
                ON CONFLICT (source_name) DO UPDATE SET
                    last_refreshed_at = NOW(),
                    refresh_status = 'success';
            """)

            conn.commit()

        logger.info(f"✓ Ingested {len(records_to_insert)} Chicago building violation records.")

    conn.close()
    return True

if __name__ == "__main__":
    run_chicago_ingest(limit=1000)
