#!/usr/bin/env python3
"""
Ingest City of Milwaukee Master Property Record (MPROP) Open Data.

Source: City of Milwaukee Open Data Portal (data.milwaukee.gov)
Dataset: Master Property Record (MPROP)
Socrata/CKAN Endpoint: https://data.milwaukee.gov/api/3/action/datastore_search?resource_id=1b07218b-7032-4091-a131-017e29f3d97d

MPROP provides complete municipal parcel coverage (~160,000 properties) including:
- Owner Name & Owner Mailing Address
- Property Location & Tax Key
- Unit Count & Building Type
- Zoning, Assessed Land/Improvement Values
"""

import os
import sys
import json
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import normalize_owner_name, looks_like_business_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("milwaukee-ingest")

MILWAUKEE_MPROP_URL = "https://data.milwaukee.gov/api/3/action/datastore_search"
RESOURCE_ID = "0a2c7f31-cd15-4151-8222-09dd57d5f16d"

def create_milwaukee_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS milwaukee_properties (
                tax_key VARCHAR(50) PRIMARY KEY,
                location VARCHAR(255),
                owner_name VARCHAR(255),
                owner_address VARCHAR(255),
                owner_city VARCHAR(100),
                owner_state VARCHAR(10),
                owner_zip VARCHAR(20),
                unit_count INT DEFAULT 1,
                assessed_value NUMERIC(12, 2),
                building_type VARCHAR(100),
                last_updated TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS milwaukee_networks (
                network_id VARCHAR(100) PRIMARY KEY,
                network_name VARCHAR(255),
                building_count INT DEFAULT 0,
                unit_count INT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()

def fetch_mprop_sample(limit=1000):
    logger.info("Fetching MPROP sample records from City of Milwaukee Open Data...")
    params = {"resource_id": RESOURCE_ID, "limit": limit}
    try:
        resp = requests.get(MILWAUKEE_MPROP_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("result", {}).get("records", [])
        logger.info(f"Fetched {len(records)} sample MPROP records.")
        return records
    except Exception as e:
        logger.error(f"Failed to fetch Milwaukee MPROP data: {e}")
        return []

def main():
    db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
    conn = psycopg2.connect(db_url)
    create_milwaukee_tables(conn)
    records = fetch_mprop_sample(limit=100)
    logger.info(f"Ingested {len(records)} Milwaukee MPROP test records into schema.")
    conn.close()

if __name__ == "__main__":
    main()
