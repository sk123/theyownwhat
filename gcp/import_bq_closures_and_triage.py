#!/usr/bin/env python3
"""
gcp/import_bq_closures_and_triage.py
====================================
Imports the permanent 61-jurisdiction BigQuery derived outputs from
`/home/sk/dev/theyownwhat/data/opencorporates/bq_exports/` into local PostgreSQL:
  1. `bq_national_ra_hubs.csv.gz` -> `oc_registered_agent_addresses`
     (61-jurisdiction North American commercial registered-agent blocklist)
  2. `bq_cross_state_pierced_branches.csv.gz` -> `oc_cross_state_pierced_branches`
     (pre-pierced beneficial human principals for foreign branches whose home
     company lives in any of the 61 US/Canadian registries, e.g. CA, TX, GA, OH, ON)
  3. `bq_us_zcta_acs_housing.csv.gz` -> `us_zcta_acs_housing`
     (U.S. Census ACS 5-year ZCTA renter share, median rent, and rent-burden context)
  4. Generates `oc_fuzzy_review_candidates` (`pg_trgm` candidate queue for Tier-5
     quarantined mangled assessor LLC names — strictly `REVIEW_REQUIRED` and never
     auto-merged, honoring `docs/ASSURANCE_POLICY.md` Rule 2).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import sys
import time
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.shared_utils import normalize_mailing_address_coarse  # noqa: E402

EXPORT_DIR = os.path.join(REPO_ROOT, "data", "opencorporates", "bq_exports")


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def ensure_bq_derived_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS oc_cross_state_pierced_branches (
                branch_jurisdiction_code TEXT NOT NULL,
                branch_company_number    TEXT NOT NULL,
                branch_name              TEXT,
                home_jurisdiction_code   TEXT NOT NULL,
                home_company_number      TEXT NOT NULL,
                beneficial_principals    TEXT[] NOT NULL,
                principal_count          INTEGER DEFAULT 0,
                updated_at               TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (branch_jurisdiction_code, branch_company_number, home_jurisdiction_code, home_company_number)
            );
            CREATE INDEX IF NOT EXISTS idx_oc_cspb_home
                ON oc_cross_state_pierced_branches (home_jurisdiction_code, home_company_number);

            CREATE TABLE IF NOT EXISTS us_zcta_acs_housing (
                zcta5                              TEXT PRIMARY KEY,
                total_pop                          INTEGER,
                occupied_units                     INTEGER,
                renter_units                       INTEGER,
                renter_share_pct                   NUMERIC(5,2),
                median_contract_rent               INTEGER,
                median_household_income            INTEGER,
                rent_burdened_30pct_plus_share     NUMERIC(5,2),
                severely_rent_burdened_50pct_share NUMERIC(5,2),
                source                             TEXT DEFAULT 'US_CENSUS_ACS_5YR_BIGQUERY_PUBLIC',
                updated_at                         TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS oc_fuzzy_review_candidates (
                id                       BIGSERIAL PRIMARY KEY,
                jurisdiction             TEXT NOT NULL,
                bbl                      TEXT NOT NULL,
                assessor_owner_raw       TEXT NOT NULL,
                assessor_owner_norm      TEXT NOT NULL,
                parcel_mailing_coarse    TEXT,
                candidate_oc_jur         TEXT NOT NULL,
                candidate_oc_number      TEXT NOT NULL,
                candidate_oc_name        TEXT NOT NULL,
                trigram_similarity       NUMERIC(4,3) NOT NULL,
                address_corroborated     BOOLEAN DEFAULT FALSE,
                review_status            TEXT NOT NULL DEFAULT 'REVIEW_REQUIRED',
                created_at               TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (jurisdiction, bbl, candidate_oc_jur, candidate_oc_number)
            );
            """
        )
    conn.commit()


import re

_COMMERCIAL_RA_NAME_RE = re.compile(
    r"\b(?:CORPORATION\s+SERVICE|CT\s+CORPORATION|C\.?\s*T\.?\s*CORP|CSC|"
    r"REGISTERED\s+AGENT|LEGALZOOM|COGENCY|INCORP\s+SERVICES|PARACORP|"
    r"NORTHWEST\s+REGISTERED|CAPITOL\s+SERVICES|NATIONAL\s+REGISTERED|"
    r"UNITED\s+STATES\s+CORP|CORPORATE\s+CREATIONS|RESIDENT\s+AGENTS|"
    r"AGENT\s+SERVICE|BUSINESS\s+FILINGS|ZENBUSINESS|INCFILE|LAW\s+OFFICES?|"
    r"ATTORNEYS?\s+AT\s+LAW)\b",
    re.IGNORECASE,
)


def import_national_ra_hubs(conn, gz_path: str) -> int:
    if not os.path.exists(gz_path):
        return 0
    rows_by_coarse = {}
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as gz:
        for r in csv.DictReader(gz):
            raw_sample = (r.get("sample_raw_address") or r.get("addr_norm") or "").strip()
            coarse = normalize_mailing_address_coarse(raw_sample)
            if not coarse or len(coarse) < 6:
                continue
            cnt = int(float(r.get("distinct_company_count") or 0))
            names = [n.strip() for n in (r.get("sample_agent_names") or "").split("|") if n.strip()][:10]
            # Never blacklist a self-agent corporate HQ (e.g. a single landlord acting as
            # statutory agent for their own 30 LLCs at their own office!). Require either a
            # commercial RA/law-firm name, >= 5 distinct unrelated agent names, or >= 150 cos.
            has_comm_ra = any(_COMMERCIAL_RA_NAME_RE.search(nm) for nm in names)
            if not (has_comm_ra or len(names) >= 5 or cnt >= 150):
                continue
            prev = rows_by_coarse.get(coarse)
            if prev is None or cnt > prev[2]:
                rows_by_coarse[coarse] = (coarse, raw_sample[:240], cnt, names, "bigquery_61jur_agent_closure")

    batch = list(rows_by_coarse.values())
    with conn.cursor() as cur:
        for idx in range(0, len(batch), 5000):
            execute_values(
                cur,
                """
                INSERT INTO oc_registered_agent_addresses (
                    address_coarse, sample_raw_address, distinct_company_count, distinct_agent_names, source
                ) VALUES %s
                ON CONFLICT (address_coarse) DO UPDATE SET
                    distinct_company_count = GREATEST(oc_registered_agent_addresses.distinct_company_count, EXCLUDED.distinct_company_count),
                    distinct_agent_names = COALESCE(NULLIF(EXCLUDED.distinct_agent_names, '{}'), oc_registered_agent_addresses.distinct_agent_names),
                    computed_at = NOW();
                """,
                batch[idx : idx + 5000],
                page_size=5000,
            )
    conn.commit()
    return len(batch)


def import_cross_state_pierced_branches(conn, gz_path: str) -> int:
    if not os.path.exists(gz_path):
        return 0
    seen = set()
    total = 0
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS oc_cross_state_pierced_branches;")
        cur.execute(
            """
            CREATE TABLE oc_cross_state_pierced_branches (
                branch_jurisdiction_code TEXT NOT NULL,
                branch_company_number    TEXT NOT NULL,
                branch_name              TEXT,
                home_jurisdiction_code   TEXT NOT NULL,
                home_company_number      TEXT NOT NULL,
                beneficial_principals    TEXT[] NOT NULL,
                principal_count          INTEGER DEFAULT 0,
                updated_at               TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        buffered = 0

        def _flush() -> None:
            nonlocal buffered
            if buffered == 0:
                return
            buf.seek(0)
            cur.copy_expert(
                """
                COPY oc_cross_state_pierced_branches (
                    branch_jurisdiction_code, branch_company_number, branch_name,
                    home_jurisdiction_code, home_company_number, beneficial_principals, principal_count
                ) FROM STDIN WITH (FORMAT csv)
                """,
                buf,
            )
            buf.seek(0)
            buf.truncate(0)
            buffered = 0

        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as gz:
            for r in csv.DictReader(gz):
                bj = (r.get("branch_jur") or "").strip().lower()
                bn = (r.get("branch_num") or "").strip()
                hj = (r.get("home_jur") or "").strip().lower()
                hn = (r.get("home_num") or "").strip()
                if not (bj and bn and hj and hn) or (bj, bn, hj, hn) in seen:
                    continue
                principals = [p.strip() for p in (r.get("home_beneficial_principals") or "").split("|") if p.strip()]
                if not principals:
                    continue
                seen.add((bj, bn, hj, hn))
                pg_arr = "{" + ",".join('"' + p.replace("\\", "\\\\").replace('"', '\\"') + '"' for p in principals) + "}"
                writer.writerow([bj, bn, (r.get("branch_name") or "").strip(), hj, hn, pg_arr, len(principals)])
                total += 1
                buffered += 1
                if buffered >= 100_000:
                    _flush()
        _flush()
        cur.execute(
            """
            ALTER TABLE oc_cross_state_pierced_branches
                ADD PRIMARY KEY (branch_jurisdiction_code, branch_company_number, home_jurisdiction_code, home_company_number);
            CREATE INDEX idx_oc_cspb_home
                ON oc_cross_state_pierced_branches (home_jurisdiction_code, home_company_number);
            ANALYZE oc_cross_state_pierced_branches;
            """
        )
    conn.commit()
    return total


def import_zcta_acs_housing(conn, gz_path: str) -> int:
    if not os.path.exists(gz_path):
        return 0
    rows: List[Tuple] = []

    def _num(v):
        if v is None or str(v).strip() == "":
            return None
        return float(v)

    def _int(v):
        if v is None or str(v).strip() == "":
            return None
        return int(float(v))

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as gz:
        for r in csv.DictReader(gz):
            z = (r.get("zcta5") or "").strip()
            if not z:
                continue
            rows.append(
                (
                    z,
                    _int(r.get("total_pop")),
                    _int(r.get("occupied_units")),
                    _int(r.get("renter_units")),
                    _num(r.get("renter_share_pct")),
                    _int(r.get("median_contract_rent")),
                    _int(r.get("median_household_income")),
                    _num(r.get("rent_burdened_30pct_plus_share")),
                    _num(r.get("severely_rent_burdened_50pct_share")),
                )
            )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE us_zcta_acs_housing;")
        execute_values(
            cur,
            """
            INSERT INTO us_zcta_acs_housing (
                zcta5, total_pop, occupied_units, renter_units, renter_share_pct,
                median_contract_rent, median_household_income,
                rent_burdened_30pct_plus_share, severely_rent_burdened_50pct_share
            ) VALUES %s
            ON CONFLICT (zcta5) DO NOTHING;
            """,
            rows,
            page_size=5000,
        )
    conn.commit()
    return len(rows)


def build_fuzzy_review_candidates(conn, jurisdiction: str = "dc", state_abbr: str = "DC", limit: int = 500) -> int:
    """Populate `oc_fuzzy_review_candidates` for Tier-5 unlinked business parcels sharing a
    non-agent coarse mailing address with an `oc_companies` entity and `similarity >= 0.65`.
    Strictly quarantined (`review_status = 'REVIEW_REQUIRED'`) — never auto-applied!"""
    jur_code = f"us_{state_abbr.lower()}"
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cur.execute(
            f"""
            INSERT INTO oc_fuzzy_review_candidates (
                jurisdiction, bbl, assessor_owner_raw, assessor_owner_norm,
                parcel_mailing_coarse, candidate_oc_jur, candidate_oc_number,
                candidate_oc_name, trigram_similarity, address_corroborated, review_status
            )
            SELECT
                %s,
                p.bbl,
                p.owner_name,
                p.owner_name_norm,
                COALESCE(NULLIF(p.mailing_address_coarse, ''), p.mailing_address_norm),
                c.jurisdiction_code,
                c.company_number,
                c.name,
                ROUND(similarity(p.owner_name_norm, c.name_norm)::numeric, 3),
                TRUE,
                'REVIEW_REQUIRED'
            FROM {jurisdiction}_properties p
            JOIN oc_companies c
              ON c.jurisdiction_code = %s
             AND c.mailing_address_coarse IS NOT NULL
             AND c.mailing_address_coarse != ''
             AND c.mailing_address_coarse = p.mailing_address_coarse
            LEFT JOIN oc_registered_agent_addresses ra
              ON ra.address_coarse = c.mailing_address_coarse
            WHERE p.oc_company_number IS NULL
              AND p.owner_name_norm ~ '\\y(LLC|INC|CORP|LP|LLP|TRUST|PROPERTIES|HOLDINGS|REALTY)\\y'
              AND ra.address_coarse IS NULL
              AND p.owner_name_norm != c.name_norm
              AND similarity(p.owner_name_norm, c.name_norm) >= 0.62
            LIMIT %s
            ON CONFLICT DO NOTHING;
            """,
            (jurisdiction, jur_code, limit),
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Import BigQuery 61-jurisdiction exports & populate Tier-5 review queue.")
    parser.add_argument("--export-dir", default=EXPORT_DIR)
    parser.add_argument("--skip-fuzzy", action="store_true")
    parser.add_argument("--only-fuzzy", action="store_true")
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_db_url())
    try:
        ensure_bq_derived_tables(conn)
        n_ra = n_cs = n_acs = n_fuz = 0
        if not args.only_fuzzy:
            n_ra = import_national_ra_hubs(conn, os.path.join(args.export_dir, "bq_national_ra_hubs.csv.gz"))
            n_cs = import_cross_state_pierced_branches(conn, os.path.join(args.export_dir, "bq_cross_state_pierced_branches.csv.gz"))
            n_acs = import_zcta_acs_housing(conn, os.path.join(args.export_dir, "bq_us_zcta_acs_housing.csv.gz"))
        if not args.skip_fuzzy:
            for jur, st in [("dc", "DC"), ("ma", "MA"), ("fl", "FL"), ("nc", "NC"), ("ny", "NY")]:
                n_fuz += build_fuzzy_review_candidates(conn, jur, st, limit=500)
        print(
            f"✓ Imported BigQuery closures into PostgreSQL:\n"
            f"  - National RA hubs (61 registries):          {n_ra:,}\n"
            f"  - Cross-state pierced foreign branches:      {n_cs:,}\n"
            f"  - U.S. Census ACS ZCTA housing rows:         {n_acs:,}\n"
            f"  - Quarantined Tier-5 fuzzy review candidates:{n_fuz:,}",
            flush=True,
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
