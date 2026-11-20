#!/usr/bin/env python3
"""
scripts/ingest_state_corporate_registries.py
============================================
High-throughput, source-only bulk corporate/LLC registry ingester for:
  1. `us_pa` — Pennsylvania Department of State (`data.pa.gov` `xvd7-5r2c` bulk CSV:
               Corporations, LLCs, LPs + Governors, Presidents, Members, Managers, GP)
  2. `us_fl` — Florida Division of Corporations / Sunbiz (`sftp.floridados.gov`
               `Quarterly/Cor/cordata.zip` 1440-byte fixed-width master:
               Corporations, LLCs, LPs + up to 6 Officers / LLC Managers / Auth Members)
  3. `us_ny` — New York Department of State (`data.ny.gov` `n9v6-gdp6` bulk CSV:
               Active Corporations, LLCs, LPs + CEOs & Process/Location addresses)
  4. `us_co` — Colorado Secretary of State (`data.colorado.gov` `4ykn-tg5h` Business
               Entities + `qpft-q492` True Names bulk CSVs)

All records populate the canonical `oc_companies`, `oc_officers`, and
`oc_registered_agent_addresses` tables (`api/nationwide_schema.py`) using
PostgreSQL `COPY` staging batches so millions of entities load in minutes with
flat memory, strict 3rd-party/commercial-agent quarantine, and zero invented data.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import io
import json
import os
import re
import subprocess
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import zipfile

import psycopg2
from psycopg2.extras import Json
import requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_opencorporates_tables
from api.oc_linker import (
    classify_oc_officer_role,
    refresh_empirical_registered_agent_addresses,
)
from api.shared_utils import (
    US_STATE_CODES,
    compose_address,
    is_excluded_third_party_entity,
    looks_like_business_name,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TheyOwnWhatPublicRecordsSync/1.0)"
}

PA_DOS_CSV_URL = "https://data.pa.gov/api/views/xvd7-5r2c/rows.csv?accessType=DOWNLOAD"
PA_DOS_META_URL = "https://data.pa.gov/api/views/xvd7-5r2c.json"
NY_DOS_CSV_URL = "https://data.ny.gov/api/views/n9v6-gdp6/rows.csv?accessType=DOWNLOAD"
NY_DOS_META_URL = "https://data.ny.gov/api/views/n9v6-gdp6.json"
CO_SOS_CSV_URL = "https://data.colorado.gov/api/views/4ykn-tg5h/rows.csv?accessType=DOWNLOAD"
CO_SOS_META_URL = "https://data.colorado.gov/api/views/4ykn-tg5h.json"
CO_TRUE_CSV_URL = "https://data.colorado.gov/api/views/qpft-q492/rows.csv?accessType=DOWNLOAD"
FL_SUNBIZ_SFTP_URL = "sftp://sftp.floridados.gov/Public/doc/Quarterly/Cor/cordata.zip"
FL_SUNBIZ_USER = "Public:PubAccess1845!"
DC_DLCP_FS_BASE = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Business_Licensing_and_Grants_WebMercator/FeatureServer"
IRS_EO_BMF_URLS = (
    "https://www.irs.gov/pub/irs-soi/eo1.csv",
    "https://www.irs.gov/pub/irs-soi/eo2.csv",
    "https://www.irs.gov/pub/irs-soi/eo3.csv",
    "https://www.irs.gov/pub/irs-soi/eo4.csv",
)

NONPROFIT_COMPANY_TYPE_RE = re.compile(
    r"(?:non[- ]?profit|not[- ]?for[- ]?profit|non[- ]?stock|charit(?:able|y)|"
    r"benevolent|relig(?:ious|ion)|church|ecclesiastical|public\s+benefit|"
    r"mutual\s+benefit|housing\s+development\s+fund|501\s*\(\s*c\s*\)|eleemosynary)",
    re.IGNORECASE,
)

SUNBIZ_TITLE_MAP = {
    "MGR": "Manager",
    "MGRM": "Managing Member",
    "AMBR": "Authorized Member",
    "MBR": "Member",
    "P": "President",
    "PRES": "President",
    "V": "Vice President",
    "VP": "Vice President",
    "SVP": "Vice President",
    "EVP": "Vice President",
    "T": "Treasurer",
    "TRES": "Treasurer",
    "S": "Secretary",
    "SEC": "Secretary",
    "AS": "Secretary",
    "D": "Director",
    "DIR": "Director",
    "C": "Chairman",
    "CHMN": "Chairman",
    "CEO": "CEO",
    "CFO": "CFO",
    "COO": "COO",
    "GP": "General Partner",
    "MGP": "General Partner",
    "OWN": "Owner",
    "TR": "Trustee",
    "TTEE": "Trustee",
}

COMMERCIAL_AGENT_FRAGMENTS = (
    "CT CORPORATION",
    "C T CORPORATION",
    "CORPORATION SERVICE",
    "CSC LAWYERS",
    "LEGALINC",
    "LEGALZOOM",
    "NORTHWEST REGISTERED",
    "REGISTERED AGENTS INC",
    "REGISTERED AGENT",
    "UNITED STATES CORPORATION AGENTS",
    "COGENCY GLOBAL",
    "NATIONAL REGISTERED AGENTS",
    "PARACORP",
    "INCORP SERVICES",
    "CAPITOL SERVICES",
    "CAPITOL CORPORATE",
    "CORPORATE CREATIONS",
    "SPIEGEL & UTRERA",
    "SECRETARY OF STATE",
    "DEPARTMENT OF STATE",
    "ZENBUSINESS",
    "BIZEFILE",
    "ROCKET LAWYER",
    "HARBOR COMPLIANCE",
    "SWYFT FILINGS",
)


def _clean_text(val: Any) -> str:
    if val is None:
        return ""
    s = " ".join(str(val).replace("\x00", " ").replace("\t", " ").replace("\r", " ").replace("\n", " ").split())
    if s in {"-", "--", ".", "N/A", "NA", "NONE", "NULL", "UNKNOWN"}:
        return ""
    return s


def _clean_zip(val: Any) -> str:
    s = _clean_text(val)
    if not s:
        return ""
    m = re.search(r"(\d{5})", s)
    return m.group(1) if m else ""


def _clean_state(val: Any, fallback: str = "") -> str:
    s = _clean_text(val).upper()
    if s in US_STATE_CODES:
        return s
    return fallback


def _parse_mmddyyyy(val: str) -> Optional[str]:
    s = _clean_text(val)
    if not s:
        return None
    m_slash = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m_slash:
        mo, dy, yr = int(m_slash.group(1)), int(m_slash.group(2)), int(m_slash.group(3))
        if 1750 <= yr <= 2035 and 1 <= mo <= 12 and 1 <= dy <= 31:
            try:
                return dt.date(yr, mo, dy).isoformat()
            except ValueError:
                return None
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 8:
        mo, dy, yr = int(digits[:2]), int(digits[2:4]), int(digits[4:8])
        if 1750 <= yr <= 2035 and 1 <= mo <= 12 and 1 <= dy <= 31:
            try:
                return dt.date(yr, mo, dy).isoformat()
            except ValueError:
                pass
        yr2, mo2, dy2 = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
        if 1750 <= yr2 <= 2035 and 1 <= mo2 <= 12 and 1 <= dy2 <= 31:
            try:
                return dt.date(yr2, mo2, dy2).isoformat()
            except ValueError:
                pass
    return None


def _is_commercial_agent(name: str) -> bool:
    if not name:
        return False
    u = name.upper()
    if u.startswith(("C/O ", "CARE OF ", "ATTN ", "THE LLC", "THE CORPORATION")):
        return True
    return any(frag in u for frag in COMMERCIAL_AGENT_FRAGMENTS)


def _tsv_cell(val: Any) -> str:
    if val is None or val == "":
        return "\\N"
    if isinstance(val, bool):
        return "t" if val else "f"
    s = str(val).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")
    return s


class BulkOCStagingWriter:
    """Streams companies, officers, and optional trade/previous names for a
    single `jurisdiction_code` into PostgreSQL using `COPY` into temporary
    tables followed by NON-DESTRUCTIVE upsert (preserving historical
    OpenCorporates `2026-11-17` rows & officers by default)."""

    COMPANY_COLS = (
        "jurisdiction_code",
        "company_number",
        "name",
        "name_norm",
        "name_prefix18",
        "company_type",
        "current_status",
        "is_active",
        "is_nonprofit",
        "incorporation_date",
        "registered_address_raw",
        "registered_address_coarse",
        "registered_state",
        "mailing_address_raw",
        "mailing_address_coarse",
        "mailing_state",
        "agent_name",
        "agent_name_norm",
        "agent_address_raw",
        "agent_address_coarse",
        "home_jurisdiction_code",
        "opencorporates_url",
    )

    OFFICER_COLS = (
        "jurisdiction_code",
        "company_number",
        "name",
        "name_norm",
        "position",
        "role_class",
        "is_corporate_officer",
        "is_current",
        "address_raw",
        "address_coarse",
    )

    PREV_NAME_COLS = (
        "jurisdiction_code",
        "company_number",
        "company_name",
        "company_name_norm",
        "name_type",
    )

    def __init__(
        self,
        conn,
        jurisdiction_code: str,
        flush_every: int = 75000,
        replace_all: bool = False,
    ):
        self.conn = conn
        self.jur = jurisdiction_code
        self.flush_every = flush_every
        self.replace_all = bool(replace_all)
        self.comp_buf: List[str] = []
        self.off_buf: List[str] = []
        self.prev_buf: List[str] = []
        self.comp_total = 0
        self.off_total = 0
        self.prev_total = 0
        self._init_temp_tables()

    def _init_temp_tables(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS tmp_oc_companies_stage;
                CREATE TEMP TABLE tmp_oc_companies_stage (
                    jurisdiction_code         TEXT NOT NULL,
                    company_number            TEXT NOT NULL,
                    name                      TEXT NOT NULL,
                    name_norm                 TEXT NOT NULL,
                    name_prefix18             TEXT,
                    company_type              TEXT,
                    current_status            TEXT,
                    is_active                 BOOLEAN DEFAULT TRUE,
                    is_nonprofit              BOOLEAN DEFAULT FALSE,
                    incorporation_date        DATE,
                    registered_address_raw    TEXT,
                    registered_address_coarse TEXT,
                    registered_state          TEXT,
                    mailing_address_raw       TEXT,
                    mailing_address_coarse    TEXT,
                    mailing_state             TEXT,
                    agent_name                TEXT,
                    agent_name_norm           TEXT,
                    agent_address_raw         TEXT,
                    agent_address_coarse      TEXT,
                    home_jurisdiction_code    TEXT,
                    opencorporates_url        TEXT
                );
                DROP TABLE IF EXISTS tmp_oc_officers_stage;
                CREATE TEMP TABLE tmp_oc_officers_stage (
                    jurisdiction_code    TEXT NOT NULL,
                    company_number       TEXT NOT NULL,
                    name                 TEXT NOT NULL,
                    name_norm            TEXT NOT NULL,
                    position             TEXT,
                    role_class           TEXT NOT NULL,
                    is_corporate_officer BOOLEAN DEFAULT FALSE,
                    is_current           BOOLEAN DEFAULT TRUE,
                    address_raw          TEXT,
                    address_coarse       TEXT
                );
                DROP TABLE IF EXISTS tmp_oc_prev_names_stage;
                CREATE TEMP TABLE tmp_oc_prev_names_stage (
                    jurisdiction_code TEXT NOT NULL,
                    company_number    TEXT NOT NULL,
                    company_name      TEXT NOT NULL,
                    company_name_norm TEXT NOT NULL,
                    name_type         TEXT
                );
                """
            )

    def add_company(
        self,
        company_number: str,
        name: str,
        *,
        company_type: str = "",
        current_status: str = "Active",
        is_active: bool = True,
        is_nonprofit: Optional[bool] = None,
        incorporation_date: Optional[str] = None,
        registered_address_raw: str = "",
        registered_state: str = "",
        mailing_address_raw: str = "",
        mailing_state: str = "",
        agent_name: str = "",
        agent_address_raw: str = "",
        home_jurisdiction_code: Optional[str] = None,
        source_url: str = "",
    ) -> None:
        name_norm = normalize_business_name(name)
        if not name_norm or not company_number:
            return
        prefix18 = name_norm[:18] if len(name_norm) >= 18 else None
        reg_coarse = normalize_mailing_address_coarse(registered_address_raw) if registered_address_raw else None
        mail_coarse = normalize_mailing_address_coarse(mailing_address_raw) if mailing_address_raw else None
        ag_coarse = normalize_mailing_address_coarse(agent_address_raw) if agent_address_raw else None
        ag_norm = normalize_business_name(agent_name) if agent_name else None
        if is_nonprofit is None:
            is_np = bool(company_type and NONPROFIT_COMPANY_TYPE_RE.search(company_type))
        else:
            is_np = bool(is_nonprofit)

        row = "\t".join(
            _tsv_cell(v)
            for v in (
                self.jur,
                company_number,
                name,
                name_norm,
                prefix18,
                company_type or None,
                current_status or None,
                is_active,
                is_np,
                incorporation_date,
                registered_address_raw or None,
                reg_coarse or None,
                registered_state or None,
                mailing_address_raw or None,
                mail_coarse or None,
                mailing_state or None,
                agent_name or None,
                ag_norm or None,
                agent_address_raw or None,
                ag_coarse or None,
                home_jurisdiction_code or None,
                source_url or None,
            )
        )
        self.comp_buf.append(row)
        self.comp_total += 1
        if len(self.comp_buf) >= self.flush_every:
            self._flush_companies()

    def add_officer(
        self,
        company_number: str,
        officer_name: str,
        position: str,
        *,
        is_corporate_flag: bool = False,
        address_raw: str = "",
    ) -> None:
        nm = _clean_text(officer_name)
        if not nm or len(nm) < 3:
            return
        if _is_commercial_agent(nm):
            role_class, is_corp = ("agent_or_incorporator", bool(is_corporate_flag))
        else:
            role_class, is_corp = classify_oc_officer_role(position, nm, is_corporate_flag=is_corporate_flag)

        # Skip storing pure incorporator/agent rows in oc_officers to keep index lean and strict
        if role_class not in ("beneficial_principal", "corporate_parent"):
            return

        name_norm = normalize_business_name(nm) if is_corp else normalize_person_name(nm)
        if not name_norm:
            return
        addr_coarse = normalize_mailing_address_coarse(address_raw) if address_raw else None

        row = "\t".join(
            _tsv_cell(v)
            for v in (
                self.jur,
                company_number,
                nm,
                name_norm,
                position or None,
                role_class,
                is_corp,
                True,
                address_raw or None,
                addr_coarse or None,
            )
        )
        self.off_buf.append(row)
        self.off_total += 1
        if len(self.off_buf) >= self.flush_every:
            self._flush_officers()

    def add_previous_name(
        self,
        company_number: str,
        alt_name: str,
        name_type: str = "trade_name",
    ) -> None:
        nm = _clean_text(alt_name)
        if not nm or not company_number:
            return
        norm = normalize_business_name(nm)
        if not norm:
            return
        row = "\t".join(
            _tsv_cell(v)
            for v in (self.jur, company_number, nm, norm, name_type or "trade_name")
        )
        self.prev_buf.append(row)
        self.prev_total += 1
        if len(self.prev_buf) >= self.flush_every:
            self._flush_prev_names()

    def _flush_companies(self) -> None:
        if not self.comp_buf:
            return
        data = io.StringIO("\n".join(self.comp_buf) + "\n")
        with self.conn.cursor() as cur:
            cur.copy_from(data, "tmp_oc_companies_stage", columns=self.COMPANY_COLS)
        self.comp_buf.clear()

    def _flush_officers(self) -> None:
        if not self.off_buf:
            return
        data = io.StringIO("\n".join(self.off_buf) + "\n")
        with self.conn.cursor() as cur:
            cur.copy_from(data, "tmp_oc_officers_stage", columns=self.OFFICER_COLS)
        self.off_buf.clear()

    def _flush_prev_names(self) -> None:
        if not self.prev_buf:
            return
        data = io.StringIO("\n".join(self.prev_buf) + "\n")
        with self.conn.cursor() as cur:
            cur.copy_from(data, "tmp_oc_prev_names_stage", columns=self.PREV_NAME_COLS)
        self.prev_buf.clear()

    def finalize(self) -> Tuple[int, int]:
        self._flush_companies()
        self._flush_officers()
        self._flush_prev_names()
        with self.conn.cursor() as cur:
            if self.replace_all:
                cur.execute("DELETE FROM oc_officers WHERE jurisdiction_code = %s", (self.jur,))
                cur.execute("DELETE FROM oc_companies WHERE jurisdiction_code = %s", (self.jur,))

            cur.execute(
                """
                INSERT INTO oc_companies (
                    jurisdiction_code, company_number, name, name_norm, name_prefix18,
                    company_type, current_status, is_active, is_nonprofit, incorporation_date,
                    registered_address_raw, registered_address_coarse, registered_state,
                    mailing_address_raw, mailing_address_coarse, mailing_state,
                    agent_name, agent_name_norm, agent_address_raw, agent_address_coarse,
                    home_jurisdiction_code, opencorporates_url, registry_url, retrieved_at, updated_at
                )
                SELECT DISTINCT ON (jurisdiction_code, company_number)
                    jurisdiction_code, company_number, name, name_norm, name_prefix18,
                    company_type, current_status, is_active, is_nonprofit, incorporation_date,
                    registered_address_raw, registered_address_coarse, registered_state,
                    mailing_address_raw, mailing_address_coarse, mailing_state,
                    agent_name, agent_name_norm, agent_address_raw, agent_address_coarse,
                    home_jurisdiction_code, opencorporates_url, opencorporates_url, NOW(), NOW()
                FROM tmp_oc_companies_stage
                ORDER BY jurisdiction_code, company_number
                ON CONFLICT (jurisdiction_code, company_number) DO UPDATE SET
                    name                      = COALESCE(NULLIF(EXCLUDED.name, ''), oc_companies.name),
                    name_norm                 = COALESCE(NULLIF(EXCLUDED.name_norm, ''), oc_companies.name_norm),
                    name_prefix18             = COALESCE(NULLIF(EXCLUDED.name_prefix18, ''), oc_companies.name_prefix18),
                    company_type              = COALESCE(NULLIF(EXCLUDED.company_type, ''), oc_companies.company_type),
                    current_status            = COALESCE(NULLIF(EXCLUDED.current_status, ''), oc_companies.current_status),
                    is_active                 = EXCLUDED.is_active,
                    is_nonprofit              = (COALESCE(oc_companies.is_nonprofit, FALSE) OR COALESCE(EXCLUDED.is_nonprofit, FALSE)),
                    incorporation_date        = COALESCE(EXCLUDED.incorporation_date, oc_companies.incorporation_date),
                    registered_address_raw    = COALESCE(NULLIF(EXCLUDED.registered_address_raw, ''), oc_companies.registered_address_raw),
                    registered_address_coarse = COALESCE(NULLIF(EXCLUDED.registered_address_coarse, ''), oc_companies.registered_address_coarse),
                    registered_state          = COALESCE(NULLIF(EXCLUDED.registered_state, ''), oc_companies.registered_state),
                    mailing_address_raw       = COALESCE(NULLIF(EXCLUDED.mailing_address_raw, ''), oc_companies.mailing_address_raw),
                    mailing_address_coarse    = COALESCE(NULLIF(EXCLUDED.mailing_address_coarse, ''), oc_companies.mailing_address_coarse),
                    mailing_state             = COALESCE(NULLIF(EXCLUDED.mailing_state, ''), oc_companies.mailing_state),
                    agent_name                = COALESCE(NULLIF(EXCLUDED.agent_name, ''), oc_companies.agent_name),
                    agent_name_norm           = COALESCE(NULLIF(EXCLUDED.agent_name_norm, ''), oc_companies.agent_name_norm),
                    agent_address_raw         = COALESCE(NULLIF(EXCLUDED.agent_address_raw, ''), oc_companies.agent_address_raw),
                    agent_address_coarse      = COALESCE(NULLIF(EXCLUDED.agent_address_coarse, ''), oc_companies.agent_address_coarse),
                    home_jurisdiction_code    = COALESCE(NULLIF(EXCLUDED.home_jurisdiction_code, ''), oc_companies.home_jurisdiction_code),
                    registry_url              = COALESCE(NULLIF(EXCLUDED.registry_url, ''), oc_companies.registry_url),
                    retrieved_at              = NOW(),
                    updated_at                = NOW();
                """
            )
            inserted_companies = cur.rowcount

            if self.replace_all:
                cur.execute(
                    """
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm, position,
                        role_class, is_corporate_officer, is_current, address_raw, address_coarse
                    )
                    SELECT DISTINCT ON (jurisdiction_code, company_number, name_norm, role_class)
                        jurisdiction_code, company_number, name, name_norm, position,
                        role_class, is_corporate_officer, is_current, address_raw, address_coarse
                    FROM tmp_oc_officers_stage
                    ORDER BY jurisdiction_code, company_number, name_norm, role_class;
                    """
                )
            else:
                # Non-destructive merge: insert new official-registry officers while preserving
                # historical OpenCorporates 2026-11-17 officer records.
                cur.execute(
                    """
                    WITH dedup_stage AS (
                        SELECT DISTINCT ON (jurisdiction_code, company_number, name_norm, role_class)
                            jurisdiction_code, company_number, name, name_norm, position,
                            role_class, is_corporate_officer, is_current, address_raw, address_coarse
                        FROM tmp_oc_officers_stage
                        ORDER BY jurisdiction_code, company_number, name_norm, role_class
                    )
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm, position,
                        role_class, is_corporate_officer, is_current, address_raw, address_coarse
                    )
                    SELECT
                        s.jurisdiction_code, s.company_number, s.name, s.name_norm, s.position,
                        s.role_class, s.is_corporate_officer, s.is_current, s.address_raw, s.address_coarse
                    FROM dedup_stage s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM oc_officers o
                        WHERE o.jurisdiction_code = s.jurisdiction_code
                          AND o.company_number = s.company_number
                          AND o.name_norm = s.name_norm
                          AND o.role_class = s.role_class
                    );
                    """
                )
            inserted_officers = cur.rowcount

            if self.prev_total > 0:
                cur.execute(
                    """
                    WITH dedup_prev AS (
                        SELECT DISTINCT ON (jurisdiction_code, company_number, company_name_norm)
                            jurisdiction_code, company_number, company_name, company_name_norm, name_type
                        FROM tmp_oc_prev_names_stage
                        ORDER BY jurisdiction_code, company_number, company_name_norm
                    )
                    INSERT INTO oc_previous_names (
                        jurisdiction_code, company_number, company_name, company_name_norm, name_type
                    )
                    SELECT
                        p.jurisdiction_code, p.company_number, p.company_name, p.company_name_norm, p.name_type
                    FROM dedup_prev p
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM oc_previous_names e
                        WHERE e.jurisdiction_code = p.jurisdiction_code
                          AND e.company_number = p.company_number
                          AND e.company_name_norm = p.company_name_norm
                    );
                    """
                )

            cur.execute("DROP TABLE IF EXISTS tmp_oc_companies_stage;")
            cur.execute("DROP TABLE IF EXISTS tmp_oc_officers_stage;")
            cur.execute("DROP TABLE IF EXISTS tmp_oc_prev_names_stage;")
        self.conn.commit()
        return inserted_companies, inserted_officers


from contextlib import contextmanager

csv.field_size_limit(10_000_000)


@contextmanager
def _stream_csv_via_curl(url: str):
    """Stream large CSV exports via native libcurl + 4MB-buffered C TextIOWrapper (avoids urllib3 iter_lines 512B overhead)."""
    proc = subprocess.Popen(
        ["curl", "-fsSL", "--compressed", "-A", HTTP_HEADERS["User-Agent"], url],
        stdout=subprocess.PIPE,
        bufsize=4 * 1024 * 1024,
    )
    assert proc.stdout is not None
    text_stream = io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace")
    try:
        yield csv.reader(text_stream)
    finally:
        text_stream.close()
        ret = proc.wait(timeout=30)
        if ret != 0:
            raise RuntimeError(f"curl stream exited with code {ret} for {url}")


def _probe_socrata_updated_date(meta_url: str) -> Optional[dt.date]:
    """Fetch O(1) Socrata view metadata (`rowsUpdatedAt`) without downloading the CSV."""
    try:
        r = requests.get(meta_url, headers=HTTP_HEADERS, timeout=15)
        if r.status_code == 200:
            ts = r.json().get("rowsUpdatedAt")
            if ts:
                return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).date()
    except Exception:
        pass
    return None


def _get_stored_external_date(conn, source_name: str) -> Optional[dt.date]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT external_last_updated FROM data_source_status WHERE source_name = %s",
                (source_name,),
            )
            row = cur.fetchone()
            if row and row[0]:
                v = row[0]
                return v.date() if hasattr(v, "date") else v
    except Exception:
        pass
    return None


def _should_skip_unchanged(
    conn,
    source_name: str,
    upstream_date: Optional[dt.date],
    only_if_changed: bool,
) -> bool:
    if not only_if_changed or upstream_date is None:
        return False
    stored = _get_stored_external_date(conn, source_name)
    if stored is not None and upstream_date <= stored:
        print(f"  ✓ [{source_name}] Upstream unchanged ({upstream_date} <= stored {stored}); skipping download.")
        return True
    return False


def _record_status(
    conn,
    source_name: str,
    source_type: str,
    details: Dict[str, Any],
    external_last_updated: Optional[dt.date] = None,
    refresh_status: str = "success",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, external_last_updated,
                last_refreshed_at, refresh_status, details
            )
            VALUES (%s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                last_refreshed_at = NOW(),
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details;
            """,
            (source_name, source_type, external_last_updated, refresh_status, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def ingest_pa_dos(conn, only_if_changed: bool = False, replace_all: bool = False) -> Tuple[int, int]:
    """Stream Pennsylvania DOS Registered Businesses & Governors/Officers (`xvd7-5r2c`)."""
    t0 = time.time()
    ext_date = _probe_socrata_updated_date(PA_DOS_META_URL)
    if _should_skip_unchanged(conn, "PA_DOS_CORPORATIONS", ext_date, only_if_changed):
        return 0, 0
    print(f"▶ [PA DOS] Streaming bulk CSV from data.pa.gov (xvd7-5r2c, upstream={ext_date})...")
    writer = BulkOCStagingWriter(conn, "us_pa", replace_all=replace_all)
    seen_filings = set()
    rows_read = 0

    with _stream_csv_via_curl(PA_DOS_CSV_URL) as reader:
        header = next(reader)
        idx = {col.strip(): i for i, col in enumerate(header)}

        i_bsrc = idx["Business Name (Source)"]
        i_bfmt = idx["Business Name Formatted"]
        i_fnum = idx["Filing Number"]
        i_a1 = idx["Address Line 1"]
        i_a2 = idx["Address Line 2"]
        i_city = idx["City"]
        i_st = idx["State"]
        i_zip = idx["Zip"]
        i_btype = idx["Type of Business Registration"]
        i_cdate = idx["Creation Date"]
        i_role = idx["Governor/Principal Officer"]
        i_last = idx["Last Name"]
        i_mid = idx["Middle Name"]
        i_first = idx["First Name"]

        for row in reader:
            rows_read += 1
            if len(row) < 14:
                continue
            fnum = _clean_text(row[i_fnum])
            bname = _clean_text(row[i_bsrc]) or _clean_text(row[i_bfmt])
            if not fnum or not bname:
                continue

            addr1 = _clean_text(row[i_a1])
            addr2 = _clean_text(row[i_a2])
            street = " ".join(p for p in (addr1, addr2) if p)
            city = _clean_text(row[i_city])
            st = _clean_state(row[i_st], fallback="PA")
            zp = _clean_zip(row[i_zip])
            reg_addr = compose_address(street, city, st, zp)

            if fnum not in seen_filings:
                seen_filings.add(fnum)
                btype = _clean_text(row[i_btype])
                cdate = _parse_mmddyyyy(row[i_cdate])
                # Record PA registered office in both registered_address and agent_address
                # so high-volume CROP hubs (>=15 companies) are automatically quarantined
                # by `refresh_empirical_registered_agent_addresses`, while genuine single-landlord
                # PA offices remain usable when non-CROP.
                writer.add_company(
                    fnum,
                    bname,
                    company_type=btype,
                    current_status="Registered",
                    is_active=True,
                    incorporation_date=cdate,
                    registered_address_raw=reg_addr,
                    registered_state=st,
                    mailing_address_raw=reg_addr,
                    mailing_state=st,
                    agent_address_raw=reg_addr,
                    source_url="https://file.dos.pa.gov/search/business",
                )

            role = _clean_text(row[i_role])
            first = _clean_text(row[i_first])
            mid = _clean_text(row[i_mid])
            last = _clean_text(row[i_last])
            if first or last:
                full_name = " ".join(p for p in (first, mid, last) if p)
                is_corp = looks_like_business_name(full_name) and not first
                writer.add_officer(
                    fnum,
                    full_name,
                    role or "Governor",
                    is_corporate_flag=is_corp,
                    address_raw="",  # PA DOS address_line1 is the entity CROP office, not personal residence
                )

            if rows_read % 500000 == 0:
                print(f"  [PA DOS] Read {rows_read:,} CSV rows ({len(seen_filings):,} unique entities)...")

    c_cnt, o_cnt = writer.finalize()
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "PA_DOS_CORPORATIONS",
        "corporate_registry",
        {
            "companies_count": c_cnt,
            "officers_count": o_cnt,
            "csv_rows_processed": rows_read,
            "refresh_mode": "live_full",
            "merge_mode": "replace_all" if replace_all else "non_destructive_upsert",
            "source_url": "https://data.pa.gov/Businesses/Registered-Businesses-in-PA-Current-by-County/xvd7-5r2c",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date,
    )
    print(f"✓ [PA DOS] Merged {c_cnt:,} PA companies and {o_cnt:,} new Governors/Officers in {elapsed}s.")
    return c_cnt, o_cnt


def ingest_ny_dos(conn, only_if_changed: bool = False, replace_all: bool = False) -> Tuple[int, int]:
    """Stream New York DOS Active Corporations (`n9v6-gdp6`) into `us_ny`."""
    t0 = time.time()
    ext_date = _probe_socrata_updated_date(NY_DOS_META_URL)
    if _should_skip_unchanged(conn, "NY_DOS_CORPORATIONS", ext_date, only_if_changed):
        return 0, 0
    print(f"▶ [NY DOS] Streaming bulk CSV from data.ny.gov (n9v6-gdp6, upstream={ext_date})...")
    writer = BulkOCStagingWriter(conn, "us_ny", replace_all=replace_all)
    rows_read = 0

    with _stream_csv_via_curl(NY_DOS_CSV_URL) as reader:
        header = next(reader)
        idx = {col.strip(): i for i, col in enumerate(header)}

        i_id = idx["DOS ID"]
        i_name = idx["Current Entity Name"]
        i_fdate = idx["Initial DOS Filing Date"]
        i_jur = idx["Jurisdiction"]
        i_etype = idx["Entity Type"]
        i_pname = idx["DOS Process Name"]
        i_pa1 = idx["DOS Process Address 1"]
        i_pa2 = idx["DOS Process Address 2"]
        i_pcity = idx["DOS Process City"]
        i_pst = idx["DOS Process State"]
        i_pzip = idx["DOS Process Zip"]
        i_ceo = idx["CEO Name"]
        i_ca1 = idx["CEO Address 1"]
        i_ca2 = idx["CEO Address 2"]
        i_ccity = idx["CEO City"]
        i_cst = idx["CEO State"]
        i_czip = idx["CEO Zip"]
        i_ra = idx["Registered Agent Name"]
        i_ra1 = idx["Registered Agent Address 1"]
        i_ra2 = idx["Registered Agent Address 2"]
        i_rcity = idx["Registered Agent City"]
        i_rst = idx["Registered Agent State"]
        i_rzip = idx["Registered Agent Zip"]
        i_la1 = idx["Location Address 1"]
        i_la2 = idx["Location Address 2"]
        i_lcity = idx["Location City"]
        i_lst = idx["Location State"]
        i_lzip = idx["Location Zip"]

        for row in reader:
            rows_read += 1
            if len(row) < 25:
                continue
            dos_id = _clean_text(row[i_id])
            ename = _clean_text(row[i_name])
            if not dos_id or not ename:
                continue

            etype = _clean_text(row[i_etype])
            fdate = _parse_mmddyyyy(row[i_fdate])

            pname = _clean_text(row[i_pname])
            pstreet = " ".join(p for p in (_clean_text(row[i_pa1]), _clean_text(row[i_pa2])) if p)
            pst = _clean_state(row[i_pst])
            proc_addr = compose_address(pstreet, _clean_text(row[i_pcity]), pst, _clean_zip(row[i_pzip]))

            lstreet = " ".join(p for p in (_clean_text(row[i_la1]), _clean_text(row[i_la2])) if p)
            lst = _clean_state(row[i_lst])
            loc_addr = compose_address(lstreet, _clean_text(row[i_lcity]), lst, _clean_zip(row[i_lzip]))

            ra_name = _clean_text(row[i_ra])
            rstreet = " ".join(p for p in (_clean_text(row[i_ra1]), _clean_text(row[i_ra2])) if p)
            rst = _clean_state(row[i_rst])
            ra_addr = compose_address(rstreet, _clean_text(row[i_rcity]), rst, _clean_zip(row[i_rzip]))

            proc_is_agent = _is_commercial_agent(pname)
            if proc_is_agent and not ra_addr:
                ra_name = pname
                ra_addr = proc_addr

            mail_addr = "" if proc_is_agent else proc_addr
            reg_addr = loc_addr or mail_addr

            home_jur = None
            raw_jur = _clean_text(row[i_jur]).upper()
            if raw_jur == "DELAWARE":
                home_jur = "us_de"

            writer.add_company(
                dos_id,
                ename,
                company_type=etype,
                current_status="Active",
                is_active=True,
                incorporation_date=fdate,
                registered_address_raw=reg_addr,
                registered_state=lst or pst or "NY",
                mailing_address_raw=mail_addr,
                mailing_state=pst or "NY",
                agent_name=ra_name,
                agent_address_raw=ra_addr,
                home_jurisdiction_code=home_jur,
                source_url="https://apps.dos.ny.gov/publicInquiry/",
            )

            ceo = _clean_text(row[i_ceo])
            if ceo and not _is_commercial_agent(ceo):
                cstreet = " ".join(p for p in (_clean_text(row[i_ca1]), _clean_text(row[i_ca2])) if p)
                cst = _clean_state(row[i_cst])
                ceo_addr = compose_address(cstreet, _clean_text(row[i_ccity]), cst, _clean_zip(row[i_czip]))
                writer.add_officer(
                    dos_id,
                    ceo,
                    "CEO / Chairman",
                    is_corporate_flag=looks_like_business_name(ceo),
                    address_raw=ceo_addr,
                )

            if rows_read % 500000 == 0:
                print(f"  [NY DOS] Read {rows_read:,} CSV rows...")

    c_cnt, o_cnt = writer.finalize()
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "NY_DOS_CORPORATIONS",
        "corporate_registry",
        {
            "companies_count": c_cnt,
            "officers_count": o_cnt,
            "csv_rows_processed": rows_read,
            "refresh_mode": "live_partial",
            "merge_mode": "replace_all" if replace_all else "non_destructive_upsert",
            "source_url": "https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date,
        refresh_status="partial_live",
    )
    print(f"✓ [NY DOS] Merged {c_cnt:,} active NY companies and {o_cnt:,} new CEOs in {elapsed}s.")
    return c_cnt, o_cnt


def ingest_co_sos(conn, only_if_changed: bool = False, replace_all: bool = False) -> Tuple[int, int]:
    """Stream Colorado SOS Business Entities (`4ykn-tg5h`) + True Names (`qpft-q492`) into `us_co`."""
    t0 = time.time()
    ext_date = _probe_socrata_updated_date(CO_SOS_META_URL)
    if _should_skip_unchanged(conn, "CO_SOS_CORPORATIONS", ext_date, only_if_changed):
        return 0, 0
    print(f"▶ [CO SOS] Streaming True Names CSV (qpft-q492) & Business Entities CSV (4ykn-tg5h, upstream={ext_date})...")
    writer = BulkOCStagingWriter(conn, "us_co", replace_all=replace_all)

    # 1. Load True Names map (`entityid -> list of true names`)
    true_names: Dict[str, List[str]] = {}
    with _stream_csv_via_curl(CO_TRUE_CSV_URL) as reader:
        _ = next(reader, None)
        for row in reader:
            if len(row) < 2:
                continue
            eid = _clean_text(row[0]).strip("'")
            tname = _clean_text(row[1]).strip("'")
            if eid and tname:
                true_names.setdefault(eid, []).append(tname)
    print(f"  [CO SOS] Loaded True Names for {len(true_names):,} Colorado entities.")

    # 2. Stream Business Entities (active / Good Standing / Delinquent real-estate-eligible entities)
    rows_read = 0
    active_statuses = {"GOOD STANDING", "DELINQUENT", "NONCOMPLIANT", "EXISTS"}
    with _stream_csv_via_curl(CO_SOS_CSV_URL) as reader:
        header = next(reader)
        idx = {col.strip().lower(): i for i, col in enumerate(header)}

        i_id = idx["entityid"]
        i_name = idx["entityname"]
        i_pa1 = idx["principaladdress1"]
        i_pa2 = idx["principaladdress2"]
        i_pcity = idx["principalcity"]
        i_pst = idx["principalstate"]
        i_pzip = idx["principalzipcode"]
        i_ma1 = idx["mailingaddress1"]
        i_ma2 = idx["mailingaddress2"]
        i_mcity = idx["mailingcity"]
        i_mst = idx["mailingstate"]
        i_mzip = idx["mailingzipcode"]
        i_status = idx["entitystatus"]
        i_jur = idx["jurisdictonofformation"]
        i_etype = idx["entitytype"]
        i_af = idx["agentfirstname"]
        i_am = idx["agentmiddlename"]
        i_al = idx["agentlastname"]
        i_aorg = idx["agentorganizationname"]
        i_apa1 = idx["agentprincipaladdress1"]
        i_apa2 = idx["agentprincipaladdress2"]
        i_apcity = idx["agentprincipalcity"]
        i_apst = idx["agentprincipalstate"]
        i_apzip = idx["agentprincipalzipcode"]
        i_fdate = idx["entityformdate"]

        for row in reader:
            rows_read += 1
            if len(row) < 30:
                continue
            status = _clean_text(row[i_status])
            if status.upper() not in active_statuses:
                continue
            eid = _clean_text(row[i_id]).strip("'")
            ename = _clean_text(row[i_name])
            if not eid or not ename:
                continue

            pstreet = " ".join(p for p in (_clean_text(row[i_pa1]), _clean_text(row[i_pa2])) if p)
            pst = _clean_state(row[i_pst], fallback="CO")
            paddr = compose_address(pstreet, _clean_text(row[i_pcity]), pst, _clean_zip(row[i_pzip]))

            mstreet = " ".join(p for p in (_clean_text(row[i_ma1]), _clean_text(row[i_ma2])) if p)
            mst = _clean_state(row[i_mst], fallback=pst)
            maddr = compose_address(mstreet, _clean_text(row[i_mcity]), mst, _clean_zip(row[i_mzip]))

            aorg = _clean_text(row[i_aorg])
            aperson = " ".join(p for p in (_clean_text(row[i_af]), _clean_text(row[i_am]), _clean_text(row[i_al])) if p)
            ag_name = aorg or aperson
            astreet = " ".join(p for p in (_clean_text(row[i_apa1]), _clean_text(row[i_apa2])) if p)
            ast = _clean_state(row[i_apst], fallback="CO")
            ag_addr = compose_address(astreet, _clean_text(row[i_apcity]), ast, _clean_zip(row[i_apzip]))

            fjur = _clean_text(row[i_jur]).upper()
            home_jur = f"us_{fjur.lower()}" if fjur in US_STATE_CODES and fjur != "CO" else None

            writer.add_company(
                eid,
                ename,
                company_type=_clean_text(row[i_etype]),
                current_status=status,
                is_active=(status.upper() == "GOOD STANDING"),
                incorporation_date=_parse_mmddyyyy(row[i_fdate]),
                registered_address_raw=paddr,
                registered_state=pst,
                mailing_address_raw=maddr or paddr,
                mailing_state=mst or pst,
                agent_name=ag_name,
                agent_address_raw=ag_addr,
                home_jurisdiction_code=home_jur,
                source_url="https://www.sos.state.co.us/biz/BusinessEntityCriteriaExt.do",
            )

            for tname in true_names.get(eid, ()):
                if tname.upper() != ename.upper() and not _is_commercial_agent(tname):
                    writer.add_officer(
                        eid,
                        tname,
                        "Owner / Principal (True Name)",
                        is_corporate_flag=looks_like_business_name(tname),
                        address_raw=paddr,
                    )

            if rows_read % 500000 == 0:
                print(f"  [CO SOS] Scanned {rows_read:,} CSV rows ({writer.comp_total:,} active entities)...")

    c_cnt, o_cnt = writer.finalize()
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "CO_SOS_CORPORATIONS",
        "corporate_registry",
        {
            "companies_count": c_cnt,
            "officers_count": o_cnt,
            "csv_rows_processed": rows_read,
            "refresh_mode": "live_partial",
            "merge_mode": "replace_all" if replace_all else "non_destructive_upsert",
            "source_url": "https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date,
        refresh_status="partial_live",
    )
    print(f"✓ [CO SOS] Merged {c_cnt:,} active CO companies and {o_cnt:,} new True Name principals in {elapsed}s.")
    return c_cnt, o_cnt


def _download_sunbiz_zip(dest_path: str) -> None:
    """Download `cordata.zip` from `sftp.floridados.gov` using parallel byte-range curl workers."""
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1_500_000_000 and zipfile.is_zipfile(dest_path):
        print(f"  [FL Sunbiz] Reusing existing {dest_path} ({os.path.getsize(dest_path):,} bytes).")
        return

    # Probe remote file size via SFTP directory listing
    listing = subprocess.check_output(
        ["curl", "-sk", "--user", FL_SUNBIZ_USER, "sftp://sftp.floridados.gov/Public/doc/Quarterly/Cor/"],
        timeout=30,
    ).decode("utf-8", errors="replace")
    total_size = 0
    for ln in listing.splitlines():
        if "cordata.zip" in ln:
            parts = ln.split()
            for tok in parts:
                if tok.isdigit() and int(tok) > 100_000_000:
                    total_size = int(tok)
                    break

    if os.path.exists(dest_path) and total_size and os.path.getsize(dest_path) == total_size:
        print(f"  [FL Sunbiz] Existing {dest_path} matches exact size ({total_size:,} bytes).")
        return

    if total_size > 100_000_000:
        num_parts = 6
        chunk = (total_size + num_parts - 1) // num_parts
        part_files = []
        print(f"  [FL Sunbiz] Downloading {total_size / (1024**3):.2f} GB across {num_parts} parallel SFTP streams...")

        def _fetch_part(idx: int, lo: int, hi: int) -> str:
            pfile = f"{dest_path}.part{idx}"
            expected = hi - lo + 1
            if os.path.exists(pfile) and os.path.getsize(pfile) == expected:
                return pfile
            for attempt in range(3):
                res = subprocess.run(
                    [
                        "curl",
                        "-sk",
                        "--user",
                        FL_SUNBIZ_USER,
                        "-r",
                        f"{lo}-{hi}",
                        "-o",
                        pfile,
                        FL_SUNBIZ_SFTP_URL,
                    ],
                    timeout=900,
                )
                if res.returncode == 0 and os.path.exists(pfile) and os.path.getsize(pfile) == expected:
                    return pfile
                time.sleep(2 * (attempt + 1))
            raise RuntimeError(f"Failed SFTP range {lo}-{hi} for cordata.zip")

        try:
            with ThreadPoolExecutor(max_workers=num_parts) as pool:
                futs = {}
                for idx in range(num_parts):
                    lo = idx * chunk
                    hi = min(total_size - 1, lo + chunk - 1)
                    futs[pool.submit(_fetch_part, idx, lo, hi)] = idx
                ordered: Dict[int, str] = {}
                for fut in as_completed(futs):
                    i = futs[fut]
                    ordered[i] = fut.result()
                    print(f"    ✓ Sunbiz SFTP shard {i + 1}/{num_parts} complete")

            with open(dest_path, "wb") as out_f:
                for idx in range(num_parts):
                    pf = ordered[idx]
                    with open(pf, "rb") as in_f:
                        while True:
                            buf = in_f.read(16 * 1024 * 1024)
                            if not buf:
                                break
                            out_f.write(buf)
                    os.remove(pf)
            return
        except Exception as exc:
            print(f"  [FL Sunbiz] Parallel range fallback ({exc}); running single-stream curl...")
            for idx in range(num_parts):
                pf = f"{dest_path}.part{idx}"
                if os.path.exists(pf):
                    os.remove(pf)

    subprocess.check_call(
        ["curl", "-sk", "--user", FL_SUNBIZ_USER, "-o", dest_path, FL_SUNBIZ_SFTP_URL],
        timeout=2400,
    )


def _probe_sunbiz_remote_meta() -> Tuple[int, Optional[dt.date]]:
    """Return `(remote_size_bytes, remote_date)` for Sunbiz `cordata.zip` via O(1) SFTP listing."""
    try:
        listing = subprocess.check_output(
            ["curl", "-sk", "--user", FL_SUNBIZ_USER, "sftp://sftp.floridados.gov/Public/doc/Quarterly/Cor/"],
            timeout=30,
        ).decode("utf-8", errors="replace")
        for ln in listing.splitlines():
            if "cordata.zip" not in ln:
                continue
            parts = ln.split()
            size = 0
            for tok in parts:
                if tok.isdigit() and int(tok) > 100_000_000:
                    size = int(tok)
                    break
            # Typical `ls -l` date tokens: Month Day Year-or-HH:MM (indices 5,6,7)
            rem_dt: Optional[dt.date] = None
            if len(parts) >= 8:
                mon_s, day_s, yr_or_tm = parts[5], parts[6], parts[7]
                yr = dt.date.today().year if ":" in yr_or_tm else int(yr_or_tm)
                try:
                    rem_dt = dt.datetime.strptime(f"{mon_s} {day_s} {yr}", "%b %d %Y").date()
                except ValueError:
                    rem_dt = None
            return size, rem_dt
    except Exception:
        pass
    return 0, None


def ingest_fl_sunbiz(
    conn,
    zip_path: str = "/tmp/sunbiz_cordata.zip",
    keep_zip: bool = False,
    only_if_changed: bool = False,
    replace_all: bool = False,
) -> Tuple[int, int]:
    """Download & stream-parse Florida Sunbiz 1440-byte corporate/LLC master (`cordata.zip`) into `us_fl`."""
    t0 = time.time()
    rem_size, ext_date = _probe_sunbiz_remote_meta()
    if only_if_changed:
        stored_ext = _get_stored_external_date(conn, "FL_SUNBIZ_CORPORATIONS")
        stored_size = 0
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT details->>'remote_zip_bytes' FROM data_source_status WHERE source_name = 'FL_SUNBIZ_CORPORATIONS'")
                r = cur.fetchone()
                if r and r[0] and str(r[0]).isdigit():
                    stored_size = int(r[0])
        except Exception:
            pass
        if (rem_size and stored_size == rem_size) or (ext_date and stored_ext and ext_date <= stored_ext):
            print(f"  ✓ [FL Sunbiz] Upstream cordata.zip unchanged (size={rem_size:,}, date={ext_date}); skipping download.")
            return 0, 0

    print(f"▶ [FL Sunbiz] Starting quarterly master corporate/LLC ingestion (upstream={ext_date})...")
    _download_sunbiz_zip(zip_path)

    writer = BulkOCStagingWriter(conn, "us_fl", replace_all=replace_all)
    lines_read = 0

    @contextmanager
    def _open_sunbiz_member(zf_obj: zipfile.ZipFile, member_name: str):
        try:
            with zf_obj.open(member_name, "r") as zstream:
                yield io.TextIOWrapper(zstream, encoding="latin-1", errors="replace")
            return
        except NotImplementedError:
            pass
        # Sunbiz cordata.zip uses Deflate64 (ZIP method 9), supported natively by Info-ZIP `unzip -p`
        proc = subprocess.Popen(
            ["unzip", "-p", zip_path, member_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=4 * 1024 * 1024,
        )
        assert proc.stdout is not None
        txt_stream = io.TextIOWrapper(proc.stdout, encoding="latin-1", errors="replace")
        try:
            yield txt_stream
        finally:
            txt_stream.close()
            ret = proc.wait(timeout=60)
            if ret not in (0, 1):
                raise RuntimeError(f"unzip -p exited with code {ret} on {member_name}")

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            print(f"  [FL Sunbiz] Streaming {len(members)} archive member(s) from {zip_path}...")
            for member in members:
                with _open_sunbiz_member(zf, member) as text_stream:
                    for line in text_stream:
                        lines_read += 1
                        if len(line) < 670:
                            continue
                        status = line[204:205]
                        if status != "A":
                            continue

                        doc_num = line[0:12].strip()
                        corp_name = _clean_text(line[12:204])
                        if not doc_num or not corp_name:
                            continue

                        ftype = line[205:220].strip()
                        pa1 = _clean_text(line[220:262])
                        pa2 = _clean_text(line[262:304])
                        pstreet = " ".join(p for p in (pa1, pa2) if p)
                        pcity = _clean_text(line[304:332])
                        pstate = _clean_state(line[332:334], fallback="FL")
                        pzip = _clean_zip(line[334:344])
                        paddr = compose_address(pstreet, pcity, pstate, pzip)

                        ma1 = _clean_text(line[346:388])
                        ma2 = _clean_text(line[388:430])
                        mstreet = " ".join(p for p in (ma1, ma2) if p)
                        mcity = _clean_text(line[430:458])
                        mstate = _clean_state(line[458:460], fallback=pstate)
                        mzip = _clean_zip(line[460:470])
                        maddr = compose_address(mstreet, mcity, mstate, mzip)

                        fdate = _parse_mmddyyyy(line[472:480])
                        home_st = _clean_state(line[503:505])
                        home_jur = f"us_{home_st.lower()}" if home_st and home_st != "FL" else None

                        ra_type = line[586:587].strip()
                        if ra_type == "P":
                            ra_last = _clean_text(line[544:564])
                            ra_first = _clean_text(line[564:586])
                            ra_name = " ".join(p for p in (ra_first, ra_last) if p)
                        else:
                            ra_name = _clean_text(line[544:586])
                        ra_addr = compose_address(
                            _clean_text(line[587:629]),
                            _clean_text(line[629:657]),
                            _clean_state(line[657:659], fallback="FL"),
                            _clean_zip(line[659:668]),
                        )

                        writer.add_company(
                            doc_num,
                            corp_name,
                            company_type=ftype,
                            current_status="Active",
                            is_active=True,
                            incorporation_date=fdate,
                            registered_address_raw=paddr,
                            registered_state=pstate,
                            mailing_address_raw=maddr or paddr,
                            mailing_state=mstate or pstate,
                            agent_name=ra_name,
                            agent_address_raw=ra_addr,
                            home_jurisdiction_code=home_jur,
                            source_url="https://search.sunbiz.org/Inquiry/CorporationSearch/ByDocumentNumber",
                        )

                        for slot in range(6):
                            base = 668 + slot * 128
                            if base + 128 > len(line):
                                break
                            raw_title = line[base : base + 4].strip().upper()
                            otype = line[base + 4 : base + 5].strip().upper()
                            if otype == "P":
                                olast = _clean_text(line[base + 5 : base + 25])
                                ofirst = _clean_text(line[base + 25 : base + 47])
                                oname = " ".join(p for p in (ofirst, olast) if p)
                                is_corp_off = False
                            else:
                                oname = _clean_text(line[base + 5 : base + 47])
                                is_corp_off = (otype == "C") or looks_like_business_name(oname)

                            if not oname:
                                continue
                            pos_label = SUNBIZ_TITLE_MAP.get(raw_title, raw_title or "Manager")
                            oaddr = compose_address(
                                _clean_text(line[base + 47 : base + 89]),
                                _clean_text(line[base + 89 : base + 117]),
                                _clean_state(line[base + 117 : base + 119]),
                                _clean_zip(line[base + 119 : base + 128]),
                            )
                            writer.add_officer(
                                doc_num,
                                oname,
                                pos_label,
                                is_corporate_flag=is_corp_off,
                                address_raw=oaddr,
                            )

                        if lines_read % 1000000 == 0:
                            print(f"  [FL Sunbiz] Scanned {lines_read:,} records ({writer.comp_total:,} active companies)...")
    finally:
        if not keep_zip and os.path.exists(zip_path):
            os.remove(zip_path)
            print(f"  [FL Sunbiz] Removed temporary {zip_path}.")

    c_cnt, o_cnt = writer.finalize()
    ra_cnt = refresh_empirical_registered_agent_addresses(conn, min_distinct_companies=15)
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "FL_SUNBIZ_CORPORATIONS",
        "corporate_registry",
        {
            "companies_count": c_cnt,
            "officers_count": o_cnt,
            "quarantined_agent_hubs": ra_cnt,
            "records_scanned": lines_read,
            "remote_zip_bytes": rem_size or None,
            "refresh_mode": "live_full",
            "merge_mode": "replace_all" if replace_all else "non_destructive_upsert",
            "source_url": "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date or dt.date.today(),
    )
    print(f"✓ [FL Sunbiz] Merged {c_cnt:,} active FL companies and {o_cnt:,} new Officers/LLC Managers in {elapsed}s.")
    return c_cnt, o_cnt


def _arcgis_epoch_to_date(ms_val: Any) -> Optional[str]:
    if ms_val is None or ms_val == "":
        return None
    try:
        ms = int(ms_val)
        d = dt.datetime.fromtimestamp(ms / 1000.0, dt.timezone.utc).date()
        if 1750 <= d.year <= 2035:
            return d.isoformat()
    except Exception:
        pass
    return None


def _probe_dc_dlcp_max_mod_date(layer_id: int = 0) -> Optional[dt.date]:
    try:
        r = requests.get(
            f"{DC_DLCP_FS_BASE}/{layer_id}/query",
            params={
                "where": "DCS_LAST_MOD_DTTM IS NOT NULL",
                "outFields": "DCS_LAST_MOD_DTTM",
                "orderByFields": "DCS_LAST_MOD_DTTM DESC",
                "resultRecordCount": 1,
                "f": "json",
            },
            headers=HTTP_HEADERS,
            timeout=20,
        )
        if r.status_code == 200:
            feats = r.json().get("features") or []
            if feats:
                ms = (feats[0].get("attributes") or {}).get("DCS_LAST_MOD_DTTM")
                if ms:
                    return dt.datetime.fromtimestamp(int(ms) / 1000.0, dt.timezone.utc).date()
    except Exception:
        pass
    return None


def _iter_arcgis_table_keyset(
    layer_id: int,
    out_fields: str,
    where_extra: str = "",
    page_size: int = 2000,
):
    """Deterministic `OBJECTID > last_oid` keyset pagination over Open Data DC ArcGIS tables."""
    url = f"{DC_DLCP_FS_BASE}/{layer_id}/query"
    last_oid = 0
    while True:
        where = f"OBJECTID > {last_oid}"
        if where_extra:
            where = f"({where_extra}) AND {where}"
        params = {
            "where": where,
            "outFields": out_fields,
            "orderByFields": "OBJECTID ASC",
            "resultRecordCount": page_size,
            "f": "json",
        }
        payload = None
        for attempt in range(4):
            try:
                resp = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=45)
                if resp.status_code == 200:
                    payload = resp.json()
                    if "error" not in payload:
                        break
            except Exception:
                pass
            time.sleep(1.5 * (attempt + 1))
        if not payload or "error" in payload:
            raise RuntimeError(f"DC DLCP layer {layer_id} query failed after retries at OBJECTID > {last_oid}")
        feats = payload.get("features") or []
        if not feats:
            break
        for feat in feats:
            attrs = feat.get("attributes") or {}
            oid = attrs.get("OBJECTID")
            if oid is not None and int(oid) > last_oid:
                last_oid = int(oid)
            yield attrs
        if len(feats) < page_size:
            break


def ingest_dc_dlcp(
    conn,
    since_days: Optional[int] = None,
    only_if_changed: bool = False,
    replace_all: bool = False,
) -> Tuple[int, int]:
    """Stream official Open Data DC DLCP Corporate Registration (Table 0),
    Trade Names (Table 1), and Beneficial Owners (Table 2) into `us_dc`."""
    t0 = time.time()
    ext_date = _probe_dc_dlcp_max_mod_date(0) or _probe_dc_dlcp_max_mod_date(2)
    if _should_skip_unchanged(conn, "DC_DLCP_CORPORATIONS", ext_date, only_if_changed):
        return 0, 0

    where_corp = ""
    where_tn = ""
    if since_days is not None and since_days > 0:
        cutoff_str = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=since_days)).strftime("%Y-%m-%d 00:00:00")
        # DC stamps DCS_LAST_MOD_DTTM on every row during nightly ETL, so incremental mode filters
        # by newly formed or newly reported entities, then pulls matching Beneficial Owners by FILE_NUMBER.
        where_corp = (
            f"EFFECTIVE_DATE >= TIMESTAMP '{cutoff_str}' "
            f"OR DATE_LAST_REPORT_FILED >= TIMESTAMP '{cutoff_str}'"
        )
        where_tn = f"EFFECTIVE_DATE >= TIMESTAMP '{cutoff_str}'"

    mode_lbl = f"incremental last {since_days}d" if where_corp else "full sync"
    print(f"▶ [DC DLCP] Streaming Open Data DC Corporate Registration + Beneficial Owners + Trade Names ({mode_lbl}, upstream={ext_date})...", flush=True)
    writer = BulkOCStagingWriter(conn, "us_dc", replace_all=replace_all)

    # 1. Table 0: Corporate Registration (507K entities full; ~1K-5K rows/month incremental)
    corp_rows = 0
    touched_fnums: List[str] = []
    corp_fields = (
        "OBJECTID,FILE_NUMBER,ENTITY_STATUS,LOCALE,MODELTYPE,BUSINESS_NAME,"
        "BUSNIESS_ADDRESS_LINE1,BUSNIESS_ADDRESS_LINE2,BUSINESS_CITY,BUSINESS_STATE,ZIPCODE,"
        "RA_NAME,RA_ADDRESS1,RA_ADDRESS2,RA_CITY,RA_STATE,RA_ZIPCODE,EFFECTIVE_DATE"
    )
    for attrs in _iter_arcgis_table_keyset(0, corp_fields, where_extra=where_corp):
        corp_rows += 1
        fnum = _clean_text(attrs.get("FILE_NUMBER"))
        bname = _clean_text(attrs.get("BUSINESS_NAME"))
        if not fnum or not bname:
            continue
        if where_corp:
            touched_fnums.append(fnum)
        status = _clean_text(attrs.get("ENTITY_STATUS"))
        is_act = "ACTIVE" in status.upper() or "GOOD STANDING" in status.upper()
        bstreet = " ".join(
            p for p in (_clean_text(attrs.get("BUSNIESS_ADDRESS_LINE1")), _clean_text(attrs.get("BUSNIESS_ADDRESS_LINE2"))) if p
        )
        bst = _clean_state(attrs.get("BUSINESS_STATE"), fallback="DC")
        baddr = compose_address(bstreet, _clean_text(attrs.get("BUSINESS_CITY")), bst, _clean_zip(attrs.get("ZIPCODE")))

        ra_name = _clean_text(attrs.get("RA_NAME"))
        rastreet = " ".join(
            p for p in (_clean_text(attrs.get("RA_ADDRESS1")), _clean_text(attrs.get("RA_ADDRESS2"))) if p
        )
        rast = _clean_state(attrs.get("RA_STATE"), fallback="DC")
        ra_addr = compose_address(rastreet, _clean_text(attrs.get("RA_CITY")), rast, _clean_zip(attrs.get("RA_ZIPCODE")))

        writer.add_company(
            fnum,
            bname,
            company_type=_clean_text(attrs.get("MODELTYPE")),
            current_status=status or "Unknown",
            is_active=is_act,
            incorporation_date=_arcgis_epoch_to_date(attrs.get("EFFECTIVE_DATE")),
            registered_address_raw=baddr,
            registered_state=bst,
            mailing_address_raw=baddr,
            mailing_state=bst,
            agent_name=ra_name,
            agent_address_raw=ra_addr,
            source_url="https://corponline.dlcp.dc.gov/",
        )
        if corp_rows % 100000 == 0:
            print(f"  [DC DLCP] Table 0 Corporate Registration: {corp_rows:,} rows...", flush=True)

    # 2. Table 2: Official Beneficial Owners (494K beneficial owners -> oc_officers!)
    bo_rows = 0
    bo_fields = "OBJECTID,NAME,ADDRESS,BUSINESSNAME,MODELTYPE,INITIALFILENUMBER,STATUS"
    if where_corp:
        # Incremental mode: fetch beneficial owners for all newly formed / newly filed entities
        uniq_fnums = sorted(set(touched_fnums))
        for start in range(0, len(uniq_fnums), 80):
            chunk = uniq_fnums[start : start + 80]
            quoted = ",".join("'" + fn.replace("'", "''") + "'" for fn in chunk)
            in_where = f"INITIALFILENUMBER IN ({quoted})"
            for attrs in _iter_arcgis_table_keyset(2, bo_fields, where_extra=in_where):
                bo_rows += 1
                raw_fnum = _clean_text(attrs.get("INITIALFILENUMBER"))
                if raw_fnum.upper().startswith("US-DC-FN"):
                    raw_fnum = raw_fnum[8:].strip()
                oname = _clean_text(attrs.get("NAME"))
                if not raw_fnum or not oname:
                    continue
                writer.add_officer(
                    raw_fnum,
                    oname,
                    "Beneficial Owner",
                    is_corporate_flag=looks_like_business_name(oname),
                    address_raw=_clean_text(attrs.get("ADDRESS")),
                )
    else:
        for attrs in _iter_arcgis_table_keyset(2, bo_fields, where_extra=""):
            bo_rows += 1
            raw_fnum = _clean_text(attrs.get("INITIALFILENUMBER"))
            if raw_fnum.upper().startswith("US-DC-FN"):
                raw_fnum = raw_fnum[8:].strip()
            oname = _clean_text(attrs.get("NAME"))
            if not raw_fnum or not oname:
                continue
            writer.add_officer(
                raw_fnum,
                oname,
                "Beneficial Owner",
                is_corporate_flag=looks_like_business_name(oname),
                address_raw=_clean_text(attrs.get("ADDRESS")),
            )
            if bo_rows % 100000 == 0:
                print(f"  [DC DLCP] Table 2 Beneficial Owners: {bo_rows:,} rows...", flush=True)

    # 3. Table 1: Trade Names / DBAs (63K -> oc_previous_names)
    tn_rows = 0
    tn_fields = "OBJECTID,TRADE_NAME,INITIAL_FILENUMBER"
    for attrs in _iter_arcgis_table_keyset(1, tn_fields, where_extra=where_tn):
        tn_rows += 1
        raw_fnum = _clean_text(attrs.get("INITIAL_FILENUMBER"))
        tname = _clean_text(attrs.get("TRADE_NAME"))
        if raw_fnum and tname:
            writer.add_previous_name(raw_fnum, tname, name_type="trade_name")

    c_cnt, o_cnt = writer.finalize()
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "DC_DLCP_CORPORATIONS",
        "corporate_registry",
        {
            "companies_count": c_cnt,
            "officers_count": o_cnt,
            "trade_names_scanned": tn_rows,
            "beneficial_owner_rows_scanned": bo_rows,
            "corporate_rows_scanned": corp_rows,
            "sync_mode": mode_lbl,
            "refresh_mode": "live_full",
            "merge_mode": "replace_all" if replace_all else "non_destructive_upsert",
            "source_url": f"{DC_DLCP_FS_BASE}",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date,
    )
    print(f"✓ [DC DLCP] Merged {c_cnt:,} DC entities, {o_cnt:,} new Beneficial Owners, {tn_rows:,} Trade Names in {elapsed}s.")
    return c_cnt, o_cnt


def ingest_ct_sots_mirror(conn) -> Tuple[int, int]:
    """Mirror freshly-synced CT SOTS `businesses` & `principals` into `oc_companies` & `oc_officers` (`us_ct`)
    non-destructively so cross-state corporate piercing uses live CT SOTS updates."""
    t0 = time.time()
    print("▶ [CT SOTS Mirror] Syncing live CT SOTS businesses & principals into oc_companies/oc_officers (`us_ct`)...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO oc_companies (
                jurisdiction_code, company_number, name, name_norm, name_prefix18,
                company_type, current_status, is_active, is_nonprofit, incorporation_date,
                registered_address_raw, registered_state,
                mailing_address_raw, mailing_state,
                registry_url, retrieved_at, updated_at
            )
            SELECT
                'us_ct',
                b.id::text,
                b.name,
                b.name_norm,
                CASE WHEN LENGTH(b.name_norm) >= 18 THEN LEFT(b.name_norm, 18) ELSE NULL END,
                b.business_type,
                b.status,
                (UPPER(COALESCE(b.status, '')) LIKE 'ACTIVE%%'),
                (COALESCE(b.business_type, '') ~* '(non[- ]?stock|non[- ]?profit|not[- ]?for[- ]?profit|charit|relig|church|benevolent|housing\\s+development\\s+fund)'),
                CASE WHEN b.date_of_formation ~ '^\\d{4}-\\d{2}-\\d{2}' THEN SUBSTRING(b.date_of_formation, 1, 10)::date ELSE NULL END,
                NULLIF(TRIM(CONCAT_WS(', ', NULLIF(b.business_address, ''), NULLIF(b.business_city, ''), NULLIF(b.business_state, ''), NULLIF(b.business_zip, ''))), ''),
                NULLIF(UPPER(TRIM(COALESCE(b.business_state, 'CT'))), ''),
                NULLIF(TRIM(CONCAT_WS(', ', NULLIF(b.mail_address, ''), NULLIF(b.mail_city, ''), NULLIF(b.mail_state, ''), NULLIF(b.mail_zip, ''))), ''),
                NULLIF(UPPER(TRIM(COALESCE(b.mail_state, ''))), ''),
                'https://data.ct.gov/Business/Business-Master/n7gp-d28j',
                NOW(),
                NOW()
            FROM businesses b
            WHERE b.id IS NOT NULL
              AND b.name IS NOT NULL
              AND b.name_norm IS NOT NULL
              AND LENGTH(b.name_norm) > 0
              AND NOT EXISTS (
                  SELECT 1 FROM oc_companies oc
                  WHERE oc.jurisdiction_code = 'us_ct' AND oc.company_number = b.id::text
              )
            ON CONFLICT (jurisdiction_code, company_number) DO NOTHING;
            """
        )
        new_co = cur.rowcount

        cur.execute(
            """
            SELECT MAX(last_refreshed_at)::date
            FROM data_source_status
            WHERE source_name IN ('businesses', 'principals')
            """
        )
        r = cur.fetchone()
        ext_dt = r[0] if (r and r[0]) else dt.date.today()

    conn.commit()
    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "CT_SOTS_CORPORATIONS",
        "corporate_registry",
        {
            "newly_mirrored_companies": new_co,
            "refresh_mode": "live_full",
            "merge_mode": "non_destructive_upsert",
            "source_url": "https://data.ct.gov/Business/Business-Master/n7gp-d28j",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_dt,
    )
    print(f"✓ [CT SOTS Mirror] Added {new_co:,} newly filed CT SOTS entities into oc_companies (`us_ct`) in {elapsed}s.")
    return new_co, 0


def ingest_irs_eo_bmf(conn, only_if_changed: bool = False) -> Tuple[int, int]:
    """Stream official monthly IRS Exempt Organizations Business Master File (`eo1.csv`..`eo4.csv`)
    and flag matching 501(c) organizations `is_nonprofit = TRUE` across all loaded US jurisdictions
    so the nonprofit-board firewall stays continuously updated even in frozen-snapshot states."""
    from email.utils import parsedate_to_datetime

    t0 = time.time()
    ext_date: Optional[dt.date] = None
    try:
        hr = requests.head(IRS_EO_BMF_URLS[0], headers=HTTP_HEADERS, timeout=15, allow_redirects=True)
        lm = hr.headers.get("Last-Modified")
        if lm:
            ext_date = parsedate_to_datetime(lm).date()
    except Exception:
        pass

    if _should_skip_unchanged(conn, "IRS_EO_BMF_NONPROFITS", ext_date, only_if_changed):
        return 0, 0

    print(f"▶ [IRS EO BMF] Streaming official 50-state 501(c) nonprofit master files (Last-Modified={ext_date})...")
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE IF EXISTS tmp_irs_nonprofits;
            CREATE TEMP TABLE tmp_irs_nonprofits (
                jurisdiction_code TEXT NOT NULL,
                name_norm         TEXT NOT NULL
            );
            """
        )

    rows_read = 0
    buf: List[str] = []
    seen = set()
    for url in IRS_EO_BMF_URLS:
        with _stream_csv_via_curl(url) as reader:
            header = next(reader, None)
            if not header:
                continue
            idx = {c.strip().upper(): i for i, c in enumerate(header)}
            i_name = idx.get("NAME", 1)
            i_st = idx.get("STATE", 5)
            for row in reader:
                rows_read += 1
                if len(row) <= max(i_name, i_st):
                    continue
                st = _clean_state(row[i_st])
                if not st:
                    continue
                norm = normalize_business_name(row[i_name])
                if not norm or len(norm) < 4:
                    continue
                key = (f"us_{st.lower()}", norm)
                if key in seen:
                    continue
                seen.add(key)
                buf.append(f"{_tsv_cell(key[0])}\t{_tsv_cell(key[1])}")
                if len(buf) >= 100000:
                    with conn.cursor() as cur:
                        cur.copy_from(io.StringIO("\n".join(buf) + "\n"), "tmp_irs_nonprofits", columns=("jurisdiction_code", "name_norm"))
                    buf.clear()
    if buf:
        with conn.cursor() as cur:
            cur.copy_from(io.StringIO("\n".join(buf) + "\n"), "tmp_irs_nonprofits", columns=("jurisdiction_code", "name_norm"))
        buf.clear()

    with conn.cursor() as cur:
        cur.execute("CREATE INDEX idx_tmp_irs_np ON tmp_irs_nonprofits (jurisdiction_code, name_norm);")
        cur.execute(
            """
            UPDATE oc_companies c
            SET is_nonprofit = TRUE, updated_at = NOW()
            FROM tmp_irs_nonprofits t
            WHERE c.jurisdiction_code = t.jurisdiction_code
              AND c.name_norm = t.name_norm
              AND COALESCE(c.is_nonprofit, FALSE) = FALSE;
            """
        )
        flagged = cur.rowcount
        cur.execute("DROP TABLE IF EXISTS tmp_irs_nonprofits;")
    conn.commit()

    elapsed = round(time.time() - t0, 1)
    _record_status(
        conn,
        "IRS_EO_BMF_NONPROFITS",
        "system",
        {
            "irs_rows_scanned": rows_read,
            "unique_501c_names": len(seen),
            "newly_flagged_oc_nonprofits": flagged,
            "source_url": "https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf",
            "elapsed_sec": elapsed,
        },
        external_last_updated=ext_date or dt.date.today(),
    )
    print(f"✓ [IRS EO BMF] Scanned {rows_read:,} IRS 501(c) rows -> newly flagged {flagged:,} oc_companies as is_nonprofit=TRUE in {elapsed}s.")
    return flagged, 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Non-destructive live refresher for free official state corporate/LLC registries & IRS 501(c) nonprofits."
    )
    parser.add_argument(
        "--states",
        nargs="+",
        default=["ct", "dc", "pa", "ny", "co", "fl", "irs_nonprofits"],
        choices=["ct", "dc", "pa", "ny", "co", "fl", "irs_nonprofits"],
        help="Public corporate/nonprofit registries to refresh.",
    )
    parser.add_argument(
        "--only-if-changed",
        action="store_true",
        help="Skip multi-GB bulk downloads when upstream Socrata/ArcGIS/SFTP/HTTP watermark has not advanced.",
    )
    parser.add_argument(
        "--dc-since-days",
        type=int,
        default=None,
        help="Incremental window (days) for Open Data DC DLCP sync (e.g. 30 for weekly scheduled runs).",
    )
    parser.add_argument(
        "--replace-all",
        action="store_true",
        help="Dangerous: wipe existing jurisdiction rows before load (default is non-destructive upsert preserving 2026-11-17 OC history).",
    )
    parser.add_argument("--keep-sunbiz-zip", action="store_true", help="Keep /tmp/sunbiz_cordata.zip after load.")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_opencorporates_tables(conn)
        any_company_update = False
        for st in args.states:
            if st == "ct":
                ingest_ct_sots_mirror(conn)
                any_company_update = True
            elif st == "dc":
                c, _ = ingest_dc_dlcp(
                    conn,
                    since_days=args.dc_since_days,
                    only_if_changed=args.only_if_changed,
                    replace_all=args.replace_all,
                )
                any_company_update = any_company_update or (c > 0)
            elif st == "pa":
                c, _ = ingest_pa_dos(conn, only_if_changed=args.only_if_changed, replace_all=args.replace_all)
                any_company_update = any_company_update or (c > 0)
            elif st == "ny":
                c, _ = ingest_ny_dos(conn, only_if_changed=args.only_if_changed, replace_all=args.replace_all)
                any_company_update = any_company_update or (c > 0)
            elif st == "co":
                c, _ = ingest_co_sos(conn, only_if_changed=args.only_if_changed, replace_all=args.replace_all)
                any_company_update = any_company_update or (c > 0)
            elif st == "fl":
                c, _ = ingest_fl_sunbiz(
                    conn,
                    keep_zip=args.keep_sunbiz_zip,
                    only_if_changed=args.only_if_changed,
                    replace_all=args.replace_all,
                )
                any_company_update = any_company_update or (c > 0)
            elif st == "irs_nonprofits":
                ingest_irs_eo_bmf(conn, only_if_changed=args.only_if_changed)

        if any_company_update:
            ra_cnt = refresh_empirical_registered_agent_addresses(conn, min_distinct_companies=15)
            print(f"✓ Quarantined {ra_cnt:,} empirical high-volume registered-agent addresses across oc_companies.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
