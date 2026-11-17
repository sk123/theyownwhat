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
NY_DOS_CSV_URL = "https://data.ny.gov/api/views/n9v6-gdp6/rows.csv?accessType=DOWNLOAD"
CO_SOS_CSV_URL = "https://data.colorado.gov/api/views/4ykn-tg5h/rows.csv?accessType=DOWNLOAD"
CO_TRUE_CSV_URL = "https://data.colorado.gov/api/views/qpft-q492/rows.csv?accessType=DOWNLOAD"
FL_SUNBIZ_SFTP_URL = "sftp://sftp.floridados.gov/Public/doc/Quarterly/Cor/cordata.zip"
FL_SUNBIZ_USER = "Public:PubAccess1845!"

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
    """Streams companies and officers for a single `jurisdiction_code` into PostgreSQL
    using `COPY` into temporary tables followed by atomic swap/upsert."""

    COMPANY_COLS = (
        "jurisdiction_code",
        "company_number",
        "name",
        "name_norm",
        "name_prefix18",
        "company_type",
        "current_status",
        "is_active",
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

    def __init__(self, conn, jurisdiction_code: str, flush_every: int = 75000):
        self.conn = conn
        self.jur = jurisdiction_code
        self.flush_every = flush_every
        self.comp_buf: List[str] = []
        self.off_buf: List[str] = []
        self.comp_total = 0
        self.off_total = 0
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

    def finalize(self) -> Tuple[int, int]:
        self._flush_companies()
        self._flush_officers()
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM oc_officers WHERE jurisdiction_code = %s", (self.jur,))
            cur.execute("DELETE FROM oc_companies WHERE jurisdiction_code = %s", (self.jur,))
            cur.execute(
                """
                INSERT INTO oc_companies (
                    jurisdiction_code, company_number, name, name_norm, name_prefix18,
                    company_type, current_status, is_active, incorporation_date,
                    registered_address_raw, registered_address_coarse, registered_state,
                    mailing_address_raw, mailing_address_coarse, mailing_state,
                    agent_name, agent_name_norm, agent_address_raw, agent_address_coarse,
                    home_jurisdiction_code, opencorporates_url, retrieved_at, updated_at
                )
                SELECT DISTINCT ON (jurisdiction_code, company_number)
                    jurisdiction_code, company_number, name, name_norm, name_prefix18,
                    company_type, current_status, is_active, incorporation_date,
                    registered_address_raw, registered_address_coarse, registered_state,
                    mailing_address_raw, mailing_address_coarse, mailing_state,
                    agent_name, agent_name_norm, agent_address_raw, agent_address_coarse,
                    home_jurisdiction_code, opencorporates_url, NOW(), NOW()
                FROM tmp_oc_companies_stage
                ORDER BY jurisdiction_code, company_number;
                """
            )
            inserted_companies = cur.rowcount

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
            inserted_officers = cur.rowcount

            cur.execute("DROP TABLE IF EXISTS tmp_oc_companies_stage;")
            cur.execute("DROP TABLE IF EXISTS tmp_oc_officers_stage;")
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


def _record_status(conn, source_name: str, source_type: str, details: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status (
                source_name, source_type, last_refreshed_at, refresh_status, details
            )
            VALUES (%s, %s, NOW(), 'success', %s)
            ON CONFLICT (source_name) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = NOW(),
                refresh_status = 'success',
                details = EXCLUDED.details;
            """,
            (source_name, source_type, Json(details)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def ingest_pa_dos(conn) -> Tuple[int, int]:
    """Stream Pennsylvania DOS Registered Businesses & Governors/Officers (`xvd7-5r2c`)."""
    t0 = time.time()
    print("▶ [PA DOS] Streaming bulk CSV from data.pa.gov (xvd7-5r2c)...")
    writer = BulkOCStagingWriter(conn, "us_pa")
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
            "source_url": "https://data.pa.gov/Businesses/Registered-Businesses-in-PA-Current-by-County/xvd7-5r2c",
            "elapsed_sec": elapsed,
        },
    )
    print(f"✓ [PA DOS] Loaded {c_cnt:,} PA companies and {o_cnt:,} Governors/Officers in {elapsed}s.")
    return c_cnt, o_cnt


def ingest_ny_dos(conn) -> Tuple[int, int]:
    """Stream New York DOS Active Corporations (`n9v6-gdp6`) into `us_ny`."""
    t0 = time.time()
    print("▶ [NY DOS] Streaming bulk CSV from data.ny.gov (n9v6-gdp6)...")
    writer = BulkOCStagingWriter(conn, "us_ny")
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
            "source_url": "https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6",
            "elapsed_sec": elapsed,
        },
    )
    print(f"✓ [NY DOS] Loaded {c_cnt:,} NY companies and {o_cnt:,} CEOs in {elapsed}s.")
    return c_cnt, o_cnt


def ingest_co_sos(conn) -> Tuple[int, int]:
    """Stream Colorado SOS Business Entities (`4ykn-tg5h`) + True Names (`qpft-q492`) into `us_co`."""
    t0 = time.time()
    print("▶ [CO SOS] Streaming True Names CSV (qpft-q492) & Business Entities CSV (4ykn-tg5h)...")
    writer = BulkOCStagingWriter(conn, "us_co")

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
            "source_url": "https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h",
            "elapsed_sec": elapsed,
        },
    )
    print(f"✓ [CO SOS] Loaded {c_cnt:,} active CO companies and {o_cnt:,} True Name principals in {elapsed}s.")
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


def ingest_fl_sunbiz(conn, zip_path: str = "/tmp/sunbiz_cordata.zip", keep_zip: bool = False) -> Tuple[int, int]:
    """Download & stream-parse Florida Sunbiz 1440-byte corporate/LLC master (`cordata.zip`) into `us_fl`."""
    t0 = time.time()
    print("▶ [FL Sunbiz] Starting quarterly master corporate/LLC ingestion...")
    _download_sunbiz_zip(zip_path)

    writer = BulkOCStagingWriter(conn, "us_fl")
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
            "source_url": "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
            "elapsed_sec": elapsed,
        },
    )
    print(f"✓ [FL Sunbiz] Loaded {c_cnt:,} active FL companies and {o_cnt:,} Officers/LLC Managers in {elapsed}s.")
    return c_cnt, o_cnt


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk ingest state corporate/LLC registries into oc_companies & oc_officers.")
    parser.add_argument(
        "--states",
        nargs="+",
        default=["pa", "ny", "co", "fl"],
        choices=["pa", "ny", "co", "fl"],
        help="State corporate registries to ingest.",
    )
    parser.add_argument("--keep-sunbiz-zip", action="store_true", help="Keep /tmp/sunbiz_cordata.zip after load.")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_opencorporates_tables(conn)
        for st in args.states:
            if st == "pa":
                ingest_pa_dos(conn)
            elif st == "ny":
                ingest_ny_dos(conn)
            elif st == "co":
                ingest_co_sos(conn)
            elif st == "fl":
                ingest_fl_sunbiz(conn, keep_zip=args.keep_sunbiz_zip)

        ra_cnt = refresh_empirical_registered_agent_addresses(conn, min_distinct_companies=15)
        print(f"✓ Quarantined {ra_cnt:,} empirical high-volume registered-agent addresses across oc_companies.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
