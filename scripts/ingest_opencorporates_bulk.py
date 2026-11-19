#!/usr/bin/env python3
"""
scripts/ingest_opencorporates_bulk.py
=====================================
Multi-process, flat-memory streaming loader for OpenCorporates bulk SFTP
snapshots (`data/opencorporates/<snapshot>/<jur>/*.csv.gz`) into PostgreSQL:
  - `oc_companies`
  - `oc_officers` (beneficial principals & corporate parents; agent rows are
    aggregated directly onto `oc_companies.agent_*` and `oc_registered_agent_addresses`
    to eliminate 60M+ redundant commercial-agent rows)
  - `oc_previous_names` (`alternative_names.csv.gz` + pipe-split `companies.previous_names`)
  - `oc_branches` (`companies.csv.gz` foreign branch -> `home_jurisdiction_code` links)
  - `oc_registered_agent_addresses` (empirical >=15-company agent hub suppression)

Engineered for 24-thread multi-core throughput:
  - Longest-Processing-Time-First (LPT) scheduling across `--workers` processes
    so the largest states (`us_fl`, `us_ny`, `us_nc`, `us_il`, `us_mi`, `us_pa`, `us_ma`)
    stream concurrently from `t=0`.
  - In `--bulk-reset` mode, drops PKs & secondary B-trees during pure heap `COPY`
    (Python `seen_cnums` guarantees uniqueness per jurisdiction) and rebuilds all
    constraints & indexes at the end with `max_parallel_maintenance_workers = 8`.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import datetime as dt
from functools import lru_cache
import gc
import io
import os
import subprocess
import sys
import time
import zlib
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_opencorporates_tables  # noqa: E402
from api.oc_linker import (  # noqa: E402
    classify_oc_officer_role,
    clean_oc_officer_name,
    extract_state_from_address,
    refresh_empirical_registered_agent_addresses,
)
from api.shared_utils import (  # noqa: E402
    is_excluded_third_party_entity,
    looks_like_business_name,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
)

WAVE1_JURISDICTIONS = [
    "us_dc",
    "us_vt",
    "us_ct",
    "us_ma",
    "us_de",
    "us_wy",
    "us_nv",
    "us_va",
    "us_md",
    "us_mn",
    "us_wi",
    "us_nc",
    "us_il",
    "us_mi",
    "us_pa",
    "us_co",
    "us_nj",
    "us_ny",
    "us_fl",
    "us_ar",
    "us_ca",
    "us_mt",
    "us_oh",
]

SECONDARY_INDEX_NAMES = [
    "idx_oc_companies_jur_name_norm",
    "idx_oc_companies_name_norm",
    "idx_oc_companies_jur_prefix18",
    "idx_oc_companies_reg_addr_coarse",
    "idx_oc_companies_mail_addr_coarse",
    "idx_oc_companies_home_entity",
    "idx_oc_officers_company",
    "idx_oc_officers_beneficial_name",
    "idx_oc_officers_addr_coarse",
    "idx_oc_prev_names_jur_norm",
    "idx_oc_prev_names_norm",
    "idx_oc_prev_names_company",
    "idx_oc_branches_home",
    "idx_oc_branches_branch",
]

# O(1) exact lowercase position lookup tables to bypass regex on >95% of officer rows
FAST_AGENT_POSITIONS = frozenset({
    "agent",
    "registered agent",
    "resident agent",
    "statutory agent",
    "incorporator",
    "organizer",
    "process",
    "authorized representative",
    "registered office",
})

FAST_BENEFICIAL_POSITIONS = frozenset({
    "director",
    "president",
    "secretary",
    "treasurer",
    "vice president",
    "manager",
    "member",
    "managing member",
    "member (owner)",
    "beneficial owner",
    "general partner",
    "partner",
    "ceo",
    "chief executive officer",
    "cfo",
    "chief financial officer",
    "coo",
    "chief operating officer",
    "chairman",
    "chairperson",
    "governor",
    "owner",
    "principal",
    "trustee",
    "shareholder",
    "sole proprietor",
    "authorized member",
    "authorized manager",
    "authorized officer",
    "executingofficer",
    "other officer w/policy-making authority",
})


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def _clean_str(val: Optional[str]) -> str:
    if not val:
        return ""
    s = " ".join(val.split())
    if s in {"-", "--", ".", "N/A", "NA", "NONE", "NULL", "UNKNOWN"}:
        return ""
    return s


@lru_cache(maxsize=250_000)
def _cached_coarse_addr(addr: str) -> str:
    if not addr:
        return ""
    return normalize_mailing_address_coarse(addr) or ""


@lru_cache(maxsize=250_000)
def _cached_extract_state(addr: str, region: str) -> Optional[str]:
    return extract_state_from_address(addr or None, explicit_state=region or None)


@lru_cache(maxsize=100_000)
def _cached_is_third_party(name: str) -> bool:
    return is_excluded_third_party_entity(name)


def _fast_classify_officer(pos: str, raw_nm: str, exp_type: str) -> Tuple[str, bool]:
    pos_low = pos.lower()
    if pos_low in FAST_AGENT_POSITIONS:
        return ("agent_or_incorporator", exp_type.lower() == "company")
    if _cached_is_third_party(raw_nm):
        return ("agent_or_incorporator", exp_type.lower() == "company")
    if pos_low in FAST_BENEFICIAL_POSITIONS or not pos_low:
        is_corp = (exp_type.lower() in {"company", "corporation", "organization", "entity"}) or looks_like_business_name(raw_nm)
        return ("corporate_parent", True) if is_corp else ("beneficial_principal", False)
    return classify_oc_officer_role(pos, raw_nm, explicit_type=exp_type)


def _valid_iso_date(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = val.strip()[:10]
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return None
    if not ("1700-01-01" <= s <= "2100-12-31"):
        return None
    # Fast calendar validation without exception overhead on 99.99% valid ISO dates
    yr, mo, dy = s[:4], s[5:7], s[8:10]
    if not (yr.isdigit() and mo.isdigit() and dy.isdigit()):
        return None
    m_i, d_i = int(mo), int(dy)
    if m_i < 1 or m_i > 12 or d_i < 1 or d_i > 31:
        return None
    if m_i in (4, 6, 9, 11) and d_i > 30:
        return None
    if m_i == 2:
        y_i = int(yr)
        leap = (y_i % 4 == 0 and (y_i % 100 != 0 or y_i % 400 == 0))
        if d_i > (29 if leap else 28):
            return None
    return s


def _valid_timestamp(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = val.strip()
    if len(s) < 10 or not s[:4].isdigit() or not ("1990" <= s[:4] <= "2100"):
        return None
    if _valid_iso_date(s[:10]) is None:
        return None
    return s


def _compose_addr(in_full: str, street: str, locality: str, region: str, postal: str) -> str:
    full = _clean_str(in_full)
    if full:
        up = full.upper()
        if up.endswith(", USA"):
            full = full[:-5].strip()
        elif up.endswith(", UNITED STATES"):
            full = full[:-15].strip()
        return full
    parts = [_clean_str(street), _clean_str(locality), _clean_str(region), _clean_str(postal)]
    return ", ".join(p for p in parts if p)


def _stream_gzip_csv(gz_path: str) -> Iterator[List[str]]:
    if not os.path.exists(gz_path) or os.path.getsize(gz_path) == 0:
        return
    proc = subprocess.Popen(
        ["gzip", "-dc", gz_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=4 * 1024 * 1024,
    )
    assert proc.stdout is not None
    try:
        text_stream = io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace", newline="")
        sanitized = (line.replace("\x00", "") if "\x00" in line else line for line in text_stream)
        reader = csv.reader(sanitized)
        for row in reader:
            yield row
    finally:
        proc.stdout.close()
        proc.wait()


def _copy_rows(cur, copy_sql: str, rows: List[Tuple]) -> None:
    if not rows:
        return
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    buf.seek(0)
    cur.copy_expert(copy_sql, buf)


def drop_constraints_and_indexes_for_bulk_reset(conn) -> None:
    with conn.cursor() as cur:
        for idx_name in SECONDARY_INDEX_NAMES:
            cur.execute(f"DROP INDEX IF EXISTS {idx_name};")
        cur.execute("ALTER TABLE oc_companies DROP CONSTRAINT IF EXISTS oc_companies_pkey;")
        cur.execute("ALTER TABLE oc_branches DROP CONSTRAINT IF EXISTS oc_branches_pkey;")
    conn.commit()


def build_constraints_and_indexes(conn) -> None:
    print("\nBuilding primary keys & parallel B-tree indexes across `oc_*` tables...", flush=True)
    t0 = time.time()
    old_isolation = conn.isolation_level
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute("SET maintenance_work_mem = '2GB';")
            cur.execute("SET max_parallel_maintenance_workers = 8;")
            ddls = [
                (
                    "oc_companies_pkey",
                    "ALTER TABLE oc_companies ADD CONSTRAINT oc_companies_pkey PRIMARY KEY (jurisdiction_code, company_number)",
                ),
                (
                    "oc_branches_pkey",
                    "ALTER TABLE oc_branches ADD CONSTRAINT oc_branches_pkey PRIMARY KEY (branch_jurisdiction_code, branch_company_number, home_jurisdiction_code, home_company_number)",
                ),
                (
                    "idx_oc_companies_jur_name_norm",
                    "CREATE INDEX IF NOT EXISTS idx_oc_companies_jur_name_norm ON oc_companies (jurisdiction_code, name_norm)",
                ),
                (
                    "idx_oc_companies_jur_prefix18",
                    "CREATE INDEX IF NOT EXISTS idx_oc_companies_jur_prefix18 ON oc_companies (jurisdiction_code, name_prefix18) WHERE name_prefix18 IS NOT NULL",
                ),
                (
                    "idx_oc_companies_home_entity",
                    "CREATE INDEX IF NOT EXISTS idx_oc_companies_home_entity ON oc_companies (home_jurisdiction_code, home_company_number) WHERE home_company_number IS NOT NULL",
                ),
                (
                    "idx_oc_officers_company",
                    "CREATE INDEX IF NOT EXISTS idx_oc_officers_company ON oc_officers (jurisdiction_code, company_number)",
                ),
                (
                    "idx_oc_officers_beneficial_name",
                    "CREATE INDEX IF NOT EXISTS idx_oc_officers_beneficial_name ON oc_officers (name_norm) WHERE role_class IN ('beneficial_principal', 'corporate_parent')",
                ),
                (
                    "idx_oc_prev_names_jur_norm",
                    "CREATE INDEX IF NOT EXISTS idx_oc_prev_names_jur_norm ON oc_previous_names (jurisdiction_code, company_name_norm)",
                ),
                (
                    "idx_oc_prev_names_company",
                    "CREATE INDEX IF NOT EXISTS idx_oc_prev_names_company ON oc_previous_names (jurisdiction_code, company_number)",
                ),
                (
                    "idx_oc_branches_home",
                    "CREATE INDEX IF NOT EXISTS idx_oc_branches_home ON oc_branches (home_jurisdiction_code, home_company_number)",
                ),
            ]
            for name, ddl in ddls:
                ti = time.time()
                cur.execute(ddl)
                print(f"  ✓ {name} built in {time.time() - ti:.1f}s", flush=True)
            cur.execute("ANALYZE oc_companies, oc_officers, oc_previous_names, oc_branches, oc_registered_agent_addresses;")
    finally:
        conn.set_isolation_level(old_isolation)
    print(f"✓ All `oc_*` constraints, indexes & ANALYZE completed in {time.time() - t0:.1f}s.", flush=True)


def _shard_match(cnum: str, shard_id: int, num_shards: int) -> bool:
    if num_shards <= 1:
        return True
    return (zlib.crc32(cnum.encode("utf-8")) % num_shards) == shard_id


def _ingest_jurisdiction_worker(
    args_tuple: Tuple[str, str, bool, str, int, int]
) -> Tuple[str, Dict[str, int], List[Tuple[str, str, int, List[str]]]]:
    jur, jur_dir, is_clean_slate, db_url, shard_id, num_shards = args_tuple
    t0 = time.time()
    tag = f"{jur}#s{shard_id+1}/{num_shards}" if num_shards > 1 else jur
    print(f"[{tag}] Starting streaming OpenCorporates load...", flush=True)

    # 1. Stream `non_reg_addresses.csv.gz`
    best_nra_rank: Dict[str, int] = {}
    best_nra_addr: Dict[str, str] = {}
    nra_path = os.path.join(jur_dir, "non_reg_addresses.csv.gz")
    nra_iter = _stream_gzip_csv(nra_path)
    nra_hdr = next(nra_iter, None)
    if nra_hdr:
        idx_map = {col: i for i, col in enumerate(nra_hdr)}
        i_cnum = idx_map.get("company_number", 0)
        i_type = idx_map.get("address_type", 2)
        i_str = idx_map.get("street_address", 3)
        i_loc = idx_map.get("locality", 4)
        i_reg = idx_map.get("region", 5)
        i_zip = idx_map.get("postal_code", 6)
        i_full = idx_map.get("in_full", 9)
        i_end = idx_map.get("end_date", 11)
        for row in nra_iter:
            if len(row) <= i_full:
                continue
            cnum = row[i_cnum].strip()
            if not cnum or (num_shards > 1 and not _shard_match(cnum, shard_id, num_shards)):
                continue
            addr = _compose_addr(row[i_full], row[i_str], row[i_loc], row[i_reg], row[i_zip])
            if not addr:
                continue
            atype = row[i_type].strip().lower()
            base_rank = 3 if ("head" in atype) else (2 if atype == "company" else 1)
            if len(row) <= i_end or not row[i_end].strip():
                base_rank += 4
            if base_rank > best_nra_rank.get(cnum, 0):
                best_nra_rank[cnum] = base_rank
                best_nra_addr[cnum] = sys.intern(addr)
    del best_nra_rank

    agent_by_cnum: Dict[str, Tuple[str, str]] = {}
    ra_addr_counts: Counter[str] = Counter()
    ra_sample_raw: Dict[str, str] = {}
    ra_agent_names: Dict[str, Set[str]] = {}

    officers_inserted = 0
    companies_inserted = 0
    prev_names_inserted = 0
    branches_inserted = 0

    copy_officers_sql = """
        COPY oc_officers (
            jurisdiction_code, company_number, officer_id, name, name_norm,
            position, role_class, is_corporate_officer, start_date, end_date,
            is_current, address_raw, address_coarse
        ) FROM STDIN WITH (FORMAT csv)
    """
    copy_companies_sql = """
        COPY oc_companies (
            jurisdiction_code, company_number, name, name_norm, name_prefix18,
            company_type, current_status, is_active, incorporation_date, dissolution_date,
            registered_address_raw, registered_address_coarse, registered_state,
            mailing_address_raw, mailing_address_coarse, mailing_state,
            agent_name, agent_name_norm, agent_address_raw, agent_address_coarse,
            home_jurisdiction_code, home_company_number, is_nonprofit, registry_url,
            opencorporates_url, retrieved_at
        ) FROM STDIN WITH (FORMAT csv)
    """
    copy_prev_sql = """
        COPY oc_previous_names (
            jurisdiction_code, company_number, company_name, company_name_norm,
            name_type, start_date, end_date
        ) FROM STDIN WITH (FORMAT csv)
    """
    copy_branches_sql = """
        COPY oc_branches (
            branch_jurisdiction_code, branch_company_number,
            home_jurisdiction_code, home_company_number, relationship_type
        ) FROM STDIN WITH (FORMAT csv)
    """

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL synchronous_commit = OFF;")
            if not is_clean_slate:
                cur.execute("DELETE FROM oc_officers WHERE jurisdiction_code = %s", (jur,))
                cur.execute("DELETE FROM oc_previous_names WHERE jurisdiction_code = %s", (jur,))
                cur.execute("DELETE FROM oc_branches WHERE branch_jurisdiction_code = %s", (jur,))
                cur.execute("DELETE FROM oc_companies WHERE jurisdiction_code = %s", (jur,))

            # 2a. Stream `officers.csv.gz`
            off_path = os.path.join(jur_dir, "officers.csv.gz")
            off_iter = _stream_gzip_csv(off_path)
            off_hdr = next(off_iter, None)
            seen_off_hashes: Set[int] = set()
            off_batch: List[Tuple] = []

            if off_hdr:
                om = {col: i for i, col in enumerate(off_hdr)}
                i_id = om.get("id", 0)
                i_cnum = om.get("company_number", 1)
                i_name = om.get("name", 3)
                i_pos = om.get("position", 7)
                i_start = om.get("start_date", 8)
                i_end = om.get("end_date", 11)
                i_type = om.get("type", 17)
                i_afull = om.get("address.in_full", 18)
                i_astr = om.get("address.street_address", 19)
                i_aloc = om.get("address.locality", 20)
                i_areg = om.get("address.region", 21)
                i_azip = om.get("address.postal_code", 22)

                for row in off_iter:
                    if len(row) <= i_name:
                        continue
                    if len(row) > i_end and row[i_end].strip():
                        continue
                    cnum = row[i_cnum].strip()
                    if not cnum or (num_shards > 1 and not _shard_match(cnum, shard_id, num_shards)):
                        continue
                    raw_nm = _clean_str(row[i_name])
                    if not raw_nm:
                        continue
                    pos = _clean_str(row[i_pos]) if len(row) > i_pos else ""
                    exp_type = row[i_type].strip() if len(row) > i_type else ""
                    addr_raw = _compose_addr(
                        row[i_afull] if len(row) > i_afull else "",
                        row[i_astr] if len(row) > i_astr else "",
                        row[i_aloc] if len(row) > i_aloc else "",
                        row[i_areg] if len(row) > i_areg else "",
                        row[i_azip] if len(row) > i_azip else "",
                    )

                    role_class, is_corp = _fast_classify_officer(pos, raw_nm, exp_type)
                    if role_class == "agent_or_incorporator":
                        if cnum not in agent_by_cnum:
                            agent_by_cnum[cnum] = (sys.intern(raw_nm[:160]), sys.intern(addr_raw[:240]))
                        if addr_raw:
                            acoarse = _cached_coarse_addr(addr_raw)
                            if len(acoarse) >= 6:
                                ra_addr_counts[acoarse] += 1
                                if acoarse not in ra_sample_raw:
                                    ra_sample_raw[acoarse] = addr_raw[:240]
                                s_names = ra_agent_names.setdefault(acoarse, set())
                                if len(s_names) < 8:
                                    s_names.add(raw_nm[:120])
                        continue

                    if role_class not in ("beneficial_principal", "corporate_parent"):
                        continue

                    cleaned_nm = clean_oc_officer_name(raw_nm) if not is_corp else raw_nm
                    name_norm = normalize_business_name(cleaned_nm) if is_corp else normalize_person_name(cleaned_nm)
                    if not name_norm:
                        continue

                    dedup_h = hash((cnum, name_norm, role_class))
                    if dedup_h in seen_off_hashes:
                        continue
                    seen_off_hashes.add(dedup_h)

                    acoarse = _cached_coarse_addr(addr_raw) if addr_raw else ""
                    off_batch.append(
                        (
                            jur,
                            cnum,
                            row[i_id].strip() or None,
                            cleaned_nm,
                            name_norm,
                            pos or None,
                            role_class,
                            "t" if is_corp else "f",
                            _valid_iso_date(row[i_start] if len(row) > i_start else None),
                            None,
                            "t",
                            addr_raw or None,
                            acoarse or None,
                        )
                    )
                    if len(off_batch) >= 150_000:
                        _copy_rows(cur, copy_officers_sql, off_batch)
                        officers_inserted += len(off_batch)
                        off_batch.clear()

            if off_batch:
                _copy_rows(cur, copy_officers_sql, off_batch)
                officers_inserted += len(off_batch)
                off_batch.clear()
            del seen_off_hashes

            # 2b. Stream `companies.csv.gz`
            comp_path = os.path.join(jur_dir, "companies.csv.gz")
            comp_iter = _stream_gzip_csv(comp_path)
            comp_hdr = next(comp_iter, None)
            seen_cnums: Set[str] = set()
            comp_batch: List[Tuple] = []
            prev_batch: List[Tuple] = []
            branch_set: Set[Tuple[str, str, str, str]] = set()

            if comp_hdr:
                cm = {col: i for i, col in enumerate(comp_hdr)}
                i_cnum = cm.get("company_number", 0)
                i_name = cm.get("name", 2)
                i_ctype = cm.get("company_type", 4)
                i_nonprof = cm.get("nonprofit", 5)
                i_status = cm.get("current_status", 6)
                i_inc = cm.get("incorporation_date", 7)
                i_diss = cm.get("dissolution_date", 8)
                i_prev = cm.get("previous_names", 15)
                i_ret = cm.get("retrieved_at", 16)
                i_regurl = cm.get("registry_url", 17)
                i_inact = cm.get("inactive", 19)
                i_rstr = cm.get("registered_address.street_address", 29)
                i_rloc = cm.get("registered_address.locality", 30)
                i_rreg = cm.get("registered_address.region", 31)
                i_rzip = cm.get("registered_address.postal_code", 32)
                i_rfull = cm.get("registered_address.in_full", 34)
                i_hjur = cm.get("home_jurisdiction_code", 35)
                i_hcnum = cm.get("home_jurisdiction_company_number", 36)

                for row in comp_iter:
                    if len(row) <= i_name:
                        continue
                    cnum = row[i_cnum].strip()
                    if not cnum or (num_shards > 1 and not _shard_match(cnum, shard_id, num_shards)) or cnum in seen_cnums:
                        continue
                    nm = _clean_str(row[i_name])
                    if not nm:
                        continue
                    nn = normalize_business_name(nm)
                    if not nn:
                        continue
                    seen_cnums.add(cnum)

                    pref18 = nn[:18] if len(nn) >= 18 else None
                    inact_str = row[i_inact].strip().lower() if len(row) > i_inact else ""
                    is_act = "f" if inact_str == "true" else "t"
                    nonprof_str = row[i_nonprof].strip().lower() if len(row) > i_nonprof else ""
                    is_nonprof = "t" if nonprof_str == "true" else "f"

                    r_region = row[i_rreg].strip() if len(row) > i_rreg else ""
                    reg_raw = _compose_addr(
                        row[i_rfull] if len(row) > i_rfull else "",
                        row[i_rstr] if len(row) > i_rstr else "",
                        row[i_rloc] if len(row) > i_rloc else "",
                        r_region,
                        row[i_rzip] if len(row) > i_rzip else "",
                    )
                    reg_coarse = _cached_coarse_addr(reg_raw) if reg_raw else ""
                    reg_state = _cached_extract_state(reg_raw, r_region) if (reg_raw or r_region) else None

                    ag_nm, ag_addr_raw = agent_by_cnum.get(cnum, ("", ""))
                    ag_nm_norm = normalize_business_name(ag_nm) if ag_nm else ""
                    ag_coarse = _cached_coarse_addr(ag_addr_raw) if ag_addr_raw else ""

                    nra_raw = best_nra_addr.get(cnum, "")
                    mail_raw = nra_raw or (reg_raw if (reg_coarse and reg_coarse != ag_coarse) else "")
                    mail_coarse = _cached_coarse_addr(mail_raw) if mail_raw else ""
                    mail_state = _cached_extract_state(mail_raw, "") if mail_raw else None

                    hj = row[i_hjur].strip().lower() if len(row) > i_hjur else ""
                    hc = row[i_hcnum].strip() if len(row) > i_hcnum else ""
                    if hj == jur and (not hc or hc == cnum):
                        hj, hc = "", ""
                    if hj and hc:
                        branch_set.add((jur, cnum, hj, hc))

                    pnames_raw = row[i_prev].strip() if len(row) > i_prev else ""
                    if pnames_raw:
                        for part in pnames_raw.split("|"):
                            pclean = _clean_str(part)
                            pnorm = normalize_business_name(pclean)
                            if pnorm and pnorm != nn:
                                prev_batch.append((jur, cnum, pclean, pnorm, "previous", None, None))

                    comp_batch.append(
                        (
                            jur,
                            cnum,
                            nm,
                            nn,
                            pref18,
                            _clean_str(row[i_ctype]) if len(row) > i_ctype else None,
                            _clean_str(row[i_status]) if len(row) > i_status else None,
                            is_act,
                            _valid_iso_date(row[i_inc] if len(row) > i_inc else None),
                            _valid_iso_date(row[i_diss] if len(row) > i_diss else None),
                            reg_raw or None,
                            reg_coarse or None,
                            reg_state,
                            mail_raw or None,
                            mail_coarse or None,
                            mail_state,
                            ag_nm or None,
                            ag_nm_norm or None,
                            ag_addr_raw or None,
                            ag_coarse or None,
                            hj or None,
                            hc or None,
                            is_nonprof,
                            _clean_str(row[i_regurl]) if len(row) > i_regurl else None,
                            f"https://opencorporates.com/companies/{jur}/{cnum}",
                            _valid_timestamp(row[i_ret] if len(row) > i_ret else None),
                        )
                    )
                    if len(comp_batch) >= 150_000:
                        _copy_rows(cur, copy_companies_sql, comp_batch)
                        companies_inserted += len(comp_batch)
                        comp_batch.clear()
                    if len(prev_batch) >= 150_000:
                        _copy_rows(cur, copy_prev_sql, prev_batch)
                        prev_names_inserted += len(prev_batch)
                        prev_batch.clear()

            if comp_batch:
                _copy_rows(cur, copy_companies_sql, comp_batch)
                companies_inserted += len(comp_batch)
                comp_batch.clear()

            # 2c. Stream `alternative_names.csv.gz`
            alt_path = os.path.join(jur_dir, "alternative_names.csv.gz")
            alt_iter = _stream_gzip_csv(alt_path)
            alt_hdr = next(alt_iter, None)
            if alt_hdr:
                am = {col: i for i, col in enumerate(alt_hdr)}
                i_cnum = am.get("company_number", 0)
                i_nm = am.get("name", 2)
                i_tp = am.get("type", 3)
                i_st = am.get("start_date", 4)
                i_ed = am.get("end_date", 5)
                for row in alt_iter:
                    if len(row) <= i_nm:
                        continue
                    cnum = row[i_cnum].strip()
                    if not cnum or cnum not in seen_cnums:
                        continue
                    anm = _clean_str(row[i_nm])
                    anorm = normalize_business_name(anm)
                    if not anorm:
                        continue
                    raw_tp = row[i_tp].strip().lower() if len(row) > i_tp else ""
                    ntype = "trading" if ("trad" in raw_tp or "assum" in raw_tp or "dba" in raw_tp or "fict" in raw_tp) else "previous"
                    prev_batch.append(
                        (
                            jur,
                            cnum,
                            anm,
                            anorm,
                            ntype,
                            _valid_iso_date(row[i_st] if len(row) > i_st else None),
                            _valid_iso_date(row[i_ed] if len(row) > i_ed else None),
                        )
                    )
                    if len(prev_batch) >= 150_000:
                        _copy_rows(cur, copy_prev_sql, prev_batch)
                        prev_names_inserted += len(prev_batch)
                        prev_batch.clear()

            if prev_batch:
                _copy_rows(cur, copy_prev_sql, prev_batch)
                prev_names_inserted += len(prev_batch)
                prev_batch.clear()

            # 2d. Bulk insert `oc_branches`
            if branch_set:
                branch_rows = [(bj, bc, hj, hc, "foreign_registration") for (bj, bc, hj, hc) in sorted(branch_set)]
                if is_clean_slate:
                    _copy_rows(cur, copy_branches_sql, branch_rows)
                else:
                    execute_values(
                        cur,
                        """
                        INSERT INTO oc_branches (
                            branch_jurisdiction_code, branch_company_number,
                            home_jurisdiction_code, home_company_number, relationship_type
                        ) VALUES %s
                        ON CONFLICT DO NOTHING;
                        """,
                        branch_rows,
                        page_size=5000,
                    )
                branches_inserted = len(branch_rows)

        conn.commit()
    finally:
        conn.close()

    ra_summaries = [
        (acoarse, ra_sample_raw.get(acoarse, acoarse), cnt, sorted(ra_agent_names.get(acoarse, ())))
        for acoarse, cnt in ra_addr_counts.items()
        if cnt >= 10
    ]
    del best_nra_addr, agent_by_cnum, ra_addr_counts, ra_sample_raw, ra_agent_names
    gc.collect()

    elapsed = time.time() - t0
    print(
        f"✓ [{tag}] loaded in {elapsed:.1f}s: companies={companies_inserted:,}, "
        f"beneficial_officers={officers_inserted:,}, alt_names={prev_names_inserted:,}, "
        f"foreign_branches={branches_inserted:,}",
        flush=True,
    )
    return (
        jur,
        {
            "companies": companies_inserted,
            "officers": officers_inserted,
            "alt_names": prev_names_inserted,
            "branches": branches_inserted,
        },
        ra_summaries,
    )


def _dir_gz_size(jur_dir: str) -> int:
    if not os.path.isdir(jur_dir):
        return 0
    return sum(
        os.path.getsize(os.path.join(jur_dir, fn))
        for fn in os.listdir(jur_dir)
        if fn.endswith(".csv.gz")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream OpenCorporates `.csv.gz` archives into PostgreSQL `oc_*` tables.")
    parser.add_argument(
        "--snapshot-dir",
        default=os.path.join(REPO_ROOT, "data", "opencorporates", "2026-11-17"),
        help="Path to local mirrored snapshot directory",
    )
    parser.add_argument("--wave1", action="store_true", help="Load all 19 Wave-1 priority jurisdictions")
    parser.add_argument("--all-us", action="store_true", help="Load all 52 US + DC + PR jurisdictions")
    parser.add_argument("--jurisdictions", nargs="*", help="Explicit list of jurisdiction codes (e.g. us_dc us_ma)")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent worker processes (default: 8)")
    parser.add_argument("--shards", type=int, default=1, help="Intra-jurisdiction CRC32 shards per target (e.g. 12 for us_fl)")
    parser.add_argument(
        "--bulk-reset",
        action="store_true",
        help="Truncate `oc_*` first, drop PKs & secondary indexes during COPY, and build parallel B-trees at end",
    )
    parser.add_argument(
        "--append-clean",
        action="store_true",
        help="Skip per-jurisdiction DELETE when appending a missing jurisdiction into an unindexed heap",
    )
    parser.add_argument(
        "--finalize-indexes",
        action="store_true",
        help="Build PKs, parallel B-tree indexes, and empirical RA hub table after loading",
    )
    args = parser.parse_args()

    if args.jurisdictions:
        raw_targets: List[str] = [j.strip().lower() for j in args.jurisdictions if j.strip()]
    elif args.all_us:
        raw_targets = sorted(
            d for d in os.listdir(args.snapshot_dir)
            if os.path.isdir(os.path.join(args.snapshot_dir, d)) and (d.startswith("us_") or d == "pr")
        )
    else:
        raw_targets = list(WAVE1_JURISDICTIONS)

    # Longest-Processing-Time-First (LPT) ordering so `us_fl` and `us_ny` launch at t=0
    targets = sorted(
        raw_targets,
        key=lambda j: -_dir_gz_size(os.path.join(args.snapshot_dir, j)),
    )

    db_url = _resolve_db_url()
    if not args.append_clean:
        conn = psycopg2.connect(db_url)
        try:
            ensure_opencorporates_tables(conn)
            if args.bulk_reset:
                print("Truncating `oc_*` tables and dropping PKs & secondary indexes for parallel heap `COPY`...", flush=True)
                with conn.cursor() as cur:
                    cur.execute(
                        "TRUNCATE TABLE oc_companies, oc_officers, oc_previous_names, oc_branches, oc_registered_agent_addresses RESTART IDENTITY;"
                    )
                conn.commit()
                drop_constraints_and_indexes_for_bulk_reset(conn)
        finally:
            conn.close()

    t_all = time.time()
    totals = Counter()
    merged_ra_counts: Counter[str] = Counter()
    merged_ra_raw: Dict[str, str] = {}
    merged_ra_names: Dict[str, Set[str]] = {}

    clean_slate_mode = bool(args.bulk_reset or args.append_clean)
    num_shards = max(1, int(args.shards))
    worker_args = [
        (jur, os.path.join(args.snapshot_dir, jur), clean_slate_mode, db_url, s_id, num_shards)
        for jur in targets
        for s_id in range(num_shards)
    ]
    print(
        f"Launching {min(args.workers, len(worker_args))} parallel workers across {len(worker_args)} task(s) "
        f"(targets={targets}, shards={num_shards})",
        flush=True,
    )

    with ProcessPoolExecutor(max_workers=min(max(1, args.workers), len(worker_args))) as pool:
        futs = {pool.submit(_ingest_jurisdiction_worker, wa): wa[0] for wa in worker_args}
        for fut in as_completed(futs):
            jur, res, ra_list = fut.result()
            totals.update(res)
            for acoarse, raw_s, cnt, nms in ra_list:
                merged_ra_counts[acoarse] += cnt
                if acoarse not in merged_ra_raw:
                    merged_ra_raw[acoarse] = raw_s
                s_nms = merged_ra_names.setdefault(acoarse, set())
                for n in nms:
                    if len(s_nms) < 10:
                        s_nms.add(n)

    conn = psycopg2.connect(db_url)
    try:
        if merged_ra_counts:
            ra_rows = [
                (
                    acoarse,
                    merged_ra_raw[acoarse],
                    cnt,
                    sorted(merged_ra_names.get(acoarse, ())),
                    "empirical_oc_agent_stream",
                )
                for acoarse, cnt in merged_ra_counts.items()
                if cnt >= 15
            ]
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO oc_registered_agent_addresses (
                        address_coarse, sample_raw_address, distinct_company_count, distinct_agent_names, source
                    ) VALUES %s
                    ON CONFLICT (address_coarse) DO UPDATE SET
                        distinct_company_count = GREATEST(oc_registered_agent_addresses.distinct_company_count, EXCLUDED.distinct_company_count),
                        distinct_agent_names = EXCLUDED.distinct_agent_names,
                        computed_at = NOW();
                    """,
                    ra_rows,
                    page_size=5000,
                )
            conn.commit()

        if args.bulk_reset or args.finalize_indexes:
            build_constraints_and_indexes(conn)
            refresh_empirical_registered_agent_addresses(conn, min_distinct_companies=15)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM oc_registered_agent_addresses")
            ra_total = int(cur.fetchone()[0] or 0)

        print(
            f"\n=== OPENCORPORATES BULK INGESTION COMPLETE ({len(targets)} jurisdictions in {time.time() - t_all:.1f}s) ===\n"
            f"  Total companies:           {totals['companies']:,}\n"
            f"  Total beneficial officers: {totals['officers']:,}\n"
            f"  Total previous/DBA names:  {totals['alt_names']:,}\n"
            f"  Total foreign branches:    {totals['branches']:,}\n"
            f"  Empirical RA hubs (>=15):  {ra_total:,}",
            flush=True,
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
