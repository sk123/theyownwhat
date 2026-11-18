#!/usr/bin/env python3
"""
ma/scrape_ma_cama.py
====================
Massachusetts Municipal CAMA Scraper & 351-Town Freshness Registrar.

Brings CT-style municipal CAMA scraping and per-town freshness transparency to
Massachusetts (`ma_properties` + `data_source_status` `MA_MUNI_<TOWN>` rows):

  1. City of Boston (`boston_assessing`):
     Backfills direct City of Boston Assessing property-card links
     (`https://properties.boston.gov/#/details?parcelId=...`), syncs Analyze
     Boston FY2026 untruncated owners / nominee realty trusts / units / ISD
     violations via `cross_enrich_boston_and_ma()`, and records `MA_MUNI_BOSTON`.

  2. City of Cambridge (`cambridge_socrata`):
     Streams live FY2027 Cambridge Assessing Property Database records from
     `https://data.cambridgema.gov/resource/eey2-rv59.json`, matches `M049-*`
     parcels by `gisid`/`map_lot` and street address, updates current owners,
     mailing addresses, assessed values, sales, and direct Cambridge Property
     Database card links (`https://www.cambridgema.gov/PropertyDatabase/{pid}`).

  3. 78 Vision Government Solutions (`vision_appraisal`, `gis.vgsi.com`) portals:
     Dynamically syncs `https://www.vgsi.com/massachusetts-online-database/`
     for external update dates and new portals, runs Fast Path direct parcel-URL
     scrapes when `cama_site_link` is already populated or Street Discovery
     (`Streets.aspx` -> `Parcel.aspx?pid=...`) otherwise, and updates current
     owners, mailing addresses, sales, assessments, building photos, and CAMA
     record-card links.

  4. 31 MapGeo (`mapgeo`, `*.mapgeo.io`) portals:
     Discovers municipal properties via `/api/datasets/properties/search` and
     enriches `ma_properties` with current owners and direct MapGeo card links.

  5. Honest 351-Municipality Baseline Status (`--sync-baseline-status`):
     Ensures every Massachusetts municipality has an explicit, truthful
     `MA_MUNI_<TOWN>` status row in `data_source_status`—distinguishing live
     scraped CAMA portals (`success`) from CAMA-queued portals (`cama_queued`)
     and annual DOR-certified MassGIS Level 3 rolls (`massgis_l3`).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import random
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, urljoin
import urllib3

from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.nationwide_schema import ensure_nationwide_parent_tables
from api.oc_linker import demangle_assessor_owner
from api.populate_cities import normalize_link_address, normalize_owner_name, source_float, source_int, source_text
from api.shared_utils import normalize_mailing_address_coarse, strip_benign_care_of_prefix
from ma.ingest_massgis import build_and_save_ma_networks, cross_enrich_boston_and_ma
from ma.ma_municipal_config import MA_MUNICIPAL_DATA_SOURCES, ma_muni_status_key

MASSACHUSETTS_VISION_DIRECTORY_URL = "https://www.vgsi.com/massachusetts-online-database/"
CAMBRIDGE_SOCRATA_URL = "https://data.cambridgema.gov/resource/eey2-rv59.json"


def _resolve_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def log(msg: str, town: Optional[str] = None) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[MA:{town}] " if town else "[MA-CAMA] "
    print(f"[{ts}] {prefix}{msg}", flush=True)


def _norm_street_addr(addr: Optional[str]) -> str:
    if not addr:
        return ""
    s = " ".join(str(addr).upper().strip().split())
    s = re.sub(r"\s+MBLU:.*$", "", s, flags=re.I).strip()
    replacements = (
        (" STREET", " ST"),
        (" AVENUE", " AVE"),
        (" ROAD", " RD"),
        (" DRIVE", " DR"),
        (" LANE", " LN"),
        (" COURT", " CT"),
        (" PLACE", " PL"),
        (" BOULEVARD", " BLVD"),
        (" CIRCLE", " CIR"),
        (" TERRACE", " TER"),
        (" PARKWAY", " PKWY"),
        (" HIGHWAY", " HWY"),
    )
    for old, new in replacements:
        if s.endswith(old):
            s = s[: -len(old)] + new
        s = s.replace(old + " ", new + " ")
    return " ".join(s.split())


def _parse_mmddyyyy(val: Optional[str]) -> Optional[dt.date]:
    if not val:
        return None
    txt = str(val).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            d = dt.datetime.strptime(txt[:10], fmt).date()
            if 1750 <= d.year <= dt.date.today().year + 1:
                return d
        except ValueError:
            continue
    return None


def _upsert_muni_status(
    conn,
    town: str,
    source_type: str,
    status: str,
    details: Dict[str, Any],
    ext_date: Optional[dt.date] = None,
    touch_refreshed: bool = True,
) -> None:
    src_key = ma_muni_status_key(town)
    with conn.cursor() as cur:
        if touch_refreshed:
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
                (src_key, source_type, ext_date, status, Json(details)),
            )
        else:
            cur.execute(
                """
                INSERT INTO data_source_status (
                    source_name, source_type, external_last_updated,
                    last_refreshed_at, refresh_status, details
                )
                VALUES (%s, %s, %s, NULL, %s, %s)
                ON CONFLICT (source_name) DO UPDATE SET
                    external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                    details = data_source_status.details || EXCLUDED.details
                WHERE data_source_status.refresh_status NOT IN ('success', 'running');
                """,
                (src_key, source_type, ext_date, status, Json(details)),
            )
        cur.execute("DELETE FROM kv_cache WHERE key IN ('completeness_matrix', 'ma_prop_stats:v1')")
    conn.commit()


def get_ma_vision_municipalities() -> Dict[str, Dict[str, Any]]:
    """Scrape VGSI's live Massachusetts directory for portal URLs and external dates."""
    out: Dict[str, Dict[str, Any]] = {}
    try:
        resp = requests.get(
            MASSACHUSETTS_VISION_DIRECTORY_URL,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return out
        for row in table.find_all("tr")[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            link = row.find("a", href=True)
            if len(cols) < 3 or not link:
                continue
            town = cols[0].upper().replace(", MA", "").strip()
            if town == "NORTH ATTLEBORO":
                town = "NORTH ATTLEBOROUGH"
            raw_date = cols[2].strip()
            ext_dt = _parse_mmddyyyy(raw_date)
            href = link["href"].strip()
            if "actdatascout.com" in href.lower():
                out[town] = {
                    "type": "actdatascout",
                    "url": href,
                    "portal_url": href,
                    "frequency": raw_date if raw_date in ("Nightly", "Weekly", "Monthly", "Daily") else "ActDataScout",
                    "external_date": ext_dt,
                }
            else:
                if not href.endswith("/"):
                    href += "/"
                out[town] = {
                    "type": "vision_appraisal",
                    "url": href,
                    "portal_url": href,
                    "frequency": raw_date if raw_date in ("Nightly", "Weekly", "Monthly", "Daily") else "Vision CAMA",
                    "external_date": ext_dt,
                }
    except Exception as exc:
        log(f"Warning: could not fetch live VGSI MA directory ({exc}); using static config.")
    return out


def get_merged_ma_cama_configs() -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in MA_MUNICIPAL_DATA_SOURCES.items()}
    for town, cfg in merged.items():
        if "external_date" not in cfg:
            cfg["external_date"] = _parse_mmddyyyy(cfg.get("vgsi_listed_date"))
    live_vision = get_ma_vision_municipalities()
    for town, live_cfg in live_vision.items():
        base = merged.get(town, {})
        base.update({k: v for k, v in live_cfg.items() if v is not None})
        merged[town] = base
    return merged


def sync_all_ma_baseline_statuses(conn, configs: Optional[Dict[str, Dict[str, Any]]] = None) -> int:
    """Populate truthful baseline `MA_MUNI_<TOWN>` status rows across all 351 MA towns."""
    cfgs = configs or get_merged_ma_cama_configs()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                borough AS town,
                COUNT(*)::int AS parcel_count,
                MAX(CASE WHEN last_sale_date <= CURRENT_DATE THEN last_sale_date END) AS max_deed_date
            FROM ma_properties
            WHERE borough IS NOT NULL AND TRIM(borough) != ''
            GROUP BY borough
            ORDER BY borough
            """
        )
        town_rows = cur.fetchall()

        cur.execute("SELECT refresh_status, last_refreshed_at FROM data_source_status WHERE source_name = 'MA'")
        ma_ds = cur.fetchone() or {}
        massgis_refreshed_at = ma_ds.get("last_refreshed_at")

    synced = 0
    with conn.cursor() as cur:
        for r in town_rows:
            town = r["town"].strip().upper()
            src_key = ma_muni_status_key(town)
            max_deed = r.get("max_deed_date")
            cfg = cfgs.get(town)

            if cfg:
                stype = f"ma_{cfg['type']}"
                ext_dt = cfg.get("external_date") or max_deed
                details = {
                    "town": town,
                    "state": "MA",
                    "cama_type": cfg["type"],
                    "portal_url": cfg.get("portal_url") or cfg.get("url"),
                    "frequency": cfg.get("frequency"),
                    "baseline_source": "MassGIS Level 3 Property Tax Parcels",
                    "parcel_count": r["parcel_count"],
                }
                cur.execute(
                    """
                    INSERT INTO data_source_status (
                        source_name, source_type, external_last_updated,
                        last_refreshed_at, refresh_status, details
                    )
                    VALUES (%s, %s, %s, %s, 'cama_queued', %s)
                    ON CONFLICT (source_name) DO UPDATE SET
                        external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                        details = data_source_status.details || EXCLUDED.details
                    WHERE data_source_status.refresh_status NOT IN ('success', 'running');
                    """,
                    (src_key, stype, ext_dt, massgis_refreshed_at, Json(details)),
                )
            else:
                details = {
                    "town": town,
                    "state": "MA",
                    "cama_type": "massgis_l3",
                    "portal_url": "https://maps.massgis.digital.mass.gov/MassMapper/MassMapper.html",
                    "frequency": "Annual (MA DOR Certified)",
                    "baseline_source": "MassGIS Level 3 Property Tax Parcels",
                    "parcel_count": r["parcel_count"],
                }
                cur.execute(
                    """
                    INSERT INTO data_source_status (
                        source_name, source_type, external_last_updated,
                        last_refreshed_at, refresh_status, details
                    )
                    VALUES (%s, 'ma_massgis_l3', %s, %s, 'massgis_l3', %s)
                    ON CONFLICT (source_name) DO UPDATE SET
                        source_type = 'ma_massgis_l3',
                        external_last_updated = COALESCE(EXCLUDED.external_last_updated, data_source_status.external_last_updated),
                        last_refreshed_at = COALESCE(data_source_status.last_refreshed_at, EXCLUDED.last_refreshed_at),
                        details = data_source_status.details || EXCLUDED.details
                    WHERE data_source_status.refresh_status NOT IN ('success', 'running');
                    """,
                    (src_key, max_deed, massgis_refreshed_at, Json(details)),
                )
            synced += 1
        cur.execute("DELETE FROM kv_cache WHERE key IN ('completeness_matrix', 'ma_prop_stats:v1')")
    conn.commit()
    log(f"Synced baseline status rows across {synced} Massachusetts municipalities.")
    return synced


def scrape_boston_cama(conn) -> Dict[str, int]:
    """Sync Analyze Boston FY2026 CAMA enrichments & direct Boston Assessing record-card URLs."""
    log("Syncing Analyze Boston FY2026 CAMA record cards & cross-enrichments...", town="BOSTON")
    _upsert_muni_status(
        conn, "BOSTON", "ma_boston_assessing", "running",
        {"message": "Syncing Analyze Boston FY2026 Property Assessments & ISD violations"},
    )
    stats = cross_enrich_boston_and_ma(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ma_properties
            SET cama_site_link = 'https://properties.boston.gov/#/details?parcelId='
                                 || LPAD(REGEXP_REPLACE(SUBSTRING(bbl FROM 6), '\\D', '', 'g'), 10, '0'),
                source_fy = COALESCE(source_fy, 2026),
                cama_last_scraped_at = NOW()
            WHERE borough = 'BOSTON'
              AND bbl LIKE 'M035-%'
              AND LENGTH(REGEXP_REPLACE(SUBSTRING(bbl FROM 6), '\\D', '', 'g')) > 0
            """
        )
        linked_cards = cur.rowcount
        cur.execute("SELECT MAX(external_last_updated) FROM data_source_status WHERE source_name LIKE 'BOSTON%'")
        ext_dt = (cur.fetchone() or [None])[0] or dt.date.today()
    conn.commit()

    details = {
        "town": "BOSTON",
        "state": "MA",
        "cama_type": "boston_assessing",
        "portal_url": "https://properties.boston.gov/",
        "source_url": "https://data.boston.gov/dataset/property-assessment",
        "frequency": "Weekly",
        "source_fy": 2026,
        "cama_cards_linked": linked_cards,
        **stats,
        "message": f"Synced {linked_cards:,} City of Boston FY2026 Assessing parcel cards & ISD code enforcement.",
    }
    _upsert_muni_status(conn, "BOSTON", "ma_boston_assessing", "success", details, ext_date=ext_dt)
    log(f"Linked {linked_cards:,} Boston Assessing record cards.", town="BOSTON")
    return {"updated": linked_cards, "owner_changes": int(stats.get("ma_untruncated_upgraded", 0) or 0)}


def scrape_cambridge_cama(conn) -> Dict[str, int]:
    """Fetch live FY2027 Cambridge Assessing Property Database (`eey2-rv59`) & update `M049-*` parcels."""
    log("Fetching live Cambridge Assessing Property Database (Socrata eey2-rv59)...", town="CAMBRIDGE")
    _upsert_muni_status(
        conn, "CAMBRIDGE", "ma_cambridge_socrata", "running",
        {"message": "Streaming Cambridge Assessing Open Data"},
    )
    session = requests.Session()
    yr_resp = session.get(
        CAMBRIDGE_SOCRATA_URL,
        params={"$select": "max(yearofassessment) AS max_yr"},
        timeout=20,
    )
    yr_resp.raise_for_status()
    target_fy_str = (yr_resp.json()[0] or {}).get("max_yr") or "2027"
    target_fy = int(target_fy_str)

    cam_rows: List[Dict[str, Any]] = []
    offset = 0
    page_size = 5000
    while True:
        r = session.get(
            CAMBRIDGE_SOCRATA_URL,
            params={
                "yearofassessment": target_fy_str,
                "$limit": page_size,
                "$offset": offset,
                "$order": "pid ASC",
            },
            timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        cam_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    log(f"Fetched {len(cam_rows):,} FY{target_fy} Cambridge Assessing rows.", town="CAMBRIDGE")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, owner_name, owner_is_truncated, last_sale_date
            FROM ma_properties
            WHERE borough = 'CAMBRIDGE'
            """
        )
        existing = cur.fetchall()

    by_bbl: Dict[str, Dict[str, Any]] = {}
    by_addr: Dict[str, List[Dict[str, Any]]] = {}
    for row in existing:
        by_bbl[row["bbl"]] = row
        na = _norm_street_addr(row.get("address"))
        if na:
            by_addr.setdefault(na, []).append(row)

    updates_by_bbl: Dict[str, Tuple] = {}
    owner_changes = 0
    max_sale_seen: Optional[dt.date] = None

    for cr in cam_rows:
        pid = source_text(cr.get("pid"))
        gisid = source_text(cr.get("gisid")) or source_text(cr.get("map_lot"))
        raw_addr = source_text(cr.get("address"))
        unit = source_text(cr.get("unit"))
        full_addr = f"{raw_addr} UNIT {unit}".strip() if (raw_addr and unit and unit.upper() != "NONE") else raw_addr

        matched_row = None
        if gisid and f"M049-{gisid}" in by_bbl:
            matched_row = by_bbl[f"M049-{gisid}"]
        elif full_addr:
            cands = by_addr.get(_norm_street_addr(full_addr)) or by_addr.get(_norm_street_addr(raw_addr)) or []
            if len(cands) == 1:
                matched_row = cands[0]

        if not matched_row:
            continue

        bbl = matched_row["bbl"]
        if bbl in updates_by_bbl:
            continue

        raw_owner = source_text(cr.get("owner_name"))
        if raw_owner and raw_owner.upper() == "NONE":
            raw_owner = None
        co_owner = source_text(cr.get("owner_coownername"))
        if co_owner and co_owner.upper() == "NONE":
            co_owner = None

        o_addr = source_text(cr.get("owner_address"))
        o_city = source_text(cr.get("owner_city"))
        o_state = source_text(cr.get("owner_state"))
        o_zip = source_text(cr.get("owner_zip"))
        mail_parts = [p for p in (o_addr, o_city, o_state, o_zip) if p and p.upper() != "NONE"]
        mail_str = ", ".join(mail_parts) if mail_parts else None

        s_dt = None
        raw_sdate = source_text(cr.get("saledate"))
        if raw_sdate and not raw_sdate.startswith("1900-01-01"):
            s_dt = _parse_mmddyyyy(raw_sdate[:10])
            if s_dt and (max_sale_seen is None or s_dt > max_sale_seen):
                max_sale_seen = s_dt
        s_price = source_float(cr.get("saleprice"))
        if s_price is not None and s_price <= 0:
            s_price = None
        assessed = source_float(cr.get("assessedvalue"))
        if assessed is not None and assessed <= 0:
            assessed = None
        yr_built = source_int(cr.get("condition_yearbuilt"))
        if yr_built is not None and not (1600 < yr_built <= 2030):
            yr_built = None
        units = source_int(cr.get("interior_numunits"))
        card_url = f"https://www.cambridgema.gov/PropertyDatabase/{pid}" if pid else "https://www.cambridgema.gov/PropertyDatabase"

        new_owner = None
        new_owner_norm = None
        if raw_owner:
            dem = demangle_assessor_owner(
                raw_owner,
                care_of=co_owner,
                mailing_address=mail_str,
                explicit_mailing_state=o_state,
                col_max_len=80,
            )
            cand_owner = dem.cleaned_owner or raw_owner
            cand_norm = dem.owner_norm or normalize_owner_name(cand_owner)
            old_sale = matched_row.get("last_sale_date")
            if (
                matched_row.get("owner_is_truncated")
                or (s_dt and (old_sale is None or s_dt >= old_sale))
                or not matched_row.get("owner_name")
            ):
                new_owner = cand_owner
                new_owner_norm = cand_norm
                if new_owner != matched_row.get("owner_name"):
                    owner_changes += 1

        clean_mail = strip_benign_care_of_prefix(mail_str) if mail_str else None
        mail_norm = normalize_link_address(clean_mail) if clean_mail else None
        mail_coarse = normalize_mailing_address_coarse(clean_mail) if clean_mail else None

        updates_by_bbl[bbl] = (
            bbl,
            new_owner,
            new_owner_norm,
            mail_str,
            mail_norm,
            mail_coarse,
            assessed,
            s_dt,
            s_price,
            yr_built,
            units,
            card_url,
            target_fy,
        )

    if updates_by_bbl:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE ma_properties AS p
                SET owner_name = COALESCE(v.owner_name, p.owner_name),
                    owner_name_norm = COALESCE(v.owner_name_norm, p.owner_name_norm),
                    owner_is_truncated = CASE WHEN v.owner_name IS NOT NULL THEN FALSE ELSE p.owner_is_truncated END,
                    mailing_address = COALESCE(v.mail_str, p.mailing_address),
                    mailing_address_norm = COALESCE(v.mail_norm, p.mailing_address_norm),
                    mailing_address_coarse = COALESCE(v.mail_coarse, p.mailing_address_coarse),
                    assessed_total = COALESCE(v.assessed, p.assessed_total),
                    last_sale_date = COALESCE(v.s_dt, p.last_sale_date),
                    last_sale_price = COALESCE(v.s_price, p.last_sale_price),
                    year_built = COALESCE(p.year_built, v.yr_built),
                    units_res = CASE WHEN COALESCE(p.units_res, 0) = 0 AND COALESCE(v.units, 0) > 0 THEN v.units ELSE p.units_res END,
                    units_total = CASE WHEN COALESCE(p.units_total, 0) = 0 AND COALESCE(v.units, 0) > 0 THEN v.units ELSE p.units_total END,
                    cama_site_link = v.card_url,
                    source_fy = v.fy,
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(
                    bbl, owner_name, owner_name_norm, mail_str, mail_norm, mail_coarse,
                    assessed, s_dt, s_price, yr_built, units, card_url, fy
                )
                WHERE p.bbl = v.bbl;
                """,
                list(updates_by_bbl.values()),
                template="(%s, %s, %s, %s, %s, %s, %s::numeric, %s::date, %s::numeric, %s::int, %s::numeric, %s, %s::int)",
                page_size=2000,
            )
        conn.commit()

    details = {
        "town": "CAMBRIDGE",
        "state": "MA",
        "cama_type": "cambridge_socrata",
        "portal_url": "https://www.cambridgema.gov/PropertyDatabase",
        "source_url": "https://data.cambridgema.gov/Property/Cambridge-Property-Database-FY2016-FY2027/eey2-rv59",
        "frequency": "Weekly",
        "source_fy": target_fy,
        "source_records": len(cam_rows),
        "matched_parcels": len(updates_by_bbl),
        "owner_updates": owner_changes,
        "message": f"Matched {len(updates_by_bbl):,} Cambridge FY{target_fy} Assessing property cards ({owner_changes:,} owner updates).",
    }
    _upsert_muni_status(conn, "CAMBRIDGE", "ma_cambridge_socrata", "success", details, ext_date=max_sale_seen or dt.date.today())
    log(f"Updated {len(updates_by_bbl):,} Cambridge parcels ({owner_changes:,} owner updates).", town="CAMBRIDGE")
    return {"updated": len(updates_by_bbl), "owner_changes": owner_changes}


def _parse_vision_parcel_html(html_bytes: bytes, page_url: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html_bytes, "html.parser")
    owner_el = soup.select_one('span[id*="lblGenOwner"], span[id*="lblOwner"]')
    loc_el = soup.select_one('*[id="MainContent_lblLocation"], *[id*="lblLocation"], *[id*="lblGenLocation"]')
    if not owner_el or not loc_el:
        return None
    owner = owner_el.get_text(" ", strip=True)
    location = loc_el.get_text(" ", strip=True)
    if not owner or not location:
        return None

    co_el = soup.select_one('span[id*="lblCoOwner"]')
    co_owner = co_el.get_text(" ", strip=True) if co_el else None

    mail_el = soup.select_one('span[id*="lblAddr1"]')
    mail_addr = ", ".join(part.strip() for part in mail_el.stripped_strings if part.strip()) if mail_el else None

    price_el = soup.select_one('span[id*="lblPrice"]')
    sale_price = None
    if price_el:
        cleaned_p = re.sub(r"[^0-9.]", "", price_el.get_text(strip=True))
        if cleaned_p:
            try:
                val = float(cleaned_p)
                if val > 0:
                    sale_price = val
            except ValueError:
                pass

    sdate_el = soup.select_one('span[id*="lblSaleDate"]')
    sale_date = _parse_mmddyyyy(sdate_el.get_text(strip=True)) if sdate_el else None

    assess_el = soup.select_one('*[id="MainContent_lblGenAssessment"], span[id*="lblAssessment"]')
    assessed = None
    if assess_el:
        cleaned_a = re.sub(r"[^0-9.]", "", assess_el.get_text(strip=True))
        if cleaned_a:
            try:
                val_a = float(cleaned_a)
                if val_a > 0:
                    assessed = val_a
            except ValueError:
                pass

    yr_el = soup.select_one('span[id*="lblYrBuilt"]')
    yr_built = source_int(yr_el.get_text(strip=True)) if yr_el else None
    if yr_built is not None and not (1600 < yr_built <= 2030):
        yr_built = None

    units = None
    occ_td = soup.find("td", string=re.compile(r"^Occupancy$", re.I))
    if occ_td:
        nxt = occ_td.find_next_sibling("td")
        if nxt:
            units = source_int(nxt.get_text(strip=True))

    photo_url = None
    img_el = soup.select_one('img[id*="imgPhoto"], img[id*="MainContent_imgPhoto"]')
    if img_el and img_el.get("src"):
        raw_src = img_el["src"].replace("\\", "/")
        raw_src = re.sub(r"(?<!:)//+", "/", raw_src)
        if "nophoto" not in raw_src.lower() and "blank" not in raw_src.lower():
            photo_url = urljoin(page_url, raw_src)

    return {
        "owner": owner,
        "co_owner": co_owner,
        "location": location,
        "mailing_address": mail_addr,
        "sale_price": sale_price,
        "sale_date": sale_date,
        "assessed_value": assessed,
        "year_built": yr_built,
        "units": units,
        "building_photo_url": photo_url,
        "cama_site_link": page_url,
    }


def scrape_vision_ma_town(conn, town: str, cfg: Dict[str, Any], max_parcels: Optional[int] = None) -> Dict[str, int]:
    """Scrape a Massachusetts Vision Government Solutions (`gis.vgsi.com`) CAMA portal."""
    base_url = cfg["url"]
    if not base_url.endswith("/"):
        base_url += "/"
    ext_dt = cfg.get("external_date")
    freq = cfg.get("frequency", "Vision CAMA")

    log(f"Starting Vision CAMA scrape at {base_url} (cadence={freq})...", town=town)
    _upsert_muni_status(
        conn, town, "ma_vision_appraisal", "running",
        {"town": town, "portal_url": base_url, "frequency": freq, "message": f"Scraping {base_url}"},
        ext_date=ext_dt,
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, owner_name, owner_is_truncated, last_sale_date, cama_site_link
            FROM ma_properties
            WHERE borough = %s
            """,
            (town,),
        )
        db_rows = cur.fetchall()

    if not db_rows:
        log(f"No ma_properties rows found for {town}; skipping.", town=town)
        return {"updated": 0, "owner_changes": 0}

    by_addr: Dict[str, List[Dict[str, Any]]] = {}
    fast_targets: List[Tuple[str, str, Dict[str, Any]]] = []
    unlinked_count = 0
    for r in db_rows:
        na = _norm_street_addr(r.get("address"))
        if na:
            by_addr.setdefault(na, []).append(r)
        existing_link = r.get("cama_site_link")
        if existing_link and "vgsi.com" in existing_link.lower():
            fast_targets.append((r["bbl"], existing_link, r))
        else:
            unlinked_count += 1

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})

    parcel_Queue: List[Tuple[Optional[str], str]] = []
    if fast_targets and unlinked_count == 0:
        for bbl, link, _r in (fast_targets[:max_parcels] if max_parcels else fast_targets):
            parcel_Queue.append((bbl, link))
    else:
        streets_url = urljoin(base_url, "Streets.aspx")
        rs = session.get(streets_url, verify=False, timeout=20)
        rs.raise_for_status()
        soup = BeautifulSoup(rs.content, "html.parser")

        letter_pages = list(dict.fromkeys(
            urljoin(base_url, a["href"])
            for a in soup.find_all("a", href=True)
            if re.search(r"Streets\.aspx\?Letter=", a["href"], re.I)
        ))
        street_hrefs_set = {
            a["href"] for a in soup.find_all("a", href=True)
            if re.search(r"Streets\.aspx\?Name=", a["href"], re.I)
        }
        if letter_pages:
            def _fetch_letter_idx(lurl: str) -> List[str]:
                try:
                    rl = session.get(lurl, verify=False, timeout=20, headers={"Referer": streets_url})
                    if not rl.ok:
                        return []
                    spl = BeautifulSoup(rl.content, "html.parser")
                    return [
                        a["href"] for a in spl.find_all("a", href=True)
                        if re.search(r"Streets\.aspx\?Name=", a["href"], re.I)
                    ]
                except Exception:
                    return []

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as lex:
                for found_hrefs in lex.map(_fetch_letter_idx, letter_pages):
                    street_hrefs_set.update(found_hrefs)

        street_hrefs = sorted(street_hrefs_set)
        log(f"Discovered {len(street_hrefs):,} street index pages across {max(1, len(letter_pages))} letter tabs on {base_url}", town=town)

        discovered_links: Dict[str, str] = {}

        def _fetch_street(shref: str) -> List[Tuple[str, str]]:
            time.sleep(random.uniform(0.03, 0.12))
            s_url = urljoin(base_url, shref)
            r_st = session.get(s_url, verify=False, timeout=20, headers={"Referer": streets_url})
            if not r_st.ok:
                return []
            sp = BeautifulSoup(r_st.content, "html.parser")
            out_links = []
            for a in sp.find_all("a", href=re.compile(r"\.aspx\?(pid|acct|uniqueid)=", re.I)):
                href = a.get("href", "")
                if re.search(r"(pid|acct)=\s*$", href, re.I) or "<%" in href:
                    continue
                addr_norm = _norm_street_addr(a.get_text(" ", strip=True))
                full_p_url = urljoin(base_url, href)
                out_links.append((addr_norm, full_p_url))
            return out_links

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for pairs in ex.map(_fetch_street, street_hrefs):
                for addr_norm, full_p_url in pairs:
                    if full_p_url not in discovered_links:
                        discovered_links[full_p_url] = addr_norm
                if max_parcels and len(discovered_links) >= max_parcels:
                    break

        items = list(discovered_links.items())
        if max_parcels:
            items = items[:max_parcels]
        parcel_Queue = [(None, purl) for purl, _an in items]
        log(f"Queued {len(parcel_Queue):,} Vision parcel pages for {town}.", town=town)

    by_bbl = {r["bbl"]: r for r in db_rows}
    updates_by_bbl: Dict[str, Tuple] = {}
    photos_found = 0
    owner_changes = 0

    def _scrape_one(item: Tuple[Optional[str], str]) -> Optional[Tuple[Optional[str], Dict[str, Any]]]:
        known_bbl, purl = item
        try:
            rp = session.get(purl, verify=False, timeout=20, headers={"Referer": urljoin(base_url, "Streets.aspx")})
            if not rp.ok:
                return None
            parsed = _parse_vision_parcel_html(rp.content, purl)
            if parsed:
                return (known_bbl, parsed)
        except Exception:
            return None
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for idx, res in enumerate(ex.map(_scrape_one, parcel_Queue), 1):
            if not res:
                continue
            known_bbl, pdata = res
            target_rows: List[Dict[str, Any]] = []
            if known_bbl and known_bbl in by_bbl:
                target_rows = [by_bbl[known_bbl]]
            else:
                na = _norm_street_addr(pdata.get("location"))
                target_rows = by_addr.get(na, [])

            if not target_rows:
                continue

            for mr in target_rows:
                bbl = mr["bbl"]
                if bbl in updates_by_bbl:
                    continue
                raw_owner = pdata["owner"]
                mail_str = pdata.get("mailing_address")
                dem = demangle_assessor_owner(
                    raw_owner,
                    care_of=pdata.get("co_owner"),
                    mailing_address=mail_str,
                    col_max_len=80,
                )
                cand_owner = dem.cleaned_owner or raw_owner
                cand_norm = dem.owner_norm or normalize_owner_name(cand_owner)
                s_dt = pdata.get("sale_date")
                old_sale = mr.get("last_sale_date")

                new_owner = None
                new_owner_norm = None
                if (
                    mr.get("owner_is_truncated")
                    or (s_dt and (old_sale is None or s_dt >= old_sale))
                    or not mr.get("owner_name")
                ):
                    new_owner = cand_owner
                    new_owner_norm = cand_norm
                    if new_owner != mr.get("owner_name"):
                        owner_changes += 1

                clean_mail = strip_benign_care_of_prefix(mail_str) if mail_str else None
                mail_norm = normalize_link_address(clean_mail) if clean_mail else None
                mail_coarse = normalize_mailing_address_coarse(clean_mail) if clean_mail else None
                photo = pdata.get("building_photo_url")
                if photo:
                    photos_found += 1

                updates_by_bbl[bbl] = (
                    bbl,
                    new_owner,
                    new_owner_norm,
                    mail_str,
                    mail_norm,
                    mail_coarse,
                    pdata.get("assessed_value"),
                    s_dt,
                    pdata.get("sale_price"),
                    pdata.get("year_built"),
                    pdata.get("units"),
                    pdata["cama_site_link"],
                    photo,
                )

            if idx % 500 == 0:
                log(f"  progress: {idx:,}/{len(parcel_Queue):,} pages scraped ({len(updates_by_bbl):,} matched)...", town=town)

    if updates_by_bbl:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE ma_properties AS p
                SET owner_name = COALESCE(v.owner_name, p.owner_name),
                    owner_name_norm = COALESCE(v.owner_name_norm, p.owner_name_norm),
                    owner_is_truncated = CASE WHEN v.owner_name IS NOT NULL THEN FALSE ELSE p.owner_is_truncated END,
                    mailing_address = COALESCE(v.mail_str, p.mailing_address),
                    mailing_address_norm = COALESCE(v.mail_norm, p.mailing_address_norm),
                    mailing_address_coarse = COALESCE(v.mail_coarse, p.mailing_address_coarse),
                    assessed_total = COALESCE(v.assessed, p.assessed_total),
                    last_sale_date = COALESCE(v.s_dt, p.last_sale_date),
                    last_sale_price = COALESCE(v.s_price, p.last_sale_price),
                    year_built = COALESCE(p.year_built, v.yr_built),
                    units_res = CASE WHEN COALESCE(p.units_res, 0) = 0 AND COALESCE(v.units, 0) > 0 THEN v.units ELSE p.units_res END,
                    units_total = CASE WHEN COALESCE(p.units_total, 0) = 0 AND COALESCE(v.units, 0) > 0 THEN v.units ELSE p.units_total END,
                    cama_site_link = v.card_url,
                    building_photo_url = COALESCE(v.photo, p.building_photo_url),
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(
                    bbl, owner_name, owner_name_norm, mail_str, mail_norm, mail_coarse,
                    assessed, s_dt, s_price, yr_built, units, card_url, photo
                )
                WHERE p.bbl = v.bbl;
                """,
                list(updates_by_bbl.values()),
                template="(%s, %s, %s, %s, %s, %s, %s::numeric, %s::date, %s::numeric, %s::int, %s::numeric, %s, %s)",
                page_size=1500,
            )
        conn.commit()

    details = {
        "town": town,
        "state": "MA",
        "cama_type": "vision_appraisal",
        "portal_url": base_url,
        "source_url": base_url,
        "frequency": freq,
        "pages_scraped": len(parcel_Queue),
        "matched_parcels": len(updates_by_bbl),
        "photos_found": photos_found,
        "owner_updates": owner_changes,
        "message": f"Scraped {len(parcel_Queue):,} Vision CAMA cards -> matched {len(updates_by_bbl):,} parcels ({photos_found:,} photos, {owner_changes:,} owner updates).",
    }
    _upsert_muni_status(conn, town, "ma_vision_appraisal", "success", details, ext_date=ext_dt or dt.date.today())
    log(f"Finished Vision scrape: {len(updates_by_bbl):,} matched, {photos_found:,} photos, {owner_changes:,} owner updates.", town=town)
    return {"updated": len(updates_by_bbl), "owner_changes": owner_changes}


def scrape_mapgeo_ma_town(conn, town: str, cfg: Dict[str, Any]) -> Dict[str, int]:
    """Discover & link parcels from a Massachusetts MapGeo (`*.mapgeo.io`) CAMA portal."""
    domain = cfg["domain"]
    base_url = f"https://{domain}"
    log(f"Starting MapGeo CAMA discovery at {base_url}...", town=town)
    _upsert_muni_status(
        conn, town, "ma_mapgeo", "running",
        {"town": town, "portal_url": f"{base_url}/", "message": f"Querying {domain} MapGeo API"},
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT bbl, address, owner_name, owner_is_truncated
            FROM ma_properties
            WHERE borough = %s
            """,
            (town,),
        )
        db_rows = cur.fetchall()

    by_prop_id: Dict[str, Dict[str, Any]] = {}
    by_addr: Dict[str, List[Dict[str, Any]]] = {}
    for r in db_rows:
        raw_pid = r["bbl"].split("-", 1)[-1].strip().upper()
        by_prop_id[raw_pid] = r
        na = _norm_street_addr(r.get("address"))
        if na:
            by_addr.setdefault(na, []).append(r)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": base_url + "/",
        "Origin": base_url,
        "X-Requested-With": "XMLHttpRequest",
    })
    search_url = f"{base_url}/api/datasets/properties/search?format=json"
    discovered: Dict[str, Dict[str, Any]] = {}
    for ch in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        try:
            resp = session.post(
                search_url,
                json={
                    "attributes": ["displayName", "ownerName", "id", "rowId"],
                    "query": {"displayName": ch, "id": "", "ownerName": ""},
                    "geometry": {"type": "FeatureCollection", "features": []},
                    "sort": [],
                },
                timeout=12,
            )
            if resp.ok and resp.text.strip().startswith("["):
                for item in resp.json():
                    iid = str(item.get("id") or "").strip()
                    if iid and iid not in discovered:
                        discovered[iid] = item
        except Exception:
            continue

    updates_by_bbl: Dict[str, Tuple] = {}
    owner_changes = 0
    for iid, item in discovered.items():
        iid_u = iid.upper()
        mr = by_prop_id.get(iid_u)
        if not mr:
            disp = _norm_street_addr(re.sub(r"#.*$", "", str(item.get("displayName") or "")))
            cands = by_addr.get(disp) or []
            if len(cands) == 1:
                mr = cands[0]
        if not mr:
            continue
        bbl = mr["bbl"]
        if bbl in updates_by_bbl:
            continue

        raw_owner = source_text(item.get("ownerName"))
        new_owner = None
        new_owner_norm = None
        if raw_owner and (mr.get("owner_is_truncated") or not mr.get("owner_name")):
            dem = demangle_assessor_owner(raw_owner, col_max_len=80)
            new_owner = dem.cleaned_owner or raw_owner
            new_owner_norm = dem.owner_norm or normalize_owner_name(new_owner)
            if new_owner != mr.get("owner_name"):
                owner_changes += 1

        card_url = f"{base_url}/datasets/properties?previewId={quote(iid)}"
        updates_by_bbl[bbl] = (bbl, new_owner, new_owner_norm, card_url)

    if updates_by_bbl:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE ma_properties AS p
                SET owner_name = COALESCE(v.owner_name, p.owner_name),
                    owner_name_norm = COALESCE(v.owner_name_norm, p.owner_name_norm),
                    owner_is_truncated = CASE WHEN v.owner_name IS NOT NULL THEN FALSE ELSE p.owner_is_truncated END,
                    cama_site_link = v.card_url,
                    cama_last_scraped_at = NOW(),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(bbl, owner_name, owner_name_norm, card_url)
                WHERE p.bbl = v.bbl;
                """,
                list(updates_by_bbl.values()),
                page_size=2000,
            )
        conn.commit()

    details = {
        "town": town,
        "state": "MA",
        "cama_type": "mapgeo",
        "portal_url": f"{base_url}/",
        "source_url": f"{base_url}/",
        "frequency": "Weekly",
        "source_records": len(discovered),
        "matched_parcels": len(updates_by_bbl),
        "owner_updates": owner_changes,
        "message": f"Discovered {len(discovered):,} MapGeo records -> linked {len(updates_by_bbl):,} parcel cards.",
    }
    _upsert_muni_status(conn, town, "ma_mapgeo", "success", details, ext_date=dt.date.today())
    log(f"Finished MapGeo sync: {len(updates_by_bbl):,} linked ({owner_changes:,} owner updates).", town=town)
    return {"updated": len(updates_by_bbl), "owner_changes": owner_changes}


def select_due_nightly_ma_towns(conn, configs: Dict[str, Dict[str, Any]], max_towns: int = 12) -> List[str]:
    """Select due MA CAMA municipalities in priority order (Nightly -> Weekly -> Unscraped -> Stale)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT source_name, refresh_status, external_last_updated, last_refreshed_at
            FROM data_source_status
            WHERE source_name LIKE 'MA_MUNI_%'
            """
        )
        by_key = {r["source_name"]: r for r in cur.fetchall()}

    now = dt.datetime.now(dt.timezone.utc)
    scored: List[Tuple[int, str]] = []
    for town, cfg in sorted(configs.items()):
        ctype = cfg.get("type")
        if ctype not in ("boston_assessing", "cambridge_socrata", "vision_appraisal", "mapgeo"):
            continue
        ds = by_key.get(ma_muni_status_key(town)) or {}
        status = ds.get("refresh_status")
        last_ref = ds.get("last_refreshed_at")
        if last_ref and last_ref.tzinfo is None:
            last_ref = last_ref.replace(tzinfo=dt.timezone.utc)
        freq = cfg.get("frequency", "")
        ext_dt = cfg.get("external_date")

        if status != "success" or last_ref is None:
            priority = 0 if freq in ("Nightly", "Daily", "Weekly") else 1
            scored.append((priority, town))
            continue

        age_hours = (now - last_ref).total_seconds() / 3600.0
        if freq in ("Nightly", "Daily") and age_hours >= 20:
            scored.append((0, town))
        elif ext_dt and ext_dt > last_ref.date():
            scored.append((1, town))
        elif freq == "Weekly" and age_hours >= 24 * 6.5:
            scored.append((2, town))
        elif age_hours >= 24 * 14:
            scored.append((3, town))

    scored.sort(key=lambda x: (x[0], x[1]))
    return [t for _p, t in scored[:max_towns]]


def run_ma_cama_scrape(
    towns: Optional[Sequence[str]] = None,
    due_nightly: bool = False,
    max_towns: int = 12,
    sync_baseline: bool = True,
    rebuild_networks_if_changed: bool = False,
    max_parcels_per_town: Optional[int] = None,
) -> int:
    conn = psycopg2.connect(_resolve_database_url())
    try:
        ensure_nationwide_parent_tables(conn)
        configs = get_merged_ma_cama_configs()
        if sync_baseline:
            sync_all_ma_baseline_statuses(conn, configs)

        target_towns: List[str] = []
        if towns:
            target_towns = [t.strip().upper() for t in towns if t.strip()]
        elif due_nightly:
            target_towns = select_due_nightly_ma_towns(conn, configs, max_towns=max_towns)

        if not target_towns:
            log("No Massachusetts CAMA towns selected for scraping.")
            return 0

        log(f"Running Massachusetts CAMA scraper for {len(target_towns)} town(s): {', '.join(target_towns)}")
        total_owner_changes = 0
        failures = 0
        for town in target_towns:
            cfg = configs.get(town)
            if not cfg:
                log(f"No live CAMA config registered for {town} (MassGIS L3 town); skipping.", town=town)
                continue
            ctype = cfg["type"]
            try:
                if ctype == "boston_assessing":
                    res = scrape_boston_cama(conn)
                elif ctype == "cambridge_socrata":
                    res = scrape_cambridge_cama(conn)
                elif ctype == "vision_appraisal":
                    res = scrape_vision_ma_town(conn, town, cfg, max_parcels=max_parcels_per_town)
                elif ctype == "mapgeo":
                    res = scrape_mapgeo_ma_town(conn, town, cfg)
                else:
                    log(f"Portal type '{ctype}' for {town} is link-only; skipping scrape.", town=town)
                    continue
                total_owner_changes += int(res.get("owner_changes", 0) or 0)
            except Exception as exc:
                failures += 1
                conn.rollback()
                log(f"ERROR scraping {town}: {exc}", town=town)
                _upsert_muni_status(
                    conn, town, f"ma_{ctype}", "failure",
                    {"town": town, "portal_url": cfg.get("portal_url"), "error": str(exc)[:400]},
                )

        with conn.cursor() as cur:
            cur.execute("DELETE FROM kv_cache WHERE key IN ('completeness_matrix', 'ma_prop_stats:v1')")
        conn.commit()

        if rebuild_networks_if_changed and total_owner_changes > 0:
            log(f"Detected {total_owner_changes:,} owner updates across MA CAMA scrapes -> rebuilding ma_networks...")
            build_and_save_ma_networks(conn)

        return 1 if failures and failures == len(target_towns) else 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Massachusetts Municipal CAMA Scraper & Completeness Sync")
    parser.add_argument("--towns", nargs="+", help="Explicit MA municipalities to scrape (e.g. BOSTON CAMBRIDGE SOMERVILLE)")
    parser.add_argument("--due-nightly", action="store_true", help="Scrape due Nightly/Weekly/unscraped MA CAMA portals")
    parser.add_argument("--max-towns", type=int, default=12, help="Max CAMA municipalities to scrape in --due-nightly mode")
    parser.add_argument("--sync-baseline-only", action="store_true", help="Only sync honest 351-town MA_MUNI_* baseline status rows")
    parser.add_argument("--rebuild-networks", action="store_true", help="Rebuild ma_networks if owner records changed")
    parser.add_argument("--max-parcels", type=int, default=None, help="Optional cap on Vision parcel pages per town for testing")
    args = parser.parse_args()

    if args.sync_baseline_only:
        conn = psycopg2.connect(_resolve_database_url())
        try:
            ensure_nationwide_parent_tables(conn)
            sync_all_ma_baseline_statuses(conn)
            return 0
        finally:
            conn.close()

    return run_ma_cama_scrape(
        towns=args.towns,
        due_nightly=args.due_nightly,
        max_towns=args.max_towns,
        sync_baseline=True,
        rebuild_networks_if_changed=args.rebuild_networks,
        max_parcels_per_town=args.max_parcels,
    )


if __name__ == "__main__":
    raise SystemExit(main())
