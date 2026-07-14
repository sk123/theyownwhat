"""
api/city_routes.py
==================
FastAPI routes for multi-city network data (NYC, DC, Baltimore, LA).
"""

import time
import json
import logging
import html
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

import api.db as db_module

logger = logging.getLogger("city-api")

router = APIRouter(prefix="/api/{city}", tags=["city"])

VALID_CITIES = {"nyc", "dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami"}
BALTIMORE_PROPERTY_LAYER_URL = "https://geodata.baltimorecity.gov/egis/rest/services/CityView/Realproperty_OB/FeatureServer/0/query"
BALTIMORE_OFFICIAL_LAYERS = [
    {
        "label": "Property",
        "url": BALTIMORE_PROPERTY_LAYER_URL,
        "key_field": "BLOCKLOT",
        "date_field": None,
        "fields": ["BLOCKLOT", "FULLADDR", "OWNER_1", "OWNER_2", "USEGROUP", "SALEDATE", "SALEPRIC"],
    },
    {
        "label": "FTA Citations",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/11/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["CitationNum", "DateNotice", "Title", "ViolationText", "TotalFine", "CitationStatus"],
    },
    {
        "label": "Open Interior Notices",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/13/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Exterior Notices",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/14/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Interior/Exterior Notices",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/15/query",
        "key_field": "BlockLot",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NoticeType", "Status", "Address"],
    },
    {
        "label": "Open Cleaning Work Orders",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/18/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 31-60 Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/19/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 61-90 Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/20/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Cleaning Work Orders 90+ Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/21/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 1-5 Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/23/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 6-10 Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/24/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Boarding Work Orders 11+ Days",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/dmxPermitsCodeEnforcement/MapServer/25/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateCreate",
        "fields": ["CB_ID", "DateCreate", "Status", "WorkOrderType", "CleanType", "CleanSize", "BLOCKLOT"],
    },
    {
        "label": "Open Vacant Building Notices",
        "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer/1/query",
        "key_field": "BLOCKLOT",
        "date_field": "DateNotice",
        "fields": ["NoticeNum", "DateNotice", "NT", "Address", "DateCancel", "DateAbate"],
    },
]

def _validate_city(city: str):
    c = city.lower().strip()
    if c not in VALID_CITIES:
        raise HTTPException(status_code=400, detail=f"Unsupported city: {city}")
    return c

def _query(sql: str, params=None) -> list[dict]:
    conn = db_module.db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]
    finally:
        db_module.db_pool.putconn(conn)


# ---------------------------------------------------------------------------
# /api/{city}/stats  — platform-level summary
# ---------------------------------------------------------------------------
_stats_cache: dict = {}
_stats_ts: dict = {}
STATS_TTL = 60  # 1 minute

@router.get("/stats")
def city_stats(city: str):
    c = _validate_city(city)
    global _stats_cache, _stats_ts
    cache_key = f"{c}_stats"
    now = time.time()
    if cache_key in _stats_cache and cache_key in _stats_ts and now - _stats_ts[cache_key] < STATS_TTL:
        return _stats_cache[cache_key]

    # Dynamically inject table names based on whitelisted city
    rows = _query(f"""
        SELECT
            COUNT(*) FILTER (WHERE COALESCE(building_count, 0) > 0)
                                                            AS networks,
            SUM(building_count)                             AS buildings,
            SUM(unit_count)                                 AS units,
            COUNT(*) FILTER (WHERE COALESCE(building_count, 0) > 10)
                                                            AS large_networks,
            MAX(building_count)                             AS largest_network,
            MAX(unit_count)                                 AS most_units
        FROM {c}_networks
    """)

    reg_row = _query(f"SELECT COUNT(*) AS registrations FROM {c}_hpd_registrations")
    prop_row = _query(f"SELECT COUNT(*) AS lots FROM {c}_properties")

    status_rows = _query("""
        SELECT last_refreshed_at, external_last_updated
        FROM data_source_status
        WHERE source_name = %s
        LIMIT 1
    """, (c.upper(),))
    status = status_rows[0] if status_rows else {}
    last_updated = status.get("external_last_updated") or status.get("last_refreshed_at")

    # City-specific display details
    city_display_names = {
        "nyc": "NYC HPD Multiple Dwelling Registrations + PLUTO",
        "dc": "D.C. GIS Computer Assisted Mass Appraisal",
        "baltimore": "Baltimore City GIS Ownership Records",
        "philadelphia": "Philadelphia OPA Property Assessments",
        "boston": "Boston Property Assessment Roll",
        "detroit": "Detroit Property Assessment Roll",
        "chicago": "Chicago Building Registrations & Cook County Assessments",
        "miami": "Miami-Dade Property Assessments + Florida SunBiz",
    }

    result = {
        "networks":        int(rows[0]["networks"]  or 0),
        "buildings":       int(rows[0]["buildings"] or 0),
        "units":           int(rows[0]["units"]     or 0),
        "large_networks":  int(rows[0]["large_networks"] or 0),
        "largest_network": int(rows[0]["largest_network"] or 0),
        "most_units":      int(rows[0]["most_units"] or 0),
        "registrations":   int(reg_row[0]["registrations"] or 0),
        "pluto_lots":      int(prop_row[0]["lots"] or 0),
        "data_source":     city_display_names.get(c, "Property Registrations"),
        "last_updated":    str(last_updated) if last_updated else None,
        "eviction_data":   _city_eviction_summary(c),
    }

    _stats_cache[cache_key] = result
    _stats_ts[cache_key] = time.time()
    return result


def _city_eviction_summary(city: str) -> dict:
    source_name = f"{city.upper()}_EVICTIONS"
    status_rows = _query("""
        SELECT external_last_updated, last_refreshed_at, refresh_status, details
        FROM data_source_status
        WHERE source_name = %s
        LIMIT 1
    """, (source_name,))
    status = status_rows[0] if status_rows else {}
    details = status.get("details") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    summary = {
        "source_available": status.get("refresh_status") == "success",
        "property_level_available": bool(details.get("property_level_available")),
        "refresh_status": status.get("refresh_status"),
        "last_updated": str(status.get("external_last_updated") or status.get("last_refreshed_at")) if status else None,
        "source_records": _safe_int(details.get("source_records")),
        "matched_parcels": _safe_int(details.get("matched_parcels")),
        "join_note": details.get("join_note") or details.get("message"),
    }

    if city != "baltimore":
        return summary

    table_rows = _query("SELECT to_regclass('public.city_eviction_events') AS table_name")
    if not table_rows or not table_rows[0].get("table_name"):
        return summary

    rows = _query("""
        SELECT
            COUNT(*) AS events_total,
            COUNT(DISTINCT case_number) AS cases_total,
            COUNT(*) FILTER (WHERE event_type ILIKE 'Petition - For Warrant of Restitution Filed') AS warrant_filings,
            COUNT(*) FILTER (WHERE event_type ILIKE '%%Evicted%%') AS evicted_events,
            MIN(event_date) AS min_event_date,
            MAX(event_date) AS max_event_date,
            MAX(evicted_date) AS max_evicted_date
        FROM city_eviction_events
        WHERE city = 'baltimore'
    """)
    ev = rows[0] if rows else {}
    summary.update({
        "events_total": _safe_int(ev.get("events_total")),
        "cases_total": _safe_int(ev.get("cases_total")),
        "warrant_filings": _safe_int(ev.get("warrant_filings")),
        "evicted_events": _safe_int(ev.get("evicted_events")),
        "min_event_date": str(ev["min_event_date"]) if ev.get("min_event_date") else None,
        "max_event_date": str(ev["max_event_date"]) if ev.get("max_event_date") else None,
        "max_evicted_date": str(ev["max_evicted_date"]) if ev.get("max_evicted_date") else None,
    })
    return summary


@router.get("/official-records/{bbl}", response_class=HTMLResponse)
def city_official_records_frame(city: str, bbl: str):
    c = _validate_city(city)
    if c != "baltimore":
        raise HTTPException(status_code=404, detail="Embedded official records are not configured for this city.")

    props = _query("""
        SELECT bbl, address, owner_name
        FROM baltimore_properties
        WHERE bbl = %s
        LIMIT 1
    """, (bbl,))
    prop = props[0] if props else {"bbl": bbl, "address": None, "owner_name": None}
    sections = [_fetch_baltimore_official_section(layer, bbl) for layer in BALTIMORE_OFFICIAL_LAYERS]
    sections = [section for section in sections if section["records"] or section.get("error")]
    return HTMLResponse(_render_baltimore_official_records_html(prop, sections))


# ---------------------------------------------------------------------------
# /api/{city}/search  — landlord / address autocomplete
# ---------------------------------------------------------------------------
@router.get("/search")
def city_search(
    city:  str,
    q:     str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
):
    c = _validate_city(city)
    term = q.strip().upper()
    term_like = f"%{term}%"
    
    # Split search terms permutation-agnostically, stripping commas/punctuation
    clean_term = term.replace(',', ' ')
    terms = [t for t in clean_term.split() if t]
    if not terms:
        return []
        
    results = []

    def append_network_result(row, match_context=None):
        matched_member_names = [name for name in (row.get("matched_member_names") or []) if name]
        context = match_context
        if not context and matched_member_names:
            context = "Matched network member: " + ", ".join(matched_member_names[:3])

        results.append({
            "type":          f"{c}_network",
            "network_key":   row["network_key"],
            "display_name":  row["display_name"],
            "building_count": int(row["building_count"] or 0),
            "unit_count":    int(row["unit_count"] or 0),
            "borough_summary": row["borough_summary"],
            "match_context": context,
            "matched_member_names": matched_member_names,
            "label":         row["display_name"],
            "sublabel":      _borough_summary_text(row["borough_summary"]),
        })

    # 1. Direct network name matches. Keep this fast and highly relevant.
    where_net = " AND ".join(["display_name ILIKE %s" for _ in terms])
    direct_net_rows = _query(f"""
        SELECT
            network_key,
            display_name,
            building_count,
            unit_count,
            borough_summary
        FROM {c}_networks
        WHERE {where_net}
        ORDER BY building_count DESC NULLS LAST, display_name
        LIMIT %s
    """, [f"%{word}%" for word in terms] + [limit])

    seen_network_keys = set()
    for r in direct_net_rows:
        seen_network_keys.add(r["network_key"])
        append_network_result(r, "Matched network name")

    # 2. Property address/owner matches (if fewer than limit results).
    remaining = limit - len(results)
    if remaining > 0:
        where_addr = " AND ".join(["p.address ILIKE %s" for _ in terms])
        where_owner = " AND ".join(["p.owner_name ILIKE %s" for _ in terms])
        prop_rows = _query(f"""
            SELECT
                p.bbl,
                p.address,
                p.borough,
                p.zip_code,
                p.owner_name,
                p.units_res,
                n.network_key,
                n.display_name AS network_name,
                n.building_count
            FROM {c}_properties p
            LEFT JOIN {c}_networks n ON p.bbl = ANY(n.bbl_list)
            WHERE ({where_addr})
               OR ({where_owner})
            ORDER BY
                CASE WHEN p.address ILIKE %s THEN 0 
                     WHEN p.owner_name ILIKE %s THEN 1
                     ELSE 2 END,
                p.units_res DESC NULLS LAST,
                p.address
            LIMIT %s
        """, [f"%{word}%" for word in terms] + [f"%{word}%" for word in terms] + [term_like, term_like, remaining])

        for r in prop_rows:
            owner_name = r["owner_name"] or ""
            address = r["address"] or ""
            match_context = None
            if all(word in address.upper() for word in terms):
                match_context = "Matched address"
            elif all(word in owner_name.upper() for word in terms):
                match_context = f"Matched owner: {owner_name}"

            results.append({
                "type":          f"{c}_property",
                "bbl":           r["bbl"],
                "address":       r["address"],
                "borough":       r["borough"],
                "zip_code":      r["zip_code"],
                "owner_name":    r["owner_name"],
                "units_res":     int(r["units_res"] or 0),
                "network_key":   r["network_key"],
                "network_name":  r["network_name"],
                "building_count": int(r["building_count"] or 0),
                "label":         r["address"],
                "sublabel":      f"{r['borough']} · {r['owner_name'] or 'Unknown owner'}",
                "match_context": match_context,
            })

    # 3. Hidden network member matches last. These explain indirect-looking
    # network hits without letting them crowd out direct name/address matches.
    remaining = limit - len(results)
    if remaining > 0:
        where_hidden_exists = " AND ".join(["n ILIKE %s" for _ in terms])
        where_member_like = " AND ".join(["member_name ILIKE %s" for _ in terms])
        where_display_not_like = " AND ".join(["display_name ILIKE %s" for _ in terms])

        hidden_net_rows = _query(f"""
            SELECT
                network_key,
                display_name,
                building_count,
                unit_count,
                borough_summary,
                ARRAY(
                    SELECT member_name
                    FROM unnest(COALESCE(member_names, ARRAY[]::text[])) AS member_name
                    WHERE {where_member_like}
                    ORDER BY length(member_name), member_name
                    LIMIT 5
                ) AS matched_member_names
            FROM {c}_networks
            WHERE NOT (network_key = ANY(%s))
              AND NOT ({where_display_not_like})
              AND EXISTS (
                  SELECT 1 FROM unnest(COALESCE(member_names, ARRAY[]::text[])) AS n
                  WHERE {where_hidden_exists}
              )
            ORDER BY building_count DESC NULLS LAST, display_name
            LIMIT %s
        """, [f"%{word}%" for word in terms] + [list(seen_network_keys)] + [f"%{word}%" for word in terms] + [f"%{word}%" for word in terms] + [remaining])

        for r in hidden_net_rows:
            seen_network_keys.add(r["network_key"])
            append_network_result(r)

    return results


# ---------------------------------------------------------------------------
# /api/{city}/network/<network_key>  — full network detail
# ---------------------------------------------------------------------------
@router.get("/network/{network_key}")
def city_network_detail(city: str, network_key: str):
    c = _validate_city(city)
    nets = _query(f"""
        SELECT
            network_key, anchor_type, display_name,
            member_names, member_addresses,
            registration_ids, bbl_list,
            building_count, unit_count, borough_summary,
            connection_signals,
            updated_at
        FROM {c}_networks
        WHERE network_key = %s
    """, (network_key,))

    if not nets:
        raise HTTPException(status_code=404, detail="Network not found")

    net = nets[0]

    # ---- Properties (buildings in this network) ----
    properties = _query(f"""
        SELECT
            p.bbl,
            p.address,
            p.borough,
            p.zip_code,
            p.owner_name,
            p.land_use,
            p.bld_class,
            p.num_floors,
            p.units_res,
            p.units_total,
            p.year_built,
            p.assessed_total,
            p.latitude,
            p.longitude,
            p.compliance_active,
            p.compliance_record_id,
            p.compliance_expiration,
            r.registration_id,
            r.last_registration_date,
            r.registration_end_date,
            s.violations_total,
            s.violations_open,
            s.violations_class_c,
            s.violations_open_c,
            s.litigations_total,
            s.litigations_open,
            s.litigations_harassment,
            s.evictions_total,
            s.last_eviction_date,
            s.is_rent_stabilized,
            s.rs_units,
            s.nhpd_subsidy,
            s.nhpd_program,
            s.nhpd_expiration
        FROM {c}_properties p
        LEFT JOIN {c}_hpd_registrations r ON r.bbl = p.bbl
        LEFT JOIN {c}_bbl_stats s ON s.bbl = p.bbl
        WHERE p.bbl = ANY(%s)
        ORDER BY p.units_res DESC NULLS LAST, p.address
    """, (net["bbl_list"],))

    # ---- Contacts (officers behind this network) ----
    contacts = _query(f"""
        SELECT
            co.contact_type,
            co.first_name,
            co.last_name,
            co.full_name,
            co.full_name_norm,
            array_agg(DISTINCT co.corporation_name ORDER BY co.corporation_name)
                FILTER (WHERE co.corporation_name IS NOT NULL AND co.corporation_name != '') AS corporations,
            (array_agg(co.business_address ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_address IS NOT NULL))[1] AS business_address,
            (array_agg(co.business_city ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_city IS NOT NULL))[1] AS business_city,
            (array_agg(co.business_state ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_state IS NOT NULL))[1] AS business_state,
            (array_agg(co.business_zip ORDER BY co.registration_id DESC)
                FILTER (WHERE co.business_zip IS NOT NULL))[1] AS business_zip
        FROM {c}_hpd_contacts co
        WHERE co.registration_id = ANY(%s)
          AND co.contact_type IN ('HEADOFFICER','INDIVIDUALOWNER','CORPORATEOWNER','AGENT')
        GROUP BY co.contact_type, co.first_name, co.last_name, co.full_name, co.full_name_norm
        ORDER BY co.full_name_norm, co.contact_type
        LIMIT 200
    """, (net["registration_ids"],))

    # ---- Borough breakdown as structured object ----
    borough_summary = net["borough_summary"]
    if isinstance(borough_summary, str):
        try:
            borough_summary = json.loads(borough_summary)
        except Exception:
            borough_summary = {}

    # ---- Portfolio-level enrichment totals ----
    portfolio_stats = _query(f"""
        SELECT
            COUNT(s.bbl) AS bbl_stats_count,
            SUM(s.violations_open) AS open_violations,
            SUM(s.violations_open_c) AS open_violations_c,
            SUM(s.violations_total) AS total_violations,
            SUM(s.litigations_open) AS open_litigations,
            SUM(s.litigations_harassment) AS harassment_findings,
            SUM(s.evictions_total) AS evictions
        FROM {c}_bbl_stats s
        WHERE s.bbl = ANY(%s)
    """, (net["bbl_list"],))
    pstats = portfolio_stats[0] if portfolio_stats else {}
    has_enrichment = bool(_safe_int(pstats.get("bbl_stats_count")))

    # Parse connection_signals
    conn_signals = net.get("connection_signals") or {}
    if isinstance(conn_signals, str):
        try:
            conn_signals = json.loads(conn_signals)
        except Exception:
            conn_signals = {}

    return {
        "network_key":    net["network_key"],
        "anchor_type":    net["anchor_type"],
        "display_name":   net["display_name"],
        "building_count": int(net["building_count"] or 0),
        "unit_count":     int(net["unit_count"] or 0),
        "borough_summary": borough_summary,
        "member_names":   net["member_names"],
        "member_addresses": net["member_addresses"],
        "connection_signals": conn_signals,
        "updated_at":     str(net["updated_at"]) if net["updated_at"] else None,
        "portfolio_stats": {
            "enrichment_available": has_enrichment,
            "open_violations":   _safe_int(pstats.get("open_violations")) if has_enrichment else None,
            "open_violations_c": _safe_int(pstats.get("open_violations_c")) if has_enrichment else None,
            "total_violations":  _safe_int(pstats.get("total_violations")) if has_enrichment else None,
            "open_litigations":  _safe_int(pstats.get("open_litigations")) if has_enrichment else None,
            "harassment_findings": _safe_int(pstats.get("harassment_findings")) if has_enrichment else None,
            "evictions":         _safe_int(pstats.get("evictions")) if has_enrichment else None,
        },
        "properties":     [_format_property(p) for p in properties],
        "contacts":       [_format_contact(c) for c in contacts],
    }


# ---------------------------------------------------------------------------
# /api/{city}/property/<bbl>  — single property lookup
# ---------------------------------------------------------------------------
@router.get("/property/{bbl}")
def city_property_detail(city: str, bbl: str):
    c = _validate_city(city)
    props = _query(f"""
        SELECT 
            p.*, 
            r.registration_id, 
            r.last_registration_date, 
            r.registration_end_date,
            s.violations_total,
            s.violations_open,
            s.violations_class_c,
            s.violations_open_c,
            s.litigations_total,
            s.litigations_open,
            s.litigations_harassment,
            s.evictions_total,
            s.last_eviction_date,
            s.is_rent_stabilized,
            s.rs_units,
            s.nhpd_subsidy,
            s.nhpd_program,
            s.nhpd_expiration
        FROM {c}_properties p
        LEFT JOIN {c}_hpd_registrations r ON r.bbl = p.bbl
        LEFT JOIN {c}_bbl_stats s ON s.bbl = p.bbl
        WHERE p.bbl = %s
        ORDER BY r.last_registration_date DESC NULLS LAST
        LIMIT 1
    """, (bbl,))

    if not props:
        raise HTTPException(status_code=404, detail="Property not found")

    prop = props[0]

    # Find network this property belongs to
    nets = _query(f"""
        SELECT network_key, display_name, building_count, unit_count
        FROM {c}_networks
        WHERE %s = ANY(bbl_list)
        LIMIT 1
    """, (bbl,))

    return {
        **_format_property(prop),
        "network": nets[0] if nets else None,
    }


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _format_property(r: dict) -> dict:
    return {
        "bbl":            r.get("bbl"),
        "address":        r.get("address"),
        "borough":        r.get("borough"),
        "zip_code":       r.get("zip_code"),
        "owner_name":     r.get("owner_name"),
        "land_use":       r.get("land_use"),
        "bld_class":      r.get("bld_class"),
        "num_floors":     _safe_num(r.get("num_floors")),
        "units_res":      _safe_int(r.get("units_res")),
        "units_total":    _safe_int(r.get("units_total")),
        "year_built":     _safe_int(r.get("year_built")),
        "assessed_total": _safe_num(r.get("assessed_total")),
        "latitude":       _safe_num(r.get("latitude")),
        "longitude":      _safe_num(r.get("longitude")),
        "last_reg_date":  str(r["last_registration_date"]) if r.get("last_registration_date") else None,
        "reg_end_date":   str(r["registration_end_date"])  if r.get("registration_end_date")  else None,
        "registration_id": r.get("registration_id"),
        "is_rent_stabilized": bool(r.get("is_rent_stabilized")) if r.get("is_rent_stabilized") is not None else None,
        "rs_units":       _safe_int(r.get("rs_units")),
        # Compliance
        "compliance_active": bool(r.get("compliance_active")) if r.get("compliance_active") is not None else None,
        "compliance_record_id": r.get("compliance_record_id"),
        "compliance_expiration": str(r["compliance_expiration"]) if r.get("compliance_expiration") else None,
        # Enrichment
        "violations_total":  _safe_int(r.get("violations_total")),
        "violations_open":   _safe_int(r.get("violations_open")),
        "violations_class_c": _safe_int(r.get("violations_class_c")),
        "violations_open_c": _safe_int(r.get("violations_open_c")),
        "litigations_total": _safe_int(r.get("litigations_total")),
        "litigations_open":  _safe_int(r.get("litigations_open")),
        "litigations_harassment": _safe_int(r.get("litigations_harassment")),
        "evictions_total":   _safe_int(r.get("evictions_total")),
        "last_eviction_date": str(r["last_eviction_date"]) if r.get("last_eviction_date") else None,
        "nhpd_subsidy":    bool(r.get("nhpd_subsidy")) if r.get("nhpd_subsidy") is not None else None,
        "nhpd_program":    r.get("nhpd_program"),
        "nhpd_expiration": str(r["nhpd_expiration"]) if r.get("nhpd_expiration") else None,
    }


def _format_contact(r: dict) -> dict:
    corps = r.get("corporations") or []
    return {
        "contact_type":   r.get("contact_type"),
        "corporations":   corps,                          # full list
        "corporation":    corps[0] if corps else None,    # first for backwards compat
        "first_name":     r.get("first_name"),
        "last_name":      r.get("last_name"),
        "full_name":      r.get("full_name"),
        "address":        r.get("business_address"),
        "city":           r.get("business_city"),
        "state":          r.get("business_state"),
        "zip":            r.get("business_zip"),
    }


def _fetch_baltimore_official_section(layer: dict, bbl: str) -> dict:
    clean_bbl = (bbl or "").replace("'", "''").strip()
    where = f"{layer['key_field']} = '{clean_bbl}'"
    params = {
        "where": where,
        "outFields": ",".join(layer["fields"]),
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 50,
    }
    if layer.get("date_field"):
        params["orderByFields"] = f"{layer['date_field']} DESC"
    try:
        response = requests.get(layer["url"], params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise RuntimeError(data["error"].get("message") or "Official source query failed.")
        records = [(feature.get("attributes") or {}) for feature in data.get("features", [])]
        return {
            "label": layer["label"],
            "fields": layer["fields"],
            "records": records,
            "source_url": response.url,
        }
    except Exception as e:
        logger.warning("Baltimore official record query failed for %s/%s: %s", layer["label"], bbl, e)
        return {
            "label": layer["label"],
            "fields": layer["fields"],
            "records": [],
            "source_url": layer["url"],
            "error": str(e),
        }


def _official_value(value, field_name: str = "") -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 8 and text.isdigit() and "DATE" in field_name.upper():
            return f"{text[:2]}/{text[2:4]}/{text[4:]}"
        return text
    if isinstance(value, (int, float)) and "DATE" in field_name.upper() and value > 100000000000:
        try:
            return time.strftime("%Y-%m-%d", time.gmtime(value / 1000))
        except Exception:
            return str(value)
    if isinstance(value, (int, float)) and "PRIC" in field_name.upper():
        try:
            return f"${int(value):,}"
        except Exception:
            return str(value)
    return str(value)


def _render_baltimore_official_records_html(prop: dict, sections: list[dict]) -> str:
    title = html.escape(prop.get("address") or prop.get("bbl") or "Baltimore property")
    subtitle = html.escape(" · ".join(part for part in [prop.get("bbl"), prop.get("owner_name")] if part) or "Official Baltimore records")
    section_html = []
    for section in sections:
        label = html.escape(section["label"])
        source_url = html.escape(section.get("source_url") or "")
        if section.get("error"):
            section_html.append(f"""
              <section>
                <div class="section-head">
                  <h2>{label}</h2>
                  <a href="{source_url}" target="_blank" rel="noreferrer">Source</a>
                </div>
                <div class="empty">Official source query failed for this layer.</div>
              </section>
            """)
            continue
        rows = []
        for record in section["records"]:
            cells = []
            for field in section["fields"]:
                value = html.escape(_official_value(record.get(field), field))
                rendered_value = value if value else '<span class="muted">—</span>'
                cells.append(f"<td>{rendered_value}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")
        headers = "".join(f"<th>{html.escape(field)}</th>" for field in section["fields"])
        body = "".join(rows)
        count = len(section["records"])
        section_html.append(f"""
          <section>
            <div class="section-head">
              <div>
                <h2>{label}</h2>
                <p>{count} official record{'s' if count != 1 else ''}</p>
              </div>
              <a href="{source_url}" target="_blank" rel="noreferrer">Source</a>
            </div>
            <div class="table-wrap">
              <table>
                <thead><tr>{headers}</tr></thead>
                <tbody>{body}</tbody>
              </table>
            </div>
          </section>
        """)

    if not section_html:
        section_html.append("<section><div class=\"empty\">No official records returned for this parcel.</div></section>")

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {{
      color-scheme: light;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f8fafc;
      color: #0f172a;
    }}
    body {{ margin: 0; background: #f8fafc; }}
    header {{
      position: sticky;
      top: 0;
      z-index: 2;
      padding: 14px 16px 12px;
      background: rgba(255,255,255,.96);
      border-bottom: 1px solid #e2e8f0;
      backdrop-filter: blur(8px);
    }}
    h1 {{ margin: 0; font-size: 15px; line-height: 1.25; font-weight: 800; }}
    header p {{ margin: 4px 0 0; color: #64748b; font-size: 11px; font-weight: 600; }}
    main {{ padding: 12px; display: grid; gap: 12px; }}
    section {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }}
    .section-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 10px 12px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }}
    h2 {{ margin: 0; font-size: 12px; font-weight: 800; }}
    .section-head p {{ margin: 2px 0 0; color: #64748b; font-size: 10px; font-weight: 700; }}
    a {{ color: #2563eb; font-size: 10px; font-weight: 800; text-decoration: none; text-transform: uppercase; letter-spacing: .04em; }}
    a:hover {{ text-decoration: underline; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; min-width: 620px; border-collapse: collapse; font-size: 11px; }}
    th {{ text-align: left; padding: 8px 10px; color: #64748b; background: #fff; border-bottom: 1px solid #e2e8f0; font-size: 9px; text-transform: uppercase; letter-spacing: .04em; white-space: nowrap; }}
    td {{ padding: 8px 10px; border-bottom: 1px solid #f1f5f9; vertical-align: top; }}
    tr:last-child td {{ border-bottom: 0; }}
    .muted, .empty {{ color: #94a3b8; }}
    .empty {{ padding: 18px 12px; font-size: 12px; font-weight: 700; text-align: center; }}
  </style>
</head>
<body>
  <header>
    <h1>{title}</h1>
    <p>{subtitle}</p>
  </header>
  <main>
    {''.join(section_html)}
  </main>
</body>
</html>"""


def _borough_summary_text(summary) -> str:
    if not summary:
        return ""
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            return summary
    if isinstance(summary, dict):
        parts = sorted(summary.items(), key=lambda x: -x[1])
        return " · ".join(f"{b} ({n})" for b, n in parts[:4])
    return ""


def _safe_num(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _safe_int(v):
    try:
        return int(float(v)) if v is not None else None
    except Exception:
        return None
