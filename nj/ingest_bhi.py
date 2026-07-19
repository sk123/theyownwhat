#!/usr/bin/env python3
"""
Ingest New Jersey DCA Bureau of Housing Inspection active-building data.

Source: public Power BI report "BHI - Active Building for OPRA"
https://app.powerbigov.us/view?r=eyJrIjoiZmI2MzIxZDEtN2UwNi00M2VlLWJiZjgtNTMzMTExYjc3YzgyIiwidCI6IjUwNzZjM2QxLTM4MDItNGI5Zi1iMzZhLWUwYTQxYmQ2NDJhNyJ9

The statewide NJGIN parcel/MOD-IV feed redacts all owner names, so this module
uses BHI registered multiple-dwelling/property-interest records for NJ owner
network coverage. The scope is therefore BHI-registered buildings, not every NJ
parcel.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import looks_like_business_name, normalize_owner_name


DATABASE_URL = os.environ.get("DATABASE_URL")

REPORT_RESOURCE_KEY = "fb6321d1-7e06-43ee-bbf8-533111b77c82"
REPORT_TENANT_ID = "5076c3d1-3802-4b9f-b36a-e0a41bd642a7"
REPORT_EMBED_URL = (
    "https://app.powerbigov.us/view?r="
    "eyJrIjoiZmI2MzIxZDEtN2UwNi00M2VlLWJiZjgtNTMzMTExYjc3YzgyIiwidCI6IjUwNzZjM2QxLTM4MDItNGI5Zi1iMzZhLWUwYTQxYmQ2NDJhNyJ9"
)
POWERBI_API_BASE = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net"
POWERBI_PAGE_LIMIT = 100_000

LOGGER = logging.getLogger("nj-bhi-ingest")


# alias, source ref, entity, property
FIELD_SPECS: Sequence[Tuple[str, str, str, str]] = (
    ("county", "p", "Property Interest", "County"),
    ("municipality", "p", "Property Interest", "Municipality"),
    ("property_interest_type", "p", "Property Interest", "Property Interest Type"),
    ("property_interest_id", "p", "Property Interest", "Property Interest ID"),
    ("property_interest_name", "p", "Property Interest", "Property Interest"),
    ("status_reason", "p", "Property Interest", "Status Reason"),
    ("bhi_registration_no", "p", "Property Interest", "BHI Registration #"),
    ("property_address", "p", "Property Interest", "Property Address"),
    ("primary_owner_name", "p", "Property Interest", "Primary Property Owner Name"),
    ("ownership_type", "p", "Property Interest", "Ownership Type"),
    ("authorized_agent_name", "p", "Property Interest", "Authorized Agent Name"),
    ("building_count", "p", "Property Interest", "Building Count"),
    ("property_units_count", "p", "Property Interest", "Units Count"),
    ("primary_owner_id", "p", "Property Interest", "Primary Owner ID"),
    ("street_number", "p", "Property Interest", "Street Number"),
    ("street_name", "p", "Property Interest", "Street Name"),
    ("street_address", "p", "Property Interest", "Street Address"),
    ("last_cyclical_inspection", "p", "Property Interest", "Last Cyclical Inspection"),
    ("most_recent_start_date", "p", "Property Interest", "Most Recent Start Date"),
    ("source_url", "p", "Property Interest", "URL"),
    ("primary_owner_address", "p", "Property Interest", "Primary Owner Address"),
    ("primary_owner_phone", "p", "Property Interest", "Primary Owner Phone"),
    ("authorized_agent_address", "p", "Property Interest", "Authorized Agent Address"),
    ("authorized_agent_phone", "p", "Property Interest", "Authorized Agent Phone"),
    ("authorized_agent_email", "p", "Property Interest", "Authorized Agent Email"),
    ("contact_redacted", "p", "Property Interest", "Contact Redacted ?"),
    ("building_id", "b", "Building", "Building ID"),
    ("building_name", "b", "Building", "Building"),
    ("building_address", "b", "Building", "Address"),
    ("building_address_line1", "b", "Building", "Address Line 1"),
    ("building_registration_no", "b", "Building", "BHI Registration Number"),
    ("block_no", "b", "Building", "Building Block"),
    ("lot_no", "b", "Building", "Lot"),
    ("certificate_of_occupancy_date", "b", "Building", "Certificate of Occypancy Date"),
    ("stories", "b", "Building", "# Stories"),
    ("number_of_stories", "b", "Building", "Number of Stories"),
    ("construction_classification", "b", "Building", "Construction of Classification"),
    ("construction_month_year", "b", "Building", "Month & Year of Construction"),
    ("building_status", "b", "Building", "Status"),
    ("building_zip", "b", "Building", "Zip Code"),
    ("building_unit_count", "b", "Building", "Unit Count"),
    ("building_link", "b", "Building", "Link"),
    ("building_county", "b", "Building", "County"),
    ("building_municipality", "b", "Building", "Municipality"),
    ("building_authorized_agent", "b", "Building", "Authorized Agent"),
    ("building_manager", "b", "Building", "Manager"),
)


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if not text or text.upper() in {"NULL", "NONE", "N/A", "NA", "UNKNOWN"}:
        return None
    return text


def clean_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def clean_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def parse_date(value: Any) -> Optional[dt.date]:
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            return dt.datetime.fromtimestamp(number / 1000, tz=dt.timezone.utc).date()
        if number > 10_000:
            try:
                return dt.date(1899, 12, 30) + dt.timedelta(days=int(number))
            except Exception:
                return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%Y", "%Y"):
        try:
            parsed = dt.datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt)
            return parsed.date()
        except ValueError:
            pass
    return None


def parse_year(value: Any) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    matches = re.findall(r"(18|19|20)\d{2}", text)
    if not matches:
        return None
    year_match = re.search(r"(?:18|19|20)\d{2}", text)
    if not year_match:
        return None
    year = int(year_match.group(0))
    return year if 1700 <= year <= dt.date.today().year + 1 else None


def split_city_state_zip(address: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    text = clean_text(address)
    if not text:
        return None, None, None
    match = re.search(r",\s*([^,]+),\s*([A-Z]{2})\s*,?\s*(\d{5}(?:-\d{4})?)\s*$", text, re.I)
    if match:
        return clean_text(match.group(1)), match.group(2).upper(), match.group(3)
    match = re.search(r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", text, re.I)
    if match:
        return None, match.group(1).upper(), match.group(2)
    return None, None, None


def normalize_address(value: Optional[str]) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[.,]", " ", text)
    replacements = {
        "SUITE": "#",
        "STE": "#",
        "UNIT": "#",
        "APARTMENT": "#",
        "APT": "#",
        "STREET": "ST",
        "AVENUE": "AVE",
        "ROAD": "RD",
        "BOULEVARD": "BLVD",
        "DRIVE": "DR",
        "PLACE": "PL",
        "NORTH": "N",
        "SOUTH": "S",
        "EAST": "E",
        "WEST": "W",
    }
    for old, new in replacements.items():
        text = re.sub(rf"\b{old}\b", new, text)
    text = re.sub(r"\s*#\s*", " # ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def building_key(row: Dict[str, Any]) -> str:
    building_id = clean_text(row.get("building_id"))
    if building_id:
        return f"BHI-{building_id}"
    raw_key = "|".join(
        clean_text(row.get(k)) or ""
        for k in (
            "bhi_registration_no",
            "building_name",
            "building_address",
            "block_no",
            "lot_no",
            "county",
            "municipality",
        )
    )
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:16]
    return f"BHI-{digest}"


def make_select(specs: Sequence[Tuple[str, str, str, str]]) -> List[Dict[str, Any]]:
    return [
        {
            "Column": {
                "Expression": {"SourceRef": {"Source": source}},
                "Property": prop,
            },
            "Name": f"{entity}.{prop}",
        }
        for _, source, entity, prop in specs
    ]


def literal(value: str) -> Dict[str, Any]:
    return {"Literal": {"Value": "'" + value.replace("'", "''") + "'"}}


def in_condition(source: str, prop: str, values: Sequence[str]) -> Dict[str, Any]:
    return {
        "Condition": {
            "In": {
                "Expressions": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": source}},
                            "Property": prop,
                        }
                    }
                ],
                "Values": [[literal(value)] for value in values],
            }
        }
    }


def not_in_condition(source: str, prop: str, values: Sequence[Any]) -> Dict[str, Any]:
    formatted = []
    for value in values:
        if value is None:
            formatted.append([{"Literal": {"Value": "null"}}])
        elif isinstance(value, bool):
            formatted.append([{"Literal": {"Value": "true" if value else "false"}}])
        else:
            formatted.append([literal(str(value))])
    return {
        "Condition": {
            "Not": {
                "Expression": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": source}},
                                    "Property": prop,
                                }
                            }
                        ],
                        "Values": formatted,
                    }
                }
            }
        }
    }


class PowerBIReport:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Origin": "https://app.powerbigov.us",
            "Referer": "https://app.powerbigov.us/",
            "X-PowerBI-ResourceKey": REPORT_RESOURCE_KEY,
        }
        self.model_id: Optional[int] = None
        self.model_info: Dict[str, Any] = {}

    def _headers(self, post: bool = False) -> Dict[str, str]:
        headers = dict(self.headers)
        headers["ActivityId"] = hashlib.md5(str(time.time()).encode()).hexdigest()
        headers["RequestId"] = hashlib.md5(os.urandom(16)).hexdigest()
        if post:
            headers["Content-Type"] = "application/json"
        return headers

    def load_model(self):
        url = f"{POWERBI_API_BASE}/public/reports/{REPORT_RESOURCE_KEY}/modelsAndExploration?preferReadOnlySession=true"
        response = self.session.get(url, headers=self._headers(), timeout=60)
        response.raise_for_status()
        payload = response.json()
        self.model_info = payload["models"][0]
        self.model_id = self.model_info["id"]
        return self.model_info

    def query(self, query_payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.model_id is None:
            self.load_model()
        body = {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": query_payload,
                    "ApplicationContext": {
                        "DatasetId": str(self.model_id),
                        "Sources": [{"ReportId": "any", "VisualId": "nj-ingest"}],
                    },
                }
            ],
            "cancelQueries": [],
            "modelId": self.model_id,
        }
        response = self.session.post(
            f"{POWERBI_API_BASE}/public/reports/querydata?synchronous=true",
            headers=self._headers(post=True),
            data=json.dumps(body),
            timeout=120,
        )
        response.raise_for_status()
        result = response.json()["results"][0]["result"]
        if result.get("error"):
            raise RuntimeError(json.dumps(result["error"]))
        return result["data"]


def base_where() -> List[Dict[str, Any]]:
    return [
        not_in_condition("c", "Redacted ?", [True]),
        in_condition("b", "Status", ["Active"]),
        not_in_condition("p", "BHI Registration #", [None]),
    ]


def semantic_query(
    specs: Sequence[Tuple[str, str, str, str]],
    count: int,
    extra_where: Optional[List[Dict[str, Any]]] = None,
    order_by: Optional[Tuple[str, str]] = None,
) -> Dict[str, Any]:
    where = base_where()
    if extra_where:
        where.extend(extra_where)
    query: Dict[str, Any] = {
        "Commands": [
            {
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "p", "Entity": "Property Interest", "Type": 0},
                            {"Name": "b", "Entity": "Building", "Type": 0},
                            {"Name": "c", "Entity": "Contact", "Type": 0},
                        ],
                        "Select": make_select(specs),
                        "Where": where,
                    },
                    "Binding": {
                        "Primary": {
                            "Groupings": [
                                {"Projections": list(range(len(specs)))}
                            ]
                        },
                        "DataReduction": {
                            "DataVolume": 3,
                            "Primary": {"Window": {"Count": count}},
                        },
                        "Version": 1,
                    },
                    "ExecutionMetricsKind": 1,
                }
            }
        ]
    }
    if order_by:
        source, prop = order_by
        query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["OrderBy"] = [
            {
                "Direction": 1,
                "Expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Source": source}},
                        "Property": prop,
                    }
                },
            }
        ]
    return query


def decode_dsr(data: Dict[str, Any], aliases: Sequence[str]) -> List[Dict[str, Any]]:
    ds = data["dsr"]["DS"][0]
    value_dicts = ds.get("ValueDicts") or {}
    decoded: List[Dict[str, Any]] = []

    for partition in ds.get("PH", []):
        rows = next(iter(partition.values()))
        schema = None
        previous: List[Any] = []
        for encoded in rows:
            if "S" in encoded:
                schema = encoded["S"]
                previous = [None] * len(schema)
            if not schema:
                continue

            c_values = list(encoded.get("C", []))
            c_idx = 0
            r_mask = encoded.get("R", 0) or 0
            null_mask = encoded.get("Ø", 0) or 0
            row_values: List[Any] = []

            for idx, column in enumerate(schema):
                name = column.get("N")
                if r_mask & (1 << idx):
                    value = previous[idx]
                elif name in encoded:
                    value = encoded.get(name)
                elif null_mask & (1 << idx):
                    value = None
                else:
                    value = c_values[c_idx] if c_idx < len(c_values) else None
                    c_idx += 1

                dict_name = column.get("DN")
                if dict_name and value is not None:
                    try:
                        value = value_dicts[dict_name][int(value)]
                    except Exception:
                        pass
                row_values.append(value)

            previous = row_values
            decoded.append({alias: row_values[i] if i < len(row_values) else None for i, alias in enumerate(aliases)})
    return decoded


def fetch_counties(report: PowerBIReport) -> List[str]:
    specs = (("county", "p", "Property Interest", "County"),)
    data = report.query(semantic_query(specs, count=100, order_by=("p", "County")))
    rows = decode_dsr(data, ["county"])
    return [row["county"] for row in rows if row.get("county")]


def fetch_bhi_rows(report: PowerBIReport, counties: Optional[Sequence[str]] = None, limit: int = 0) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    all_counties = counties or fetch_counties(report)
    aliases = [alias for alias, *_ in FIELD_SPECS]
    rows: List[Dict[str, Any]] = []
    county_counts: Dict[str, int] = {}

    for county in all_counties:
        LOGGER.info("Fetching BHI active buildings for %s...", county)
        data = report.query(
            semantic_query(
                FIELD_SPECS,
                count=POWERBI_PAGE_LIMIT,
                extra_where=[in_condition("p", "County", [county])],
                order_by=("p", "BHI Registration #"),
            )
        )
        county_rows = decode_dsr(data, aliases)
        if len(county_rows) >= 30_000:
            raise RuntimeError(
                f"County chunk for {county} hit the 30,000-row Power BI cap; split by municipality before ingesting."
            )
        county_counts[county] = len(county_rows)
        rows.extend(county_rows)
        LOGGER.info("  %s rows", f"{len(county_rows):,}")
        if limit and len(rows) >= limit:
            rows = rows[:limit]
            break

    metadata = {
        "counties": list(all_counties),
        "county_counts": county_counts,
        "report_last_refresh": report.model_info.get("LastRefreshTime"),
        "report_next_refresh": report.model_info.get("NextRefreshTime"),
        "report_display_name": report.model_info.get("displayName"),
    }
    return rows, metadata


def transform_rows(rows: Iterable[Dict[str, Any]]) -> Tuple[List[tuple], List[tuple], List[tuple], List[tuple], Dict[str, int]]:
    properties: Dict[str, tuple] = {}
    registrations: Dict[str, tuple] = {}
    contacts: Dict[Tuple[str, str, str, str], tuple] = {}
    stats: Dict[str, tuple] = {}
    duplicates = 0

    for raw in rows:
        key = building_key(raw)
        if key in properties:
            duplicates += 1
            continue

        owner_name = clean_text(raw.get("primary_owner_name"))
        owner_norm = normalize_owner_name(owner_name or "")
        owner_address = clean_text(raw.get("primary_owner_address"))
        owner_phone = clean_text(raw.get("primary_owner_phone"))
        agent_name = clean_text(raw.get("authorized_agent_name") or raw.get("building_authorized_agent"))
        agent_address = clean_text(raw.get("authorized_agent_address"))
        agent_phone = clean_text(raw.get("authorized_agent_phone"))
        agent_email = clean_text(raw.get("authorized_agent_email"))
        agent_norm = normalize_owner_name(agent_name or "")

        county = clean_text(raw.get("county") or raw.get("building_county"))
        municipality = clean_text(raw.get("municipality") or raw.get("building_municipality"))
        address = clean_text(raw.get("building_address") or raw.get("property_address") or raw.get("street_address"))
        zip_code = clean_text(raw.get("building_zip"))
        building_units = clean_int(raw.get("building_unit_count"))
        property_units = clean_int(raw.get("property_units_count"))
        units = building_units if building_units is not None else property_units
        stories = clean_float(raw.get("number_of_stories")) or clean_float(raw.get("stories"))
        year_built = parse_year(raw.get("construction_month_year"))
        last_inspection = parse_date(raw.get("last_cyclical_inspection"))
        source_url = clean_text(raw.get("source_url") or raw.get("building_link"))

        properties[key] = (
            key,
            address,
            county,
            zip_code,
            owner_name,
            owner_norm,
            owner_address,
            normalize_address(owner_address),
            None,
            None,
            clean_text(raw.get("property_interest_type")),
            clean_text(raw.get("construction_classification")),
            stories,
            units,
            units,
            year_built,
            None,
            None,
            None,
            True,
            source_url,
            None,
            clean_text(raw.get("bhi_registration_no") or raw.get("building_registration_no")),
            clean_text(raw.get("property_interest_id")),
            clean_text(raw.get("property_interest_name")),
            clean_text(raw.get("building_id")),
            clean_text(raw.get("building_name")),
            clean_text(raw.get("block_no")),
            clean_text(raw.get("lot_no")),
            municipality,
            county,
            clean_text(raw.get("ownership_type")),
            agent_name,
            agent_address,
            agent_phone,
            agent_email,
            owner_address,
            owner_phone,
            last_inspection,
            source_url,
            is_true(raw.get("contact_redacted")),
            Json(raw),
        )

        registrations[key] = (
            key,
            key,
            clean_text(raw.get("building_id")),
            address,
            municipality,
            zip_code,
            county,
            clean_text(raw.get("bhi_registration_no") or raw.get("building_registration_no")),
            parse_date(raw.get("most_recent_start_date")),
            None,
            clean_text(raw.get("bhi_registration_no") or raw.get("building_registration_no")),
            clean_text(raw.get("property_interest_id")),
            clean_text(raw.get("property_interest_name")),
            source_url,
        )

        if owner_name:
            owner_is_business = looks_like_business_name(owner_name)
            city, state, zip_part = split_city_state_zip(owner_address)
            contact_type = "CORPORATEOWNER" if owner_is_business else "INDIVIDUALOWNER"
            contact_key = (key, contact_type, owner_norm, normalize_address(owner_address))
            contacts[contact_key] = (
                key,
                contact_type,
                owner_name if owner_is_business else None,
                owner_norm if owner_is_business else None,
                None,
                None,
                owner_name,
                owner_norm,
                owner_address,
                city,
                state,
                zip_part,
                "Primary Property Owner",
                owner_phone,
                None,
                is_true(raw.get("contact_redacted")),
            )

        if agent_name and agent_norm and agent_norm != owner_norm:
            city, state, zip_part = split_city_state_zip(agent_address)
            contact_key = (key, "AGENT", agent_norm, normalize_address(agent_address))
            contacts[contact_key] = (
                key,
                "AGENT",
                agent_name if looks_like_business_name(agent_name) else None,
                agent_norm if looks_like_business_name(agent_name) else None,
                None,
                None,
                agent_name,
                agent_norm,
                agent_address,
                city,
                state,
                zip_part,
                "Authorized Agent",
                agent_phone,
                agent_email,
                is_true(raw.get("contact_redacted")),
            )

        stats[key] = (
            key,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            0,
            0,
            0,
            None,
            0,
            None,
            False,
            0,
            False,
            None,
            None,
        )

    summary = {
        "deduped_buildings": len(properties),
        "registrations": len(registrations),
        "contacts": len(contacts),
        "duplicates_skipped": duplicates,
    }
    return list(properties.values()), list(registrations.values()), list(contacts.values()), list(stats.values()), summary


def apply_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as handle:
        ddl = handle.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def update_status(conn, status: str, details: Dict[str, Any], external_last_updated: Optional[str] = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, last_refreshed_at, refresh_status, details, external_last_updated)
            VALUES ('NJ_BHI_ACTIVE_BUILDINGS', 'city_dataset', NOW(), %s, %s::jsonb, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details,
                external_last_updated = EXCLUDED.external_last_updated;
            """,
            (status, json.dumps(details, default=str), external_last_updated),
        )
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, last_refreshed_at, refresh_status, details, external_last_updated)
            VALUES ('NJ', 'city_dataset', NOW(), %s, %s::jsonb, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details,
                external_last_updated = EXCLUDED.external_last_updated;
            """,
            (status, json.dumps(details, default=str), external_last_updated),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def load_database(
    conn,
    properties: List[tuple],
    registrations: List[tuple],
    contacts: List[tuple],
    stats: List[tuple],
):
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE nj_networks, nj_properties, nj_hpd_contacts, nj_hpd_registrations, nj_bbl_stats CASCADE;"
        )
        execute_values(
            cur,
            """
            INSERT INTO nj_properties (
                bbl, address, borough, zip_code, owner_name, owner_name_norm,
                mailing_address, mailing_address_norm, owner_email, owner_email_norm,
                land_use, bld_class, num_floors, units_res, units_total, year_built,
                assessed_total, latitude, longitude, compliance_active,
                compliance_record_id, compliance_expiration, bhi_registration_no,
                property_interest_id, property_interest_name, building_id, building_name,
                block_no, lot_no, municipality, county, ownership_type,
                authorized_agent_name, authorized_agent_address, authorized_agent_phone,
                authorized_agent_email, primary_owner_address, primary_owner_phone,
                last_cyclical_inspection, source_url, contact_redacted, raw
            )
            VALUES %s;
            """,
            properties,
            page_size=2000,
        )
        execute_values(
            cur,
            """
            INSERT INTO nj_hpd_registrations (
                registration_id, bbl, bin, building_address, building_city, building_zip,
                borough, lifecycle_stage, last_registration_date, registration_end_date,
                bhi_registration_no, property_interest_id, property_interest_name, source_url
            )
            VALUES %s;
            """,
            registrations,
            page_size=2000,
        )
        execute_values(
            cur,
            """
            INSERT INTO nj_hpd_contacts (
                registration_id, contact_type, corporation_name, corporation_name_norm,
                first_name, last_name, full_name, full_name_norm, business_address,
                business_city, business_state, business_zip, source_role, phone, email, redacted
            )
            VALUES %s;
            """,
            contacts,
            page_size=2000,
        )
        execute_values(
            cur,
            """
            INSERT INTO nj_bbl_stats (
                bbl, violations_total, violations_open, violations_class_c, violations_class_b,
                violations_class_a, violations_open_c, last_violation_date,
                litigations_total, litigations_open, litigations_harassment, last_litigation_date,
                evictions_total, last_eviction_date, is_rent_stabilized, rs_units,
                nhpd_subsidy, nhpd_program, nhpd_expiration
            )
            VALUES %s;
            """,
            stats,
            page_size=2000,
        )
    conn.commit()


def ingest_bhi(dry_run: bool = False, limit: int = 0, counties: Optional[Sequence[str]] = None, skip_networks: bool = False):
    start = time.time()
    conn = get_conn()
    try:
        apply_schema(conn)
        update_status(conn, "running", {"message": "Fetching NJ BHI active-building report", "source_url": REPORT_EMBED_URL})

        report = PowerBIReport()
        model_info = report.load_model()
        rows, source_metadata = fetch_bhi_rows(report, counties=counties, limit=limit)
        properties, registrations, contacts, stats, transform_summary = transform_rows(rows)
        details = {
            "message": "NJ BHI active-building ingest complete",
            "source_url": REPORT_EMBED_URL,
            "source_scope": "BHI-registered active buildings; not all NJ parcels",
            "source_records": len(rows),
            **transform_summary,
            **source_metadata,
        }

        if dry_run:
            LOGGER.info("Dry run complete: %s", json.dumps(details, indent=2, default=str))
            update_status(conn, "success", {**details, "dry_run": True}, model_info.get("LastRefreshTime"))
            return details

        load_database(conn, properties, registrations, contacts, stats)
        update_status(conn, "success", details, model_info.get("LastRefreshTime"))

        if not skip_networks:
            from nj.build_nj_networks import build_nj_networks

            build_nj_networks(conn=conn)

        LOGGER.info("NJ BHI ingest complete in %.1fs: %s buildings", time.time() - start, len(properties))
        return details
    except Exception as exc:
        LOGGER.exception("NJ BHI ingest failed")
        try:
            update_status(conn, "failure", {"message": str(exc), "source_url": REPORT_EMBED_URL})
        except Exception:
            pass
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest NJ DCA BHI active-building OPRA data.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and transform without writing NJ tables.")
    parser.add_argument("--limit", type=int, default=0, help="Limit transformed rows for testing.")
    parser.add_argument("--county", action="append", help="Restrict to one or more county names.")
    parser.add_argument("--skip-networks", action="store_true", help="Skip NJ network rebuild after ingest.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    counties = [c.strip().upper() for c in args.county] if args.county else None
    ingest_bhi(dry_run=args.dry_run, limit=args.limit, counties=counties, skip_networks=args.skip_networks)


if __name__ == "__main__":
    main()

