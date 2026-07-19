#!/usr/bin/env python3
"""
Enrich city *_bbl_stats tables with National Housing Preservation Database data.

This script uses the local "Active and Inconclusive Properties.xlsx" export when
present. It is intentionally city-generic so every city explorer dataset gets the
same subsidy pass, including datasets whose property IDs are not tax parcels
(for example NJ BHI building keys).
"""

import argparse
import json
import logging
import math
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")
DEFAULT_DATA_FILE = os.environ.get("NHPD_DATA_FILE", "data/Active and Inconclusive Properties.xlsx")

CITY_STATES = {
    "nyc": "NY",
    "dc": "DC",
    "baltimore": "MD",
    "boston": "MA",
    "detroit": "MI",
    "philadelphia": "PA",
    "chicago": "IL",
    "miami": "FL",
    "minneapolis": "MN",
    "nj": "NJ",
}

STATE_LOCALITY_ALIASES = {
    "DC": ["WASHINGTON"],
    "NY": ["NEW YORK", "MANHATTAN", "BROOKLYN", "BRONX", "QUEENS", "STATEN ISLAND"],
    "MD": ["BALTIMORE"],
    "MA": ["BOSTON"],
    "MI": ["DETROIT"],
    "PA": ["PHILADELPHIA"],
    "IL": ["CHICAGO"],
    "FL": ["MIAMI", "MIAMI DADE"],
    "MN": ["MINNEAPOLIS"],
}

SUBSIDY_PREFIXES = [
    "S8_1", "S8_2", "S202_1", "S202_2", "S236_1", "S236_2",
    "FHA_1", "FHA_2", "LIHTC_1", "LIHTC_2", "RHS515_1", "RHS515_2",
    "RHS538_1", "RHS538_2", "HOME_1", "HOME_2", "PH_1", "PH_2",
    "State_1", "State_2", "Pbv_1", "Pbv_2", "Mr_1", "Mr_2", "NHTF_1", "NHTF_2",
]

CORE_COLUMNS = {
    "NHPDPropertyID", "PropertyName", "PropertyAddress", "City", "State", "Zip",
    "Latitude", "Longitude", "PropertyStatus", "ActiveSubsidies",
    "TotalInconclusiveSubsidies", "TotalInactiveSubsidies", "TotalUnits",
    "EarliestEndDate", "LatestEndDate",
}
PROGRAM_COLUMNS = {f"{prefix}_ProgramName" for prefix in SUBSIDY_PREFIXES}
READ_COLUMNS = CORE_COLUMNS | PROGRAM_COLUMNS


@dataclass(frozen=True)
class NhpdRecord:
    nhpd_id: Optional[int]
    property_name: str
    address: str
    street_norm: str
    city_norm: str
    state: str
    zip5: str
    latitude: Optional[float]
    longitude: Optional[float]
    program: str
    expiration: Optional[Any]


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        return bool(cur.fetchone()[0])


def clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize_zip(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(value):
            return ""
        return str(int(value)).zfill(5)[:5]
    text = str(value).strip()
    try:
        if re.fullmatch(r"\d+(?:\.0)?", text):
            return str(int(float(text))).zfill(5)[:5]
    except Exception:
        pass
    digits = re.sub(r"\D", "", text)
    return digits.zfill(5)[:5] if digits else ""


def normalize_city(value: Any) -> str:
    text = clean_text(value).upper()
    text = re.sub(r"\b(TOWNSHIP|TWP|BOROUGH|BORO|CITY|TOWN|VILLAGE)\b", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def street_part(value: Any) -> str:
    text = clean_text(value).upper()
    if not text:
        return ""
    return text.split(",")[0].strip()


def normalize_street(value: Any) -> str:
    text = street_part(value)
    if not text:
        return ""

    replacements = {
        "AVENUE": "AVE",
        "STREET": "ST",
        "ROAD": "RD",
        "DRIVE": "DR",
        "BOULEVARD": "BLVD",
        "PLACE": "PL",
        "COURT": "CT",
        "CIRCLE": "CIR",
        "LANE": "LN",
        "TERRACE": "TER",
        "PARKWAY": "PKWY",
        "HIGHWAY": "HWY",
        "APARTMENTS": "APTS",
        "APARTMENT": "APT",
        "MOUNT": "MT",
        "SAINT": "ST",
        "NORTH": "N",
        "SOUTH": "S",
        "EAST": "E",
        "WEST": "W",
    }
    for long, short in replacements.items():
        text = re.sub(rf"\b{long}\b", short, text)

    text = re.sub(r"\b(APT|UNIT|BLDG|BUILDING|#)\s*[A-Z0-9-]+\b", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_zip_from_address(value: Any) -> str:
    text = clean_text(value).upper()
    match = re.search(r"(\d{5})(?:-\d{4})?\s*$", text)
    return match.group(1) if match else ""


def strip_trailing_location(value: Any, state_code: str, locality: str = "") -> str:
    text = clean_text(value).upper()
    if not text:
        return ""
    if "," in text:
        return text.split(",")[0].strip()

    text = re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", text).strip()
    text = re.sub(rf"\s+\b{re.escape(state_code.upper())}\b\s*$", "", text).strip()

    aliases = [locality, *STATE_LOCALITY_ALIASES.get(state_code.upper(), [])]
    for alias in sorted({normalize_city(a) for a in aliases if a}, key=len, reverse=True):
        if not alias:
            continue
        text = re.sub(rf"\s+\b{re.escape(alias)}\b\s*$", "", text).strip()

    return text


def parse_float(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        result = float(value)
    except Exception:
        return None
    if not math.isfinite(result):
        return None
    return result


def extract_programs_for_row(row: pd.Series) -> str:
    programs = []
    for prefix in SUBSIDY_PREFIXES:
        col = f"{prefix}_ProgramName"
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            programs.append(str(row[col]).strip())

    return ", ".join(sorted(set(programs))) if programs else "NHPD subsidy record"


def extract_expiration(row: pd.Series):
    for column in ("LatestEndDate", "EarliestEndDate"):
        value = row.get(column)
        if pd.isna(value):
            continue
        if isinstance(value, pd.Timestamp):
            return value.date()
        return value
    return None


def load_nhpd_records(data_file: Path) -> dict[str, list[NhpdRecord]]:
    if not data_file.exists():
        raise FileNotFoundError(f"NHPD data file not found: {data_file}")

    logger.info("Reading NHPD dataset from %s...", data_file)
    df = pd.read_excel(data_file, usecols=lambda c: c in READ_COLUMNS)
    if "State" not in df.columns or "PropertyAddress" not in df.columns:
        raise ValueError("NHPD workbook is missing required State/PropertyAddress columns")

    records_by_state: dict[str, list[NhpdRecord]] = defaultdict(list)
    for _, row in df.iterrows():
        state = clean_text(row.get("State")).upper()
        if state not in set(CITY_STATES.values()):
            continue
        address = clean_text(row.get("PropertyAddress"))
        street_norm = normalize_street(address)
        if not street_norm:
            continue
        nhpd_id = None
        raw_id = row.get("NHPDPropertyID")
        if pd.notna(raw_id):
            try:
                nhpd_id = int(float(raw_id))
            except Exception:
                nhpd_id = None
        records_by_state[state].append(NhpdRecord(
            nhpd_id=nhpd_id,
            property_name=clean_text(row.get("PropertyName")),
            address=address,
            street_norm=street_norm,
            city_norm=normalize_city(row.get("City")),
            state=state,
            zip5=normalize_zip(row.get("Zip")),
            latitude=parse_float(row.get("Latitude")),
            longitude=parse_float(row.get("Longitude")),
            program=extract_programs_for_row(row),
            expiration=extract_expiration(row),
        ))

    logger.info("Loaded %s NHPD rows across configured states", sum(len(v) for v in records_by_state.values()))
    return records_by_state


def build_indexes(records: Iterable[NhpdRecord]) -> dict[str, Any]:
    by_street_zip = defaultdict(list)
    by_street_city = defaultdict(list)
    coord_grid = defaultdict(list)

    for record in records:
        if record.zip5:
            by_street_zip[(record.street_norm, record.zip5)].append(record)
        if record.city_norm:
            by_street_city[(record.street_norm, record.city_norm)].append(record)
        if record.latitude is not None and record.longitude is not None:
            coord_grid[(round(record.latitude, 3), round(record.longitude, 3))].append(record)

    return {
        "by_street_zip": by_street_zip,
        "by_street_city": by_street_city,
        "coord_grid": coord_grid,
    }


def choose_record(candidates: list[NhpdRecord], city_norm: str = "", zip5: str = "") -> Optional[NhpdRecord]:
    if not candidates:
        return None

    def score(record: NhpdRecord) -> tuple[int, int, int, str]:
        return (
            0 if zip5 and record.zip5 == zip5 else 1,
            0 if city_norm and record.city_norm == city_norm else 1,
            0 if record.nhpd_id is not None else 1,
            record.property_name,
        )

    return sorted(candidates, key=score)[0]


def coordinate_match(indexes: dict[str, Any], latitude: Any, longitude: Any) -> Optional[NhpdRecord]:
    lat_val = parse_float(latitude)
    lon_val = parse_float(longitude)
    if lat_val is None or lon_val is None:
        return None

    best = None
    best_dist = 0.00075  # roughly 75 meters before latitude adjustment; used only as fallback
    for d_lat in (-0.001, 0, 0.001):
        for d_lon in (-0.001, 0, 0.001):
            grid_key = (round(lat_val + d_lat, 3), round(lon_val + d_lon, 3))
            for record in indexes["coord_grid"].get(grid_key, []):
                if record.latitude is None or record.longitude is None:
                    continue
                dist = ((lat_val - record.latitude) ** 2 + (lon_val - record.longitude) ** 2) ** 0.5
                if dist < best_dist:
                    best = record
                    best_dist = dist
    return best


def fetch_properties(conn, city: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        city_expr = "municipality" if city == "nj" else "borough"
        cur.execute(
            f"""
            SELECT
                bbl,
                address,
                zip_code,
                {city_expr} AS locality,
                latitude,
                longitude
            FROM {city}_properties
            WHERE bbl IS NOT NULL
            """
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def ensure_stats_columns(conn, city: str):
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {city}_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_subsidy BOOLEAN DEFAULT FALSE")
        cur.execute(f"ALTER TABLE {city}_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_program TEXT")
        cur.execute(f"ALTER TABLE {city}_bbl_stats ADD COLUMN IF NOT EXISTS nhpd_expiration DATE")
        cur.execute(f"ALTER TABLE {city}_bbl_stats ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
    conn.commit()


def reset_city_nhpd(conn, city: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {city}_bbl_stats
            SET nhpd_subsidy = FALSE,
                nhpd_program = NULL,
                nhpd_expiration = NULL,
                updated_at = NOW()
            WHERE COALESCE(nhpd_subsidy, FALSE)
               OR nhpd_program IS NOT NULL
               OR nhpd_expiration IS NOT NULL
            """
        )
        reset_count = cur.rowcount
    conn.commit()
    return reset_count


def update_source_status(conn, source_name: str, status: str, details: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_source_status
                (source_name, source_type, last_refreshed_at, refresh_status, details)
            VALUES (%s, 'city_enrichment', NOW(), %s, %s::jsonb)
            ON CONFLICT (source_name)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_refreshed_at = EXCLUDED.last_refreshed_at,
                refresh_status = EXCLUDED.refresh_status,
                details = EXCLUDED.details
            """,
            (source_name, status, json.dumps(details, default=str)),
        )
        cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
    conn.commit()


def enrich_city(conn, city: str, state_code: str, state_records: list[NhpdRecord], data_file: Path, dry_run: bool = False) -> dict:
    props_table = f"{city}_properties"
    stats_table = f"{city}_bbl_stats"
    if not table_exists(conn, props_table) or not table_exists(conn, stats_table):
        details = {"message": "Required city tables are not loaded", "city": city}
        update_source_status(conn, f"{city.upper()}_NHPD_ENRICHMENT", "unavailable", details)
        return details

    ensure_stats_columns(conn, city)
    indexes = build_indexes(state_records)
    properties = fetch_properties(conn, city)
    logger.info("Loaded %s properties from %s", len(properties), props_table)

    matches_by_bbl = {}
    method_counts = Counter()
    nhpd_ids = set()

    for prop in properties:
        bbl = prop.get("bbl")
        locality = normalize_city(prop.get("locality"))
        zip5 = normalize_zip(prop.get("zip_code")) or extract_zip_from_address(prop.get("address"))
        street = normalize_street(strip_trailing_location(prop.get("address"), state_code, locality))
        if not street:
            continue

        record = None
        method = None

        candidates = indexes["by_street_zip"].get((street, zip5)) if zip5 else None
        record = choose_record(candidates or [], locality, zip5)
        if record:
            method = "street_zip"

        if record is None:
            record = coordinate_match(indexes, prop.get("latitude"), prop.get("longitude"))
            if record:
                method = "coordinates"

        if record is None and locality:
            candidates = indexes["by_street_city"].get((street, locality))
            record = choose_record(candidates or [], locality, zip5)
            if record:
                method = "street_city"

        if record is None:
            continue

        matches_by_bbl[bbl] = (
            bbl,
            True,
            record.program,
            record.expiration,
        )
        method_counts[method] += 1
        if record.nhpd_id is not None:
            nhpd_ids.add(record.nhpd_id)

    details = {
        "message": "NHPD subsidy enrichment complete",
        "city": city,
        "state": state_code,
        "source_file": str(data_file),
        "source_file_mtime": datetime.fromtimestamp(data_file.stat().st_mtime).isoformat(),
        "nhpd_state_records": len(state_records),
        "properties_examined": len(properties),
        "matched_properties": len(matches_by_bbl),
        "unique_nhpd_properties_matched": len(nhpd_ids),
        "match_methods": dict(method_counts),
    }

    if dry_run:
        logger.info("%s dry-run summary: %s", city.upper(), json.dumps(details, default=str))
        return details

    reset_count = reset_city_nhpd(conn, city)
    details["stale_flags_reset"] = reset_count

    matches = list(matches_by_bbl.values())
    if matches:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {stats_table} (
                    bbl,
                    nhpd_subsidy,
                    nhpd_program,
                    nhpd_expiration,
                    updated_at
                )
                VALUES %s
                ON CONFLICT (bbl) DO UPDATE SET
                    nhpd_subsidy = EXCLUDED.nhpd_subsidy,
                    nhpd_program = EXCLUDED.nhpd_program,
                    nhpd_expiration = EXCLUDED.nhpd_expiration,
                    updated_at = NOW()
                """,
                matches,
                template="(%s, %s, %s, %s, NOW())",
                page_size=2000,
            )
    conn.commit()

    update_source_status(conn, f"{city.upper()}_NHPD_ENRICHMENT", "success", details)
    logger.info(
        "%s: matched %s/%s properties to %s unique NHPD properties",
        city.upper(),
        len(matches_by_bbl),
        len(properties),
        len(nhpd_ids),
    )
    return details


def enrich_all_cities(cities: Optional[list[str]] = None, data_file: str = DEFAULT_DATA_FILE, dry_run: bool = False) -> dict:
    selected = [city.lower() for city in (cities or list(CITY_STATES.keys()))]
    unknown = [city for city in selected if city not in CITY_STATES]
    if unknown:
        raise ValueError(f"Unsupported cities: {', '.join(unknown)}")

    path = Path(data_file)
    records_by_state = load_nhpd_records(path)

    results = {}
    with get_db_connection() as conn:
        for city in selected:
            state_code = CITY_STATES[city]
            logger.info("=== Enriching %s with NHPD data (%s) ===", city.upper(), state_code)
            try:
                results[city] = enrich_city(
                    conn=conn,
                    city=city,
                    state_code=state_code,
                    state_records=records_by_state.get(state_code, []),
                    data_file=path,
                    dry_run=dry_run,
                )
            except Exception as exc:
                logger.exception("%s NHPD enrichment failed", city.upper())
                if not dry_run:
                    try:
                        update_source_status(conn, f"{city.upper()}_NHPD_ENRICHMENT", "failure", {
                            "message": str(exc),
                            "city": city,
                            "source_file": str(path),
                        })
                    except Exception:
                        logger.exception("Failed to update failure status for %s", city.upper())
                results[city] = {"error": str(exc)}

        if not dry_run:
            total_matches = sum(int(v.get("matched_properties") or 0) for v in results.values() if isinstance(v, dict))
            update_source_status(conn, "NHPD_ENRICHMENT", "success", {
                "message": "All-city NHPD subsidy enrichment complete",
                "source_file": str(path),
                "cities": selected,
                "matched_properties": total_matches,
                "results": results,
            })

    return results


def main():
    parser = argparse.ArgumentParser(description="Enrich all city bbl_stats tables with NHPD subsidy records.")
    parser.add_argument("--city", action="append", help="Restrict to one or more city keys.")
    parser.add_argument("--data-file", default=DEFAULT_DATA_FILE, help="Path to NHPD Active and Inconclusive workbook.")
    parser.add_argument("--dry-run", action="store_true", help="Calculate matches without writing to the database.")
    args = parser.parse_args()

    enrich_all_cities(cities=args.city, data_file=args.data_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
