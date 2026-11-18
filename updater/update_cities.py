#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from api.populate_cities import populate_city, get_connection, CITIES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _record_city_failure(conn, city: str, error_msg: str):
    try:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_source_status
                    (source_name, source_type, last_refreshed_at, refresh_status, details)
                VALUES (%s, 'city_dataset', NOW(), 'failure', %s::jsonb)
                ON CONFLICT (source_name) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    last_refreshed_at = NOW(),
                    refresh_status = 'failure',
                    details = EXCLUDED.details
                """,
                (city.upper(), json.dumps({"error": error_msg[:500]})),
            )
            cur.execute("DELETE FROM kv_cache WHERE key = 'completeness_matrix'")
        conn.commit()
    except Exception as status_exc:
        logger.error("Could not record failure status for %s: %s", city, status_exc)


def run_update(full: bool = True, target_cities: list[str] | None = None) -> int:
    cities = target_cities or CITIES
    logger.info("Starting multi-city property ingestion & network rebuilding: %s", ", ".join(cities))
    failures = []
    try:
        conn = get_connection()
        try:
            for city in cities:
                try:
                    populate_city(conn, city)
                    if city == "dc":
                        try:
                            from scripts.enrich_dc_cama import main as run_dc_cama_enrichment
                            run_dc_cama_enrichment()
                            logger.info("✓ DC CAMA year_built / units enrichment completed.")
                        except Exception as dc_cama_exc:
                            logger.warning("DC CAMA enrichment skipped/failed: %s", dc_cama_exc)
                    if city in {"boston", "miami", "philadelphia", "dc", "detroit", "baltimore"}:
                        try:
                            from scripts.enrich_gis_recent_sales import (
                                _ensure_sale_columns,
                                enrich_baltimore_sales,
                                enrich_boston_from_massgis,
                                enrich_dc_cama_sales,
                                enrich_detroit_sales,
                                enrich_miami_from_fdor,
                                enrich_philadelphia_opa,
                            )
                            _ensure_sale_columns(conn)
                            {
                                "boston": enrich_boston_from_massgis,
                                "miami": enrich_miami_from_fdor,
                                "philadelphia": enrich_philadelphia_opa,
                                "dc": enrich_dc_cama_sales,
                                "detroit": enrich_detroit_sales,
                                "baltimore": enrich_baltimore_sales,
                            }[city](conn)
                        except Exception as sale_exc:
                            logger.warning("%s GIS recent-sale enrichment skipped/failed: %s", city.upper(), sale_exc)
                    if city == "boston":
                        try:
                            from ma.scrape_ma_cama import scrape_boston_cama
                            scrape_boston_cama(conn)
                            logger.info("✓ Synced Boston Assessing & ISD updates into unified MA dataset.")
                        except Exception as ma_sync_exc:
                            logger.warning("MA Boston cross-enrichment sync skipped/failed: %s", ma_sync_exc)
                    logger.info("✓ %s sync and network rebuild completed.", city.upper())
                except Exception as city_exc:
                    logger.error("✘ %s sync failed (continuing to remaining cities): %s", city.upper(), city_exc)
                    _record_city_failure(conn, city, str(city_exc))
                    failures.append((city, str(city_exc)))
        finally:
            conn.close()
    except Exception as e:
        logger.error("Database connection error during multi-city update: %s", e)
        return 1

    if failures:
        logger.warning(
            "Multi-city update completed with %d failure(s): %s",
            len(failures),
            ", ".join(f"{c} ({err[:80]})" for c, err in failures),
        )
        return 1

    logger.info("✓ All requested cities synced and rebuilt successfully.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-city property update runner")
    parser.add_argument("--full", action="store_true", default=True, help="Perform full ingestion and network rebuild")
    parser.add_argument(
        "--city",
        nargs="+",
        default=None,
        help=f"Optional city subset to refresh (choices: {', '.join(CITIES)}, or 'all')",
    )
    args = parser.parse_args()
    selected = None
    if args.city and "all" not in [c.lower() for c in args.city]:
        selected = []
        for raw in args.city:
            c = raw.lower().strip()
            if c not in CITIES:
                parser.error(f"Unsupported city '{raw}'. Valid choices: {', '.join(CITIES)}")
            selected.append(c)
    sys.exit(run_update(full=args.full, target_cities=selected))
