"""
api/landing_cache.py
====================
Daily pre-computed & persisted cache for the landing page snapshot and city summaries.
Eliminates 35+ per-visit database scans by storing a slim, source-only snapshot in
PostgreSQL `kv_cache` (24-hour TTL with stale-while-revalidate background refresh).
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import decimal
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

import api.db as db_module

logger = logging.getLogger("landing-cache")

LANDING_CACHE_KEY = "landing_snapshot:v1"
DAILY_TTL_SECONDS = 86400  # 24 hours
MEMORY_CHECK_TTL_SECONDS = 15  # Re-check DB created_at every 15s so rebuilds appear immediately

DATASET_KEY_TO_CITY_PREFIX = {
    "NY": "nyc",
    "NYS": "ny",
    "DC": "dc",
    "BALTIMORE": "baltimore",
    "DETROIT": "detroit",
    "PHILADELPHIA": "philadelphia",
    "CHICAGO": "chicago",
    "MIAMI": "miami",
    "MINNEAPOLIS": "minneapolis",
    "NJ": "nj",
    "MA": "ma",
    "FL": "fl",
    "CO": "co",
    "VT": "vt",
    "WI": "wi",
    "NC": "nc",
    "AR": "ar",
    "VA": "va",
}

CODE_CASE_DATASET_KEYS = {
    "CT", "NY", "MA", "DC", "DETROIT", "BALTIMORE", "PHILADELPHIA", "MINNEAPOLIS", "NYS",
}
EVICTION_DATASET_KEYS = {
    "CT", "NY", "BALTIMORE", "NYS",
}

_LOCK = threading.Lock()
_REFRESH_INFLIGHT = False
_MEM_SNAPSHOT: Dict[str, Any] | None = None
_MEM_TS: float = 0.0


def _json_default(obj: Any) -> Any:
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (dt.datetime, dt.date)):
        return obj.isoformat()
    return str(obj)


def _ensure_kv_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_cache (
            key text PRIMARY KEY,
            value jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = %s
          AND c.relkind IN ('r', 'm', 'p', 'v')
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def _slim_network_row(net: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only fields rendered by DatasetLanding cards/tooltips to keep payload tiny."""
    keep_keys = (
        "network_key",
        "network_id",
        "id",
        "entity_id",
        "entity_name",
        "primary_entity_name",
        "display_name",
        "label",
        "sublabel",
        "property_count",
        "value",
        "building_count",
        "business_count",
        "network_business_count",
        "linked_business_count",
        "unit_count",
        "total_assessed_value",
        "open_violations",
        "open_violations_c",
        "total_violations",
        "violation_count",
        "eviction_count",
        "evictions_total",
    )
    return {k: net[k] for k in keep_keys if k in net and net[k] is not None}


def _code_case_val(net: Dict[str, Any]) -> int:
    for field in ("violation_count", "total_violations", "open_violations"):
        val = net.get(field)
        try:
            n = int(val)
            if n > 0:
                return n
        except (TypeError, ValueError):
            continue
    return 0


def build_and_store_landing_snapshot(conn=None) -> Dict[str, Any]:
    """Recompute the full landing snapshot from loaded source tables and persist to kv_cache."""
    global _MEM_SNAPSHOT, _MEM_TS
    t0 = time.time()

    own_conn = False
    if conn is None:
        if db_module.db_pool is not None:
            conn = db_module.db_pool.getconn()
            use_pool = True
        else:
            db_url = os.environ.get("DATABASE_URL")
            if not db_url:
                raise RuntimeError("DATABASE_URL is not configured")
            conn = psycopg2.connect(db_url)
            use_pool = False
        own_conn = True
    else:
        use_pool = False

    # Ensure db_pool is initialized if running standalone from CLI
    if db_module.db_pool is None:
        db_module.init_db_pool()

    from api.city_routes import city_stats, city_top_networks

    stats_by_dataset: Dict[str, Any] = {}
    top_properties: Dict[str, List[Dict[str, Any]]] = {}
    top_evictions: Dict[str, List[Dict[str, Any]]] = {}
    top_code: Dict[str, List[Dict[str, Any]]] = {}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_kv_table(cur)

            # 1. Connecticut summary + top-3 rankings
            try:
                from api.main import _calculate_ct_dashboard_summary, get_landlord_monitor

                cur.execute(
                    """
                    SELECT value
                    FROM kv_cache
                    WHERE key = 'dashboard_summary:v2:ct:STATEWIDE'
                      AND created_at > NOW() - INTERVAL '24 hours'
                    """
                )
                ct_cached = cur.fetchone()
                if ct_cached and ct_cached.get("value"):
                    stats_by_dataset["CT"] = ct_cached["value"]
                else:
                    ct_sum = _calculate_ct_dashboard_summary(cur, "STATEWIDE")
                    stats_by_dataset["CT"] = ct_sum
                    cur.execute(
                        """
                        INSERT INTO kv_cache (key, value, created_at)
                        VALUES ('dashboard_summary:v2:ct:STATEWIDE', %s::jsonb, NOW())
                        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                        """,
                        (json.dumps(ct_sum, default=_json_default),),
                    )

                if _table_exists(cur, "cached_insights"):
                    cur.execute(
                        """
                        SELECT network_id, network_name AS entity_name, primary_entity_id AS entity_id,
                               primary_entity_name, property_count, business_count, linked_business_count,
                               unit_count, total_assessed_value
                        FROM cached_insights
                        WHERE UPPER(title) = 'STATEWIDE'
                        ORDER BY property_count DESC, unit_count DESC
                        LIMIT 3
                        """
                    )
                    top_properties["CT"] = [_slim_network_row(dict(r)) for r in cur.fetchall()]

                try:
                    ct_ev = get_landlord_monitor(city="STATEWIDE", dimension="network", sort_by="evictions", conn=conn)
                    if isinstance(ct_ev, list):
                        top_evictions["CT"] = [_slim_network_row(r) for r in ct_ev[:3]]
                except Exception as exc:
                    logger.warning("CT eviction top-3 snapshot warning: %s", exc)

                try:
                    ct_cd = get_landlord_monitor(city="STATEWIDE", dimension="network", sort_by="violations", conn=conn)
                    if isinstance(ct_cd, list):
                        ranked_cd = sorted(ct_cd, key=_code_case_val, reverse=True)
                        top_code["CT"] = [_slim_network_row(r) for r in ranked_cd if _code_case_val(r) > 0][:3]
                except Exception as exc:
                    logger.warning("CT code top-3 snapshot warning: %s", exc)
            except Exception as exc:
                logger.warning("CT snapshot section failed: %s", exc)

            # 2. All Multi-City & Statewide Explorer Datasets (parallelized across pool connections)
            active_datasets: List[Tuple[str, str]] = [
                (dataset_key, prefix)
                for dataset_key, prefix in DATASET_KEY_TO_CITY_PREFIX.items()
                if _table_exists(cur, f"{prefix}_networks") and _table_exists(cur, f"{prefix}_properties")
            ]

        def _compute_single_dataset(item: Tuple[str, str]):
            dataset_key, prefix = item
            d_stats = None
            d_prop: List[Dict[str, Any]] = []
            d_ev: List[Dict[str, Any]] = []
            d_cd: List[Dict[str, Any]] | None = None

            try:
                d_stats = city_stats(prefix, force_refresh=True)
            except Exception as exc:
                logger.warning("Snapshot stats failed for %s (%s): %s", dataset_key, prefix, exc)

            try:
                nets_prop = city_top_networks(
                    prefix, limit=3, min_buildings=1, sort_by="buildings", include_institutional=False, force_refresh=True
                )
                d_prop = [_slim_network_row(r) for r in nets_prop[:3]]
            except Exception as exc:
                logger.warning("Snapshot portfolio top-3 failed for %s: %s", dataset_key, exc)

            if dataset_key in EVICTION_DATASET_KEYS:
                try:
                    nets_ev = city_top_networks(
                        prefix, limit=3, min_buildings=1, sort_by="evictions", include_institutional=False, force_refresh=True
                    )
                    d_ev = [_slim_network_row(r) for r in nets_ev[:3]]
                except Exception as exc:
                    logger.warning("Snapshot eviction top-3 failed for %s: %s", dataset_key, exc)
            else:
                d_ev = list(d_prop)

            if dataset_key in CODE_CASE_DATASET_KEYS:
                try:
                    nets_cd = city_top_networks(
                        prefix, limit=3, min_buildings=1, sort_by="code_cases", include_institutional=False, force_refresh=True
                    )
                    filtered_cd = [r for r in nets_cd if _code_case_val(r) > 0][:3]
                    d_cd = [_slim_network_row(r) for r in filtered_cd]
                except Exception as exc:
                    logger.warning("Snapshot code top-3 failed for %s: %s", dataset_key, exc)

            return dataset_key, d_stats, d_prop, d_ev, d_cd

        if active_datasets:
            with ThreadPoolExecutor(max_workers=min(6, len(active_datasets))) as pool:
                futures = [pool.submit(_compute_single_dataset, item) for item in active_datasets]
                for fut in as_completed(futures):
                    d_key, d_stats, d_prop, d_ev, d_cd = fut.result()
                    if d_stats is not None:
                        stats_by_dataset[d_key] = d_stats
                    top_properties[d_key] = d_prop
                    top_evictions[d_key] = d_ev
                    if d_cd is not None:
                        top_code[d_key] = d_cd

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            snapshot = {
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "ttl_seconds": DAILY_TTL_SECONDS,
                "stats_by_dataset": stats_by_dataset,
                "top_networks": {
                    "properties": top_properties,
                    "evictions": top_evictions,
                    "code": top_code,
                },
            }

            cur.execute(
                """
                INSERT INTO kv_cache (key, value, created_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    created_at = NOW()
                """,
                (LANDING_CACHE_KEY, json.dumps(snapshot, default=_json_default)),
            )
        conn.commit()

        _MEM_SNAPSHOT = snapshot
        _MEM_TS = time.time()
        logger.info(
            "Built & cached daily landing snapshot (%d datasets) in %.2fs",
            len(stats_by_dataset),
            time.time() - t0,
        )
        return snapshot
    finally:
        if own_conn and conn is not None:
            if use_pool and db_module.db_pool is not None:
                db_module.db_pool.putconn(conn)
            else:
                conn.close()


def trigger_landing_snapshot_refresh_async() -> None:
    global _REFRESH_INFLIGHT
    with _LOCK:
        if _REFRESH_INFLIGHT:
            return
        _REFRESH_INFLIGHT = True

    def _worker():
        global _REFRESH_INFLIGHT
        try:
            build_and_store_landing_snapshot()
        except Exception:
            logger.exception("Background landing snapshot refresh failed")
        finally:
            with _LOCK:
                _REFRESH_INFLIGHT = False

    threading.Thread(target=_worker, daemon=True, name="landing-snapshot-refresh").start()


def get_landing_snapshot(conn, refresh: bool = False) -> Dict[str, Any]:
    global _MEM_SNAPSHOT, _MEM_TS
    now = time.time()

    if not refresh and _MEM_SNAPSHOT is not None and (now - _MEM_TS) < MEMORY_CHECK_TTL_SECONDS:
        return _MEM_SNAPSHOT

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        _ensure_kv_table(cur)
        cur.execute(
            """
            SELECT value, (created_at > NOW() - INTERVAL '24 hours') AS is_fresh
            FROM kv_cache
            WHERE key = %s
            """,
            (LANDING_CACHE_KEY,),
        )
        row = cur.fetchone()
        if row and row.get("value") and not refresh:
            val = row["value"]
            _MEM_SNAPSHOT = val
            _MEM_TS = now
            if not row.get("is_fresh"):
                trigger_landing_snapshot_refresh_async()
            return val

    if not refresh:
        # Never block a live visitor HTTP request on a cold full-country build;
        # kick off async warm-up and return immediately so DatasetLanding can use per-city endpoints.
        trigger_landing_snapshot_refresh_async()
        return {
            "warming": True,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "ttl_seconds": 10,
            "stats_by_dataset": {},
            "top_networks": {"properties": {}, "evictions": {}, "code": {}},
        }

    return build_and_store_landing_snapshot(conn=conn)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    build_and_store_landing_snapshot()
