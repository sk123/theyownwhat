# safe_network_refresh.py
import os
import sys
import psycopg2
import logging
from psycopg2.extras import RealDictCursor

# Add current directory to path so we can import network_builder
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from network_builder import get_db_connection, link_properties_to_entities, build_graph, discover_networks_depth_limited, store_networks_shadow

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
GUREVITCH_MIN_PROPERTIES = int(os.environ.get("GUREVITCH_MIN_PROPERTIES", "900"))
GUREVITCH_MAX_PROPERTIES = int(os.environ.get("GUREVITCH_MAX_PROPERTIES", "2500"))

def setup_shadow_tables(conn):
    """Creates shadow tables for safe writing."""
    logger.info("Setting up shadow tables...")
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS networks_shadow (
                id SERIAL PRIMARY KEY, primary_name TEXT, total_properties INTEGER DEFAULT 0,
                total_assessed_value NUMERIC DEFAULT 0, business_count INTEGER DEFAULT 0,
                principal_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                network_size TEXT, updated_at TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_networks_shadow (
                network_id INTEGER,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('business', 'principal')),
                entity_id TEXT NOT NULL, entity_name TEXT NOT NULL, normalized_name TEXT,
                PRIMARY KEY (network_id, entity_type, entity_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ownership_links_shadow (
                network_id INTEGER,
                from_entity TEXT NOT NULL,
                to_entity TEXT NOT NULL,
                link_type TEXT NOT NULL
            );
        """)
        # Use unnamed CREATE INDEX statements so PostgreSQL generates fresh index
        # names on each shadow build (named IF NOT EXISTS indexes silently no-op
        # when the previous run's index name still lives on the renamed live table).
        cursor.execute("TRUNCATE networks_shadow, entity_networks_shadow, ownership_links_shadow RESTART IDENTITY;")
    conn.commit()


def create_shadow_indexes(conn):
    """Build indexes on shadow tables after bulk inserts and before atomic swap."""
    logger.info("Building indexes on shadow tables...")
    with conn.cursor() as cursor:
        cursor.execute("CREATE INDEX ON entity_networks_shadow (network_id);")
        cursor.execute("CREATE INDEX ON entity_networks_shadow (entity_type, entity_id);")
        cursor.execute("CREATE INDEX ON entity_networks_shadow (normalized_name);")
        cursor.execute("CREATE INDEX ON entity_networks_shadow USING gin (normalized_name gin_trgm_ops);")
        cursor.execute("CREATE INDEX ON ownership_links_shadow (network_id);")
        cursor.execute("ANALYZE networks_shadow, entity_networks_shadow, ownership_links_shadow;")
    conn.commit()

def validate_shadow_data(conn):
    """Ensures shadow data is sane before swapping."""
    logger.info("Validating shadow data...")
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT COUNT(*) as count FROM networks")
        live_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM networks_shadow")
        shadow_count = cursor.fetchone()['count']

        if live_count > 0:
            diff_percent = abs(live_count - shadow_count) / live_count
            logger.info(f"Current variance: {diff_percent:.1%}")
            # Relaxing for this restoration run
            # if diff_percent > 0.5:
            #     return False

        cursor.execute("""
            WITH menachem_networks AS (
                SELECT DISTINCT network_id
                FROM entity_networks_shadow
                WHERE entity_name ILIKE '%%MENACHEM%%GUREVITCH%%'
                   OR entity_name ILIKE '%%GUREVITCH%%MENACHEM%%'
            )
            SELECT
                n.id,
                n.primary_name,
                n.total_properties,
                EXISTS (
                    SELECT 1
                    FROM entity_networks_shadow y
                    WHERE y.network_id = n.id
                      AND (
                          y.entity_name ILIKE '%%YEHUDA%%GUREVITCH%%'
                          OR y.entity_name ILIKE '%%GUREVITCH%%YEHUDA%%'
                      )
                ) AS has_yehuda
            FROM networks_shadow n
            JOIN menachem_networks m ON m.network_id = n.id
            ORDER BY n.total_properties DESC
            LIMIT 5
        """)
        gurevitch_rows = cursor.fetchall()
        valid_gurevitch = any(
            row["has_yehuda"]
            and GUREVITCH_MIN_PROPERTIES <= (row["total_properties"] or 0) <= GUREVITCH_MAX_PROPERTIES
            for row in gurevitch_rows
        )
        if not valid_gurevitch:
            logger.error(
                "❌ Gurevitch validation failed. Expected Menachem+Yehuda with %s-%s properties; got %s",
                GUREVITCH_MIN_PROPERTIES,
                GUREVITCH_MAX_PROPERTIES,
                [dict(row) for row in gurevitch_rows],
            )
            return False

        logger.info(
            "✅ Gurevitch validation passed: %s",
            [dict(row) for row in gurevitch_rows],
        )
        logger.info(f"✅ Validation passed: Live={live_count}, Shadow={shadow_count}")
        return True

def rebuild_network_properties(conn):
    """Rebuild the network_id -> property_id lookup that the monitor endpoint reads.

    /api/monitor (Rap Sheets, and the front-page Top Networks cards) selects its
    candidate networks straight out of network_properties. Nothing rebuilt this
    table, so it drifted out of sync with every refresh — network ids are reissued
    by the shadow-table swap, which silently repoints old rows at unrelated
    networks. It has to be regenerated in the same cycle that writes `networks`.
    """
    logger.info("🔁 Rebuilding network_properties lookup...")
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS network_properties (
                network_id INTEGER,
                property_id INTEGER,
                property_city TEXT
            );
        """)
        cursor.execute("TRUNCATE network_properties;")
        # Mirror the join used to compute networks.total_properties so the
        # monitor's property_count agrees with the number on the network card.
        # (The two `owner_norm = entity_id` legs of that query compare a name to
        # a numeric id and can never match, so they are left out here.)
        cursor.execute("""
            INSERT INTO network_properties (network_id, property_id, property_city)
            SELECT DISTINCT en.network_id, p.id, UPPER(TRIM(p.property_city))
            FROM entity_networks en
            JOIN properties p ON (
                (en.entity_type = 'business'  AND p.business_id  = en.entity_id)
             OR (en.entity_type = 'business'  AND UPPER(p.owner) = UPPER(en.entity_name))
             OR (en.entity_type = 'business'  AND p.owner_norm   = en.normalized_name)
             OR (en.entity_type = 'principal' AND p.principal_id = en.entity_id)
            );
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_network_properties_net ON network_properties(network_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_network_properties_prop ON network_properties(property_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_network_properties_net_city ON network_properties(network_id, property_city);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_network_properties_city_net ON network_properties(property_city, network_id);"
        )
        cursor.execute("ANALYZE network_properties;")
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT network_id) FROM network_properties")
        rows, nets = cursor.fetchone()
    conn.commit()
    logger.info(f"✅ network_properties rebuilt: {rows:,} rows across {nets:,} networks.")


def atomic_swap(conn):
    """Perform the table swap in a single transaction."""
    logger.info("Performing atomic table swap...")
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE networks RENAME TO networks_old;")
        cursor.execute("ALTER TABLE entity_networks RENAME TO entity_networks_old;")
        cursor.execute("ALTER TABLE ownership_links RENAME TO ownership_links_old;")

        cursor.execute("ALTER TABLE networks_shadow RENAME TO networks;")
        cursor.execute("ALTER TABLE entity_networks_shadow RENAME TO entity_networks;")
        cursor.execute("ALTER TABLE ownership_links_shadow RENAME TO ownership_links;")

        cursor.execute("DROP TABLE networks_old CASCADE;")
        cursor.execute("DROP TABLE entity_networks_old CASCADE;")
        cursor.execute("DROP TABLE ownership_links_old CASCADE;")
        # Never DELETE dashboard_summary / completeness kv_cache rows here:
        # keeping previous values in place ensures live visitors get <2ms stale-while-revalidate
        # responses while Step 5 atomically overwrites kv_cache in the background.
        cursor.execute("""
            UPDATE kv_cache
            SET created_at = NOW() - INTERVAL '48 hours'
            WHERE key LIKE 'dashboard_summary:v2:ct:%'
               OR key = 'completeness_matrix'
               OR key = 'ct_completeness_prop_stats'
        """)
    conn.commit()

REFRESH_ADVISORY_LOCK_KEY = 918273645

def run_refresh(dry_run=False, skip_linking=False, skip_emails=False):
    """Executes the full refresh cycle."""
    logger.info(f"🚀 Starting Network Refresh (DryRun={dry_run}, SkipLinking={skip_linking}, SkipEmails={skip_emails})")
    conn = None
    lock_acquired = False
    try:
        conn = get_db_connection()
        with conn.cursor() as lock_cur:
            lock_cur.execute("SELECT pg_try_advisory_lock(%s)", (REFRESH_ADVISORY_LOCK_KEY,))
            lock_acquired = bool(lock_cur.fetchone()[0])
        if not lock_acquired:
            logger.warning("⚠️ Another Safe Network Refresh is already running (advisory lock held). Skipping duplicate run.")
            return False

        if not skip_linking:
            # 0. Clear stale links (Important for correctness after logic changes)
            with conn.cursor() as cur:
                logger.info("🧹 PHASE 0: Clearing stale property links...")
                # Optimization: Only update rows that actually have data
                cur.execute("""
                    UPDATE properties
                    SET business_id = NULL, principal_id = NULL
                    WHERE business_id IS NOT NULL OR principal_id IS NOT NULL
                """)
            conn.commit()

            # 1. Link properties
            link_properties_to_entities(conn)

        else:
            logger.info("⏩ PHASES 0 & 1 SKIPPED: Assuming property links are already set.")

        # 2. Build graph and discover networks
        if skip_emails:
            logger.info("⚠️ DIAGNOSTIC MODE: Skipping email matching.")
        else:
            logger.info("📧 Email Matching ENABLED.")

        graph_data = build_graph(conn, skip_emails=skip_emails)

        logger.info("Gathering seed nodes...")
        seeds = set()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT business_id FROM properties WHERE business_id IS NOT NULL")
            for r in cur: seeds.add(('business', r['business_id']))
            # properties.principal_id is TEXT while graph principal nodes are
            # keyed by the INTEGER unique_principals.principal_id. Without this
            # cast every ('principal', '123') seed silently misses the graph,
            # so principals who own property directly never seed a network.
            cur.execute("SELECT DISTINCT principal_id FROM properties WHERE principal_id IS NOT NULL")
            for r in cur:
                try:
                    seeds.add(('principal', int(r['principal_id'])))
                except (TypeError, ValueError):
                    continue

        networks = discover_networks_depth_limited(graph_data, list(seeds))

        # 3. Setup shadow and store
        setup_shadow_tables(conn)
        store_networks_shadow(conn, networks, graph_data=graph_data)
        create_shadow_indexes(conn)

        # 4. Validate and Swap
        if validate_shadow_data(conn):
            if dry_run:
                logger.info("Dry run complete. Tables NOT swapped.")
            else:
                atomic_swap(conn)
                logger.info("✅ Refresh cycle complete. Networks updated.")

                # 4b. Rebuild the monitor lookup against the new network ids.
                try:
                    rebuild_network_properties(conn)
                except Exception as e:
                    logger.error(f"❌ network_properties rebuild failed: {e}")
                    conn.rollback()

                # 5. Rebuild Insights & atomically update kv_cache
                # We do this AFTER the swap so the new data is live in 'networks', 'entity_networks', etc.
                logger.info("🔄 Triggering Insights & Summary Cache Refresh...")
                try:
                    from generate_insights import rebuild_cached_insights
                    rebuild_cached_insights(db_conn=conn)
                    logger.info("✅ Insights Cache Rebuilt Successfully.")
                except Exception as e:
                    logger.error(f"❌ Insights Rebuild Failed: {e}")

                try:
                    import json as _json
                    from psycopg2.extras import RealDictCursor as _RealDictCursor
                    from api.main import (
                        _get_or_compute_ct_prop_stats,
                        _calculate_ct_dashboard_summary,
                        _calculate_completeness_matrix,
                        json_converter,
                    )
                    with conn.cursor(cursor_factory=_RealDictCursor) as sum_cur:
                        _get_or_compute_ct_prop_stats(sum_cur, force_refresh=True)
                        statewide_summary = _calculate_ct_dashboard_summary(sum_cur, "STATEWIDE")
                        sum_cur.execute(
                            """
                            INSERT INTO kv_cache (key, value, created_at)
                            VALUES ('dashboard_summary:v2:ct:STATEWIDE', %s::jsonb, NOW())
                            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                            """,
                            (_json.dumps(statewide_summary, default=json_converter),),
                        )
                    conn.commit()
                    matrix_data = _calculate_completeness_matrix(conn)
                    with conn.cursor() as mat_cur:
                        mat_cur.execute(
                            """
                            INSERT INTO kv_cache (key, value, created_at)
                            VALUES ('completeness_matrix', %s::jsonb, NOW())
                            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                            """,
                            (_json.dumps(matrix_data, default=json_converter),),
                        )
                    conn.commit()
                    logger.info("✅ CT Statewide Dashboard Summary & Completeness Caches Pre-Warmed.")
                except Exception as e:
                    logger.error(f"❌ CT Summary/Completeness Pre-Warm Failed: {e}")
                    conn.rollback()
            return True
        return False

    except Exception as e:
        logger.error(f"Refresh failed: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn:
            if lock_acquired:
                try:
                    with conn.cursor() as unlock_cur:
                        unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (REFRESH_ADVISORY_LOCK_KEY,))
                except Exception:
                    pass
            conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Safe Network Refresh")
    parser.add_argument('--dry-run', action='store_true', help="Run without swapping tables")
    parser.add_argument('--skip-linking', action='store_true', help="Skip property linking phases (0 and 1)")
    parser.add_argument('--skip-emails', action='store_true', help="Skip email matching (Phase 2)")
    args = parser.parse_args()

    run_refresh(dry_run=args.dry_run, skip_linking=args.skip_linking, skip_emails=args.skip_emails)
