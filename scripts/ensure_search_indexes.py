#!/usr/bin/env python3
"""
scripts/ensure_search_indexes.py
================================
Idempotently builds missing pg_trgm, building_count DESC, and primary_human_name
expression indexes CONCURRENTLY across all active city & statewide jurisdiction tables.

Uses CREATE INDEX CONCURRENTLY (autocommit) so live read traffic is never blocked.
Safe to re-run at any time.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import psycopg2

ALL_JURISDICTIONS = (
    # City / NJ explorers (historically created without gin_trgm_ops or bldg_count DESC)
    "nyc", "dc", "baltimore", "boston", "detroit", "philadelphia",
    "chicago", "miami", "minneapolis", "nj",
    # Statewide explorers
    "ma", "fl", "ny", "il", "ca", "co", "vt", "wi", "nc", "ar", "va", "mt", "mn", "oh",
)


def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{name}",))
    return cur.fetchone()[0] is not None


def _existing_indexes(cur, table_name: str) -> dict[str, str]:
    cur.execute(
        """
        SELECT i.relname AS indexname, pg_get_indexdef(i.oid) AS indexdef
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = %s
          AND ix.indisvalid
        """,
        (table_name,),
    )
    return {name: defn for name, defn in cur.fetchall()}


def _column_type(cur, table_name: str, column_name: str) -> str | None:
    cur.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    return row[0] if row else None


def build_missing_indexes(
    dsn: str,
    targets: list[str],
    *,
    dry_run: bool = False,
    concurrently: bool = False,
) -> int:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    created = 0
    conc = "CONCURRENTLY " if concurrently else ""

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        # Plain CREATE INDEX acquires ShareLock (allows all concurrent SELECT reads without
        # waiting on unrelated batch transactions' virtualxid snapshots).
        cur.execute("SET lock_timeout = '3000ms';" if not concurrently else "SET lock_timeout = '0';")
        cur.execute("SET statement_timeout = '0';")

        for jur in targets:
            net_tbl = f"{jur}_networks"
            prop_tbl = f"{jur}_properties"

            if _table_exists(cur, net_tbl):
                idxs = _existing_indexes(cur, net_tbl)
                defs_lower = [d.lower() for d in idxs.values()]
                sig_type = _column_type(cur, net_tbl, "connection_signals")
                sig_expr = (
                    "(connection_signals->>'primary_human_name')"
                    if sig_type == "jsonb"
                    else "((connection_signals)::jsonb->>'primary_human_name')"
                )

                specs = []
                if not any("gin_trgm_ops" in d and "display_name" in d for d in defs_lower):
                    specs.append((
                        f"{jur}_networks_display_trgm_idx",
                        f"CREATE INDEX {conc}IF NOT EXISTS {jur}_networks_display_trgm_idx "
                        f"ON public.{net_tbl} USING gin (display_name gin_trgm_ops)",
                    ))
                if not any("(building_count desc)" in d for d in defs_lower):
                    specs.append((
                        f"{jur}_networks_bldg_count_idx",
                        f"CREATE INDEX {conc}IF NOT EXISTS {jur}_networks_bldg_count_idx "
                        f"ON public.{net_tbl} USING btree (building_count DESC)",
                    ))
                if sig_type and not any("primary_human_name" in d for d in defs_lower):
                    specs.append((
                        f"idx_{jur}_nets_primary_human_upper",
                        f"CREATE INDEX {conc}IF NOT EXISTS idx_{jur}_nets_primary_human_upper "
                        f"ON public.{net_tbl} USING btree (UPPER({sig_expr})) "
                        f"WHERE ({sig_expr} IS NOT NULL)",
                    ))

                for idx_name, ddl in specs:
                    if dry_run:
                        print(f"[DRY-RUN] {ddl}")
                        continue
                    t0 = time.time()
                    print(f"▶ Building {idx_name} on {net_tbl}...", flush=True)
                    try:
                        cur.execute(ddl)
                        created += 1
                        print(f"  ✓ {idx_name} built in {time.time() - t0:.1f}s", flush=True)
                    except psycopg2.errors.LockNotAvailable:
                        print(f"  ⚠ Skipped {idx_name} (table locked by active rebuild)", flush=True)
                    except Exception as exc:
                        print(f"  ✗ Failed {idx_name}: {exc}", flush=True)

            if _table_exists(cur, prop_tbl):
                idxs = _existing_indexes(cur, prop_tbl)
                defs_lower = [d.lower() for d in idxs.values()]
                specs = []
                if not any("gin_trgm_ops" in d and "(address " in d for d in defs_lower):
                    specs.append((
                        f"{jur}_properties_addr_trgm_idx",
                        f"CREATE INDEX {conc}IF NOT EXISTS {jur}_properties_addr_trgm_idx "
                        f"ON public.{prop_tbl} USING gin (address gin_trgm_ops)",
                    ))
                if not any("gin_trgm_ops" in d and "(owner_name " in d for d in defs_lower):
                    specs.append((
                        f"{jur}_properties_owner_trgm_idx",
                        f"CREATE INDEX {conc}IF NOT EXISTS {jur}_properties_owner_trgm_idx "
                        f"ON public.{prop_tbl} USING gin (owner_name gin_trgm_ops)",
                    ))

                for idx_name, ddl in specs:
                    if dry_run:
                        print(f"[DRY-RUN] {ddl}")
                        continue
                    t0 = time.time()
                    print(f"▶ Building {idx_name} on {prop_tbl}...", flush=True)
                    try:
                        cur.execute(ddl)
                        created += 1
                        print(f"  ✓ {idx_name} built in {time.time() - t0:.1f}s", flush=True)
                    except psycopg2.errors.LockNotAvailable:
                        print(f"  ⚠ Skipped {idx_name} (table locked by active rebuild)", flush=True)
                    except Exception as exc:
                        print(f"  ✗ Failed {idx_name}: {exc}", flush=True)

    conn.close()
    return created


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure search indexes across all jurisdictions.")
    parser.add_argument("--targets", nargs="*", default=list(ALL_JURISDICTIONS))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--concurrently", action="store_true")
    args = parser.parse_args()

    dsn = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    created = build_missing_indexes(dsn, args.targets, dry_run=args.dry_run, concurrently=args.concurrently)
    print(f"Done. Created {created} index(es).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
