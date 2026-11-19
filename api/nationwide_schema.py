"""
api/nationwide_schema.py
========================
Canonical PostgreSQL 15 partitioned nationwide property/network schema (`us_*`)
and global OpenCorporates bulk registry tables (`oc_*`).

Design guarantees:
  1. Child partitions for any jurisdiction `jur` (e.g., `'ma'`, `'fl'`) are named
     `{jur}_properties`, `{jur}_networks`, `{jur}_hpd_registrations`,
     `{jur}_hpd_contacts`, and `{jur}_bbl_stats`, with `DEFAULT '{jur}'` on
     `jurisdiction` and local `UNIQUE` indexes on `(bbl)` / `(network_key)` /
     `(registration_id)`.
  2. Existing city endpoints (`api/city_routes.py`), enforcement enrichers
     (`updater/enrich_city_enforcement.py`), NHPD enrichers, and verification
     batteries work against `{jur}_*` with zero SQL rewrites, while global
     cross-jurisdiction queries can scan `us_properties` and `us_networks`
     joined directly to `oc_*`.
"""

from __future__ import annotations

import re
from typing import Iterable

JURISDICTION_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{1,23}$")


def validate_jurisdiction_slug(jurisdiction: str) -> str:
    slug = (jurisdiction or "").strip().lower()
    if not JURISDICTION_SLUG_RE.fullmatch(slug):
        raise ValueError(f"Invalid jurisdiction slug: {jurisdiction!r}")
    return slug


def ensure_opencorporates_tables(conn) -> None:
    """Idempotently create global OpenCorporates bulk registry tables and indexes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS oc_companies (
                jurisdiction_code         TEXT NOT NULL,
                company_number            TEXT NOT NULL,
                name                      TEXT NOT NULL,
                name_norm                 TEXT NOT NULL,
                name_prefix18             TEXT,
                company_type              TEXT,
                current_status            TEXT,
                is_active                 BOOLEAN DEFAULT TRUE,
                incorporation_date        DATE,
                dissolution_date          DATE,
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
                home_company_number       TEXT,
                is_nonprofit              BOOLEAN DEFAULT FALSE,
                registry_url              TEXT,
                opencorporates_url        TEXT,
                retrieved_at              TIMESTAMPTZ,
                updated_at                TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (jurisdiction_code, company_number)
            );

            ALTER TABLE oc_companies ADD COLUMN IF NOT EXISTS is_nonprofit BOOLEAN DEFAULT FALSE;
            ALTER TABLE oc_companies ADD COLUMN IF NOT EXISTS registry_url TEXT;

            CREATE INDEX IF NOT EXISTS idx_oc_companies_jur_name_norm
                ON oc_companies (jurisdiction_code, name_norm);
            CREATE INDEX IF NOT EXISTS idx_oc_companies_jur_prefix18
                ON oc_companies (jurisdiction_code, name_prefix18)
                WHERE name_prefix18 IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_oc_companies_home_entity
                ON oc_companies (home_jurisdiction_code, home_company_number)
                WHERE home_company_number IS NOT NULL;

            CREATE TABLE IF NOT EXISTS oc_officers (
                officer_row_id       BIGSERIAL PRIMARY KEY,
                jurisdiction_code    TEXT NOT NULL,
                company_number       TEXT NOT NULL,
                officer_id           TEXT,
                name                 TEXT NOT NULL,
                name_norm            TEXT NOT NULL,
                position             TEXT,
                role_class           TEXT NOT NULL DEFAULT 'other',
                is_corporate_officer BOOLEAN DEFAULT FALSE,
                start_date           DATE,
                end_date             DATE,
                is_current           BOOLEAN DEFAULT TRUE,
                address_raw          TEXT,
                address_coarse       TEXT,
                occupation           TEXT,
                nationality          TEXT,
                created_at           TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_oc_officers_company
                ON oc_officers (jurisdiction_code, company_number);
            CREATE INDEX IF NOT EXISTS idx_oc_officers_beneficial_name
                ON oc_officers (name_norm)
                WHERE role_class IN ('beneficial_principal', 'corporate_parent');

            CREATE TABLE IF NOT EXISTS oc_previous_names (
                id                BIGSERIAL PRIMARY KEY,
                jurisdiction_code TEXT NOT NULL,
                company_number    TEXT NOT NULL,
                company_name      TEXT NOT NULL,
                company_name_norm TEXT NOT NULL,
                name_type         TEXT DEFAULT 'previous',
                start_date        DATE,
                end_date          DATE
            );

            ALTER TABLE oc_previous_names ADD COLUMN IF NOT EXISTS name_type TEXT DEFAULT 'previous';

            CREATE INDEX IF NOT EXISTS idx_oc_prev_names_jur_norm
                ON oc_previous_names (jurisdiction_code, company_name_norm);
            CREATE INDEX IF NOT EXISTS idx_oc_prev_names_company
                ON oc_previous_names (jurisdiction_code, company_number);

            CREATE TABLE IF NOT EXISTS oc_branches (
                branch_jurisdiction_code TEXT NOT NULL,
                branch_company_number    TEXT NOT NULL,
                home_jurisdiction_code   TEXT NOT NULL,
                home_company_number      TEXT NOT NULL,
                relationship_type        TEXT DEFAULT 'foreign_registration',
                PRIMARY KEY (
                    branch_jurisdiction_code,
                    branch_company_number,
                    home_jurisdiction_code,
                    home_company_number
                )
            );

            CREATE INDEX IF NOT EXISTS idx_oc_branches_home
                ON oc_branches (home_jurisdiction_code, home_company_number);

            CREATE TABLE IF NOT EXISTS oc_registered_agent_addresses (
                address_coarse         TEXT PRIMARY KEY,
                sample_raw_address     TEXT,
                distinct_company_count INTEGER NOT NULL DEFAULT 0,
                distinct_agent_names   TEXT[],
                source                 TEXT DEFAULT 'empirical_oc_threshold',
                computed_at            TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
    conn.commit()


def ensure_nationwide_parent_tables(conn) -> None:
    """Idempotently create LIST-partitioned parent tables (`us_*`) keyed by `jurisdiction`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS us_properties (
                jurisdiction           TEXT NOT NULL,
                bbl                    TEXT NOT NULL,
                address                TEXT,
                borough                TEXT,
                zip_code               TEXT,
                owner_name             TEXT,
                owner_name_norm        TEXT,
                owner_co_name          TEXT,
                owner_is_truncated     BOOLEAN DEFAULT FALSE,
                mailing_address        TEXT,
                mailing_address_norm   TEXT,
                mailing_address_coarse TEXT,
                mailing_state          TEXT,
                owner_email            TEXT,
                owner_email_norm       TEXT,
                land_use               TEXT,
                bld_class              TEXT,
                num_floors             NUMERIC,
                units_res              NUMERIC,
                units_total            NUMERIC,
                year_built             INTEGER,
                assessed_total         NUMERIC,
                last_sale_date         DATE,
                last_sale_price        NUMERIC,
                latitude               NUMERIC,
                longitude              NUMERIC,
                compliance_active      BOOLEAN DEFAULT FALSE,
                compliance_record_id   TEXT,
                compliance_expiration  DATE,
                oc_jurisdiction_code   TEXT,
                oc_company_number      TEXT,
                oc_match_tier          SMALLINT,
                oc_match_confidence    NUMERIC(4,3),
                cama_site_link         TEXT,
                building_photo_url     TEXT,
                cama_last_scraped_at   TIMESTAMPTZ,
                source_fy              INTEGER,
                source_system          TEXT,
                created_at             TIMESTAMP DEFAULT NOW(),
                updated_at             TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (jurisdiction, bbl)
            ) PARTITION BY LIST (jurisdiction);

            ALTER TABLE us_properties ADD COLUMN IF NOT EXISTS cama_site_link TEXT;
            ALTER TABLE us_properties ADD COLUMN IF NOT EXISTS building_photo_url TEXT;
            ALTER TABLE us_properties ADD COLUMN IF NOT EXISTS cama_last_scraped_at TIMESTAMPTZ;
            ALTER TABLE us_properties ADD COLUMN IF NOT EXISTS source_fy INTEGER;

            CREATE TABLE IF NOT EXISTS us_networks (
                jurisdiction       TEXT NOT NULL,
                id                 BIGSERIAL,
                network_key        TEXT NOT NULL,
                anchor_type        TEXT NOT NULL,
                display_name       TEXT,
                member_names       TEXT[],
                member_addresses   TEXT[],
                registration_ids   TEXT[],
                bbl_list           TEXT[],
                building_count     INTEGER DEFAULT 0,
                unit_count         INTEGER DEFAULT 0,
                borough_summary    JSONB,
                connection_signals TEXT DEFAULT '{}',
                oc_company_ids     TEXT[],
                created_at         TIMESTAMP DEFAULT NOW(),
                updated_at         TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (jurisdiction, network_key)
            ) PARTITION BY LIST (jurisdiction);

            CREATE TABLE IF NOT EXISTS us_hpd_registrations (
                jurisdiction           TEXT NOT NULL,
                registration_id        TEXT NOT NULL,
                bbl                    TEXT,
                bin                    TEXT,
                building_address       TEXT,
                building_city          TEXT,
                building_zip           TEXT,
                borough                TEXT,
                lifecycle_stage        TEXT,
                last_registration_date DATE,
                registration_end_date  DATE,
                created_at             TIMESTAMP DEFAULT NOW(),
                updated_at             TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (jurisdiction, registration_id)
            ) PARTITION BY LIST (jurisdiction);

            CREATE TABLE IF NOT EXISTS us_hpd_contacts (
                jurisdiction          TEXT NOT NULL,
                contact_id            BIGSERIAL,
                registration_id       TEXT,
                contact_type          TEXT,
                corporation_name      TEXT,
                corporation_name_norm TEXT,
                first_name            TEXT,
                last_name             TEXT,
                full_name             TEXT,
                full_name_norm        TEXT,
                business_address      TEXT,
                business_city         TEXT,
                business_state        TEXT,
                business_zip          TEXT,
                source_tag            TEXT,
                created_at            TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (jurisdiction, contact_id)
            ) PARTITION BY LIST (jurisdiction);

            ALTER TABLE us_hpd_contacts ADD COLUMN IF NOT EXISTS source_tag TEXT;

            CREATE TABLE IF NOT EXISTS us_bbl_stats (
                jurisdiction           TEXT NOT NULL,
                bbl                    TEXT NOT NULL,
                violations_total       INTEGER DEFAULT 0,
                violations_open        INTEGER DEFAULT 0,
                violations_class_c     INTEGER DEFAULT 0,
                violations_class_b     INTEGER DEFAULT 0,
                violations_class_a     INTEGER DEFAULT 0,
                violations_open_c      INTEGER DEFAULT 0,
                violations_last_365d   INTEGER DEFAULT 0,
                last_violation_date    DATE,
                litigations_total      INTEGER DEFAULT 0,
                litigations_open       INTEGER DEFAULT 0,
                litigations_harassment INTEGER DEFAULT 0,
                last_litigation_date   DATE,
                evictions_total        INTEGER DEFAULT 0,
                last_eviction_date     DATE,
                updated_at             TIMESTAMPTZ DEFAULT NOW(),
                is_rent_stabilized     BOOLEAN DEFAULT FALSE,
                rs_units               INTEGER DEFAULT 0,
                nhpd_subsidy           BOOLEAN DEFAULT FALSE,
                nhpd_program           TEXT,
                nhpd_expiration        DATE,
                PRIMARY KEY (jurisdiction, bbl)
            ) PARTITION BY LIST (jurisdiction);

            ALTER TABLE us_bbl_stats ADD COLUMN IF NOT EXISTS violations_last_365d INTEGER DEFAULT 0;
            """
        )
    conn.commit()


def ensure_jurisdiction_partition(conn, jurisdiction: str) -> None:
    """Create `{jur}_*` child partitions of `us_*` with local unique & search indexes."""
    jur = validate_jurisdiction_slug(jurisdiction)
    ensure_opencorporates_tables(conn)
    ensure_nationwide_parent_tables(conn)

    with conn.cursor() as cur:
        # Check if `{jur}_properties` already exists as a standalone legacy table vs partition
        cur.execute(
            """
            SELECT c.relispartition, inh.inhparent::regclass::text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid
            WHERE n.nspname = 'public' AND c.relname = %s
            """,
            (f"{jur}_properties",),
        )
        existing = cur.fetchone()
        if existing is not None and not existing[0]:
            raise RuntimeError(
                f"Table {jur}_properties already exists as a standalone non-partitioned table; "
                "refusing to overwrite legacy table without explicit migration."
            )

        cur.execute(
            f"""
            CREATE EXTENSION IF NOT EXISTS pg_trgm;

            CREATE TABLE IF NOT EXISTS {jur}_properties
                PARTITION OF us_properties FOR VALUES IN ('{jur}');
            ALTER TABLE {jur}_properties ALTER COLUMN jurisdiction SET DEFAULT '{jur}';
            CREATE UNIQUE INDEX IF NOT EXISTS {jur}_properties_bbl_uidx
                ON {jur}_properties (bbl);
            CREATE INDEX IF NOT EXISTS {jur}_properties_owner_norm_idx
                ON {jur}_properties (owner_name_norm);
            CREATE INDEX IF NOT EXISTS {jur}_properties_borough_idx
                ON {jur}_properties (borough);
            CREATE INDEX IF NOT EXISTS {jur}_properties_addr_trgm_idx
                ON {jur}_properties USING GIN (address gin_trgm_ops);
            CREATE INDEX IF NOT EXISTS {jur}_properties_owner_trgm_idx
                ON {jur}_properties USING GIN (owner_name gin_trgm_ops);
            CREATE INDEX IF NOT EXISTS {jur}_properties_oc_entity_idx
                ON {jur}_properties (oc_jurisdiction_code, oc_company_number)
                WHERE oc_company_number IS NOT NULL;

            CREATE TABLE IF NOT EXISTS {jur}_networks
                PARTITION OF us_networks FOR VALUES IN ('{jur}');
            ALTER TABLE {jur}_networks ALTER COLUMN jurisdiction SET DEFAULT '{jur}';
            CREATE UNIQUE INDEX IF NOT EXISTS {jur}_networks_key_uidx
                ON {jur}_networks (network_key);
            CREATE INDEX IF NOT EXISTS {jur}_networks_bldg_count_idx
                ON {jur}_networks (building_count DESC);
            CREATE INDEX IF NOT EXISTS {jur}_networks_unit_count_idx
                ON {jur}_networks (unit_count DESC);
            CREATE INDEX IF NOT EXISTS {jur}_networks_display_trgm_idx
                ON {jur}_networks USING GIN (display_name gin_trgm_ops);
            CREATE INDEX IF NOT EXISTS {jur}_networks_bbl_gin_idx
                ON {jur}_networks USING GIN (bbl_list);

            CREATE TABLE IF NOT EXISTS {jur}_hpd_registrations
                PARTITION OF us_hpd_registrations FOR VALUES IN ('{jur}');
            ALTER TABLE {jur}_hpd_registrations ALTER COLUMN jurisdiction SET DEFAULT '{jur}';
            CREATE UNIQUE INDEX IF NOT EXISTS {jur}_hpd_registrations_id_uidx
                ON {jur}_hpd_registrations (registration_id);
            CREATE INDEX IF NOT EXISTS {jur}_hpd_registrations_bbl_idx
                ON {jur}_hpd_registrations (bbl);

            CREATE TABLE IF NOT EXISTS {jur}_hpd_contacts
                PARTITION OF us_hpd_contacts FOR VALUES IN ('{jur}');
            ALTER TABLE {jur}_hpd_contacts ALTER COLUMN jurisdiction SET DEFAULT '{jur}';
            CREATE INDEX IF NOT EXISTS {jur}_hpd_contacts_reg_id_idx
                ON {jur}_hpd_contacts (registration_id);
            CREATE INDEX IF NOT EXISTS {jur}_hpd_contacts_full_name_norm_idx
                ON {jur}_hpd_contacts (full_name_norm);

            CREATE TABLE IF NOT EXISTS {jur}_bbl_stats
                PARTITION OF us_bbl_stats FOR VALUES IN ('{jur}');
            ALTER TABLE {jur}_bbl_stats ALTER COLUMN jurisdiction SET DEFAULT '{jur}';
            CREATE UNIQUE INDEX IF NOT EXISTS {jur}_bbl_stats_bbl_uidx
                ON {jur}_bbl_stats (bbl);
            """
        )
    conn.commit()


def bootstrap_nationwide_schema(conn, jurisdictions: Iterable[str] = ()) -> None:
    """Bootstrap global `oc_*`, parent `us_*`, and any requested child partitions."""
    ensure_opencorporates_tables(conn)
    ensure_nationwide_parent_tables(conn)
    for jur in jurisdictions:
        ensure_jurisdiction_partition(conn, jur)
