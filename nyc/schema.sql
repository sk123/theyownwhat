-- =============================================================================
-- NYC HPD Module Schema
-- Isolated tables for NYC data — does NOT touch CT tables (properties,
-- businesses, principals, evictions, entity_networks).
--
-- Run once:  psql $DATABASE_URL -f nyc/schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- nyc_hpd_registrations
-- One row per HPD-registered building.  Links buildings to contacts via
-- registration_id, and to PLUTO parcels via BBL.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nyc_hpd_registrations (
    registration_id         TEXT PRIMARY KEY,
    bbl                     TEXT,           -- Borough-Block-Lot (joins to nyc_properties)
    bin                     TEXT,           -- Building Identification Number
    building_address        TEXT,
    building_city           TEXT,
    building_zip            TEXT,
    borough                 TEXT,           -- MANHATTAN, BROOKLYN, BRONX, QUEENS, STATEN ISLAND
    lifecycle_stage         TEXT,
    last_registration_date  DATE,
    registration_end_date   DATE,
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- nyc_hpd_contacts
-- Officers/officers/agents per registration.  This is the principals
-- substitute: HeadOfficer and IndividualOwner rows carry real person names.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nyc_hpd_contacts (
    contact_id              BIGSERIAL PRIMARY KEY,
    registration_id         TEXT,               -- links to nyc_hpd_registrations (no FK — independently paged)
    contact_type            TEXT,           -- HeadOfficer | IndividualOwner | CorporateOwner | Agent | Officer
    corporation_name        TEXT,
    corporation_name_norm   TEXT,           -- normalized for graph matching
    first_name              TEXT,
    last_name               TEXT,
    full_name               TEXT,           -- "LASTNAME FIRSTNAME" (person) or corp name
    full_name_norm          TEXT,           -- normalized for graph matching
    business_address        TEXT,
    business_city           TEXT,
    business_state          TEXT,
    business_zip            TEXT,
    created_at              TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- nyc_properties
-- Residential PLUTO parcels (land_use 01-04).
-- Keyed by BBL — joins to nyc_hpd_registrations.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nyc_properties (
    bbl                     TEXT PRIMARY KEY,
    address                 TEXT,
    borough                 TEXT,
    zip_code                TEXT,
    owner_name              TEXT,
    owner_name_norm         TEXT,           -- normalized for graph matching
    land_use                TEXT,           -- 01=1-2fam, 02=multifam walkup, 03=multifam elevator, 04=mixed
    bld_class               TEXT,           -- building class code (e.g. D4, C6)
    num_floors              NUMERIC,
    units_res               NUMERIC,
    units_total             NUMERIC,
    year_built              INTEGER,
    assessed_total          NUMERIC,
    latitude                NUMERIC,
    longitude               NUMERIC,
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- nyc_networks
-- Ownership clusters built by build_nyc_networks.py using Union-Find over:
--   1. Shared normalized person name (HeadOfficer / IndividualOwner)
--   2. Shared mailing address (business_address + zip) across contacts
--   3. Shared corporation_name_norm across contacts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nyc_networks (
    id                      BIGSERIAL PRIMARY KEY,
    network_key             TEXT UNIQUE NOT NULL,   -- canonical anchor string
    anchor_type             TEXT NOT NULL,          -- 'person' | 'corp' | 'address'
    display_name            TEXT,
    member_names            TEXT[],                 -- all norm names in cluster
    member_addresses        TEXT[],                 -- all shared mailing addresses
    registration_ids        TEXT[],                 -- HPD registration IDs
    bbl_list                TEXT[],                 -- PLUTO BBLs in network
    building_count          INTEGER DEFAULT 0,
    unit_count              INTEGER DEFAULT 0,
    borough_summary         JSONB,                  -- {"BROOKLYN": 12, "BRONX": 5}
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_nyc_reg_bbl
    ON nyc_hpd_registrations(bbl);

CREATE INDEX IF NOT EXISTS idx_nyc_contacts_reg_id
    ON nyc_hpd_contacts(registration_id);

CREATE INDEX IF NOT EXISTS idx_nyc_contacts_name_norm
    ON nyc_hpd_contacts(full_name_norm);

CREATE INDEX IF NOT EXISTS idx_nyc_contacts_corp_norm
    ON nyc_hpd_contacts(corporation_name_norm);

CREATE INDEX IF NOT EXISTS idx_nyc_contacts_type
    ON nyc_hpd_contacts(contact_type);

CREATE INDEX IF NOT EXISTS idx_nyc_contacts_addr
    ON nyc_hpd_contacts(business_address, business_zip);

CREATE INDEX IF NOT EXISTS idx_nyc_props_owner_norm
    ON nyc_properties(owner_name_norm);

CREATE INDEX IF NOT EXISTS idx_nyc_props_borough
    ON nyc_properties(borough);

CREATE INDEX IF NOT EXISTS idx_nyc_networks_key
    ON nyc_networks(network_key);

CREATE INDEX IF NOT EXISTS idx_nyc_networks_anchor
    ON nyc_networks(anchor_type);

-- ---------------------------------------------------------------------------
-- nyc_bbl_stats
-- Pre-aggregated enrichment per BBL, populated by nyc/enrich_hpd.py.
-- Sources:
--   HPD Violations   (wvxf-dwi5) — housing maintenance code violations
--   HPD Litigations  (59kj-x8nc) — court cases filed by HPD
--   DOI Evictions    (6z8x-wfk4) — marshal-executed evictions since 2017
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nyc_bbl_stats (
    bbl                     TEXT PRIMARY KEY,

    -- Violations (Housing Maintenance Code)
    violations_total        INTEGER DEFAULT 0,
    violations_open         INTEGER DEFAULT 0,
    violations_class_c      INTEGER DEFAULT 0,   -- immediately hazardous
    violations_class_b      INTEGER DEFAULT 0,   -- hazardous
    violations_class_a      INTEGER DEFAULT 0,   -- non-hazardous
    violations_open_c       INTEGER DEFAULT 0,   -- open Class C (most urgent)
    last_violation_date     DATE,

    -- HPD Litigations (housing court cases)
    litigations_total       INTEGER DEFAULT 0,
    litigations_open        INTEGER DEFAULT 0,
    litigations_harassment  INTEGER DEFAULT 0,   -- harassment findings
    last_litigation_date    DATE,

    -- Marshal Evictions (executed)
    evictions_total         INTEGER DEFAULT 0,
    last_eviction_date      DATE,

    -- Meta
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nyc_bbl_stats_violations
    ON nyc_bbl_stats(violations_open_c DESC);

CREATE INDEX IF NOT EXISTS idx_nyc_bbl_stats_evictions
    ON nyc_bbl_stats(evictions_total DESC);
