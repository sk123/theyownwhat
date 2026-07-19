-- =============================================================================
-- New Jersey BHI Module Schema
--
-- The app's city explorer expects {city}_properties, {city}_networks,
-- {city}_hpd_registrations, {city}_hpd_contacts, and {city}_bbl_stats.
-- For NJ those tables are backed by the public DCA Bureau of Housing Inspection
-- OPRA active-building report, not by statewide parcel owner names.
-- =============================================================================

CREATE TABLE IF NOT EXISTS nj_properties (
    bbl                     TEXT PRIMARY KEY,
    address                 TEXT,
    borough                 TEXT,
    zip_code                TEXT,
    owner_name              TEXT,
    owner_name_norm         TEXT,
    mailing_address         TEXT,
    mailing_address_norm    TEXT,
    owner_email             TEXT,
    owner_email_norm        TEXT,
    land_use                TEXT,
    bld_class               TEXT,
    num_floors              NUMERIC,
    units_res               NUMERIC,
    units_total             NUMERIC,
    year_built              INTEGER,
    assessed_total          NUMERIC,
    latitude                NUMERIC,
    longitude               NUMERIC,
    compliance_active       BOOLEAN DEFAULT FALSE,
    compliance_record_id    TEXT,
    compliance_expiration   DATE,

    -- NJ BHI-specific provenance and registration fields.
    bhi_registration_no     TEXT,
    property_interest_id    TEXT,
    property_interest_name  TEXT,
    building_id             TEXT,
    building_name           TEXT,
    block_no                TEXT,
    lot_no                  TEXT,
    municipality            TEXT,
    county                  TEXT,
    ownership_type          TEXT,
    authorized_agent_name   TEXT,
    authorized_agent_address TEXT,
    authorized_agent_phone  TEXT,
    authorized_agent_email  TEXT,
    primary_owner_address   TEXT,
    primary_owner_phone     TEXT,
    last_cyclical_inspection DATE,
    source_url              TEXT,
    contact_redacted        BOOLEAN,
    raw                     JSONB,

    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nj_hpd_registrations (
    registration_id         TEXT PRIMARY KEY,
    bbl                     TEXT,
    bin                     TEXT,
    building_address        TEXT,
    building_city           TEXT,
    building_zip            TEXT,
    borough                 TEXT,
    lifecycle_stage         TEXT,
    last_registration_date  DATE,
    registration_end_date   DATE,

    bhi_registration_no     TEXT,
    property_interest_id    TEXT,
    property_interest_name  TEXT,
    source_url              TEXT,

    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nj_hpd_contacts (
    contact_id              BIGSERIAL PRIMARY KEY,
    registration_id         TEXT,
    contact_type            TEXT,
    corporation_name        TEXT,
    corporation_name_norm   TEXT,
    first_name              TEXT,
    last_name               TEXT,
    full_name               TEXT,
    full_name_norm          TEXT,
    business_address        TEXT,
    business_city           TEXT,
    business_state          TEXT,
    business_zip            TEXT,

    source_role             TEXT,
    phone                   TEXT,
    email                   TEXT,
    redacted                BOOLEAN DEFAULT FALSE,

    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nj_networks (
    id                      BIGSERIAL PRIMARY KEY,
    network_key             TEXT UNIQUE NOT NULL,
    anchor_type             TEXT NOT NULL,
    display_name            TEXT,
    member_names            TEXT[],
    member_addresses        TEXT[],
    registration_ids        TEXT[],
    bbl_list                TEXT[],
    building_count          INTEGER DEFAULT 0,
    unit_count              INTEGER DEFAULT 0,
    borough_summary         JSONB,
    connection_signals      TEXT DEFAULT '{}',
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nj_bbl_stats (
    bbl                     TEXT PRIMARY KEY,
    violations_total        INTEGER DEFAULT 0,
    violations_open         INTEGER DEFAULT 0,
    violations_class_c      INTEGER DEFAULT 0,
    violations_class_b      INTEGER DEFAULT 0,
    violations_class_a      INTEGER DEFAULT 0,
    violations_open_c       INTEGER DEFAULT 0,
    last_violation_date     DATE,
    litigations_total       INTEGER DEFAULT 0,
    litigations_open        INTEGER DEFAULT 0,
    litigations_harassment  INTEGER DEFAULT 0,
    last_litigation_date    DATE,
    evictions_total         INTEGER DEFAULT 0,
    last_eviction_date      DATE,
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    is_rent_stabilized      BOOLEAN DEFAULT FALSE,
    rs_units                INTEGER DEFAULT 0,
    nhpd_subsidy            BOOLEAN DEFAULT FALSE,
    nhpd_program            TEXT,
    nhpd_expiration         DATE
);

CREATE INDEX IF NOT EXISTS idx_nj_props_owner_norm
    ON nj_properties(owner_name_norm);

CREATE INDEX IF NOT EXISTS idx_nj_props_registration
    ON nj_properties(bhi_registration_no);

CREATE INDEX IF NOT EXISTS idx_nj_props_county_muni
    ON nj_properties(county, municipality);

CREATE INDEX IF NOT EXISTS idx_nj_props_block_lot
    ON nj_properties(county, municipality, block_no, lot_no);

CREATE INDEX IF NOT EXISTS idx_nj_contacts_reg_id
    ON nj_hpd_contacts(registration_id);

CREATE INDEX IF NOT EXISTS idx_nj_contacts_name_norm
    ON nj_hpd_contacts(full_name_norm);

CREATE INDEX IF NOT EXISTS idx_nj_contacts_corp_norm
    ON nj_hpd_contacts(corporation_name_norm);

CREATE INDEX IF NOT EXISTS idx_nj_networks_key
    ON nj_networks(network_key);

