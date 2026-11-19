#!/usr/bin/env python3
"""
gcp/bq_ct_treatment.py
======================
Runs the full CT-standard Principal Deduplication + Dispersed-Hub Quarantine +
Multi-Hop Corporate Graph Closure directly in Google Cloud BigQuery against the
61-jurisdiction OpenCorporates bulk mirror (`theyownwhat:opencorporates_20261117`).

Governing principles (`docs/ASSURANCE_POLICY.md`):
  - Epistemic honesty & strict jurisdiction-variance awareness: every state has
    an explicit BigQuery-audited `JurisdictionProfile` encoding officer-disclosure
    tier, state-specific agent/incorporator role exclusions, null-position safety,
    and HQ-address fallback requirements.
  - Zero false mega-merges on common surnames: computes per-jurisdiction empirical
    dispersion (`distinct_non_ra_zips`, `distinct_non_ra_addrs`, `top_addr_share`)
    so homonymous names (`DAVID KIM`, `JOHN SMITH`, `MARIA GARCIA`) NEVER merge
    by name alone, while corroborated middle-initial/name variants at a shared
    company or non-RA HQ (`DON R HANKEY`, `DON R. HANKEY`, `DON RUFUS HANKEY`,
    `DON ROBERT HANKEY`, `DON HANKEY`) unify cleanly into one canonical principal.
  - All derived tables live in `theyownwhat:opencorporates_20261117`, which enforces
    a 21-day table TTL (auto-expiring Dec. 8, 2026 ahead of credit expiry).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Sequence, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXPORT_DIR = os.path.join(REPO_ROOT, "data", "opencorporates", "bq_ct_exports")
GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"

DEFAULT_PROJECT = "theyownwhat"
DEFAULT_BUCKET = "theyownwhat-oc-staging-20261117"
DEFAULT_DATASET = "opencorporates_20261117"


@dataclass(frozen=True)
class JurisdictionProfile:
    oc_jur: str
    app_jur: str
    tier: int  # 1 = rich officers + addrs, 2 = rich officers / sparse addrs, 3 = agent-only in OC
    beneficial_officers_available: bool
    allow_null_position: bool
    # Optional allowlist regex (strictest mode, e.g. us_ny CEO-only, us_il manager/member, us_mi corp officers)
    allow_positions_regex: Optional[str] = None
    # Additional state-specific exclusions beyond universal agent/incorporator/organizer/attorney patterns
    extra_exclude_positions_regex: Optional[str] = None
    notes: str = ""


# BigQuery-audited profiles across all 20 active jurisdictions (verified 2026-11-19)
JURISDICTION_PROFILES: Dict[str, JurisdictionProfile] = {
    # --- Tier 1: Rich beneficial principals + high officer-address coverage ---
    "us_ca": JurisdictionProfile(
        oc_jur="us_ca", app_jur="ca", tier=1, beneficial_officers_available=True,
        allow_null_position=True,
        extra_exclude_positions_regex=r"^(agent,?|applicant|other|none specified)$",
        notes="31.96M rows (98.7% addr); 2.12M LLC-12 NULL-position officers kept after agent-match filtering; trailing comma on 'agent,' excluded.",
    ),
    "us_fl": JurisdictionProfile(
        oc_jur="us_fl", app_jur="fl", tier=1, beneficial_officers_available=True,
        allow_null_position=True,
        extra_exclude_positions_regex=r"^(ra|agent|registered agent)$",
        notes="44.79M rows (99.99% addr); untitled NULL rows duplicating company RA name/address excluded.",
    ),
    "us_ma": JurisdictionProfile(
        oc_jur="us_ma", app_jur="ma", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(resident agent|registered agent|clerk of|signatory only)\b",
        notes="7.22M rows (67% addr); company HQ fallback fills remaining 33%.",
    ),
    "us_nc": JurisdictionProfile(
        oc_jur="us_nc", app_jur="nc", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(registered agent|incorporator|organizer)\b",
        notes="5.68M rows (94% addr); rich multi-title positions.",
    ),
    "us_il": JurisdictionProfile(
        oc_jur="us_il", app_jur="il", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        allow_positions_regex=r"^(manager|member|managing member|general partner|partner|president|secretary|treasurer|director|officer)$",
        notes="5.57M rows (100% addr); strictly keeps manager/member/officers and drops 3.45M agents + incorporators.",
    ),
    "us_mt": JurisdictionProfile(
        oc_jur="us_mt", app_jur="mt", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(subscriber|organizer|incorporator)\b",
        notes="2.43M rows (99.6% addr); excludes subscriber/organizer.",
    ),
    "us_vt": JurisdictionProfile(
        oc_jur="us_vt", app_jur="vt", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"^alternate name for\b",
        notes="965K rows (99.3% addr); excludes 'alternate name for ...' free-text noise.",
    ),
    "us_dc": JurisdictionProfile(
        oc_jur="us_dc", app_jur="dc", tier=1, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"^(contact|organizer|incorporator)$",
        notes="975K rows (100% addr); keeps CEO/director/governor/executingofficer, drops contact/organizer.",
    ),
    # --- Tier 2: Rich beneficial principals, sparse officer addresses (mandatory company-HQ join) ---
    "us_pa": JurisdictionProfile(
        oc_jur="us_pa", app_jur="philadelphia", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(incorporator|organizer|interested individual)\b",
        notes="4.96M rows (36% addr); mandatory company-HQ join + excludes incorporator/organizer/interested individual.",
    ),
    "us_mn": JurisdictionProfile(
        oc_jur="us_mn", app_jur="mn", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(individual contact for agent|individual agent|organizer|incorporator)\b",
        notes="2.81M rows (70% addr); excludes agent contact variants + organizer/incorporator.",
    ),
    "us_ar": JurisdictionProfile(
        oc_jur="us_ar", app_jur="ar", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(incorporator|organizer|designated ar representative)\b",
        notes="2.69M rows (29% addr); beneficial rows lack officer addr -> mandatory company HQ corroboration.",
    ),
    "us_va": JurisdictionProfile(
        oc_jur="us_va", app_jur="va", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        extra_exclude_positions_regex=r"\b(incorporator|organizer|registered agent)\b",
        notes="3.47M rows (61% addr); 15-char truncated officer titles.",
    ),
    "us_ct": JurisdictionProfile(
        oc_jur="us_ct", app_jur="ct", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        notes="3.06M rows (36% addr); benchmark state already running local unique_principals.",
    ),
    "us_ny": JurisdictionProfile(
        oc_jur="us_ny", app_jur="ny", tier=2, beneficial_officers_available=True,
        allow_null_position=False,
        allow_positions_regex=r"^(ceo|chief executive officer|president)$",
        notes="7.31M rows; ONLY DOS corporation CEOs disclosed (0% LLC members); drops 5.78M dos process agents & principal executive office rows.",
    ),
    # --- Tier 3: Agent/incorporator-only in OC (NEVER promote agents as principals!) ---
    "us_mi": JurisdictionProfile(
        oc_jur="us_mi", app_jur="detroit", tier=3, beneficial_officers_available=True,
        allow_null_position=False,
        allow_positions_regex=r"^(president|secretary|treasurer|director|general partner|vice president|ceo|manager|member)$",
        notes="3.40M rows are 98.6% agents; ONLY ~46K explicit corp-officer rows eligible; HQ + cross-state branch piercing carries main load.",
    ),
    "us_oh": JurisdictionProfile(
        oc_jur="us_oh", app_jur="oh", tier=3, beneficial_officers_available=False,
        allow_null_position=False,
        notes="7.30M rows are 100% agent/incorporator -> ZERO principals promoted; HQ + cross-state branch piercing only.",
    ),
    "us_co": JurisdictionProfile(
        oc_jur="us_co", app_jur="co", tier=3, beneficial_officers_available=False,
        allow_null_position=False,
        notes="3.02M rows are 100% agent -> ZERO principals promoted; HQ + cross-state branch piercing only.",
    ),
    "us_md": JurisdictionProfile(
        oc_jur="us_md", app_jur="baltimore", tier=3, beneficial_officers_available=False,
        allow_null_position=False,
        notes="2.15M rows are 100% agent -> ZERO principals promoted; HQ + cross-state branch piercing only.",
    ),
    "us_wi": JurisdictionProfile(
        oc_jur="us_wi", app_jur="wi", tier=3, beneficial_officers_available=False,
        allow_null_position=False,
        notes="1.54M rows are 100% agent -> ZERO principals promoted; HQ + cross-state branch piercing only.",
    ),
    "us_nj": JurisdictionProfile(
        oc_jur="us_nj", app_jur="nj", tier=3, beneficial_officers_available=False,
        allow_null_position=False,
        notes="0 officer rows in OC bulk -> HQ + cross-state branch piercing only.",
    ),
}

# Broad multi-demographic common surnames guarded even at 2+ non-RA addresses
COMMON_SURNAMES_SQL = (
    "'SMITH','JOHNSON','WILLIAMS','BROWN','JONES','GARCIA','MILLER','DAVIS',"
    "'RODRIGUEZ','MARTINEZ','HERNANDEZ','LOPEZ','GONZALEZ','WILSON','ANDERSON',"
    "'THOMAS','TAYLOR','MOORE','JACKSON','MARTIN','LEE','PEREZ','THOMPSON',"
    "'WHITE','HARRIS','SANCHEZ','CLARK','RAMIREZ','LEWIS','ROBINSON','WALKER',"
    "'YOUNG','ALLEN','KING','WRIGHT','SCOTT','TORRES','NGUYEN','HILL','FLORES',"
    "'GREEN','ADAMS','NELSON','BAKER','HALL','RIVERA','CAMPBELL','MITCHELL',"
    "'CARTER','ROBERTS','KIM','CHEN','WANG','LI','LIU','ZHANG','YANG','WU',"
    "'LIN','HUANG','ZHOU','TRAN','PHAM','LE','PARK','CHANG','CHOI','JUNG',"
    "'PATEL','SHAH','SINGH','KUMAR','GUPTA','COHEN','LEVY','GOLDSTEIN',"
    "'SCHWARTZ','FRIEDMAN','KATZ','OLSON','LARSON','PETERSON','CARLSON',"
    "'MURPHY','KELLY','SULLIVAN','WALSH','RYAN','O','DIAZ','CRUZ','REYES',"
    "'MORALES','GUTIERREZ','ORTIZ','CHAVEZ','RUIZ','MENDOZA','ALVAREZ','CASTILLO'"
)

# SQL expression converting a raw address string into suite-aware `NUM STREET[#SUITE]|ZIP5`
# (preserves suite/unit numbers so multi-tenant high-rises like 4751 Wilshire Blvd Ste 110
# stay distinct from unrelated tenants in Ste 400 of the same tower!)
def _sql_suite_addr(expr: str) -> str:
    return f"""(
      CASE
        WHEN {expr} IS NULL OR LENGTH(TRIM({expr})) < 8 THEN NULL
        WHEN REGEXP_CONTAINS(UPPER({expr}), r'\\b(P\\.?\\s*O\\.?\\s*BOX|POST\\s+OFFICE\\s+BOX|UNDELIVERABLE|NO\\s+ADDRESS|UNKNOWN|99999|REGISTERED\\s+AGENT|CORPORATION\\s+SERVICE|CT\\s+CORPORATION|C\\s*T\\s+CORP|LEGALZOOM|COGENCY|INCORP\\s+SERVICES|PARACORP|NORTHWEST\\s+REGISTERED|CAPITOL\\s+SERVICES|UNITED\\s+STATES\\s+CORP|CORPORATE\\s+CREATIONS|RESIDENT\\s+AGENTS|NATIONAL\\s+REGISTERED)\\b') THEN NULL
        ELSE (
          SELECT
            CASE
              WHEN num IS NOT NULL AND st IS NOT NULL AND zip5 IS NOT NULL AND num != '0' AND LENGTH(st) >= 2
              THEN CONCAT(
                LTRIM(num, '0'), ' ', st,
                IF(ste IS NOT NULL AND ste != '', CONCAT(' #', ste), ''),
                '|', zip5
              )
              ELSE NULL
            END
          FROM (
            SELECT
              REGEXP_EXTRACT(cleaned, r'^(\\d{{1,6}})\\s+(?:(?:NORTH|SOUTH|EAST|WEST|[NESW])\\s+)?(?:[A-Z0-9]{{2,}})') AS num,
              REGEXP_EXTRACT(cleaned, r'^\\d{{1,6}}\\s+(?:(?:NORTH|SOUTH|EAST|WEST|[NESW])\\s+)?([A-Z0-9]{{2,}})') AS st,
              REGEXP_EXTRACT(cleaned, r'\\b(?:SUITE|STE|UNIT|APT|RM|ROOM|FL|FLOOR|BLDG)\\s+([0-9][A-Z0-9]{{0,5}}|[A-Z][0-9]{{1,5}}|[A-Z]{{1,2}})\\b') AS ste,
              REGEXP_EXTRACT(UPPER({expr}), r'\\b(\\d{{5}})(?:-\\d{{4}})?\\b(?:[^\\d]*)$') AS zip5
            FROM (
              SELECT REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    UPPER(TRIM({expr})),
                    r'^(?:C/O|C\\\\O|ATTN\\.?:?)\\s+[^,0-9]{{1,55}},?\\s*', ''
                  ),
                  r'[^A-Z0-9 ]+', ' '
                ),
                r'\\s+', ' '
              ) AS cleaned
            )
          )
        )
      END
    )"""


# SQL expression converting a raw address string into building-level coarse `NUM STREET|ZIP5`
def _sql_coarse_addr(expr: str) -> str:
    return f"""(
      CASE
        WHEN {expr} IS NULL OR LENGTH(TRIM({expr})) < 8 THEN NULL
        WHEN REGEXP_CONTAINS(UPPER({expr}), r'\\b(P\\.?\\s*O\\.?\\s*BOX|POST\\s+OFFICE\\s+BOX|UNDELIVERABLE|NO\\s+ADDRESS|UNKNOWN|99999|REGISTERED\\s+AGENT|CORPORATION\\s+SERVICE|CT\\s+CORPORATION|C\\s*T\\s+CORP|LEGALZOOM|COGENCY|INCORP\\s+SERVICES|PARACORP|NORTHWEST\\s+REGISTERED|CAPITOL\\s+SERVICES|UNITED\\s+STATES\\s+CORP|CORPORATE\\s+CREATIONS|RESIDENT\\s+AGENTS|NATIONAL\\s+REGISTERED)\\b') THEN NULL
        ELSE (
          SELECT
            CASE
              WHEN num IS NOT NULL AND st IS NOT NULL AND zip5 IS NOT NULL AND num != '0' AND LENGTH(st) >= 2
              THEN CONCAT(LTRIM(num, '0'), ' ', st, '|', zip5)
              ELSE NULL
            END
          FROM (
            SELECT
              REGEXP_EXTRACT(cleaned, r'^(\\d{{1,6}})\\s+(?:(?:NORTH|SOUTH|EAST|WEST|[NESW])\\s+)?(?:[A-Z0-9]{{2,}})') AS num,
              REGEXP_EXTRACT(cleaned, r'^\\d{{1,6}}\\s+(?:(?:NORTH|SOUTH|EAST|WEST|[NESW])\\s+)?([A-Z0-9]{{2,}})') AS st,
              REGEXP_EXTRACT(UPPER({expr}), r'\\b(\\d{{5}})(?:-\\d{{4}})?\\b(?:[^\\d]*)$') AS zip5
            FROM (
              SELECT REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    UPPER(TRIM({expr})),
                    r'^(?:C/O|C\\\\O|ATTN\\.?:?)\\s+[^,0-9]{{1,55}},?\\s*', ''
                  ),
                  r'[^A-Z0-9 ]+', ' '
                ),
                r'\\s+', ' '
              ) AS cleaned
            )
          )
        )
      END
    )"""


def run_bq_sql(
    sql: str,
    *,
    project: str,
    dry_run: bool = False,
    json_output: bool = False,
    timeout: int = 1800,
) -> str:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", dir=EXPORT_DIR, delete=False, encoding="utf-8"
    ) as tf:
        tf.write(sql)
        sql_path = tf.name
    rel_sql = os.path.relpath(sql_path, os.path.join(REPO_ROOT, "data"))
    try:
        bq_flags = [
            f"--project_id={project}",
            "query",
            "--use_legacy_sql=false",
        ]
        if dry_run:
            bq_flags.append("--dry_run")
        if json_output:
            bq_flags.extend(["--format=prettyjson", "--max_rows=500"])
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
            "-v", f"{os.path.join(REPO_ROOT, 'data')}:/data",
            GCLOUD_IMAGE,
            "bash", "-c",
            f"bq {' '.join(bq_flags)} < /data/{rel_sql}",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if proc.returncode != 0:
            raise RuntimeError(
                f"BigQuery failed (exit {proc.returncode}):\nSTDOUT:\n{proc.stdout[-2000:]}\nSTDERR:\n{proc.stderr[-2000:]}"
            )
        return proc.stdout
    finally:
        try:
            os.unlink(sql_path)
        except OSError:
            pass


def build_ra_coarse_blocklist_sql(project: str, dataset: str) -> str:
    suite_expr_nat = _sql_suite_addr("sample_raw_address")
    suite_expr_dir = _sql_suite_addr("COALESCE(NULLIF(addr_in_full, ''), CONCAT(COALESCE(addr_street_address,''), ' ', COALESCE(addr_locality,''), ' ', COALESCE(addr_region,''), ' ', SUBSTR(COALESCE(addr_postal_code,''),1,5)))")
    return f"""
    -- Suite-aware 61-jurisdiction commercial RA / incorporator / maildrop blocklist.
    -- Only flags an address when it serves >=25 companies with commercial RA names OR >=10 distinct
    -- unrelated agent names (so a single real-estate operator acting as self-agent in their own suite,
    -- like Don Hankey at 4751 Wilshire Blvd Ste 110, is NEVER wrongly classified as a commercial RA!).
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_ra_coarse_blocklist` AS
    WITH from_agents_direct AS (
      SELECT
        {suite_expr_dir} AS addr_coarse,
        COUNT(DISTINCT CONCAT(jurisdiction_code, '/', company_number)) AS total_ra_cos,
        COUNT(DISTINCT UPPER(REGEXP_REPLACE(TRIM(name), r'[^A-Z0-9 ]+', ''))) AS distinct_agents,
        COUNT(DISTINCT jurisdiction_code) AS max_jurs,
        LOGICAL_OR(REGEXP_CONTAINS(UPPER(COALESCE(name, '')), r'\\b(CORPORATION SERVICE|CT CORPORATION|C T CORP|REGISTERED AGENT|LEGALZOOM|COGENCY|INCORP|PARACORP|NORTHWEST REGISTERED|CAPITOL SERVICES|NATIONAL REGISTERED|UNITED STATES CORP|CORPORATE CREATIONS|RESIDENT AGENTS|AGENT SERVICE|BUSINESS FILINGS|HARVARD BUSINESS|ZENBUSINESS|ROCKET LAWYER|MYCORPORATION|SWYFT|BIZEZ|INCFILE|TAILOR BRANDS)\\b')) AS has_commercial_ra_name
      FROM `{project}.{dataset}.officers`
      WHERE REGEXP_CONTAINS(LOWER(TRIM(COALESCE(position, ''))), r'^(agent,?|registered agent|resident agent|statutory agent|incorporator|organizer|dos process agent)$')
        AND COALESCE(end_date, '') = ''
      GROUP BY 1
      HAVING addr_coarse IS NOT NULL
         AND (
           (has_commercial_ra_name AND total_ra_cos >= 10)
           OR (distinct_agents >= 10 AND total_ra_cos >= 25)
           OR (total_ra_cos >= 150 AND distinct_agents >= 3)
         )
    ),
    from_national AS (
      SELECT
        {suite_expr_nat} AS addr_coarse,
        SUM(distinct_company_count) AS total_ra_cos,
        MAX(jurisdiction_count) AS max_jurs
      FROM `{project}.{dataset}.derived_national_ra_hubs`
      WHERE REGEXP_CONTAINS(UPPER(COALESCE(sample_agent_names, '')), r'\\b(CORPORATION SERVICE|CT CORPORATION|C T CORP|REGISTERED AGENT|LEGALZOOM|COGENCY|INCORP|PARACORP|NORTHWEST REGISTERED|CAPITOL SERVICES|NATIONAL REGISTERED|UNITED STATES CORP|CORPORATE CREATIONS|RESIDENT AGENTS|AGENT SERVICE|BUSINESS FILINGS|ZENBUSINESS|INCFILE)\\b')
         OR distinct_company_count >= 150
      GROUP BY 1
    )
    SELECT
      addr_coarse,
      MAX(total_ra_cos) AS total_ra_cos,
      MAX(max_jurs) AS max_jurs
    FROM (
      SELECT addr_coarse, total_ra_cos, max_jurs FROM from_national
      UNION ALL
      SELECT addr_coarse, total_ra_cos, max_jurs FROM from_agents_direct
    )
    WHERE addr_coarse IS NOT NULL
    GROUP BY 1;
    """


def build_jurisdiction_principals_sql(
    profile: JurisdictionProfile,
    *,
    project: str,
    dataset: str,
) -> Tuple[str, str]:
    jur = profile.oc_jur
    if not profile.beneficial_officers_available:
        # Tier-3 agent-only jurisdictions: create clean empty schema tables so downstream joins are uniform
        return (
            f"""
            CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_principal_links` (
              jurisdiction_code STRING,
              company_number STRING,
              canonical_principal_id STRING,
              canonical_name STRING,
              pair_key STRING,
              mid_init STRING,
              is_homonymous_guarded BOOL,
              corroboration_mode STRING,
              raw_officer_name STRING,
              position STRING,
              addr_coarse STRING
            );
            """,
            f"""
            CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_unique_principals` (
              jurisdiction_code STRING,
              canonical_principal_id STRING,
              canonical_name STRING,
              pair_key STRING,
              mid_init STRING,
              is_homonymous_guarded BOOL,
              corroboration_mode STRING,
              distinct_companies INT64,
              distinct_active_companies INT64,
              primary_hq_coarse STRING,
              name_variants ARRAY<STRING>
            );
            """,
        )

    pos_filter_parts = [
        "NOT REGEXP_CONTAINS(LOWER(TRIM(COALESCE(o.position, ''))), r'\\b(agent|incorporator|organizer|process|attorney|counsel|statutory|filing|subscriber|representative)\\b')"
    ]
    if not profile.allow_null_position:
        pos_filter_parts.append("TRIM(COALESCE(o.position, '')) != ''")
    if profile.allow_positions_regex:
        pos_filter_parts.append(
            f"REGEXP_CONTAINS(LOWER(TRIM(COALESCE(o.position, ''))), r'{profile.allow_positions_regex}')"
        )
    if profile.extra_exclude_positions_regex:
        pos_filter_parts.append(
            f"NOT REGEXP_CONTAINS(LOWER(TRIM(COALESCE(o.position, ''))), r'{profile.extra_exclude_positions_regex}')"
        )
    pos_where = "\n          AND ".join(pos_filter_parts)

    off_addr_raw = "COALESCE(NULLIF(o.addr_in_full, ''), CONCAT(COALESCE(o.addr_street_address,''), ' ', COALESCE(o.addr_locality,''), ' ', COALESCE(o.addr_region,''), ' ', SUBSTR(COALESCE(o.addr_postal_code,''),1,5)))"
    nra_addr_raw = "COALESCE(NULLIF(in_full, ''), CONCAT(COALESCE(street_address,''), ' ', COALESCE(locality,''), ' ', COALESCE(region,''), ' ', SUBSTR(COALESCE(postal_code,''),1,5)))"
    comp_addr_raw = "COALESCE(NULLIF(reg_in_full, ''), CONCAT(COALESCE(reg_street_address,''), ' ', COALESCE(reg_locality,''), ' ', COALESCE(reg_region,''), ' ', SUBSTR(COALESCE(reg_postal_code,''),1,5)))"

    sql_links = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_principal_links`
    CLUSTER BY company_number, canonical_principal_id AS
    WITH ra_block AS (
      SELECT addr_coarse FROM `{project}.{dataset}.derived_ct_ra_coarse_blocklist`
    ),
    -- Company-level registered agents (used to strip untitled NULL-position RA duplicates in CA/FL)
    comp_agents AS (
      SELECT
        company_number,
        ARRAY_AGG(DISTINCT UPPER(REGEXP_REPLACE(TRIM(name), r'[^A-Z0-9 ]+', '')) IGNORE NULLS) AS agent_names_norm
      FROM `{project}.{dataset}.officers`
      WHERE jurisdiction_code = '{jur}'
        AND REGEXP_CONTAINS(LOWER(TRIM(COALESCE(position, ''))), r'^(agent,?|registered agent|resident agent|statutory agent|ra)$')
      GROUP BY 1
    ),
    -- Best non-RA suite-aware physical HQ address per company from non_reg_addresses + companies.reg_in_full
    comp_hq AS (
      SELECT
        a.company_number,
        ANY_VALUE(a.addr_coarse) AS comp_hq_coarse,
        ANY_VALUE(a.addr_bldg) AS comp_hq_bldg
      FROM (
        SELECT
          company_number,
          {_sql_suite_addr(nra_addr_raw)} AS addr_coarse,
          {_sql_coarse_addr(nra_addr_raw)} AS addr_bldg,
          1 AS prio
        FROM `{project}.{dataset}.non_reg_addresses`
        WHERE jurisdiction_code = '{jur}'
          AND COALESCE(end_date, '') = ''
        UNION ALL
        SELECT
          company_number,
          {_sql_suite_addr(comp_addr_raw)} AS addr_coarse,
          {_sql_coarse_addr(comp_addr_raw)} AS addr_bldg,
          2 AS prio
        FROM `{project}.{dataset}.companies`
        WHERE jurisdiction_code = '{jur}'
      ) a
      LEFT JOIN ra_block rb ON rb.addr_coarse = a.addr_coarse
      WHERE a.addr_coarse IS NOT NULL AND rb.addr_coarse IS NULL
      GROUP BY 1
    ),
    -- Clean beneficial human officer rows (strictly excluding corporate officers, agents, law/RA entities)
    raw_humans AS (
      SELECT
        o.jurisdiction_code,
        o.company_number,
        TRIM(o.name) AS raw_name,
        TRIM(COALESCE(o.position, '')) AS position,
        COALESCE(
          CASE WHEN rb_off.addr_coarse IS NULL THEN off_suite ELSE NULL END,
          chq.comp_hq_coarse
        ) AS addr_coarse,
        COALESCE(
          CASE WHEN rb_off.addr_coarse IS NULL THEN off_bldg ELSE NULL END,
          chq.comp_hq_bldg
        ) AS addr_bldg,
        UPPER(REGEXP_REPLACE(TRIM(o.name), r'[^A-Z0-9 ]+', '')) AS raw_compact,
        -- Flip LAST, FIRST when comma-separated and right side isn't a suffix
        TRIM(
          CASE
            WHEN STRPOS(TRIM(o.name), ',') > 0
             AND ARRAY_LENGTH(SPLIT(TRIM(o.name), ',')) = 2
             AND NOT REGEXP_CONTAINS(UPPER(TRIM(SPLIT(TRIM(o.name), ',')[OFFSET(1)])), r'^(JR|SR|II|III|IV|V|ESQ|MD|PHD|CPA|TR|TRS|TRUSTEE)\\.?$')
            THEN CONCAT(TRIM(SPLIT(TRIM(o.name), ',')[OFFSET(1)]), ' ', TRIM(SPLIT(TRIM(o.name), ',')[OFFSET(0)]))
            ELSE TRIM(o.name)
          END
        ) AS flipped_name
      FROM (
        SELECT
          o.*,
          {_sql_suite_addr(off_addr_raw)} AS off_suite,
          {_sql_coarse_addr(off_addr_raw)} AS off_bldg
        FROM `{project}.{dataset}.officers` o
        WHERE o.jurisdiction_code = '{jur}'
          AND COALESCE(o.end_date, '') = ''
          AND TRIM(COALESCE(o.name, '')) != ''
          AND LOWER(COALESCE(o.type, '')) NOT IN ('company', 'corporation', 'organization', 'entity')
          AND {pos_where}
          AND NOT REGEXP_CONTAINS(UPPER(o.name), r'\\b(LLC|L\\.L\\.C|LLP|LLLP|LP|L\\.P|PLLC|P\\.L\\.L\\.C|PC|P\\.C|PA|P\\.A|INC|INCORPORATED|CORP|CORPORATION|LTD|LIMITED|COMPANY|COMPANIES|CO\\.|PARTNERS|PARTNERSHIP|HOLDINGS|HOLDING|PROPERTIES|PROPERTY|REALTY|MANAGEMENT|MGMT|DEVELOPMENT|GROUP|VENTURES|ENTERPRISES|CAPITAL|INVESTMENTS|EQUITIES|ADVISORS|ASSOCIATES|ASSOCIATION|TRUST|FOUNDATION|FUND|SERVICES|SOLUTIONS|AUTHORITY|UNIVERSITY|HOSPITAL|CHURCH|BANK|MORTGAGE|SERVICING|LAW|OFFICE|OFFICES|ATTORNEY|ATTORNEYS|COUNSEL|CPA|REGISTERED AGENT|CORPORATION SERVICE|CT CORPORATION|LEGALZOOM|COGENCY|INCORP|PARACORP|NONE|UNKNOWN|VACANT|OWNER|SAME AS|SEE ATTACHED)\\b')
          AND NOT REGEXP_CONTAINS(o.name, r'[0-9@/&+()]')
      ) o
      LEFT JOIN ra_block rb_off ON rb_off.addr_coarse = o.off_suite
      LEFT JOIN comp_hq chq ON chq.company_number = o.company_number
      LEFT JOIN comp_agents ca ON ca.company_number = o.company_number
      WHERE NOT (
        TRIM(COALESCE(o.position, '')) = ''
        AND ca.agent_names_norm IS NOT NULL
        AND UPPER(REGEXP_REPLACE(TRIM(o.name), r'[^A-Z0-9 ]+', '')) IN UNNEST(ca.agent_names_norm)
      )
    ),
    parsed AS (
      SELECT
        jurisdiction_code,
        company_number,
        raw_name,
        position,
        addr_coarse,
        addr_bldg,
        REGEXP_EXTRACT(addr_coarse, r'\\|(\\d{{5}})$') AS zip5,
        flipped_name,
        toks[OFFSET(0)] AS first_tok,
        toks[OFFSET(ARRAY_LENGTH(toks) - 1)] AS last_tok,
        CONCAT(
          LEAST(toks[OFFSET(0)], toks[OFFSET(ARRAY_LENGTH(toks) - 1)]),
          '|',
          GREATEST(toks[OFFSET(0)], toks[OFFSET(ARRAY_LENGTH(toks) - 1)])
        ) AS pair_key,
        CASE WHEN ARRAY_LENGTH(toks) >= 3 THEN toks[OFFSET(1)] ELSE '' END AS mid_tok,
        CASE WHEN ARRAY_LENGTH(toks) >= 3 THEN SUBSTR(toks[OFFSET(1)], 1, 1) ELSE '' END AS mid_init
      FROM (
        SELECT
          *,
          ARRAY(
            SELECT t
            FROM UNNEST(REGEXP_EXTRACT_ALL(UPPER(flipped_name), r'[A-Z]+')) AS t
            WHERE t NOT IN ('JR','SR','II','III','IV','V','MD','ESQ','PHD','CPA','TR','TRS','TRST','TRUSTEE','TRUSTEES','ETAL','MR','MRS','MS','DR')
          ) AS toks
        FROM raw_humans
      )
      WHERE ARRAY_LENGTH(toks) BETWEEN 2 AND 4
        AND LENGTH(toks[OFFSET(0)]) >= 2
        AND LENGTH(toks[OFFSET(ARRAY_LENGTH(toks) - 1)]) >= 2
    ),
    -- Compute empirical per-jurisdiction homonym dispersion + building-level HQ concentration
    pair_addr_counts AS (
      SELECT
        pair_key,
        addr_bldg,
        COUNT(DISTINCT company_number) AS addr_cos
      FROM parsed
      WHERE addr_bldg IS NOT NULL
      GROUP BY 1, 2
    ),
    pair_top_addr AS (
      SELECT
        pair_key,
        MAX(addr_cos) AS top_addr_cos
      FROM pair_addr_counts
      GROUP BY 1
    ),
    pair_stats AS (
      SELECT
        p.pair_key,
        COUNT(DISTINCT p.company_number) AS total_cos,
        COUNT(DISTINCT p.addr_bldg) AS distinct_addrs,
        COUNT(DISTINCT p.zip5) AS distinct_zips,
        COUNT(DISTINCT NULLIF(p.mid_init, '')) AS distinct_mid_inits,
        ANY_VALUE(NULLIF(p.mid_init, '')) AS sole_mid_init,
        COALESCE(pta.top_addr_cos, 0) AS top_addr_cos,
        SAFE_DIVIDE(COALESCE(pta.top_addr_cos, 0), NULLIF(COUNT(DISTINCT p.company_number), 0)) AS top_addr_share,
        LOGICAL_OR(p.last_tok IN ({COMMON_SURNAMES_SQL})) AS has_common_surname
      FROM parsed p
      LEFT JOIN pair_top_addr pta ON pta.pair_key = p.pair_key
      GROUP BY p.pair_key, pta.top_addr_cos
    ),
    -- Within the SAME company, infer dominant middle initial for rows with same pair_key
    comp_pair_mid AS (
      SELECT
        company_number,
        pair_key,
        ANY_VALUE(NULLIF(mid_init, '')) AS comp_mid_init,
        COUNT(DISTINCT NULLIF(mid_init, '')) AS comp_mid_cnt
      FROM parsed
      GROUP BY 1, 2
    ),
    -- Within the SAME building HQ address, infer dominant middle initial for rows with same pair_key
    addr_pair_mid AS (
      SELECT
        addr_bldg,
        pair_key,
        ANY_VALUE(NULLIF(mid_init, '')) AS addr_mid_init,
        COUNT(DISTINCT NULLIF(mid_init, '')) AS addr_mid_cnt
      FROM parsed
      WHERE addr_bldg IS NOT NULL
      GROUP BY 1, 2
    ),
    assigned AS (
      SELECT
        p.jurisdiction_code,
        p.company_number,
        p.raw_name,
        p.flipped_name,
        p.position,
        p.addr_coarse,
        p.addr_bldg,
        p.pair_key,
        p.mid_tok,
        -- Resolve effective middle initial using company -> HQ -> unambiguous statewide sole middle initial
        COALESCE(
          NULLIF(p.mid_init, ''),
          CASE WHEN cpm.comp_mid_cnt = 1 THEN cpm.comp_mid_init END,
          CASE WHEN apm.addr_mid_cnt = 1 THEN apm.addr_mid_init END,
          CASE WHEN ps.distinct_mid_inits = 1 AND NOT (
            (ps.distinct_zips >= 8 AND ps.top_addr_share < 0.30)
            OR (ps.has_common_surname AND ps.distinct_addrs >= 2 AND ps.top_addr_share < 0.65)
          ) THEN ps.sole_mid_init END,
          ''
        ) AS eff_mid_init,
        (
          (ps.distinct_zips >= 8 AND ps.top_addr_share < 0.30)
          OR (ps.distinct_mid_inits >= 3 AND ps.top_addr_share < 0.35)
          OR (ps.has_common_surname AND ps.distinct_addrs >= 2 AND ps.top_addr_share < 0.65)
        ) AS is_homonymous_guarded
      FROM parsed p
      JOIN pair_stats ps ON ps.pair_key = p.pair_key
      LEFT JOIN comp_pair_mid cpm ON cpm.company_number = p.company_number AND cpm.pair_key = p.pair_key
      LEFT JOIN addr_pair_mid apm ON apm.addr_bldg = p.addr_bldg AND apm.pair_key = p.pair_key
    ),
    keyed AS (
      SELECT
        *,
        CASE
          WHEN NOT is_homonymous_guarded THEN
            CONCAT('{jur}:P:', pair_key, ':', COALESCE(NULLIF(eff_mid_init, ''), '_'))
          WHEN addr_coarse IS NOT NULL THEN
            CONCAT('{jur}:PHQ:', pair_key, ':', COALESCE(NULLIF(eff_mid_init, ''), '_'), '@', addr_coarse)
          ELSE
            CONCAT('{jur}:PCO:', pair_key, ':', COALESCE(NULLIF(eff_mid_init, ''), '_'), '#', company_number)
        END AS canonical_principal_id,
        CASE
          WHEN NOT is_homonymous_guarded THEN 'statewide_unambiguous_name'
          WHEN addr_coarse IS NOT NULL THEN 'homonym_guarded_by_hq'
          ELSE 'homonym_isolated_to_company'
        END AS corroboration_mode
      FROM assigned
    ),
    -- Pick cleanest, most frequently attested canonical display name per canonical_principal_id
    canon_name_freq AS (
      SELECT
        canonical_principal_id,
        flipped_name,
        ANY_VALUE(raw_name) AS sample_raw,
        MAX(LENGTH(mid_tok)) AS max_mid_len,
        COUNT(DISTINCT company_number) AS co_freq
      FROM keyed
      GROUP BY 1, 2
    ),
    canon_names AS (
      SELECT
        canonical_principal_id,
        ARRAY_AGG(
          flipped_name
          ORDER BY
            CASE WHEN max_mid_len > 1 THEN 2 WHEN max_mid_len = 1 THEN 1 ELSE 0 END DESC,
            co_freq DESC,
            IF(STRPOS(sample_raw, ',') = 0, 1, 0) DESC,
            LENGTH(flipped_name) DESC,
            flipped_name ASC
          LIMIT 1
        )[OFFSET(0)] AS canonical_name
      FROM canon_name_freq
      GROUP BY 1
    )
    SELECT
      k.jurisdiction_code,
      k.company_number,
      k.canonical_principal_id,
      cn.canonical_name,
      k.pair_key,
      k.eff_mid_init AS mid_init,
      k.is_homonymous_guarded,
      k.corroboration_mode,
      ANY_VALUE(k.raw_name) AS raw_officer_name,
      STRING_AGG(DISTINCT NULLIF(k.position, ''), ', ' ORDER BY NULLIF(k.position, '') LIMIT 3) AS position,
      ANY_VALUE(k.addr_coarse) AS addr_coarse
    FROM keyed k
    JOIN canon_names cn ON cn.canonical_principal_id = k.canonical_principal_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
    """

    sql_unique = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_unique_principals`
    CLUSTER BY pair_key, canonical_principal_id AS
    WITH active_cos AS (
      SELECT
        company_number,
        COALESCE(LOWER(TRIM(inactive)) NOT IN ('true', 't', '1', 'yes'), TRUE) AS is_active
      FROM `{project}.{dataset}.companies`
      WHERE jurisdiction_code = '{jur}'
    ),
    hq_freq AS (
      SELECT
        canonical_principal_id,
        addr_coarse,
        COUNT(DISTINCT company_number) AS c_cnt
      FROM `{project}.{dataset}.derived_ct_{jur}_principal_links`
      WHERE addr_coarse IS NOT NULL
      GROUP BY 1, 2
    ),
    top_hq AS (
      SELECT
        canonical_principal_id,
        ARRAY_AGG(addr_coarse ORDER BY c_cnt DESC, addr_coarse ASC LIMIT 1)[OFFSET(0)] AS primary_hq_coarse
      FROM hq_freq
      GROUP BY 1
    )
    SELECT
      l.jurisdiction_code,
      l.canonical_principal_id,
      ANY_VALUE(l.canonical_name) AS canonical_name,
      ANY_VALUE(l.pair_key) AS pair_key,
      ANY_VALUE(l.mid_init) AS mid_init,
      LOGICAL_OR(l.is_homonymous_guarded) AS is_homonymous_guarded,
      ANY_VALUE(l.corroboration_mode) AS corroboration_mode,
      COUNT(DISTINCT l.company_number) AS distinct_companies,
      COUNT(DISTINCT IF(COALESCE(ac.is_active, TRUE), l.company_number, NULL)) AS distinct_active_companies,
      ANY_VALUE(thq.primary_hq_coarse) AS primary_hq_coarse,
      ARRAY_AGG(DISTINCT l.raw_officer_name IGNORE NULLS LIMIT 12) AS name_variants
    FROM `{project}.{dataset}.derived_ct_{jur}_principal_links` l
    LEFT JOIN active_cos ac ON ac.company_number = l.company_number
    LEFT JOIN top_hq thq ON thq.canonical_principal_id = l.canonical_principal_id
    GROUP BY l.jurisdiction_code, l.canonical_principal_id;
    """
    return sql_links, sql_unique


def build_jurisdiction_hq_and_closure_sql(
    profile: JurisdictionProfile,
    *,
    project: str,
    dataset: str,
) -> Tuple[str, str]:
    jur = profile.oc_jur
    nra_addr_raw = "COALESCE(NULLIF(in_full, ''), CONCAT(COALESCE(street_address,''), ' ', COALESCE(locality,''), ' ', COALESCE(region,''), ' ', SUBSTR(COALESCE(postal_code,''),1,5)))"
    comp_addr_raw = "COALESCE(NULLIF(reg_in_full, ''), CONCAT(COALESCE(reg_street_address,''), ' ', COALESCE(reg_locality,''), ' ', COALESCE(reg_region,''), ' ', SUBSTR(COALESCE(reg_postal_code,''),1,5)))"

    sql_disp = f"""
    -- Stage 2: Quota-Free Dispersed Intermediary Hub Quarantine (CT network_builder.py standard)
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_dispersed_hq_blocklist` AS
    WITH ra_block AS (
      SELECT addr_coarse FROM `{project}.{dataset}.derived_ct_ra_coarse_blocklist`
    ),
    comp_addrs AS (
      SELECT DISTINCT company_number, addr_coarse
      FROM (
        SELECT company_number, {_sql_suite_addr(nra_addr_raw)} AS addr_coarse
        FROM `{project}.{dataset}.non_reg_addresses`
        WHERE jurisdiction_code = '{jur}' AND COALESCE(end_date, '') = ''
        UNION ALL
        SELECT company_number, {_sql_suite_addr(comp_addr_raw)} AS addr_coarse
        FROM `{project}.{dataset}.companies`
        WHERE jurisdiction_code = '{jur}'
      )
      WHERE addr_coarse IS NOT NULL
        AND addr_coarse NOT IN (SELECT addr_coarse FROM ra_block)
    ),
    addr_principal_counts AS (
      SELECT
        ca.addr_coarse,
        pl.canonical_principal_id,
        COUNT(DISTINCT ca.company_number) AS p_cos
      FROM comp_addrs ca
      JOIN `{project}.{dataset}.derived_ct_{jur}_principal_links` pl
        ON pl.company_number = ca.company_number
      GROUP BY 1, 2
    ),
    addr_totals AS (
      SELECT
        ca.addr_coarse,
        COUNT(DISTINCT ca.company_number) AS total_companies,
        COUNT(DISTINCT apc.canonical_principal_id) AS distinct_principals,
        COALESCE(MAX(apc.p_cos), 0) AS top_principal_cos
      FROM comp_addrs ca
      LEFT JOIN addr_principal_counts apc ON apc.addr_coarse = ca.addr_coarse
      GROUP BY 1
    )
    SELECT
      '{jur}' AS jurisdiction_code,
      addr_coarse,
      total_companies,
      distinct_principals,
      top_principal_cos,
      ROUND(SAFE_DIVIDE(top_principal_cos, NULLIF(total_companies, 0)), 4) AS top_principal_share,
      CASE
        WHEN distinct_principals >= 12 AND SAFE_DIVIDE(top_principal_cos, NULLIF(total_companies, 0)) < 0.35
        THEN 'dispersed_intermediary_hub'
        WHEN total_companies >= 60 AND distinct_principals = 0
        THEN 'anonymous_high_fanout_hub'
        ELSE 'keep'
      END AS quarantine_reason
    FROM addr_totals
    WHERE (distinct_principals >= 12 AND SAFE_DIVIDE(top_principal_cos, NULLIF(total_companies, 0)) < 0.35)
       OR (total_companies >= 60 AND distinct_principals = 0);
    """

    sql_edges = f"""
    -- Stage 3: CT Depth-4 Corporate Graph Closure (Closely-held principals + corporate parents + cohesive HQs)
    CREATE OR REPLACE TABLE `{project}.{dataset}.derived_ct_{jur}_company_edges` AS
    WITH comp_meta AS (
      SELECT
        c.company_number,
        TRIM(c.name) AS name,
        TRIM(REGEXP_REPLACE(UPPER(REGEXP_REPLACE(TRIM(c.name), r'[^A-Z0-9 ]+', ' ')), r'\\s+', ' ')) AS name_norm,
        COALESCE(LOWER(TRIM(c.nonprofit)) IN ('true', 't', '1', 'yes'), FALSE)
          OR REGEXP_CONTAINS(UPPER(COALESCE(c.company_type, '')), r'NON[- ]?PROFIT|CHARIT') AS is_nonprofit,
        COALESCE(LOWER(TRIM(c.inactive)) NOT IN ('true', 't', '1', 'yes'), TRUE) AS is_active
      FROM `{project}.{dataset}.companies` c
      WHERE c.jurisdiction_code = '{jur}'
    ),
    -- Count distinct deduplicated principals per company (enforce CT closely-held governance cap: <=12, nonprofits <=2)
    comp_p_counts AS (
      SELECT
        pl.company_number,
        COUNT(DISTINCT pl.canonical_principal_id) AS p_cnt
      FROM `{project}.{dataset}.derived_ct_{jur}_principal_links` pl
      GROUP BY 1
    ),
    eligible_principal_links AS (
      SELECT
        pl.canonical_principal_id,
        pl.company_number
      FROM `{project}.{dataset}.derived_ct_{jur}_principal_links` pl
      JOIN `{project}.{dataset}.derived_ct_{jur}_unique_principals` up
        ON up.canonical_principal_id = pl.canonical_principal_id
      JOIN comp_meta cm ON cm.company_number = pl.company_number
      JOIN comp_p_counts cpc ON cpc.company_number = pl.company_number
      WHERE up.distinct_companies BETWEEN 2 AND 250
        AND cpc.p_cnt <= IF(cm.is_nonprofit, 2, 12)
    ),
    -- Star-anchor edges per deduplicated principal (O(E) linear edges instead of O(N^2) clique!)
    principal_anchor AS (
      SELECT
        canonical_principal_id,
        MIN(company_number) AS anchor_cnum
      FROM eligible_principal_links
      GROUP BY 1
    ),
    principal_edges AS (
      SELECT DISTINCT
        LEAST(pa.anchor_cnum, epl.company_number) AS src_cnum,
        GREATEST(pa.anchor_cnum, epl.company_number) AS dst_cnum,
        'deduped_principal' AS edge_type,
        pa.canonical_principal_id AS bridge_key
      FROM eligible_principal_links epl
      JOIN principal_anchor pa ON pa.canonical_principal_id = epl.canonical_principal_id
      WHERE pa.anchor_cnum != epl.company_number
    ),
    -- Corporate parent officer -> uniquely-named active NON-NONPROFIT in-state company
    -- (strictly excludes 501(c)(3) nonprofit LIHTC / welfare-exemption MGPs that co-GP
    -- unrelated private developers' limited partnerships!)
    unique_active_norm AS (
      SELECT
        name_norm,
        ANY_VALUE(company_number) AS parent_cnum
      FROM comp_meta
      WHERE is_active AND NOT is_nonprofit AND LENGTH(name_norm) >= 6
      GROUP BY 1
      HAVING COUNT(DISTINCT company_number) = 1
    ),
    corp_parent_edges_raw AS (
      SELECT DISTINCT
        LEAST(o.company_number, uan.parent_cnum) AS src_cnum,
        GREATEST(o.company_number, uan.parent_cnum) AS dst_cnum,
        'corporate_parent' AS edge_type,
        uan.name_norm AS bridge_key
      FROM `{project}.{dataset}.officers` o
      JOIN unique_active_norm uan
        ON uan.name_norm = TRIM(REGEXP_REPLACE(UPPER(REGEXP_REPLACE(TRIM(o.name), r'[^A-Z0-9 ]+', ' ')), r'\\s+', ' '))
      WHERE o.jurisdiction_code = '{jur}'
        AND COALESCE(o.end_date, '') = ''
        AND NOT REGEXP_CONTAINS(LOWER(TRIM(COALESCE(o.position, ''))), r'\\b(agent|incorporator|organizer|process)\\b')
        AND (
          LOWER(COALESCE(o.type, '')) IN ('company', 'corporation', 'organization', 'entity')
          OR REGEXP_CONTAINS(UPPER(o.name), r'\\b(LLC|INC|CORP|LP|LLP|HOLDINGS|PROPERTIES|REALTY|MANAGEMENT|GROUP|CAPITAL|PARTNERS|INVESTMENTS|COMPANY|COMPANIES|DEVELOPMENT)\\b')
        )
        AND NOT REGEXP_CONTAINS(UPPER(o.name), r'\\b(CORPORATION SERVICE|CT CORPORATION|LEGALZOOM|COGENCY|INCORP|PARACORP|REGISTERED AGENT|BANK|MORTGAGE|SERVICING|1031|EXCHANGE|EXCHANGES|SAFE HARBOR|ACCOMMODATOR|ASSET PRESERVATION|STEWART HOLDINGS|IPX1031|LENDING CENTER|REGIONAL CENTER|ESCROW|TITLE|TITLES|STATUTORY TRUST|FOUNDATION FOR AFFORDABLE|RV REGISTRATION|ACTION SERVICES|FILING SERVICES|CORPORATE SERVICES|BUSINESS SERVICES|LAW OFFICE|ATTORNEYS|PLLC|LLP|CPA|CPAS|ACCOUNTING|ACCOUNTANTS|LNR PARTNERS|SPECIAL SERVIC|AGENCY AGENTS|FORVIS MAZARS)\\b|\\bTAX\\s+(?:INC|LLC|SERVICES|GROUP|PREP)\\b|\\b(P\\.?C\\.?|P\\.?A\\.?)\\s*$')
        AND uan.name_norm NOT IN ('MANAGER LLC', 'MEMBER LLC', 'MANAGING MEMBER LLC', 'GENERAL PARTNER LLC', 'OWNER LLC', 'HOLDINGS LLC', 'MANAGEMENT LLC')
        AND o.company_number != uan.parent_cnum
    ),
    corp_parent_valid AS (
      SELECT bridge_key
      FROM corp_parent_edges_raw
      GROUP BY 1
      HAVING COUNT(*) <= 400
    ),
    corp_parent_edges AS (
      SELECT r.*
      FROM corp_parent_edges_raw r
      JOIN corp_parent_valid v ON v.bridge_key = r.bridge_key
    ),
    -- Cohesive non-RA, non-dispersed shared corporate HQ edges (suite-aware & passing CT cohesion guard!)
    ra_and_disp AS (
      SELECT addr_coarse FROM `{project}.{dataset}.derived_ct_ra_coarse_blocklist`
      UNION DISTINCT
      SELECT addr_coarse FROM `{project}.{dataset}.derived_ct_{jur}_dispersed_hq_blocklist`
    ),
    clean_hq_cos AS (
      SELECT DISTINCT
        a.company_number,
        a.addr_coarse
      FROM (
        SELECT company_number, {_sql_suite_addr(nra_addr_raw)} AS addr_coarse
        FROM `{project}.{dataset}.non_reg_addresses`
        WHERE jurisdiction_code = '{jur}' AND COALESCE(end_date, '') = ''
        UNION ALL
        SELECT company_number, {_sql_suite_addr(comp_addr_raw)} AS addr_coarse
        FROM `{project}.{dataset}.companies`
        WHERE jurisdiction_code = '{jur}'
      ) a
      JOIN comp_meta cm ON cm.company_number = a.company_number AND cm.is_active AND NOT cm.is_nonprofit
      LEFT JOIN ra_and_disp blk ON blk.addr_coarse = a.addr_coarse
      WHERE a.addr_coarse IS NOT NULL AND blk.addr_coarse IS NULL
    ),
    cohesive_hq_groups AS (
      SELECT
        addr_coarse,
        MIN(company_number) AS anchor_cnum,
        COUNT(DISTINCT company_number) AS co_cnt
      FROM clean_hq_cos
      GROUP BY 1
      HAVING co_cnt BETWEEN 2 AND 250
    ),
    hq_edges AS (
      SELECT DISTINCT
        LEAST(chg.anchor_cnum, chc.company_number) AS src_cnum,
        GREATEST(chg.anchor_cnum, chc.company_number) AS dst_cnum,
        'cohesive_hq' AS edge_type,
        chg.addr_coarse AS bridge_key
      FROM clean_hq_cos chc
      JOIN cohesive_hq_groups chg ON chg.addr_coarse = chc.addr_coarse
      WHERE chg.anchor_cnum != chc.company_number
    )
    SELECT '{jur}' AS jurisdiction_code, * FROM principal_edges
    UNION ALL
    SELECT '{jur}' AS jurisdiction_code, * FROM corp_parent_edges
    UNION ALL
    SELECT '{jur}' AS jurisdiction_code, * FROM hq_edges;
    """
    return sql_disp, sql_edges


def audit_jurisdiction_sql(profile: JurisdictionProfile, *, project: str, dataset: str) -> str:
    jur = profile.oc_jur
    return f"""
    SELECT
      '{jur}' AS jur,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_unique_principals`) AS unique_principals,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_unique_principals` WHERE is_homonymous_guarded) AS homonym_guarded_principals,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_unique_principals` WHERE distinct_companies >= 2) AS multi_co_principals,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_principal_links`) AS principal_company_links,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_dispersed_hq_blocklist`) AS dispersed_hq_hubs_blocked,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_company_edges` WHERE edge_type = 'deduped_principal') AS edges_principal,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_company_edges` WHERE edge_type = 'corporate_parent') AS edges_corp_parent,
      (SELECT COUNT(*) FROM `{project}.{dataset}.derived_ct_{jur}_company_edges` WHERE edge_type = 'cohesive_hq') AS edges_cohesive_hq
    """


def audit_top_bridges_sql(profile: JurisdictionProfile, *, project: str, dataset: str) -> str:
    jur = profile.oc_jur
    return f"""
    WITH top_cp AS (
      SELECT 'top_corp_parent' AS kind, bridge_key, COUNT(*) AS edges
      FROM `{project}.{dataset}.derived_ct_{jur}_company_edges`
      WHERE edge_type = 'corporate_parent'
      GROUP BY 1, 2
      ORDER BY edges DESC
      LIMIT 6
    ),
    top_pr AS (
      SELECT 'top_principal' AS kind, bridge_key, COUNT(*) AS edges
      FROM `{project}.{dataset}.derived_ct_{jur}_company_edges`
      WHERE edge_type = 'deduped_principal'
      GROUP BY 1, 2
      ORDER BY edges DESC
      LIMIT 6
    )
    SELECT * FROM top_cp
    UNION ALL
    SELECT * FROM top_pr
    ORDER BY kind, edges DESC
    """


def audit_ca_spot_checks_sql(*, project: str, dataset: str) -> str:
    return f"""
    WITH hankey AS (
      SELECT 'DON HANKEY CHECK' AS test_case, canonical_principal_id, canonical_name,
             is_homonymous_guarded, corroboration_mode, distinct_companies, distinct_active_companies,
             primary_hq_coarse, name_variants
      FROM `{project}.{dataset}.derived_ct_us_ca_unique_principals`
      WHERE pair_key = 'DON|HANKEY'
    ),
    homonym_stress AS (
      SELECT 'HOMONYM STRESS (DAVID KIM / JOHN SMITH)' AS test_case,
             canonical_principal_id, canonical_name, is_homonymous_guarded, corroboration_mode,
             distinct_companies, distinct_active_companies, primary_hq_coarse, name_variants
      FROM `{project}.{dataset}.derived_ct_us_ca_unique_principals`
      WHERE pair_key IN ('DAVID|KIM', 'JOHN|SMITH')
      ORDER BY distinct_companies DESC
      LIMIT 6
    )
    SELECT * FROM hankey
    UNION ALL
    SELECT * FROM homonym_stress
    """


def main() -> int:
    parser = argparse.ArgumentParser(description="Run BigQuery CT-Treatment across OpenCorporates jurisdictions.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--jurisdictions", nargs="+", default=["us_ca"], help="OC jurisdiction codes (e.g. us_ca us_fl) or 'all'.")
    parser.add_argument("--skip-ra-blocklist", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Report exact BigQuery bytes scanned & estimated cost without executing.")
    args = parser.parse_args()

    jurs = list(JURISDICTION_PROFILES.keys()) if "all" in args.jurisdictions else args.jurisdictions
    unknown = [j for j in jurs if j not in JURISDICTION_PROFILES]
    if unknown:
        print(f"ERROR: Unknown jurisdictions: {unknown}. Known: {sorted(JURISDICTION_PROFILES.keys())}", file=sys.stderr)
        return 2

    if not args.skip_ra_blocklist:
        print(f"[BQ-CT] Step 0: Building 61-jurisdiction coarse RA blocklist (`derived_ct_ra_coarse_blocklist`)...", flush=True)
        t0 = time.time()
        out = run_bq_sql(
            build_ra_coarse_blocklist_sql(args.project, args.dataset),
            project=args.project,
            dry_run=args.dry_run,
        )
        print(f"  -> {out.strip() or 'done'} ({time.time() - t0:.1f}s)", flush=True)

    for j in jurs:
        prof = JURISDICTION_PROFILES[j]
        print(
            f"\n=== [BQ-CT] {prof.oc_jur.upper()} (Tier {prof.tier} | beneficial_officers={prof.beneficial_officers_available}) ===",
            flush=True,
        )
        print(f"  Profile notes: {prof.notes}", flush=True)

        sql_links, sql_unique = build_jurisdiction_principals_sql(prof, project=args.project, dataset=args.dataset)
        t1 = time.time()
        print(f"  [1a/4] Principal deduplication links (`derived_ct_{j}_principal_links`)...", flush=True)
        out1a = run_bq_sql(sql_links, project=args.project, dry_run=args.dry_run)
        print(f"    -> {out1a.strip() or 'done'} ({time.time() - t1:.1f}s)", flush=True)

        if not args.dry_run:
            t1b = time.time()
            print(f"  [1b/4] Unique principals table (`derived_ct_{j}_unique_principals`)...", flush=True)
            out1b = run_bq_sql(sql_unique, project=args.project, dry_run=False)
            print(f"    -> {out1b.strip() or 'done'} ({time.time() - t1b:.1f}s)", flush=True)

            sql_disp, sql_edges = build_jurisdiction_hq_and_closure_sql(prof, project=args.project, dataset=args.dataset)
            t2a = time.time()
            print(f"  [2a/4] Dispersed intermediary HQ quarantine (`derived_ct_{j}_dispersed_hq_blocklist`)...", flush=True)
            out2a = run_bq_sql(sql_disp, project=args.project, dry_run=False)
            print(f"    -> {out2a.strip() or 'done'} ({time.time() - t2a:.1f}s)", flush=True)

            t2b = time.time()
            print(f"  [2b/4] CT corporate-graph edges (`derived_ct_{j}_company_edges`)...", flush=True)
            out2b = run_bq_sql(sql_edges, project=args.project, dry_run=False)
            print(f"    -> {out2b.strip() or 'done'} ({time.time() - t2b:.1f}s)", flush=True)

            print(f"  [3/4] Quality audit summary for {prof.oc_jur.upper()}...", flush=True)
            aud = run_bq_sql(
                audit_jurisdiction_sql(prof, project=args.project, dataset=args.dataset),
                project=args.project,
                json_output=True,
            )
            print(aud, flush=True)
            if prof.beneficial_officers_available:
                print(f"  [3b/4] Top bridge keys audit for {prof.oc_jur.upper()} (intermediary check):", flush=True)
                top_b = run_bq_sql(
                    audit_top_bridges_sql(prof, project=args.project, dataset=args.dataset),
                    project=args.project,
                    json_output=True,
                )
                print(top_b, flush=True)
            if j == "us_ca":
                print("  [4/4 CA SPOT CHECKS] Don Hankey + David Kim / John Smith homonym stress tests:", flush=True)
                spot = run_bq_sql(
                    audit_ca_spot_checks_sql(project=args.project, dataset=args.dataset),
                    project=args.project,
                    json_output=True,
                )
                print(spot, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
