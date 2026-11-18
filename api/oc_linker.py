"""
api/oc_linker.py
================
Assessor owner-string de-mangler, OpenCorporates officer/branch graph resolver,
empirical registered-agent hub detector, and 5-tier scoped deterministic
parcel-to-company linker.

Governing policy (`docs/ASSURANCE_POLICY.md` & `README.md` Source-Only Policy):
  - Never invent links or guess across homonymous LLCs across states.
  - Registered agents, incorporators, commercial maildrops, banks, and mortgage
    servicers are strictly suppressed from bridging entities or corroborating
    addresses.
  - Ambiguous homonyms without address/branch corroboration are quarantined
    (`oc_company_number = None`, Tier 5) rather than force-matched.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import re
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from psycopg2.extras import RealDictCursor, execute_values

from api.shared_utils import (
    US_STATE_CODES,
    extract_corroborated_series_spv_key,
    extract_owner_family_key,
    extract_series_stem_info,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    load_registrar_address_substrings,
    looks_like_business_name,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_owner_name,
    normalize_person_name,
    strip_benign_care_of_prefix,
)

# Out-of-state formation hubs allowed ONLY when corroborated by Tier-1 address
# or an explicit `oc_branches` foreign registration in the parcel's state.
FOREIGN_FORMATION_HUBS = frozenset({"us_de", "us_wy", "us_nv"})

# Ultra-generic corporate names that are never allowed to match on Tier 2
# (name-only without corroborating address), even if only one is active in-state.
GENERIC_HOMONYM_CORPORATE_STEMS = frozenset({
    "PROPERTIES",
    "PROPERTY",
    "HOLDINGS",
    "HOLDING",
    "INVESTMENTS",
    "INVESTMENT",
    "REALTY",
    "REAL ESTATE",
    "VENTURES",
    "ENTERPRISES",
    "MANAGEMENT",
    "ASSOCIATES",
    "PARTNERS",
    "CAPITAL",
    "GROUP",
    "DEVELOPMENT",
    "ACQUISITIONS",
    "EQUITY",
    "ASSETS",
    "HOMES",
    "HOUSING",
    "LAND",
    "TRUST",
})

_CARE_OF_EXTRACT_RE = re.compile(
    r"(?:^|\s)(?:C\s*/\s*O|CARE\s+OF|ATTN\.?:?|ATTENTION:?|%)\s+(.+?)$",
    re.IGNORECASE,
)

# Trailing tenancy, survivorship, and trust boilerplate common on county tax rolls
_ASSESSOR_BOILERPLATE_PATTERNS = [
    re.compile(r"\b(?:ET\s+AL|ETAL|ET\s+UX|ETUX|ET\s+VIR|ETVIR)\b\.?$", re.IGNORECASE),
    re.compile(
        r"\b(?:JT\s+TEN|JTWROS|WROS|TEN\s+IN\s+COM|TEN\s+ENT|TIC|T/E|T/C|SURV|H/W|H\s*&\s*W)\b\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:U/D/T|UDT|U/A|U/A/D|DATED\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}.*)$",
        re.IGNORECASE,
    ),
]

# Truncated trailing corporate tokens observed on 25/30/35/40-char fixed-width CAMA rolls
_DANGLING_TRUNCATED_SUFFIX_RE = re.compile(
    r"\s+(?:LL|L\s+L|INCORP|INCOR|INC?|CORPO|CORP?|LIMITE|LIMIT|LIM|"
    r"PARTNERSHI|PARTNERSH|PARTNER|PROPERTIES|PROPERTIE|PROPERTI|PROPERT|"
    r"HOLDING|INVESTMENT|INVESTMEN|MANAGEMEN|DEVELOPMEN|ASSOCIATE|ASSOCIA)$"
)

_AGENT_OR_INCORPORATOR_ROLE_RE = re.compile(
    r"\b(?:AGENT|REGISTERED\s+AGENT|RESIDENT\s+AGENT|STATUTORY\s+AGENT|"
    r"INCORPORATOR|ORGANIZER|PROCESS|AUTHORIZED\s+REP(?:RESENTATIVE)?|"
    r"ATTORNEY|COUNSEL|CPA|ACCOUNTANT|FILING)\b",
    re.IGNORECASE,
)

_BENEFICIAL_ROLE_RE = re.compile(
    r"\b(?:BENEFICIAL\s+OWN(?:ER|ERSHIP)|MEMBER|MANAGING\s+MEMBER|MGR|MANAGER|"
    r"GENERAL\s+PARTNER|PARTNER|PRESIDENT|CEO|CHIEF\s+EXECUTIVE|CFO|CHIEF\s+FINANCIAL|"
    r"COO|CHIEF\s+OPERATING|TREASURER|SECRETARY|DIRECTOR|VICE\s+PRESIDENT|VP|"
    r"OWNER|PRINCIPAL|SOLE\s+PROPRIETOR|TRUSTEE|GOVERNOR|CHAIRMAN|CHAIRPERSON|"
    r"SHAREHOLDER|POLICY[- ]MAKING\s+AUTHORITY|EXECUTING\s*OFFICER|"
    r"AUTHORIZED\s+(?:MEMBER|MANAGER|OFFICER))\b",
    re.IGNORECASE,
)

_TRAILING_OFFICER_ROLE_IN_NAME_RE = re.compile(
    r"[\s,;-]+\b(?:AUTHORIZED\s+(?:PERSON|MEMBER|MANAGER|OFFICER|REPRESENTATIVE|SIGNATORY)|"
    r"MANAGING\s+MEMBER|SOLE\s+MEMBER|BENEFICIAL\s+OWNER|GENERAL\s+PARTNER|"
    r"PRESIDENT|VICE\s+PRESIDENT|TREASURER|SECRETARY|MANAGER|MEMBER)\b\.?\s*$",
    re.IGNORECASE,
)

_CORPORATE_SUFFIX_STRIP_RE = re.compile(
    r"(?:\s+\b(?:LIMITED\s+LIABILITY\s+COMPANY|LIMITED\s+PARTNERSHIP|"
    r"LIMITED\s+LIABILITY\s+PARTNERSHIP|INCORPORATED|CORPORATION|COMPANY|LIMITED|"
    r"LLC|LLP|LP|INC|CORP|LTD|CO|PLLC|PC)\b)+$"
)


@dataclass(frozen=True)
class AssessorOwnerRecord:
    raw_owner: str
    cleaned_owner: str
    owner_norm: str
    is_business: bool
    care_of_entity: Optional[str]
    is_truncated: bool
    prefix18: Optional[str]
    mailing_state: Optional[str]


@dataclass(frozen=True)
class OCCandidate:
    jurisdiction_code: str
    company_number: str
    name: str
    name_norm: str
    is_active: bool = True
    incorporation_date: Optional[dt.date] = None
    dissolution_date: Optional[dt.date] = None
    registered_address_coarse: str = ""
    mailing_address_coarse: str = ""
    agent_address_coarse: str = ""
    officer_addresses_coarse: Tuple[str, ...] = ()
    home_jurisdiction_code: Optional[str] = None
    home_company_number: Optional[str] = None
    matched_previous_name_norm: Optional[str] = None
    previous_name_end_date: Optional[dt.date] = None
    matched_name_type: Optional[str] = None


@dataclass(frozen=True)
class OCMatchResult:
    jurisdiction_code: str
    company_number: str
    canonical_jurisdiction_code: str
    canonical_company_number: str
    matched_name: str
    match_tier: int
    confidence: float
    match_reason: str


def extract_state_from_address(address: Optional[str], explicit_state: Optional[str] = None) -> Optional[str]:
    """Extract a validated 2-letter USPS state code from explicit state or mailing address."""
    if explicit_state:
        st = explicit_state.strip().upper()
        if st in US_STATE_CODES:
            return st
    if not address:
        return None
    cleaned = re.sub(r"[.,]", " ", str(address).upper())
    cleaned = re.sub(r"\b(?:USA|US|UNITED\s+STATES)\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    m = re.search(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\s*$", cleaned)
    if m and m.group(1) in US_STATE_CODES:
        return m.group(1)
    m_end = re.search(r"\b([A-Z]{2})\s*$", cleaned)
    if m_end and m_end.group(1) in US_STATE_CODES:
        return m_end.group(1)
    return None


def is_generic_homonym_stem(name_norm: str) -> bool:
    if not name_norm:
        return True
    stem = _CORPORATE_SUFFIX_STRIP_RE.sub("", name_norm).strip()
    stem = re.sub(r"^THE\s+", "", stem).strip()
    if not stem:
        return True
    return stem in GENERIC_HOMONYM_CORPORATE_STEMS


def demangle_assessor_owner(
    owner1: Optional[str],
    owner2: Optional[str] = None,
    care_of: Optional[str] = None,
    mailing_address: Optional[str] = None,
    explicit_mailing_state: Optional[str] = None,
    col_max_len: Optional[int] = None,
) -> AssessorOwnerRecord:
    """Clean fixed-width tax-assessor owner strings without fabricating information."""
    raw1 = (owner1 or "").strip()
    raw2 = (owner2 or "").strip()
    raw_co = (care_of or "").strip()

    extracted_co: Optional[str] = None
    if raw_co:
        extracted_co = re.sub(
            r"^\s*(?:C\s*/\s*O|CARE\s+OF|ATTN\.?:?|ATTENTION:?|%)\s*",
            "",
            raw_co,
            flags=re.IGNORECASE,
        ).strip() or None

    working = raw1
    m_co = _CARE_OF_EXTRACT_RE.search(working)
    if m_co:
        candidate_co = m_co.group(1).strip(" ,;-")
        if candidate_co and not extracted_co:
            extracted_co = candidate_co
        working = working[: m_co.start()].strip(" ,;-")

    if raw2:
        m_co2 = _CARE_OF_EXTRACT_RE.match(raw2)
        if m_co2:
            candidate_co2 = m_co2.group(1).strip(" ,;-")
            if candidate_co2 and not extracted_co:
                extracted_co = candidate_co2
        elif raw2.upper().startswith(("C/O ", "CARE OF ", "ATTN ", "%")):
            if not extracted_co:
                extracted_co = re.sub(
                    r"^(?:C/O|CARE OF|ATTN|%)[\s:.-]*", "", raw2, flags=re.IGNORECASE
                ).strip()
        else:
            # Determine if raw2 is a continuation of a split corporate name vs co-owner
            if looks_like_business_name(working) or looks_like_business_name(f"{working} {raw2}"):
                working = f"{working} {raw2}".strip()
            else:
                working = f"{working} & {raw2}".strip()

    # Detect fixed-width column truncation BEFORE stripping suffixes
    raw_len = len(raw1)
    hit_fixed_width = bool(col_max_len and col_max_len >= 20 and raw_len >= col_max_len - 1)
    upper_working = re.sub(r"[,.'`\"]", "", working.upper()).strip()
    has_dangling_token = bool(
        hit_fixed_width
        or (len(upper_working) >= 24 and _DANGLING_TRUNCATED_SUFFIX_RE.search(upper_working)
            and not upper_working.endswith((" LLC", " INC", " CORP", " LP", " LLP", " LTD")))
    )

    cleaned = working
    changed = True
    while changed:
        changed = False
        for pat in _ASSESSOR_BOILERPLATE_PATTERNS:
            next_val = pat.sub("", cleaned).strip(" ,;&/-")
            if next_val != cleaned and next_val:
                cleaned = next_val
                changed = True

    is_biz = (
        looks_like_business_name(cleaned)
        or bool(re.search(r"^\d+\s+[A-Z]", cleaned.upper()))
        or (has_dangling_token and (not looks_like_person_owner(cleaned) or "," not in raw1))
    )
    owner_norm = normalize_business_name(cleaned) if is_biz else normalize_person_name(cleaned)
    prefix18 = owner_norm[:18] if len(owner_norm) >= 18 else None
    mail_state = extract_state_from_address(mailing_address, explicit_state=explicit_mailing_state)

    return AssessorOwnerRecord(
        raw_owner=" ".join(p for p in (raw1, raw2) if p).strip(),
        cleaned_owner=cleaned,
        owner_norm=owner_norm,
        is_business=is_biz,
        care_of_entity=extracted_co,
        is_truncated=has_dangling_token,
        prefix18=prefix18,
        mailing_state=mail_state,
    )


def clean_oc_officer_name(raw_name: Optional[str]) -> str:
    """Strip trailing filing-form role labels accidentally typed into `officers.name`
    (e.g. `'DAVID TADESSE AUTHORIZED PERSON'` -> `'DAVID TADESSE'`) without touching
    legitimate corporate names (`'GENERAL PARTNER HOLDINGS LLC'`) or single-word names."""
    nm = " ".join(str(raw_name or "").strip().split())
    if not nm or looks_like_business_name(nm):
        return nm
    m = _TRAILING_OFFICER_ROLE_IN_NAME_RE.search(nm)
    if not m:
        return nm
    candidate = nm[: m.start()].strip(" ,;-")
    if candidate and looks_like_person_owner(candidate):
        return candidate
    return nm


def classify_oc_officer_role(
    position: Optional[str],
    officer_name: Optional[str],
    is_corporate_flag: bool = False,
    explicit_type: Optional[str] = None,
) -> Tuple[str, bool]:
    """Classify an OpenCorporates officer row into `(role_class, is_corporate_officer)`.

    Returns one of:
      - `'agent_or_incorporator'`: Statutory/registered agents, incorporators, organizers, attorneys, CPAs
      - `'corporate_parent'`: Holding LLC / LP / Corp serving as member, manager, GP, or parent
      - `'beneficial_principal'`: Human member, manager, general partner, officer, beneficial owner, or director
      - `'other'`: Ambiguous or non-controlling role
    """
    pos = (position or "").strip()
    nm = clean_oc_officer_name(officer_name)
    exp_type = (explicit_type or "").strip().lower()
    explicit_corp = bool(is_corporate_flag) or exp_type in {"company", "corporation", "organization", "entity"}
    if not nm or is_excluded_third_party_entity(nm):
        return ("agent_or_incorporator", explicit_corp)

    if pos and _AGENT_OR_INCORPORATOR_ROLE_RE.search(pos):
        return ("agent_or_incorporator", explicit_corp)

    is_corp = explicit_corp or looks_like_business_name(nm)
    if not pos or _BENEFICIAL_ROLE_RE.search(pos):
        if is_corp:
            return ("corporate_parent", True)
        return ("beneficial_principal", False)

    return ("other", is_corp)


def _usable_corroborating_address(
    coarse_addr: Optional[str],
    registered_agent_addrs: Set[str],
) -> bool:
    if not coarse_addr or len(coarse_addr) < 6:
        return False
    if coarse_addr in registered_agent_addrs:
        return False
    if is_excluded_third_party_address(coarse_addr, coarse_addr):
        return False
    return True


def _candidate_matches_parcel_address(
    cand: OCCandidate,
    parcel_coarse_addrs: Set[str],
    registered_agent_addrs: Set[str],
) -> bool:
    if not parcel_coarse_addrs:
        return False
    usable_parcel_addrs = {
        a for a in parcel_coarse_addrs if _usable_corroborating_address(a, registered_agent_addrs)
    }
    if not usable_parcel_addrs:
        return False

    cand_addrs: Set[str] = set()
    for addr in (cand.mailing_address_coarse, cand.registered_address_coarse, *cand.officer_addresses_coarse):
        if not addr:
            continue
        # Never treat the company's own statutory agent address as an operational match
        if cand.agent_address_coarse and addr == cand.agent_address_coarse:
            continue
        if _usable_corroborating_address(addr, registered_agent_addrs):
            cand_addrs.add(addr)

    return bool(usable_parcel_addrs & cand_addrs)


def _build_match_result(cand: OCCandidate, tier: int, confidence: float, reason: str) -> OCMatchResult:
    canon_jur = cand.home_jurisdiction_code or cand.jurisdiction_code
    canon_num = cand.home_company_number or cand.company_number
    return OCMatchResult(
        jurisdiction_code=cand.jurisdiction_code,
        company_number=cand.company_number,
        canonical_jurisdiction_code=canon_jur,
        canonical_company_number=canon_num,
        matched_name=cand.name,
        match_tier=tier,
        confidence=confidence,
        match_reason=reason,
    )


def score_oc_candidates(
    demangled: AssessorOwnerRecord,
    parcel_state: str,
    parcel_mailing_coarse: Optional[str],
    candidates: Sequence[OCCandidate],
    *,
    extra_parcel_coarse_addrs: Iterable[str] = (),
    parcel_last_sale_date: Optional[dt.date] = None,
    registered_agent_addrs: Optional[Set[str]] = None,
) -> Optional[OCMatchResult]:
    """Deterministic 5-tier parcel-to-OpenCorporates entity matcher.

    Enforces strict anti-homonym corroboration:
      - Tier 1 (0.99): Exact `name_norm` + corroborating non-agent coarse address.
      - Tier 2 (0.92): Unique active company in `{us_<parcel_state>, us_<mailing_state>}`
        with a non-generic name stem (never uncorroborated DE/WY/NV unless local).
      - Tier 3 (0.90): Truncated assessor string (`len>=18`, `is_truncated=True`) prefix-matching
        full company `name_norm` + corroborating non-agent coarse address.
      - Tier 4 (0.88): Historical `oc_previous_names` match in local/mailing state
        corroborated by non-agent coarse address or deed sale date prior to name change.
      - Tier 5 (`None`): Ambiguous homonyms or uncorroborated out-of-state entities
        are quarantined without guessing.
    """
    if not demangled.is_business or not demangled.owner_norm:
        return None
    if is_excluded_third_party_entity(demangled.cleaned_owner) or is_excluded_third_party_entity(demangled.owner_norm):
        return None

    ra_addrs = registered_agent_addrs or set()
    p_state = (parcel_state or "").strip().lower()
    local_jur = f"us_{p_state}" if p_state else ""
    raw_mail_jur = f"us_{demangled.mailing_state.lower()}" if demangled.mailing_state else ""
    # Only trust `mailing_state` as an operational home jurisdiction for Tier 2/4 when
    # the mailing address itself is NOT a commercial registered agent / 3rd-party hub.
    mail_is_operational = (
        raw_mail_jur == local_jur
        or _usable_corroborating_address(parcel_mailing_coarse, ra_addrs)
    )
    mail_jur = raw_mail_jur if mail_is_operational else ""
    direct_jurs = {j for j in (local_jur, mail_jur) if j}
    allowed_jurs = direct_jurs | ({raw_mail_jur} if raw_mail_jur else set()) | set(FOREIGN_FORMATION_HUBS)

    parcel_addrs: Set[str] = set()
    if parcel_mailing_coarse:
        parcel_addrs.add(parcel_mailing_coarse)
    for extra in extra_parcel_coarse_addrs:
        if extra:
            parcel_addrs.add(extra)

    scoped = [c for c in candidates if c.jurisdiction_code in allowed_jurs]
    if not scoped:
        return None

    # --- Tier 1: Exact normalized name + corroborating non-agent coarse address ---
    exact_name_cands = [
        c for c in scoped
        if c.name_norm == demangled.owner_norm and c.matched_previous_name_norm is None
    ]
    tier1_matches = [
        c for c in exact_name_cands
        if _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
    ]
    if len(tier1_matches) == 1:
        return _build_match_result(tier1_matches[0], 1, 0.99, "exact_name_and_non_agent_address")
    if len(tier1_matches) > 1:
        # Prefer active in-state registration if it uniquely breaks the tie (e.g., foreign branch + home entity)
        active_local_t1 = [c for c in tier1_matches if c.is_active and c.jurisdiction_code == local_jur]
        if len(active_local_t1) == 1:
            return _build_match_result(active_local_t1[0], 1, 0.99, "exact_name_and_address_local_tiebreak")
        # Check if multiple Tier-1 matches are simply a branch and its own home_company
        canon_ids = {
            (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
            for c in tier1_matches
        }
        if len(canon_ids) == 1:
            chosen = active_local_t1[0] if active_local_t1 else tier1_matches[0]
            return _build_match_result(chosen, 1, 0.99, "exact_name_and_address_branch_home_pair")
        return None  # Quarantine ambiguous multi-entity collision

    # --- Tier 1b: Exact DBA / trading name + corroborating non-agent coarse address ---
    dba_cands = [
        c for c in scoped
        if c.matched_previous_name_norm == demangled.owner_norm
        and c.matched_name_type == "trading"
        and _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
    ]
    if dba_cands:
        canon_dba = {
            (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
            for c in dba_cands
        }
        if len(canon_dba) == 1:
            local_dba = [c for c in dba_cands if c.is_active and c.jurisdiction_code == local_jur]
            chosen = local_dba[0] if local_dba else dba_cands[0]
            return _build_match_result(chosen, 1, 0.95, "dba_trading_name_with_address")

    # --- Tier 2: Unique active entity in `{local_jur, mail_jur}` (non-generic stem) ---
    if not is_generic_homonym_stem(demangled.owner_norm):
        direct_exact = [c for c in exact_name_cands if c.jurisdiction_code in direct_jurs]
        active_direct = [c for c in direct_exact if c.is_active]
        canon_active_direct = {
            (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
            for c in active_direct
        }
        if len(canon_active_direct) == 1 and active_direct:
            # Prefer the local state registration record when both local branch and mail-state home appear
            local_pref = [c for c in active_direct if c.jurisdiction_code == local_jur]
            chosen = local_pref[0] if local_pref else active_direct[0]
            return _build_match_result(chosen, 2, 0.92, "unique_active_scoped_jurisdiction_entity")
        if len(canon_active_direct) > 1:
            # Multiple distinct active homonyms in scoped state(s) without address tie-breaker -> Tier 5 quarantine
            return None

    # --- Tier 3: Truncated assessor owner prefix (`>= 18` chars) + exact non-agent address ---
    if demangled.is_truncated and demangled.prefix18 and len(demangled.owner_norm) >= 18:
        prefix_cands = [
            c for c in scoped
            if c.matched_previous_name_norm is None
            and c.name_norm.startswith(demangled.owner_norm)
            and _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
        ]
        canon_prefix = {
            (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
            for c in prefix_cands
        }
        if len(canon_prefix) == 1 and prefix_cands:
            return _build_match_result(prefix_cands[0], 3, 0.90, "truncated_assessor_prefix_with_address")

    # --- Tier 4: Historical `oc_previous_names` match in `{local_jur, mail_jur}` ---
    prev_cands = [
        c for c in scoped
        if c.jurisdiction_code in direct_jurs
        and c.matched_previous_name_norm == demangled.owner_norm
        and c.matched_name_type in (None, "previous")
    ]
    if prev_cands:
        addr_corroborated_prev = [
            c for c in prev_cands if _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
        ]
        if len(addr_corroborated_prev) == 1:
            return _build_match_result(addr_corroborated_prev[0], 4, 0.88, "previous_company_name_with_address")
        if (
            len(prev_cands) == 1
            and not is_generic_homonym_stem(demangled.owner_norm)
            and parcel_last_sale_date is not None
            and (
                prev_cands[0].previous_name_end_date is None
                or parcel_last_sale_date <= prev_cands[0].previous_name_end_date
            )
        ):
            return _build_match_result(prev_cands[0], 4, 0.88, "previous_company_name_aligned_with_sale_date")

    # --- Tier 5: Quarantine ---
    return None


def refresh_empirical_registered_agent_addresses(
    conn,
    *,
    min_distinct_companies: int = 15,
) -> int:
    """Populate `oc_registered_agent_addresses` from high-cardinality agent hubs in `oc_companies`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO oc_registered_agent_addresses (
                address_coarse,
                sample_raw_address,
                distinct_company_count,
                distinct_agent_names,
                source,
                computed_at
            )
            SELECT
                agent_address_coarse,
                MAX(agent_address_raw) AS sample_raw_address,
                COUNT(DISTINCT (jurisdiction_code, company_number))::int AS distinct_company_count,
                (ARRAY_AGG(DISTINCT agent_name) FILTER (WHERE agent_name IS NOT NULL))[1:10] AS distinct_agent_names,
                'empirical_oc_agent_hub',
                NOW()
            FROM oc_companies
            WHERE agent_address_coarse IS NOT NULL
              AND agent_address_coarse != ''
            GROUP BY agent_address_coarse
            HAVING COUNT(DISTINCT (jurisdiction_code, company_number)) >= %s
            ON CONFLICT (address_coarse) DO UPDATE SET
                sample_raw_address = EXCLUDED.sample_raw_address,
                distinct_company_count = EXCLUDED.distinct_company_count,
                distinct_agent_names = EXCLUDED.distinct_agent_names,
                computed_at = NOW();
            """,
            (min_distinct_companies,),
        )
        upserted = cur.rowcount
    conn.commit()
    return upserted


def load_oc_registered_agent_addresses(conn) -> Set[str]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT address_coarse FROM oc_registered_agent_addresses")
            return {row[0] for row in cur.fetchall() if row and row[0]}
    except Exception:
        conn.rollback()
        return set()


_EXCLUDED_OC_COMPANY_TYPE_RE = re.compile(
    r"\b(?:RELIGIOUS|CHURCH\s+CORP|SAVINGS\s+BANK|BANK\s+STOCK|BANK\s+NON[- ]STOCK|^BANK$|"
    r"BANK\s*-\s*(?:DOMESTIC|FOREIGN)|CREDIT\s+UNION|INSURANCE\s+(?:COMPANY|STOCK|NON[- ]STOCK)|"
    r"^INSURANCE$|MUNICIPAL\s*-\s*(?:DOMESTIC|FOREIGN)|ELECTRIC\s+MEMBERSHIP\s+COOP|^SCHOOL$)\b",
    re.IGNORECASE,
)
_STREET_OR_POBOX_START_RE = re.compile(r"^(?:\d+\s+[A-Z0-9]|PO BOX \d+\b)")


def resolve_oc_company_principals(
    conn,
    company_keys: Sequence[Tuple[str, str]],
    *,
    max_hops: int = 3,
) -> Dict[Tuple[str, str], Dict[str, List[str]]]:
    """Traverse up to `max_hops` across bidirectional `oc_branches`, `home_jurisdiction_code`,
    and corporate-parent `oc_officers` to return:
      `{ (jur, num): {
           'humans': [...],
           'holding_entities': [...],
           'reached_companies': ['us_de/123', ...],
           'hq_addresses': ['1025 CATHEDRAL STREET BALTIMORE MD', ...],
           'raw_hq_addresses': ['1025 CATHEDRAL STREET, BALTIMORE, MD, 21201', ...],
         } }`."""
    seed_keys = list({(j, n) for j, n in company_keys if j and n})
    if not seed_keys:
        return {}

    ra_addrs = load_oc_registered_agent_addresses(conn)
    registrar_substrings = load_registrar_address_substrings(conn)

    seed_set = set(seed_keys)
    frontier: Set[Tuple[str, str]] = set(seed_keys)
    visited: Set[Tuple[str, str]] = set()
    excluded_company_keys: Set[Tuple[str, str]] = set()
    all_nonprofit_keys: Set[Tuple[str, str]] = set()
    parent_edges: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
    direct_humans: Dict[Tuple[str, str], Set[str]] = {}
    direct_hq_addrs: Dict[Tuple[str, str], Set[str]] = {}
    direct_raw_hq_addrs: Dict[Tuple[str, str], Set[str]] = {}
    entity_labels: Dict[Tuple[str, str], str] = {}

    # Professional counsel/CPA markers & board-governance cap so nonprofit/utility/hospital/public
    # boards of directors never star-bridge unrelated private LLCs.
    counsel_re = re.compile(r"\b(?:ESQ|ESQUIRE|C\.?P\.?A\.?)\b", re.IGNORECASE)
    MAX_CLOSELY_HELD_OFFICERS = 12
    MAX_NONPROFIT_CLOSELY_HELD_OFFICERS = 2

    def _harvest_hq_addr(ckey: Tuple[str, str], raw_candidate: Optional[str], agent_coarse_set: Set[str]) -> Optional[str]:
        if not raw_candidate:
            return None
        stripped = strip_benign_care_of_prefix(raw_candidate)
        if not stripped:
            return None
        coarse = normalize_mailing_address_coarse(stripped)
        if not coarse or not _STREET_OR_POBOX_START_RE.match(coarse):
            return None
        if coarse in agent_coarse_set or coarse in ra_addrs:
            return None
        if is_excluded_third_party_address(raw_candidate, coarse, registrar_substrings):
            return None
        direct_hq_addrs.setdefault(ckey, set()).add(coarse)
        direct_raw_hq_addrs.setdefault(ckey, set()).add(stripped)
        return coarse

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT to_regclass('public.oc_cross_state_pierced_branches') IS NOT NULL AS has_cspb")
        has_cspb = bool(cur.fetchone()["has_cspb"])
        for _hop in range(max_hops):
            batch = [k for k in frontier if k not in visited]
            if not batch:
                break
            visited.update(batch)
            next_frontier: Set[Tuple[str, str]] = set()
            corp_officer_norms: Dict[Tuple[str, str], Set[str]] = {}
            unnumbered_home_links: List[Tuple[Tuple[str, str], str, str]] = []

            for idx in range(0, len(batch), 4000):
                chunk = batch[idx : idx + 4000]
                c_jurs = [k[0] for k in chunk]
                c_nums = [k[1] for k in chunk]

                # 1. Company metadata + non-agent HQ/mailing addresses + direct `home_company` links
                cur.execute(
                    """
                    SELECT c.jurisdiction_code, c.company_number, c.name, c.name_norm,
                           c.company_type, c.is_nonprofit,
                           c.registered_address_raw, c.mailing_address_raw,
                           c.agent_address_raw, c.agent_address_coarse,
                           c.home_jurisdiction_code, c.home_company_number
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN oc_companies c
                      ON c.jurisdiction_code = k.j AND c.company_number = k.n
                    """,
                    (c_jurs, c_nums),
                )
                for row in cur.fetchall():
                    key = (row["jurisdiction_code"], row["company_number"])
                    cname = row.get("name") or ""
                    ctype = (row.get("company_type") or "").strip()
                    if cname:
                        entity_labels[key] = cname
                    if (cname and is_excluded_third_party_entity(cname)) or (
                        ctype and _EXCLUDED_OC_COMPANY_TYPE_RE.search(ctype)
                    ):
                        excluded_company_keys.add(key)
                        continue
                    if row.get("is_nonprofit") or any(
                        tok in ctype.upper() for tok in ("NONPROFIT", "NON-PROFIT", "CHARITABLE", "NOT-FOR-PROFIT")
                    ):
                        all_nonprofit_keys.add(key)

                    hj = (row.get("home_jurisdiction_code") or "").strip()
                    hn = (row.get("home_company_number") or "").strip()
                    is_foreign_branch = bool((hj and hj != key[0]) or ("FOREIGN" in ctype.upper()))

                    agent_coarse_set: Set[str] = set()
                    if row.get("agent_address_coarse"):
                        agent_coarse_set.add(row["agent_address_coarse"])
                    if row.get("agent_address_raw"):
                        ac = normalize_mailing_address_coarse(strip_benign_care_of_prefix(row["agent_address_raw"]))
                        if ac:
                            agent_coarse_set.add(ac)
                    _harvest_hq_addr(key, row.get("mailing_address_raw"), agent_coarse_set)
                    # Foreign-branch `registered_address_raw` is statutorily an in-state agent/office for
                    # service of process (e.g. PA/NY/FL CSC or CT Corp office), never executive HQ.
                    if not is_foreign_branch and (key[0] not in ("us_de", "us_wy", "us_nv") or key in seed_set):
                        _harvest_hq_addr(key, row.get("registered_address_raw"), agent_coarse_set)

                    if hj and hn:
                        parent_edges.setdefault(key, set()).add((hj, hn))
                        next_frontier.add((hj, hn))
                    elif hj and hj != key[0] and row.get("name_norm") and not is_generic_homonym_stem(row["name_norm"]):
                        unnumbered_home_links.append((key, hj, row["name_norm"]))

                # 2a. Explicit `oc_branches` branch -> home links
                cur.execute(
                    """
                    SELECT b.branch_jurisdiction_code, b.branch_company_number,
                           b.home_jurisdiction_code, b.home_company_number
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN oc_branches b
                      ON b.branch_jurisdiction_code = k.j AND b.branch_company_number = k.n
                    """,
                    (c_jurs, c_nums),
                )
                for row in cur.fetchall():
                    bkey = (row["branch_jurisdiction_code"], row["branch_company_number"])
                    hkey = (row["home_jurisdiction_code"], row["home_company_number"])
                    if bkey not in excluded_company_keys:
                        parent_edges.setdefault(bkey, set()).add(hkey)
                        next_frontier.add(hkey)

                # 2b. Bidirectional home -> branch links via BOTH `oc_branches` AND `idx_oc_companies_home_entity`
                # (so anonymous DE/WY shells harvest HQ addresses & beneficial owners disclosed on foreign branch filings)
                cur.execute(
                    """
                    SELECT b.branch_jurisdiction_code, b.branch_company_number,
                           b.home_jurisdiction_code, b.home_company_number
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN oc_branches b
                      ON b.home_jurisdiction_code = k.j AND b.home_company_number = k.n
                    UNION
                    SELECT c.jurisdiction_code AS branch_jurisdiction_code,
                           c.company_number AS branch_company_number,
                           c.home_jurisdiction_code,
                           c.home_company_number
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN oc_companies c
                      ON c.home_jurisdiction_code = k.j AND c.home_company_number = k.n
                    WHERE c.home_company_number IS NOT NULL
                    """,
                    (c_jurs, c_nums, c_jurs, c_nums),
                )
                for row in cur.fetchall():
                    bkey = (row["branch_jurisdiction_code"], row["branch_company_number"])
                    hkey = (row["home_jurisdiction_code"], row["home_company_number"])
                    if hkey not in excluded_company_keys:
                        parent_edges.setdefault(hkey, set()).add(bkey)
                        next_frontier.add(bkey)

                # 3. Beneficial human officers & corporate parent officers
                cur.execute(
                    """
                    SELECT o.jurisdiction_code, o.company_number, o.name, o.name_norm,
                           o.role_class, o.is_corporate_officer
                    FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                    JOIN oc_officers o
                      ON o.jurisdiction_code = k.j AND o.company_number = k.n
                    WHERE COALESCE(o.is_current, TRUE) = TRUE
                      AND o.role_class IN ('beneficial_principal', 'corporate_parent')
                    """,
                    (c_jurs, c_nums),
                )
                for row in cur.fetchall():
                    ckey = (row["jurisdiction_code"], row["company_number"])
                    if ckey in excluded_company_keys:
                        continue
                    raw_nm = row.get("name") or ""
                    if counsel_re.search(raw_nm):
                        continue
                    nm = clean_oc_officer_name(raw_nm)
                    if not nm or is_excluded_third_party_entity(nm):
                        continue
                    if row["role_class"] == "beneficial_principal" and not row.get("is_corporate_officer"):
                        if looks_like_person_owner(nm):
                            direct_humans.setdefault(ckey, set()).add(nm)
                        elif looks_like_business_name(nm) and row.get("name_norm"):
                            corp_officer_norms.setdefault(ckey, set()).add(row["name_norm"])
                    elif row["role_class"] == "corporate_parent" and row.get("name_norm"):
                        corp_officer_norms.setdefault(ckey, set()).add(row["name_norm"])

                touched_human_keys: Set[Tuple[str, str]] = set(chunk)

                # 3b. Pre-pierced 61-jurisdiction BigQuery closure (`oc_cross_state_pierced_branches`)
                if has_cspb:
                    cur.execute(
                        """
                        SELECT cs.branch_jurisdiction_code, cs.branch_company_number,
                               cs.home_jurisdiction_code, cs.home_company_number, cs.beneficial_principals
                        FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                        JOIN oc_cross_state_pierced_branches cs
                          ON cs.branch_jurisdiction_code = k.j AND cs.branch_company_number = k.n
                        UNION ALL
                        SELECT cs.branch_jurisdiction_code, cs.branch_company_number,
                               cs.home_jurisdiction_code, cs.home_company_number, cs.beneficial_principals
                        FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                        JOIN oc_cross_state_pierced_branches cs
                          ON cs.home_jurisdiction_code = k.j AND cs.home_company_number = k.n
                        """,
                        (c_jurs, c_nums, c_jurs, c_nums),
                    )
                    for row in cur.fetchall():
                        bkey = (row["branch_jurisdiction_code"], row["branch_company_number"])
                        hkey = (row["home_jurisdiction_code"], row["home_company_number"])
                        for raw_p in row.get("beneficial_principals") or ():
                            if counsel_re.search(raw_p):
                                continue
                            p_clean = clean_oc_officer_name(raw_p)
                            if p_clean and not is_excluded_third_party_entity(p_clean) and looks_like_person_owner(p_clean):
                                if bkey in visited and bkey not in excluded_company_keys:
                                    direct_humans.setdefault(bkey, set()).add(p_clean)
                                    touched_human_keys.add(bkey)
                                if hkey in visited and hkey not in excluded_company_keys:
                                    direct_humans.setdefault(hkey, set()).add(p_clean)
                                    touched_human_keys.add(hkey)

                # Enforce board-governance caps AFTER both `oc_officers` and `oc_cross_state_pierced_branches`
                # so large corporate boards (>12 officers) and volunteer nonprofit boards (>2 officers) never
                # star-bridge unrelated companies.
                for ckey in touched_human_keys:
                    cap = (
                        MAX_NONPROFIT_CLOSELY_HELD_OFFICERS
                        if ckey in all_nonprofit_keys
                        else MAX_CLOSELY_HELD_OFFICERS
                    )
                    if len(direct_humans.get(ckey, ())) > cap:
                        direct_humans.pop(ckey, None)

            # 4. Resolve unnumbered foreign-branch `(home_jurisdiction_code, name_norm)` links when unique
            if unnumbered_home_links:
                target_jurs = list({hj for _, hj, _ in unnumbered_home_links})
                target_norms = list({nn for _, _, nn in unnumbered_home_links})
                by_home_jur_norm: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}
                for nidx in range(0, len(target_norms), 4000):
                    nchunk = target_norms[nidx : nidx + 4000]
                    cur.execute(
                        """
                        SELECT jurisdiction_code, company_number, name, name_norm
                        FROM oc_companies
                        WHERE jurisdiction_code = ANY(%s)
                          AND name_norm = ANY(%s)
                          AND COALESCE(is_active, TRUE) = TRUE
                        """,
                        (target_jurs, nchunk),
                    )
                    for hrow in cur.fetchall():
                        by_home_jur_norm.setdefault(
                            (hrow["jurisdiction_code"], hrow["name_norm"]), []
                        ).append((hrow["jurisdiction_code"], hrow["company_number"], hrow["name"]))
                for branch_key, hj, nn in unnumbered_home_links:
                    matches = by_home_jur_norm.get((hj, nn), [])
                    if len(matches) == 1:
                        p_jur, p_num, p_name = matches[0]
                        parent_edges.setdefault(branch_key, set()).add((p_jur, p_num))
                        entity_labels[(p_jur, p_num)] = p_name
                        next_frontier.add((p_jur, p_num))

            # 5. Resolve corporate parent officer names within the same jurisdiction or DE when unique
            all_parent_norms = list({n for s in corp_officer_norms.values() for n in s})
            if all_parent_norms:
                search_jurs = list({k[0] for k in corp_officer_norms} | {"us_de"})
                by_jur_norm: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}
                for nidx in range(0, len(all_parent_norms), 4000):
                    nchunk = all_parent_norms[nidx : nidx + 4000]
                    cur.execute(
                        """
                        SELECT jurisdiction_code, company_number, name, name_norm
                        FROM oc_companies
                        WHERE jurisdiction_code = ANY(%s)
                          AND name_norm = ANY(%s)
                          AND COALESCE(is_active, TRUE) = TRUE
                        """,
                        (search_jurs, nchunk),
                    )
                    for prow in cur.fetchall():
                        by_jur_norm.setdefault(
                            (prow["jurisdiction_code"], prow["name_norm"]), []
                        ).append((prow["jurisdiction_code"], prow["company_number"], prow["name"]))

                for child_key, pnorms in corp_officer_norms.items():
                    child_jur = child_key[0]
                    for pnorm in pnorms:
                        matches = by_jur_norm.get((child_jur, pnorm), [])
                        if not matches and child_jur != "us_de":
                            matches = by_jur_norm.get(("us_de", pnorm), [])
                        if len(matches) == 1:
                            p_jur, p_num, p_name = matches[0]
                            parent_edges.setdefault(child_key, set()).add((p_jur, p_num))
                            entity_labels[(p_jur, p_num)] = p_name
                            next_frontier.add((p_jur, p_num))

            frontier = next_frontier

    # Roll up humans, holding entities, reached corporate nodes, and verified HQ addresses for each seed key
    resolved: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
    for seed in seed_keys:
        if seed in excluded_company_keys:
            resolved[seed] = {
                "humans": [],
                "holding_entities": [],
                "reached_companies": [],
                "hq_addresses": [],
                "raw_hq_addresses": [],
            }
            continue
        seen_nodes: Set[Tuple[str, str]] = {seed}
        stack = [seed]
        humans: Set[str] = set()
        holdings: Set[str] = set()
        hq_addrs: Set[str] = set()
        raw_hq_addrs: Set[str] = set()
        while stack:
            curr = stack.pop()
            if curr in excluded_company_keys:
                continue
            humans.update(direct_humans.get(curr, ()))
            hq_addrs.update(direct_hq_addrs.get(curr, ()))
            raw_hq_addrs.update(direct_raw_hq_addrs.get(curr, ()))
            for parent_node in parent_edges.get(curr, ()):
                if parent_node in seen_nodes or parent_node in excluded_company_keys:
                    continue
                seen_nodes.add(parent_node)
                if parent_node in entity_labels:
                    holdings.add(entity_labels[parent_node])
                stack.append(parent_node)
        if len(humans) > MAX_CLOSELY_HELD_OFFICERS:
            humans = set()
        resolved[seed] = {
            "humans": sorted(humans),
            "holding_entities": sorted(holdings),
            "reached_companies": sorted(f"{j}/{n}" for j, n in seen_nodes if (j, n) not in excluded_company_keys),
            "hq_addresses": sorted(hq_addrs),
            "raw_hq_addresses": sorted(raw_hq_addrs),
        }
    return resolved


def cluster_with_oc_and_series(
    properties: Sequence[dict],
    network_groups: Dict[str, List[dict]],
    oc_resolved: Dict[Tuple[str, str], Dict[str, List[str]]],
    *,
    is_public_fn=None,
    generic_owner_names: Iterable[str] = (),
) -> Tuple[Dict[str, List[dict]], int]:
    """Unified second-stage corporate-registry & corporate-series network clustering
    used across ALL municipal and statewide jurisdictions:
      1. Shared OC entity / corporate-parent / foreign-branch graph (`reached_companies`)
      2. Shared verified non-agent corporate HQ / principal-office / mailing address (`hq_addresses`)
      3. Shared OC beneficial human principal (`humans`)
      4. Corroborated multi-word numbered corporate series SPVs (`extract_corroborated_series_spv_key`)
    Preserves anti-meganet caps for unrelated families while allowing single/dual corporate
    series portfolios (e.g. `BALTIMORE SFR PORTFOLIO 1A..VI`, `BALTIMORE EXCEL 01..S11`,
    `PROGRESS RESIDENTIAL`, `FKH SFR`) to unify above 300 buildings without artificial splitting.
    """
    if not network_groups or not properties:
        return network_groups, 0

    generic_set = {str(x).upper() for x in generic_owner_names}
    pub_check = is_public_fn or is_excluded_third_party_entity

    bbl_to_root: Dict[str, str] = {}
    root_to_props: Dict[str, List[dict]] = {}
    for root_bbl, grp in network_groups.items():
        dedup = {p["bbl"]: p for p in grp if p.get("bbl")}
        if not dedup:
            continue
        for b in dedup:
            bbl_to_root[b] = root_bbl
        root_to_props[root_bbl] = list(dedup.values())

    if not root_to_props:
        return network_groups, 0

    parent_root = {r: r for r in root_to_props}
    comp_size = {r: len(props) for r, props in root_to_props.items()}
    comp_families: Dict[str, Set[str]] = {}
    comp_pub: Dict[str, bool] = {}
    for r, props in root_to_props.items():
        fams: Set[str] = set()
        is_pub = False
        for p in props:
            raw_nm = p.get("owner_name") or ""
            norm_nm = p.get("owner_name_norm") or raw_nm
            if pub_check(raw_nm) or pub_check(norm_nm):
                is_pub = True
            fk = extract_owner_family_key(raw_nm) or norm_nm
            if fk:
                fams.add(fk)
        comp_families[r] = fams
        comp_pub[r] = is_pub

    def find_r(r: str) -> str:
        while parent_root[r] != r:
            parent_root[r] = parent_root[parent_root[r]]
            r = parent_root[r]
        return r

    def union_r(ra: str, rb: str, max_unrelated_bbls: int = 500, max_families: int = 60) -> bool:
        ua, ub = find_r(ra), find_r(rb)
        if ua == ub or comp_pub.get(ua) or comp_pub.get(ub):
            return False
        combined_fams = comp_families[ua] | comp_families[ub]
        if len(combined_fams) > max_families:
            return False
        # Single/closely-related owner families (<=3 distinct family stems) represent one
        # corporate series operator and must not be artificially capped at 300-500 parcels.
        eff_max_bbls = 25_000 if len(combined_fams) <= 3 else max_unrelated_bbls
        if comp_size[ua] + comp_size[ub] > eff_max_bbls:
            return False
        parent_root[ua] = ub
        comp_size[ub] += comp_size[ua]
        comp_families[ub] = combined_fams
        return True

    by_oc_node: Dict[str, List[str]] = {}
    by_oc_hq: Dict[str, List[str]] = {}
    hq_family_fanout: Dict[str, Set[str]] = {}
    by_oc_human: Dict[str, List[str]] = {}
    human_entity_fanout: Dict[str, Set[Tuple[str, str]]] = {}
    by_series_spv: Dict[str, List[str]] = {}
    series_explicit_variants: Dict[str, Set[str]] = {}
    series_base_variants: Dict[str, Set[str]] = {}
    series_explicit_geos: Dict[str, Set[str]] = {}
    series_base_geos: Dict[str, Set[str]] = {}

    _INSTITUTIONAL_SERIES_KEYWORDS = frozenset({
        "SFR", "PROPCO", "BORROWER", "ASSETS", "PORTFOLIO", "RENTAL",
        "HOMES", "RESIDENTIAL", "REALTOKEN", "LOTUS", "EXCEL", "PROGRAM",
    })
    _ZIP5_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\s*$")

    for p in properties:
        b = p.get("bbl")
        if not b or b not in bbl_to_root:
            continue
        raw_nm = p.get("owner_name") or ""
        if pub_check(raw_nm):
            continue
        fam_key = extract_owner_family_key(raw_nm) or (p.get("owner_name_norm") or raw_nm)

        spv_stem, has_explicit_series, is_generic_landscape = extract_series_stem_info(raw_nm)
        if spv_stem:
            geo_scope = (p.get("county") or p.get("borough") or "").strip().upper()
            bucket_key = f"{spv_stem}||{geo_scope}" if (is_generic_landscape and geo_scope) else spv_stem
            norm_biz = normalize_business_name(raw_nm)
            by_series_spv.setdefault(bucket_key, []).append(b)
            geo_tokens: Set[str] = set()
            if geo_scope:
                geo_tokens.add(f"G:{geo_scope}")
            muni = (p.get("municipality") or "").strip().upper()
            if muni:
                geo_tokens.add(f"M:{muni}")
            m_zip = _ZIP5_RE.search(str(p.get("mailing_address") or ""))
            if m_zip:
                geo_tokens.add(f"Z:{m_zip.group(1)}")
            if has_explicit_series:
                series_explicit_variants.setdefault(bucket_key, set()).add(norm_biz)
                series_explicit_geos.setdefault(bucket_key, set()).update(geo_tokens)
            else:
                series_base_variants.setdefault(bucket_key, set()).add(norm_biz)
                series_base_geos.setdefault(bucket_key, set()).update(geo_tokens)

        oj, on = p.get("oc_jurisdiction_code"), p.get("oc_company_number")
        if oj and on:
            info = oc_resolved.get((oj, on))
            nodes = info.get("reached_companies") if info else None
            if nodes:
                for node_id in nodes:
                    by_oc_node.setdefault(node_id, []).append(b)
            else:
                by_oc_node.setdefault(f"{oj}/{on}", []).append(b)

            if info:
                for hq in info.get("hq_addresses", ()):
                    by_oc_hq.setdefault(hq, []).append(b)
                    if fam_key:
                        hq_family_fanout.setdefault(hq, set()).add(fam_key)
                for h_name in info.get("humans", ()):
                    if not looks_like_person_owner(h_name):
                        continue
                    h_norm = normalize_owner_name(h_name)
                    if h_norm and h_norm not in generic_set and not pub_check(h_norm):
                        by_oc_human.setdefault(h_norm, []).append(b)
                        human_entity_fanout.setdefault(h_norm, set()).add((oj, on))

    merges = 0

    # 1. Shared OC entity / corporate parent / foreign branch
    for bbls in by_oc_node.values():
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other, max_unrelated_bbls=1500, max_families=60):
                merges += 1

    # 2. Shared verified non-agent corporate HQ / principal-office / mailing address
    for hq, bbls in by_oc_hq.items():
        fams = hq_family_fanout.get(hq, set())
        max_fams_at_hq = 4 if hq.startswith("PO BOX ") else 25
        if len(fams) > max_fams_at_hq:
            continue
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other, max_unrelated_bbls=1500, max_families=25):
                merges += 1

    # 3. Shared OC beneficial human principal
    for h_norm, bbls in by_oc_human.items():
        fanout = len(human_entity_fanout.get(h_norm, ()))
        tokens = h_norm.split()
        max_fanout = 60 if len(tokens) >= 3 else 20
        if fanout > max_fanout:
            continue
        max_fams = 80 if len(tokens) >= 3 else 30
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other, max_unrelated_bbls=1500, max_families=max_fams):
                merges += 1

    # 4. Corroborated corporate series & container-suffix siblings (unifies numbered/Roman/year
    #    tranches AND their unnumbered base entity, while scoping generic landscape stems by county)
    for bucket_key, bbls in by_series_spv.items():
        exp_vars = series_explicit_variants.get(bucket_key, set())
        base_vars = series_base_variants.get(bucket_key, set())
        total_vars = len(exp_vars | base_vars)
        if total_vars < 2 or total_vars > 500:
            continue
        raw_stem = bucket_key.split("||", 1)[0]
        stem_words = set(raw_stem.split())
        has_inst_kw = bool(stem_words & _INSTITUTIONAL_SERIES_KEYWORDS)

        allow_series_merge = False
        if len(exp_vars) >= 2:
            # >=2 distinct numbered/lettered/Roman/year series tranches (e.g. Crestwood 1/2/3/IV,
            # Tricon SFR 2020..2026, FKH SFR Propco A..Q, Shelby Company 2/3 + base Shelby Company)
            allow_series_merge = True
        elif len(exp_vars) == 1 and len(base_vars) >= 1:
            # 1 numbered tranche + its unnumbered base entity: require geographic/ZIP overlap or SPV keyword
            exp_geos = series_explicit_geos.get(bucket_key, set())
            b_geos = series_base_geos.get(bucket_key, set())
            if has_inst_kw or bool(exp_geos & b_geos):
                allow_series_merge = True
        elif len(exp_vars) == 0 and len(base_vars) >= 2 and "||" not in bucket_key and total_vars <= 8:
            # Non-generic multi-word corporate name differing only by trailing legal/container suffix
            # (e.g. 'CHAMPLAIN HOUSING TRUST' + 'CHAMPLAIN HOUSING TRUST INC') in overlapping geography
            b_geos = series_base_geos.get(bucket_key, set())
            if len(b_geos) >= 1:
                allow_series_merge = True

        if not allow_series_merge:
            continue
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other, max_unrelated_bbls=25_000, max_families=10):
                merges += 1

    merged_groups: Dict[str, List[dict]] = {}
    for r, props in root_to_props.items():
        merged_groups.setdefault(find_r(r), []).extend(props)
    return merged_groups, merges


def link_jurisdiction_parcels_to_oc(
    conn,
    jurisdiction: str,
    parcel_state: str,
    *,
    col_max_len: Optional[int] = None,
) -> Dict[str, int]:
    """Batch-link `{jurisdiction}_properties` against `oc_companies` / `oc_previous_names`
    when OpenCorporates data is populated. Safely no-ops with zero updates if `oc_companies` is empty."""
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM oc_companies LIMIT 1)")
        has_oc_data = bool(cur.fetchone()[0])
    if not has_oc_data:
        return {"evaluated": 0, "matched": 0, "tier1": 0, "tier2": 0, "tier3": 0, "tier4": 0, "quarantined": 0}

    ra_addrs = load_oc_registered_agent_addresses(conn)
    table = f"{jurisdiction}_properties"

    demangled_by_bbl: Dict[str, AssessorOwnerRecord] = {}
    coarse_by_bbl: Dict[str, str] = {}
    sale_by_bbl: Dict[str, Optional[dt.date]] = {}
    existing_oc_by_bbl: Dict[str, Tuple[Optional[str], Optional[str], Optional[int]]] = {}
    lookup_norms: Set[str] = set()
    lookup_prefixes: Set[str] = set()
    scoped_jurs: Set[str] = {f"us_{parcel_state.lower()}", *FOREIGN_FORMATION_HUBS}

    with conn.cursor(name=f"oc_link_scan_{jurisdiction}", cursor_factory=RealDictCursor) as scan_cur:
        scan_cur.itersize = 50000
        scan_cur.execute(
            f"""
            SELECT bbl, owner_name, owner_co_name, owner_is_truncated,
                   mailing_address, mailing_address_coarse, mailing_state, last_sale_date,
                   oc_jurisdiction_code, oc_company_number, oc_match_tier
            FROM {table}
            WHERE owner_name IS NOT NULL
            """
        )
        for row in scan_cur:
            bbl = row["bbl"]
            existing_oc_by_bbl[bbl] = (
                row.get("oc_jurisdiction_code"),
                row.get("oc_company_number"),
                row.get("oc_match_tier"),
            )
            rec = demangle_assessor_owner(
                row.get("owner_name"),
                care_of=row.get("owner_co_name"),
                mailing_address=row.get("mailing_address"),
                explicit_mailing_state=row.get("mailing_state"),
                col_max_len=col_max_len,
            )
            if row.get("owner_is_truncated") and not rec.is_truncated:
                rec = AssessorOwnerRecord(
                    raw_owner=rec.raw_owner,
                    cleaned_owner=rec.cleaned_owner,
                    owner_norm=rec.owner_norm,
                    is_business=rec.is_business,
                    care_of_entity=rec.care_of_entity,
                    is_truncated=True,
                    prefix18=rec.prefix18,
                    mailing_state=rec.mailing_state,
                )
            if not rec.is_business or not rec.owner_norm:
                continue
            demangled_by_bbl[bbl] = rec
            coarse_by_bbl[bbl] = (
                row.get("mailing_address_coarse")
                or normalize_mailing_address_coarse(row.get("mailing_address") or "")
            )
            sale_by_bbl[bbl] = row.get("last_sale_date")
            lookup_norms.add(rec.owner_norm)
            if rec.is_truncated and rec.prefix18:
                lookup_prefixes.add(rec.prefix18)
            if rec.mailing_state:
                scoped_jurs.add(f"us_{rec.mailing_state.lower()}")

    if not demangled_by_bbl:
        return {"evaluated": 0, "matched": 0, "tier1": 0, "tier2": 0, "tier3": 0, "tier4": 0, "quarantined": 0}

    candidates_by_norm: Dict[str, List[OCCandidate]] = {}
    candidates_by_prefix18: Dict[str, List[OCCandidate]] = {}
    jur_list = sorted(scoped_jurs)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        norm_list = sorted(lookup_norms)
        for idx in range(0, len(norm_list), 5000):
            chunk = norm_list[idx : idx + 5000]
            cur.execute(
                """
                SELECT c.*,
                       ARRAY(
                           SELECT DISTINCT o.address_coarse
                           FROM oc_officers o
                           WHERE o.jurisdiction_code = c.jurisdiction_code
                             AND o.company_number = c.company_number
                             AND o.role_class IN ('beneficial_principal', 'corporate_parent')
                             AND o.address_coarse IS NOT NULL AND o.address_coarse != ''
                       ) AS officer_addrs
                FROM oc_companies c
                WHERE c.jurisdiction_code = ANY(%s)
                  AND c.name_norm = ANY(%s)
                """,
                (jur_list, chunk),
            )
            for r in cur.fetchall():
                cand = OCCandidate(
                    jurisdiction_code=r["jurisdiction_code"],
                    company_number=r["company_number"],
                    name=r["name"],
                    name_norm=r["name_norm"],
                    is_active=bool(r.get("is_active", True)),
                    incorporation_date=r.get("incorporation_date"),
                    dissolution_date=r.get("dissolution_date"),
                    registered_address_coarse=r.get("registered_address_coarse") or "",
                    mailing_address_coarse=r.get("mailing_address_coarse") or "",
                    agent_address_coarse=r.get("agent_address_coarse") or "",
                    officer_addresses_coarse=tuple(r.get("officer_addrs") or ()),
                    home_jurisdiction_code=r.get("home_jurisdiction_code"),
                    home_company_number=r.get("home_company_number"),
                )
                candidates_by_norm.setdefault(cand.name_norm, []).append(cand)

            # Historical previous names & trading/DBA names
            cur.execute(
                """
                SELECT p.company_name_norm AS prev_norm, p.end_date AS prev_end_date,
                       p.name_type AS prev_name_type, c.*
                FROM oc_previous_names p
                JOIN oc_companies c
                  ON c.jurisdiction_code = p.jurisdiction_code
                 AND c.company_number = p.company_number
                WHERE p.jurisdiction_code = ANY(%s)
                  AND p.company_name_norm = ANY(%s)
                """,
                (jur_list, chunk),
            )
            for r in cur.fetchall():
                cand = OCCandidate(
                    jurisdiction_code=r["jurisdiction_code"],
                    company_number=r["company_number"],
                    name=r["name"],
                    name_norm=r["name_norm"],
                    is_active=bool(r.get("is_active", True)),
                    incorporation_date=r.get("incorporation_date"),
                    dissolution_date=r.get("dissolution_date"),
                    registered_address_coarse=r.get("registered_address_coarse") or "",
                    mailing_address_coarse=r.get("mailing_address_coarse") or "",
                    agent_address_coarse=r.get("agent_address_coarse") or "",
                    home_jurisdiction_code=r.get("home_jurisdiction_code"),
                    home_company_number=r.get("home_company_number"),
                    matched_previous_name_norm=r["prev_norm"],
                    previous_name_end_date=r.get("prev_end_date"),
                    matched_name_type=r.get("prev_name_type"),
                )
                candidates_by_norm.setdefault(r["prev_norm"], []).append(cand)

        if lookup_prefixes:
            pref_list = sorted(lookup_prefixes)
            for idx in range(0, len(pref_list), 2000):
                pchunk = pref_list[idx : idx + 2000]
                cur.execute(
                    """
                    SELECT c.*
                    FROM oc_companies c
                    WHERE c.jurisdiction_code = ANY(%s)
                      AND c.name_prefix18 = ANY(%s)
                    """,
                    (jur_list, pchunk),
                )
                for r in cur.fetchall():
                    cand = OCCandidate(
                        jurisdiction_code=r["jurisdiction_code"],
                        company_number=r["company_number"],
                        name=r["name"],
                        name_norm=r["name_norm"],
                        is_active=bool(r.get("is_active", True)),
                        registered_address_coarse=r.get("registered_address_coarse") or "",
                        mailing_address_coarse=r.get("mailing_address_coarse") or "",
                        agent_address_coarse=r.get("agent_address_coarse") or "",
                        home_jurisdiction_code=r.get("home_jurisdiction_code"),
                        home_company_number=r.get("home_company_number"),
                    )
                    if r.get("name_prefix18"):
                        candidates_by_prefix18.setdefault(r["name_prefix18"], []).append(cand)

    updates: List[Tuple[Optional[str], Optional[str], Optional[int], Optional[float], str]] = []
    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    quarantined = 0
    total_matched = 0

    # Clear stale OC links on newly excluded 3rd-party parcels
    for bbl, (ex_jur, ex_num, _) in existing_oc_by_bbl.items():
        if bbl not in demangled_by_bbl and (ex_jur is not None or ex_num is not None):
            updates.append((None, None, None, None, bbl))

    for bbl, rec in demangled_by_bbl.items():
        pool: List[OCCandidate] = list(candidates_by_norm.get(rec.owner_norm, ()))
        if rec.is_truncated and rec.prefix18:
            pool.extend(candidates_by_prefix18.get(rec.prefix18, ()))
        match = score_oc_candidates(
            rec,
            parcel_state=parcel_state,
            parcel_mailing_coarse=coarse_by_bbl.get(bbl),
            candidates=pool,
            parcel_last_sale_date=sale_by_bbl.get(bbl),
            registered_agent_addrs=ra_addrs,
        )
        ex_jur, ex_num, ex_tier = existing_oc_by_bbl.get(bbl, (None, None, None))
        if match is not None:
            total_matched += 1
            tier_counts[match.match_tier] = tier_counts.get(match.match_tier, 0) + 1
            if (
                ex_jur != match.canonical_jurisdiction_code
                or ex_num != match.canonical_company_number
                or ex_tier != match.match_tier
            ):
                updates.append(
                    (
                        match.canonical_jurisdiction_code,
                        match.canonical_company_number,
                        match.match_tier,
                        match.confidence,
                        bbl,
                    )
                )
        else:
            quarantined += 1
            if ex_jur is not None or ex_num is not None:
                updates.append((None, None, None, None, bbl))

    if updates:
        with conn.cursor() as cur:
            for idx in range(0, len(updates), 5000):
                batch = updates[idx : idx + 5000]
                execute_values(
                    cur,
                    f"""
                    UPDATE {table} AS p
                    SET oc_jurisdiction_code = v.jur,
                        oc_company_number = v.cnum,
                        oc_match_tier = v.tier::smallint,
                        oc_match_confidence = v.conf::numeric
                    FROM (VALUES %s) AS v(jur, cnum, tier, conf, bbl)
                    WHERE p.bbl = v.bbl
                    """,
                    batch,
                )
        conn.commit()

    return {
        "evaluated": len(demangled_by_bbl),
        "matched": total_matched,
        "updated_rows": len(updates),
        "tier1": tier_counts[1],
        "tier2": tier_counts[2],
        "tier3": tier_counts[3],
        "tier4": tier_counts[4],
        "quarantined": quarantined,
    }
