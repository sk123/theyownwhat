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

from api.linking_rules import (
    get_learned_officer_role_suffixes,
    get_learned_ra_allowlist,
    get_learned_third_party_hubs,
)
from api.shared_utils import (
    US_STATE_CODES,
    canonicalize_business_name,
    canonicalize_person_name,
    extract_corroborated_series_spv_key,
    extract_owner_family_key,
    extract_postal_street_zip_key,
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
    if m:
        candidate = nm[: m.start()].strip(" ,;-")
        if candidate and looks_like_person_owner(candidate):
            return candidate
    learned_roles = get_learned_officer_role_suffixes()
    if learned_roles:
        nm_up = nm.upper()
        for role_tok in sorted(learned_roles, key=len, reverse=True):
            if nm_up.endswith(" " + role_tok):
                candidate = nm[: len(nm) - len(role_tok)].strip(" ,;-")
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

        # --- Tier 2b (CT parity): Legal-suffix-stripped & canonical business stem match ---
        # Links assessor rolls that omit trailing legal suffixes ('CHESTNUT HILL REALTY' <->
        # 'CHESTNUT HILL REALTY CORP', 'HAMILTON UNION ASSOCIATES' <-> 'HAMILTON UNION ASSOCIATES LP')
        # without conflating distinct 'REALTY' vs 'PROPERTIES' entities unless address-corroborated.
        owner_legal_stem = re.sub(r"^THE\s+", "", _CORPORATE_SUFFIX_STRIP_RE.sub("", demangled.owner_norm).strip()).strip()
        owner_canon_biz = canonicalize_business_name(demangled.cleaned_owner)
        if owner_legal_stem and len(owner_legal_stem) >= 8 and len(owner_legal_stem.split()) >= 2:
            legal_stem_cands = [
                c for c in scoped
                if c.matched_previous_name_norm is None
                and re.sub(r"^THE\s+", "", _CORPORATE_SUFFIX_STRIP_RE.sub("", c.name_norm).strip()).strip() == owner_legal_stem
            ]
            addr_stem_cands = [
                c for c in legal_stem_cands
                if _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
            ]
            if addr_stem_cands:
                canon_addr_ids = {
                    (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
                    for c in addr_stem_cands
                }
                if len(canon_addr_ids) == 1:
                    local_cb = [c for c in addr_stem_cands if c.is_active and c.jurisdiction_code == local_jur]
                    chosen = local_cb[0] if local_cb else addr_stem_cands[0]
                    return _build_match_result(chosen, 1, 0.95, "legal_stem_with_address")
            if len(owner_legal_stem) >= 10 and looks_like_business_name(demangled.cleaned_owner):
                active_local_ls = [
                    c for c in legal_stem_cands
                    if c.is_active and c.jurisdiction_code == local_jur
                ]
                canon_local_ids = {
                    (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
                    for c in active_local_ls
                }
                if len(canon_local_ids) == 1 and active_local_ls:
                    return _build_match_result(active_local_ls[0], 2, 0.89, "unique_active_legal_stem")

        if owner_canon_biz and len(owner_canon_biz) >= 8 and len(owner_canon_biz.split()) >= 2:
            canon_addr_only = [
                c for c in scoped
                if c.matched_previous_name_norm is None
                and canonicalize_business_name(c.name) == owner_canon_biz
                and _candidate_matches_parcel_address(c, parcel_addrs, ra_addrs)
            ]
            if canon_addr_only:
                canon_cb_ids = {
                    (c.home_jurisdiction_code or c.jurisdiction_code, c.home_company_number or c.company_number)
                    for c in canon_addr_only
                }
                if len(canon_cb_ids) == 1:
                    local_cb = [c for c in canon_addr_only if c.is_active and c.jurisdiction_code == local_jur]
                    chosen = local_cb[0] if local_cb else canon_addr_only[0]
                    return _build_match_result(chosen, 1, 0.93, "canonical_business_stem_with_address")

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
    """Populate `oc_registered_agent_addresses` using quota-free structural 3rd-party
    intermediary signals rather than raw company counts (so a genuine landlord serving
    as Resident Agent for its own 400 LLCs at its own HQ — e.g. 39 Brighton Ave Allston,
    1 Washington Mall Boston, 60 Leo Birmingham Pkwy, 300 Independence Dr, 399 Whalley Ave —
    is NEVER falsely blacklisted as a commercial registered agent).

    An address with >= `min_distinct_companies` filings is flagged ONLY when:
      (a) >= 5 filings name a known commercial RA / law / CPA firm in `agent_name`, OR
      (b) >= 10 external statutory-agent clients (`agent_address_coarse == addr` while the
          company's own `mailing_address_coarse` / `registered_address_coarse` is elsewhere,
          accounting for >= 40% of agent filings at `addr`), OR
      (c) >= 5 distinct out-of-state `home_jurisdiction_code` charters share `addr`
          (CT Corp / CSC / Cogency foreign-branch service-of-process hub), OR
      (d) >= 12 officer-bearing companies at `addr` have mutually dispersed officers
          (the single most common beneficial/corporate-parent officer covers < 20% of them).
    """
    with conn.cursor() as cur:
        cur.execute("SET LOCAL work_mem = '256MB'")
        cur.execute(
            """
            DELETE FROM oc_registered_agent_addresses
            WHERE source IN ('empirical_oc_agent_hub', 'empirical_oc_shared_office_hub', 'empirical_oc_address_hub')
            """
        )
        cur.execute(
            """
            DELETE FROM oc_registered_agent_addresses
            WHERE source IN ('empirical_oc_agent_hub', 'empirical_oc_shared_office_hub');
            """
        )
        # Step 1: Single-pass scan over `oc_companies` computing structural RA & foreign-branch stats
        cur.execute(
            """
            CREATE TEMP TABLE _ra_cand_stats ON COMMIT DROP AS
            SELECT
                v.addr_coarse,
                MAX(v.sample_raw) AS sample_raw_address,
                COUNT(*)::int AS total_cos,
                COUNT(*) FILTER (WHERE v.is_agent_slot)::int AS agent_cos,
                COUNT(*) FILTER (
                    WHERE v.is_agent_slot
                      AND hq.own_hq_coarse IS NOT NULL
                      AND hq.own_hq_coarse != ''
                      AND hq.own_hq_coarse != v.addr_coarse
                )::int AS external_agent_clients,
                COUNT(DISTINCT c.home_jurisdiction_code) FILTER (
                    WHERE c.home_jurisdiction_code IS NOT NULL
                      AND c.home_jurisdiction_code != ''
                      AND c.home_jurisdiction_code != c.jurisdiction_code
                )::int AS foreign_home_jurs,
                COUNT(*) FILTER (
                    WHERE c.agent_name IS NOT NULL
                      AND c.agent_name ~* '(CORPORATION SERVICE|\\mCSC\\M|C\\.?\\s*T\\.?\\s*CORP|COGENCY|INCORP\\s+SERVICES|REGISTERED\\s+AGENTS?\\s+INC|NORTHWEST\\s+REGISTERED|NATIONAL\\s+REGISTERED|UNITED\\s+AGENT|LEGALINC|LEGALZOOM|PARACORP|CAPITOL\\s+CORPORATE|VCORP|ATTORNEYS?\\s+AT\\s+LAW|LAW\\s+(?:FIRM|OFFICES?|GROUP|PC|PLLC))'
                )::int AS known_ra_agent_cos
            FROM oc_companies c
            CROSS JOIN LATERAL (
                SELECT COALESCE(NULLIF(c.mailing_address_coarse, ''), NULLIF(c.registered_address_coarse, '')) AS own_hq_coarse
            ) hq
            CROSS JOIN LATERAL (
                SELECT c.agent_address_coarse AS addr_coarse, c.agent_address_raw AS sample_raw, TRUE AS is_agent_slot
                WHERE c.agent_address_coarse IS NOT NULL AND c.agent_address_coarse != ''
                UNION ALL
                SELECT c.registered_address_coarse, c.registered_address_raw, FALSE
                WHERE c.registered_address_coarse IS NOT NULL AND c.registered_address_coarse != ''
                  AND c.registered_address_coarse IS DISTINCT FROM c.agent_address_coarse
                UNION ALL
                SELECT c.mailing_address_coarse, c.mailing_address_raw, FALSE
                WHERE c.mailing_address_coarse IS NOT NULL AND c.mailing_address_coarse != ''
                  AND c.mailing_address_coarse IS DISTINCT FROM c.agent_address_coarse
                  AND c.mailing_address_coarse IS DISTINCT FROM c.registered_address_coarse
            ) v
            GROUP BY v.addr_coarse
            HAVING COUNT(*) >= %s;
            """,
            (min_distinct_companies,),
        )

        # Step 2: Insert deterministic commercial RA & multi-charter foreign-branch hubs immediately
        # (never flag self-registered landlord/corporate HQs like 1 Washington Mall, 60 Leo Birmingham Pkwy,
        # or 269 Mill Rd simply because property LLCs list site addresses in `registered_address_coarse`).
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
                addr_coarse,
                sample_raw_address,
                total_cos,
                NULL::text[],
                CASE
                    WHEN known_ra_agent_cos >= 5 THEN 'empirical_oc_agent_hub'
                    ELSE 'empirical_oc_shared_office_hub'
                END,
                NOW()
            FROM _ra_cand_stats
            WHERE known_ra_agent_cos >= 5
               OR (foreign_home_jurs >= 5 AND total_cos >= 15)
            ON CONFLICT (address_coarse) DO UPDATE SET
                sample_raw_address = EXCLUDED.sample_raw_address,
                distinct_company_count = EXCLUDED.distinct_company_count,
                source = EXCLUDED.source,
                computed_at = NOW();
            """
        )
        upserted = cur.rowcount

        # Step 3: For remaining external-agent candidates (`external_agent_clients >= 10`) OR large shared
        # offices (`total_cos >= 35`), sample up to 80 companies per address and check officer dispersion
        # via `idx_oc_officers_company`. Flag ONLY when officers are genuinely dispersed (`top_officer_share < 0.25`).
        cur.execute(
            """
            CREATE TEMP TABLE _ra_need_off ON COMMIT DROP AS
            SELECT addr_coarse, sample_raw_address, total_cos,
                   (external_agent_clients >= 10 AND external_agent_clients >= 0.40 * GREATEST(agent_cos, 1)) AS is_ext_agent
            FROM _ra_cand_stats
            WHERE (
                  total_cos >= 35
                  OR (external_agent_clients >= 10 AND external_agent_clients >= 0.40 * GREATEST(agent_cos, 1))
              )
              AND NOT (
                  known_ra_agent_cos >= 5
                  OR (foreign_home_jurs >= 5 AND total_cos >= 15)
              );
            CREATE UNIQUE INDEX ON _ra_need_off (addr_coarse);
            """
        )
        cur.execute("SELECT COUNT(*) FROM _ra_need_off;")
        need_off_cnt = cur.fetchone()[0]
        if need_off_cnt:
            cur.execute(
                """
                WITH sampled_cos AS (
                    SELECT addr_coarse, jurisdiction_code, company_number
                    FROM (
                        SELECT v.addr_coarse, c.jurisdiction_code, c.company_number,
                               ROW_NUMBER() OVER (PARTITION BY v.addr_coarse) AS rn
                        FROM oc_companies c
                        CROSS JOIN LATERAL (
                            SELECT c.agent_address_coarse AS addr_coarse
                            WHERE c.agent_address_coarse IS NOT NULL AND c.agent_address_coarse != ''
                            UNION ALL
                            SELECT c.registered_address_coarse
                            WHERE c.registered_address_coarse IS NOT NULL AND c.registered_address_coarse != ''
                              AND c.registered_address_coarse IS DISTINCT FROM c.agent_address_coarse
                            UNION ALL
                            SELECT c.mailing_address_coarse
                            WHERE c.mailing_address_coarse IS NOT NULL AND c.mailing_address_coarse != ''
                              AND c.mailing_address_coarse IS DISTINCT FROM c.agent_address_coarse
                              AND c.mailing_address_coarse IS DISTINCT FROM c.registered_address_coarse
                        ) v
                        JOIN _ra_need_off n ON n.addr_coarse = v.addr_coarse
                    ) ranked
                    WHERE rn <= 80
                ),
                co_off AS (
                    SELECT sc.addr_coarse, sc.jurisdiction_code, sc.company_number, o.name_norm
                    FROM sampled_cos sc
                    JOIN oc_officers o
                      ON o.jurisdiction_code = sc.jurisdiction_code
                     AND o.company_number = sc.company_number
                    WHERE COALESCE(o.is_current, TRUE) = TRUE
                      AND o.role_class IN ('beneficial_principal', 'corporate_parent')
                      AND o.name_norm IS NOT NULL AND o.name_norm != ''
                ),
                totals AS (
                    SELECT addr_coarse, COUNT(DISTINCT (jurisdiction_code, company_number))::int AS cos_with_off
                    FROM co_off
                    GROUP BY addr_coarse
                ),
                top_off AS (
                    SELECT addr_coarse, MAX(off_cos)::int AS top_off_cos
                    FROM (
                        SELECT addr_coarse, name_norm, COUNT(DISTINCT (jurisdiction_code, company_number)) AS off_cos
                        FROM co_off
                        GROUP BY addr_coarse, name_norm
                    ) s
                    GROUP BY addr_coarse
                )
                INSERT INTO oc_registered_agent_addresses (
                    address_coarse,
                    sample_raw_address,
                    distinct_company_count,
                    distinct_agent_names,
                    source,
                    computed_at
                )
                SELECT
                    n.addr_coarse,
                    n.sample_raw_address,
                    n.total_cos,
                    NULL::text[],
                    CASE WHEN n.is_ext_agent THEN 'empirical_oc_agent_hub' ELSE 'empirical_oc_shared_office_hub' END,
                    NOW()
                FROM _ra_need_off n
                JOIN totals t ON t.addr_coarse = n.addr_coarse
                JOIN top_off to2 ON to2.addr_coarse = n.addr_coarse
                WHERE t.cos_with_off >= 10
                  AND (to2.top_off_cos::float / t.cos_with_off) < 0.25
                ON CONFLICT (address_coarse) DO UPDATE SET
                    sample_raw_address = EXCLUDED.sample_raw_address,
                    distinct_company_count = EXCLUDED.distinct_company_count,
                    source = EXCLUDED.source,
                    computed_at = NOW();
                """
            )
            upserted += cur.rowcount
            allowlist = sorted(get_learned_ra_allowlist())
            if allowlist:
                cur.execute(
                    "DELETE FROM oc_registered_agent_addresses WHERE address_coarse = ANY(%s)",
                    (allowlist,),
                )
    conn.commit()
    return upserted


def load_oc_registered_agent_addresses(conn) -> Set[str]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT address_coarse FROM oc_registered_agent_addresses")
            addrs = {row[0] for row in cur.fetchall() if row and row[0]}
        addrs |= set(get_learned_third_party_hubs())
        addrs -= set(get_learned_ra_allowlist())
        return addrs
    except Exception:
        conn.rollback()
        return set(get_learned_third_party_hubs()) - set(get_learned_ra_allowlist())


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
    MAX_CUMULATIVE_ROLLUP_OFFICERS = 24
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
            company_agent_person_norm: Dict[Tuple[str, str], str] = {}

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
                           c.agent_name, c.agent_address_raw, c.agent_address_coarse,
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

                    raw_agent_nm = (row.get("agent_name") or "").strip()
                    if raw_agent_nm and not looks_like_business_name(raw_agent_nm):
                        ag_pnorm = normalize_person_name(clean_oc_officer_name(raw_agent_nm))
                        if ag_pnorm:
                            company_agent_person_norm[key] = ag_pnorm

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

                    raw_mail_cand = row.get("mailing_address_raw")
                    if raw_mail_cand:
                        # On a non-seed foreign branch of a non-shell domestic charter (e.g. `us_wa` Costco
                        # registered as foreign in `us_mn`), an in-branch-state mailing address ('405 2 AVE S MPLS MN')
                        # is a local statutory service-of-process agent, whereas an out-of-branch home-state HQ
                        # ('269 MILL RD CHELMSFORD MA' on a `us_pa` foreign branch of `us_ma`) is executive HQ.
                        allow_branch_mail = True
                        if is_foreign_branch and key not in seed_set and hj and hj not in FOREIGN_FORMATION_HUBS:
                            branch_st = key[0].replace("us_", "").upper()
                            m_st = extract_state_from_address(raw_mail_cand)
                            if m_st and m_st == branch_st:
                                allow_branch_mail = False
                        if allow_branch_mail:
                            _harvest_hq_addr(key, raw_mail_cand, agent_coarse_set)
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
                           o.position, o.role_class, o.is_corporate_officer
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
                            # Suppress untitled Sunbiz/SOS officer rows that simply duplicate the filing's
                            # registered agent (e.g. `agent_name='BRIAN BUSSEY'` + `position=NULL`).
                            if not (row.get("position") or "").strip():
                                ag_nm = company_agent_person_norm.get(ckey)
                                if ag_nm and normalize_person_name(nm) == ag_nm:
                                    continue
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

        # 6. CT-Parity Sideways Bipartite Hop:
        #    Walk from verified non-homonym human principals -> their OTHER in-state
        #    closely-held OC companies (even non-property-owning management/holding LLCs)
        #    -> those companies' non-agent HQs & co-principals, bridging parcel LLCs
        #    connected through an intermediate management/holding entity just like CT!
        seed_jurs = list({k[0] for k in seed_keys if k[0]})
        human_to_seed_ckeys: Dict[str, Set[Tuple[str, str]]] = {}
        for ckey, hset in direct_humans.items():
            if ckey in excluded_company_keys or ckey in all_nonprofit_keys:
                continue
            for raw_h in hset:
                hn = normalize_person_name(raw_h)
                toks = hn.split()
                if len(toks) < 2:
                    continue
                surname = toks[-1]
                if len(surname) < 4 or surname in _COMMON_HOMONYM_SURNAMES:
                    continue
                human_to_seed_ckeys.setdefault(hn, set()).add(ckey)

        if human_to_seed_ckeys and seed_jurs:
            cand_human_norms = sorted(human_to_seed_ckeys.keys())
            human_other_cos: Dict[str, Set[Tuple[str, str]]] = {}
            for hidx in range(0, len(cand_human_norms), 2000):
                hchunk = cand_human_norms[hidx : hidx + 2000]
                cur.execute(
                    """
                    SELECT o.name_norm, o.jurisdiction_code, o.company_number
                    FROM oc_officers o
                    WHERE o.name_norm = ANY(%s)
                      AND o.jurisdiction_code = ANY(%s)
                      AND o.role_class = 'beneficial_principal'
                      AND COALESCE(o.is_current, TRUE) = TRUE
                    """,
                    (hchunk, seed_jurs),
                )
                for row in cur.fetchall():
                    hn = row["name_norm"]
                    oc_k = (row["jurisdiction_code"], row["company_number"])
                    s = human_other_cos.setdefault(hn, set())
                    if len(s) <= 85:
                        s.add(oc_k)

            # Only traverse principals with bounded closely-held fan-out (2..80 companies)
            # so common names / serial agents never explode sideways.
            sib_to_humans: Dict[Tuple[str, str], Set[str]] = {}
            for hn, coset in human_other_cos.items():
                if not (2 <= len(coset) <= 80):
                    continue
                for oc_k in coset:
                    if oc_k in excluded_company_keys:
                        continue
                    sib_to_humans.setdefault(oc_k, set()).add(hn)

            new_sib_keys = [k for k in sib_to_humans if k not in visited]
            if new_sib_keys:
                sib_valid: Set[Tuple[str, str]] = set()
                sib_officers: Dict[Tuple[str, str], Set[str]] = {}
                for sidx in range(0, len(new_sib_keys), 4000):
                    schunk = new_sib_keys[sidx : sidx + 4000]
                    sj = [k[0] for k in schunk]
                    sn = [k[1] for k in schunk]
                    cur.execute(
                        """
                        SELECT c.jurisdiction_code, c.company_number, c.name, c.company_type,
                               c.is_nonprofit, c.registered_address_raw, c.mailing_address_raw,
                               c.agent_address_raw, c.agent_address_coarse
                        FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                        JOIN oc_companies c
                          ON c.jurisdiction_code = k.j AND c.company_number = k.n
                        WHERE COALESCE(c.is_active, TRUE) = TRUE
                        """,
                        (sj, sn),
                    )
                    for row in cur.fetchall():
                        sk = (row["jurisdiction_code"], row["company_number"])
                        cname = row.get("name") or ""
                        ctype = (row.get("company_type") or "").strip()
                        if (
                            (cname and is_excluded_third_party_entity(cname))
                            or (ctype and _EXCLUDED_OC_COMPANY_TYPE_RE.search(ctype))
                            or row.get("is_nonprofit")
                            or any(tok in ctype.upper() for tok in ("NONPROFIT", "NON-PROFIT", "CHARITABLE", "NOT-FOR-PROFIT"))
                        ):
                            excluded_company_keys.add(sk)
                            continue
                        if cname:
                            entity_labels[sk] = cname
                        ag_set: Set[str] = set()
                        if row.get("agent_address_coarse"):
                            ag_set.add(row["agent_address_coarse"])
                        if row.get("agent_address_raw"):
                            ac = normalize_mailing_address_coarse(strip_benign_care_of_prefix(row["agent_address_raw"]))
                            if ac:
                                ag_set.add(ac)
                        _harvest_hq_addr(sk, row.get("mailing_address_raw"), ag_set)
                        if "FOREIGN" not in ctype.upper():
                            _harvest_hq_addr(sk, row.get("registered_address_raw"), ag_set)
                        sib_valid.add(sk)

                    valid_chunk = [k for k in schunk if k in sib_valid]
                    if not valid_chunk:
                        continue
                    cur.execute(
                        """
                        SELECT o.jurisdiction_code, o.company_number, o.name, o.is_corporate_officer
                        FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                        JOIN oc_officers o
                          ON o.jurisdiction_code = k.j AND o.company_number = k.n
                        WHERE COALESCE(o.is_current, TRUE) = TRUE
                          AND o.role_class = 'beneficial_principal'
                        """,
                        ([k[0] for k in valid_chunk], [k[1] for k in valid_chunk]),
                    )
                    for row in cur.fetchall():
                        sk = (row["jurisdiction_code"], row["company_number"])
                        raw_nm = row.get("name") or ""
                        if row.get("is_corporate_officer") or counsel_re.search(raw_nm):
                            continue
                        nm = clean_oc_officer_name(raw_nm)
                        if nm and not is_excluded_third_party_entity(nm) and looks_like_person_owner(nm):
                            sib_officers.setdefault(sk, set()).add(nm)

                # Attach closely-held (<= 6 officers) intermediate companies onto seed companies
                # strictly WITHOUT passive-investor chaining across independent landlord empires:
                #   (a) `shared_hq`: intermediate company operates from the EXACT same verified
                #       non-agent HQ address as `seed_ck` (true co-located management/holding co).
                #   (b) `same_family_only`: every officer on `sk` shares `hn`'s surname (or sole
                #       principal), so `sk` can contribute its verified HQ without injecting
                #       unrelated co-investors who also invest with other landlords elsewhere.
                for sk, hnorms in sib_to_humans.items():
                    if sk in excluded_company_keys:
                        continue
                    sk_offs = sib_officers.get(sk) or direct_humans.get(sk) or set()
                    if len(sk_offs) > 6:
                        continue
                    sk_hqs = direct_hq_addrs.get(sk) or set()
                    sk_surnames = {
                        normalize_person_name(o).split()[-1]
                        for o in sk_offs
                        if normalize_person_name(o)
                    }
                    for hn in hnorms:
                        hn_surname = hn.split()[-1]
                        low_fanout = len(human_other_cos.get(hn) or ()) <= 25
                        same_family_only = bool(sk_surnames) and sk_surnames == {hn_surname}
                        for seed_ck in human_to_seed_ckeys.get(hn, ()):
                            seed_hqs = direct_hq_addrs.get(seed_ck) or set()
                            shared_hq = bool(sk_hqs and seed_hqs and (sk_hqs & seed_hqs))
                            if shared_hq:
                                parent_edges.setdefault(seed_ck, set()).add(sk)
                                if same_family_only and sk_offs and sk not in direct_humans:
                                    direct_humans[sk] = set(sk_offs)
                            elif low_fanout and same_family_only and sk_hqs:
                                parent_edges.setdefault(seed_ck, set()).add(sk)
                                if sk_offs and sk not in direct_humans:
                                    direct_humans[sk] = set(sk_offs)

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
        if len(humans) > MAX_CUMULATIVE_ROLLUP_OFFICERS:
            humans = set()
        resolved[seed] = {
            "humans": sorted(humans),
            "holding_entities": sorted(holdings),
            "reached_companies": sorted(f"{j}/{n}" for j, n in seen_nodes if (j, n) not in excluded_company_keys),
            "hq_addresses": sorted(hq_addrs),
            "raw_hq_addresses": sorted(raw_hq_addrs),
        }
    return resolved


_OC_PERSON_ROLE_STRIP_RE = re.compile(
    r"\b(?:TRUSTEES?|TRST|TTES?|TRS|TS|TT|TR|LE|JR|SR|II|III|IV|ESQ|ESQUIRE|MD|PHD|CPA)\b",
    re.IGNORECASE,
)

# High-frequency U.S. surnames where bare two-word officer strings (`JOHN SMITH`, `RONALD BROWN`,
# `DAVID KATZ`, `JAY PATEL`) require HQ or co-principal corroboration before sideways/sparse-HQ
# bridging so unrelated homonyms statewide never merge.
_COMMON_HOMONYM_SURNAMES = frozenset({
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
    "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON",
    "THOMAS", "TAYLOR", "MOORE", "JACKSON", "MARTIN", "LEE", "PEREZ", "THOMPSON",
    "WHITE", "HARRIS", "SANCHEZ", "CLARK", "RAMIREZ", "LEWIS", "ROBINSON", "WALKER",
    "YOUNG", "ALLEN", "KING", "WRIGHT", "SCOTT", "TORRES", "NGUYEN", "HILL",
    "FLORES", "GREEN", "ADAMS", "NELSON", "BAKER", "HALL", "RIVERA", "CAMPBELL",
    "MITCHELL", "CARTER", "ROBERTS", "PATEL", "SHAH", "SINGH", "CHEN", "WANG",
    "KIM", "PARK", "TRAN", "PHAM", "LIN", "LIU", "YANG", "WU", "HUANG", "ZHANG",
    "KATZ", "COHEN", "LEVY", "GOLDSTEIN", "FRIEDMAN", "SCHWARTZ", "MILLER", "MURPHY",
    "KELLY", "SULLIVAN", "WALSH", "RYAN", "O'BRIEN", "OBRIEN", "MCCARTHY",
})


def _extract_oc_person_mid_and_surname(raw_name: str) -> Tuple[str, str, List[str]]:
    """Return `(middle_initial, surname, ordered_clean_tokens)` for CT-parity person matching."""
    if not raw_name:
        return ("", "", [])
    s = str(raw_name).upper().strip()
    if "," in s:
        last_p, rest_p = s.split(",", 1)
        s = f"{rest_p.strip()} {last_p.strip()}"
    if " & " in s or " AND " in s or " / " in s or "+" in s:
        s = re.split(r"\s*(?:&|\+|/|\bAND\b)\s*", s, maxsplit=1)[0]
    s = re.sub(r"[,.'`\"]", "", s)
    s = _OC_PERSON_ROLE_STRIP_RE.sub(" ", s)
    raw_toks = [t for t in s.split() if t]
    mid = ""
    if len(raw_toks) == 3 and len(raw_toks[1]) == 1 and raw_toks[1].isalpha():
        mid = raw_toks[1]
    norm = normalize_person_name(raw_name)
    ordered = norm.split() if norm else []
    surname = ordered[-1] if len(ordered) >= 2 else ""
    return (mid, surname, ordered)


def cluster_with_oc_and_series(
    properties: Sequence[dict],
    network_groups: Dict[str, List[dict]],
    oc_resolved: Dict[Tuple[str, str], Dict[str, List[str]]],
    *,
    is_public_fn=None,
    generic_owner_names: Iterable[str] = (),
) -> Tuple[Dict[str, List[dict]], int]:
    """Unified CT-parity corporate-registry, direct-principal/trustee, co-principal &
    corporate-series network clustering with ZERO artificial family/entity count quotas:
      1. Shared OC entity / corporate-parent / foreign-branch graph (`reached_companies`)
      2. Shared verified non-agent corporate HQ (`hq_addresses`, ZIP-unified + island-dispersion guarded)
      3. Order-independent canonical OC human principal (`canonicalize_person_name`), middle-name
         variant alignment (`ALEXANDRA ANN SAMIA SOLIA` <-> `ALEXANDRA SAMIA SOLIA`), direct
         human/trustee parcel <-> corporate officer bridging, and same-surname co-principal bridges,
         guarded by structural HQ/co-principal cohesion rather than manufactured LLC quotas.
      4. Corroborated multi-word numbered corporate series SPVs (`extract_series_stem_info`).
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
    comp_pub: Dict[str, bool] = {}
    comp_hqs: Dict[str, Set[str]] = {}
    comp_humans: Dict[str, Set[str]] = {}
    for r, props in root_to_props.items():
        is_pub = False
        hq_set: Set[str] = set()
        for p in props:
            raw_nm = p.get("owner_name") or ""
            norm_nm = p.get("owner_name_norm") or raw_nm
            if pub_check(raw_nm) or pub_check(norm_nm):
                is_pub = True
            bk = p.get("_building_key") or p.get("_coarse_mail")
            if bk:
                hq_set.add(bk)
        comp_pub[r] = is_pub
        comp_hqs[r] = hq_set
        comp_humans[r] = set()

    def find_r(r: str) -> str:
        while parent_root[r] != r:
            parent_root[r] = parent_root[parent_root[r]]
            r = parent_root[r]
        return r

    def union_r(
        ra: str,
        rb: str,
        max_total_bbls: int = 25_000,
        *,
        require_hq_compat: bool = False,
    ) -> bool:
        ua, ub = find_r(ra), find_r(rb)
        if ua == ub or comp_pub.get(ua) or comp_pub.get(ub):
            return False
        if comp_size[ua] + comp_size[ub] > max_total_bbls:
            return False
        if require_hq_compat and comp_size[ua] >= 15 and comp_size[ub] >= 15:
            hqs_a = comp_hqs.get(ua)
            hqs_b = comp_hqs.get(ub)
            if hqs_a and hqs_b and not (hqs_a & hqs_b):
                # Do not chain two independent >=15-building empires at completely disjoint
                # HQs via a single shared officer/attorney/JV unless >=2 beneficial principals overlap.
                if len(comp_humans.get(ua, set()) & comp_humans.get(ub, set())) < 2:
                    return False
        parent_root[ua] = ub
        comp_size[ub] += comp_size[ua]
        comp_hqs[ub].update(comp_hqs[ua])
        comp_humans[ub].update(comp_humans[ua])
        return True

    # Unify neighborhood aliases on OC `raw_hq_addresses` via `(coarse_street, zip5)`
    hq_zip_counts: Dict[str, Dict[str, int]] = {}
    for info in oc_resolved.values():
        for raw_hq in info.get("raw_hq_addresses", ()):
            zk = extract_postal_street_zip_key(raw_hq)
            chq = normalize_mailing_address_coarse(raw_hq)
            if zk and chq:
                zm = hq_zip_counts.setdefault(zk, {})
                zm[chq] = zm.get(chq, 0) + 1
    hq_coarse_alias: Dict[str, str] = {}
    for zk, cmap in hq_zip_counts.items():
        if len(cmap) > 1:
            canon_hq = max(cmap.items(), key=lambda kv: (kv[1], len(kv[0]), kv[0]))[0]
            for cv in cmap:
                hq_coarse_alias[cv] = canon_hq

    # Pre-index active OC human principals & middle initials across `oc_resolved`
    active_oc_canons: Set[str] = set()
    oc_canon_mids: Dict[str, Set[str]] = {}
    oc_canon_ordered_variants: Dict[str, Set[Tuple[str, ...]]] = {}
    oc_canon_hqs: Dict[str, Set[str]] = {}
    oc_canon_surnames: Dict[str, Set[str]] = {}
    oc_canon_primary_surname: Dict[str, str] = {}
    for (oj, on), info in oc_resolved.items():
        ent_hqs = {hq_coarse_alias.get(h, h) for h in info.get("hq_addresses", ()) if h}
        for h_name in info.get("humans", ()):
            if not looks_like_person_owner(h_name):
                continue
            hc = canonicalize_person_name(h_name)
            if not hc or hc in generic_set or pub_check(hc):
                continue
            mid, surname, ordered = _extract_oc_person_mid_and_surname(h_name)
            active_oc_canons.add(hc)
            if mid:
                oc_canon_mids.setdefault(hc, set()).add(mid)
            if ordered:
                oc_canon_ordered_variants.setdefault(hc, set()).add(tuple(ordered))
            if ent_hqs:
                oc_canon_hqs.setdefault(hc, set()).update(ent_hqs)
            if surname and len(surname) >= 4:
                oc_canon_surnames.setdefault(surname, set()).add(hc)
                if hc not in oc_canon_primary_surname:
                    oc_canon_primary_surname[hc] = surname

    # Align 4-token ('ALEXANDRA ANN SAMIA SOLIA' -> 'ALEXANDRA SAMIA SOLIA') and corroborated
    # 3-token ('LEONARD JOSEPH SAMIA' -> 'LEONARD SAMIA') middle-name variants onto active base canons
    canon_person_alias: Dict[str, str] = {}
    for hc, ord_sets in oc_canon_ordered_variants.items():
        for ord_toks in ord_sets:
            if len(ord_toks) >= 4:
                dropped = " ".join((ord_toks[0], *ord_toks[2:]))
                base_c = canonicalize_person_name(dropped)
                if base_c and base_c != hc and base_c in active_oc_canons:
                    mid_char = ord_toks[1][0]
                    base_mids = oc_canon_mids.get(base_c) or set()
                    if not base_mids or mid_char in base_mids:
                        canon_person_alias[hc] = base_c
                        break
            elif len(ord_toks) == 3:
                dropped = f"{ord_toks[0]} {ord_toks[2]}"
                base_c = canonicalize_person_name(dropped)
                if base_c and base_c != hc and base_c in active_oc_canons:
                    mid_char = ord_toks[1][0]
                    base_mids = oc_canon_mids.get(base_c) or set()
                    if base_mids and mid_char not in base_mids:
                        continue
                    surname = ord_toks[2]
                    shared_hq = bool((oc_canon_hqs.get(hc) or set()) & (oc_canon_hqs.get(base_c) or set()))
                    rare_surname = len(surname) >= 4 and len(oc_canon_surnames.get(surname) or ()) <= 8
                    if shared_hq or rare_surname:
                        canon_person_alias[hc] = base_c
                        break

    def _resolve_human_key(raw_person: str) -> Tuple[str, str]:
        hc = canonicalize_person_name(raw_person)
        if not hc:
            return ("", "")
        hc = canon_person_alias.get(hc, hc)
        mid, _, _ = _extract_oc_person_mid_and_surname(raw_person)
        mids = oc_canon_mids.get(hc) or set()
        if mid and len(mids) >= 2:
            return (f"{hc}||MID:{mid}", mid)
        return (hc, mid)

    by_oc_node: Dict[str, List[str]] = {}
    by_oc_hq: Dict[str, List[str]] = {}
    hq_family_fanout: Dict[str, Set[str]] = {}
    hq_company_humans: Dict[str, Dict[Tuple[str, str], Set[str]]] = {}
    by_oc_human: Dict[str, List[str]] = {}
    human_entity_bbls: Dict[str, Dict[Tuple[str, str], List[str]]] = {}
    entity_hqs: Dict[Tuple[str, str], Set[str]] = {}
    entity_humans: Dict[Tuple[str, str], Set[str]] = {}
    human_entity_fanout: Dict[str, Set[Tuple[str, str]]] = {}
    human_family_fanout: Dict[str, Set[str]] = {}
    human_hq_entity_counts: Dict[str, Dict[str, Set[Tuple[str, str]]]] = {}
    human_coprincipal_entity_counts: Dict[str, Dict[str, Set[Tuple[str, str]]]] = {}
    human_geos: Dict[str, Set[str]] = {}
    human_hqs_all: Dict[str, Set[str]] = {}

    by_series_spv: Dict[str, List[str]] = {}
    series_explicit_variants: Dict[str, Set[str]] = {}
    series_base_variants: Dict[str, Set[str]] = {}
    series_explicit_geos: Dict[str, Set[str]] = {}
    series_base_geos: Dict[str, Set[str]] = {}
    direct_person_parcels: List[Tuple[str, str, str, str, str]] = []
    person_parcel_mail_diversity: Dict[str, Set[str]] = {}

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
        is_person_prop = bool(p.get("_is_person")) if "_is_person" in p else looks_like_person_owner(raw_nm)
        geo_scope = (p.get("county") or p.get("borough") or "").strip().upper()
        prop_coarse_mail = p.get("_building_key") or p.get("_coarse_mail") or ""
        if not prop_coarse_mail and p.get("mailing_address") and (is_person_prop or p.get("oc_company_number")):
            prop_coarse_mail = normalize_mailing_address_coarse(
                strip_benign_care_of_prefix(p.get("mailing_address") or "")
            )
        if prop_coarse_mail:
            prop_coarse_mail = hq_coarse_alias.get(prop_coarse_mail, prop_coarse_mail)

        if is_person_prop and active_oc_canons:
            raw_canon = p.get("_canon_person") or canonicalize_person_name(raw_nm)
            mapped_canon = canon_person_alias.get(raw_canon, raw_canon)
            if mapped_canon and mapped_canon in active_oc_canons:
                p_hkey, p_mid = _resolve_human_key(raw_nm)
                if p_hkey:
                    direct_person_parcels.append((b, p_hkey, p_mid, prop_coarse_mail, geo_scope))
                    if prop_coarse_mail:
                        person_parcel_mail_diversity.setdefault(p_hkey, set()).add(prop_coarse_mail)

        if not is_person_prop:
            spv_stem, has_explicit_series, is_generic_landscape = extract_series_stem_info(raw_nm)
            if spv_stem:
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
            fam_key = p.get("_family_key") or (
                canonicalize_person_name(raw_nm)
                if is_person_prop
                else extract_owner_family_key(raw_nm)
            ) or (p.get("owner_name_norm") or raw_nm)
            ckey = (oj, on)
            info = oc_resolved.get(ckey)
            nodes = info.get("reached_companies") if info else None
            if nodes:
                for node_id in nodes:
                    by_oc_node.setdefault(node_id, []).append(b)
            else:
                by_oc_node.setdefault(f"{oj}/{on}", []).append(b)

            if info:
                ent_hkeys: List[str] = []
                for h_name in info.get("humans", ()):
                    if not looks_like_person_owner(h_name):
                        continue
                    h_key, _ = _resolve_human_key(h_name)
                    if h_key and h_key not in generic_set and not pub_check(h_key):
                        ent_hkeys.append(h_key)

                ent_hqs = {hq_coarse_alias.get(hq, hq) for hq in info.get("hq_addresses", ()) if hq}
                if prop_coarse_mail:
                    ent_hqs.add(prop_coarse_mail)
                if ent_hqs:
                    entity_hqs.setdefault(ckey, set()).update(ent_hqs)
                    comp_hqs[find_r(bbl_to_root[b])].update(ent_hqs)
                if ent_hkeys:
                    entity_humans.setdefault(ckey, set()).update(ent_hkeys)

                for hq in info.get("hq_addresses", ()):
                    canon_hq = hq_coarse_alias.get(hq, hq)
                    by_oc_hq.setdefault(canon_hq, []).append(b)
                    if fam_key:
                        hq_family_fanout.setdefault(canon_hq, set()).add(fam_key)
                    if ent_hkeys:
                        hq_company_humans.setdefault(canon_hq, {}).setdefault(ckey, set()).update(ent_hkeys)

                for h_key in ent_hkeys:
                    by_oc_human.setdefault(h_key, []).append(b)
                    human_entity_bbls.setdefault(h_key, {}).setdefault(ckey, []).append(b)
                    human_entity_fanout.setdefault(h_key, set()).add(ckey)
                    if fam_key:
                        human_family_fanout.setdefault(h_key, set()).add(fam_key)
                    if geo_scope:
                        human_geos.setdefault(h_key, set()).add(geo_scope)
                    for chq in ent_hqs:
                        human_hqs_all.setdefault(h_key, set()).add(chq)
                        human_hq_entity_counts.setdefault(h_key, {}).setdefault(chq, set()).add(ckey)
                    for peer_h in ent_hkeys:
                        if peer_h != h_key:
                            human_coprincipal_entity_counts.setdefault(h_key, {}).setdefault(peer_h, set()).add(ckey)

    # Collect corroborated direct human/trustee parcels per `h_key` (attached in Step 3 to the
    # matching disambiguated person sub-cluster so common names never bridge unrelated landlords).
    human_direct_parcels: Dict[str, List[Tuple[str, str, str]]] = {}
    for b, p_hkey, p_mid, p_mail, p_geo in direct_person_parcels:
        if p_hkey not in by_oc_human:
            continue
        base_hc = p_hkey.split("||", 1)[0]
        mids = oc_canon_mids.get(base_hc) or set()
        if p_mid and mids and p_mid not in mids:
            continue
        shared_mail = bool(p_mail and p_mail in (human_hqs_all.get(p_hkey) or set()))
        shared_geo = bool(p_geo and p_geo in (human_geos.get(p_hkey) or set()))
        _, surname, ord_toks = _extract_oc_person_mid_and_surname(base_hc)
        rare_or_full = (
            len(ord_toks) >= 3
            or (len(surname) >= 4 and len(oc_canon_surnames.get(surname) or ()) <= 8)
        ) and len(person_parcel_mail_diversity.get(p_hkey) or ()) <= 4
        if shared_mail or (shared_geo and len(person_parcel_mail_diversity.get(p_hkey) or ()) <= 3) or rare_or_full:
            human_direct_parcels.setdefault(p_hkey, []).append((b, p_mail, p_geo))

    merges = 0

    # 1. Shared OC entity / corporate parent / foreign branch (zero family quota)
    for bbls in by_oc_node.values():
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other):
                merges += 1

    # 2. Shared verified non-agent corporate HQ (quota-free disjoint-principal island check)
    for hq, bbls in by_oc_hq.items():
        fams = hq_family_fanout.get(hq, set())
        co_humans = hq_company_humans.get(hq) or {}
        if hq.startswith("PO BOX ") and len(fams) >= 5 and len(co_humans) < 2:
            continue
        if len(co_humans) >= 6:
            # Build disjoint human-principal islands across officer-bearing companies at `hq`
            ckeys = list(co_humans.keys())
            par_c = {ck: ck for ck in ckeys}

            def _find_c(x):
                while par_c[x] != x:
                    par_c[x] = par_c[par_c[x]]
                    x = par_c[x]
                return x

            h_anchor: Dict[str, Tuple[str, str]] = {}
            for ck, hset in co_humans.items():
                for hk in hset:
                    prev = h_anchor.get(hk)
                    if prev is None:
                        h_anchor[hk] = ck
                    else:
                        ra_c, rb_c = _find_c(prev), _find_c(ck)
                        if ra_c != rb_c:
                            par_c[ra_c] = rb_c
            isl_counts: Dict[Tuple[str, str], int] = {}
            for ck in ckeys:
                rc = _find_c(ck)
                isl_counts[rc] = isl_counts.get(rc, 0) + 1
            if len(isl_counts) >= 5 and (max(isl_counts.values()) / len(ckeys)) < 0.35:
                continue

        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other):
                merges += 1

    # 3. Shared OC beneficial human principal with quota-free cohesion AND same-person
    #    disambiguation:
    #    - Quarantines uncorroborated CPAs/incorporators spanning >=8 unrelated families with
    #      no HQ or co-principal concentration.
    #    - Partitions multi-HQ / common statewide names ('RONALD BROWN', 'DAVID KATZ', 'JAY PATEL')
    #      into disambiguated same-person sub-clusters corroborated by shared HQ, Pass-1 root, or
    #      shared co-principal — so Hamilton Co's 157 Ronald Brown LLCs unify 100% without ever
    #      absorbing 15 unrelated Ronald Browns across Massachusetts!
    accepted_humans: Set[str] = set()
    for h_key, bbls in by_oc_human.items():
        ents = human_entity_fanout.get(h_key) or set()
        fams = human_family_fanout.get(h_key) or set()
        if len(fams) >= 8 and len(ents) >= 8:
            hq_map = human_hq_entity_counts.get(h_key) or {}
            top2_hq_ents = sum(sorted((len(s) for s in hq_map.values()), reverse=True)[:2]) if hq_map else 0
            top_hq_share = top2_hq_ents / max(len(ents), 1)

            cop_map = human_coprincipal_entity_counts.get(h_key) or {}
            max_cop_ents = max((len(s) for s in cop_map.values()), default=0)
            cop_share = max_cop_ents / max(len(ents), 1)

            root_counts: Dict[str, int] = {}
            for b in set(bbls):
                rr = find_r(bbl_to_root[b])
                root_counts[rr] = root_counts.get(rr, 0) + 1
            top_root_share = (max(root_counts.values()) / max(len(set(bbls)), 1)) if root_counts else 0.0

            if top_hq_share < 0.30 and cop_share < 0.35 and top_root_share < 0.35:
                continue

        accepted_humans.add(h_key)
        for b in bbls:
            comp_humans[find_r(bbl_to_root[b])].add(h_key)

    # Now merge entities within each accepted human's disambiguated same-person sub-clusters
    for h_key in accepted_humans:
        ent_bbl_map = human_entity_bbls.get(h_key) or {}
        ent_list = list(ent_bbl_map.keys())
        if not ent_list:
            continue
        base_hc = h_key.split("||", 1)[0]
        ord_toks = base_hc.split()
        surname = oc_canon_primary_surname.get(base_hc, "")
        all_hqs_h = human_hqs_all.get(h_key) or set()
        is_uncommon_surname = bool(len(surname) >= 4 and surname not in _COMMON_HOMONYM_SURNAMES)
        globally_unambiguous = (
            (len(all_hqs_h) <= 2 and (len(ent_list) <= 8 or is_uncommon_surname))
            or (len(ord_toks) >= 3 and len(all_hqs_h) <= 3)
        )

        if globally_unambiguous:
            person_subclusters: List[List[Tuple[str, str]]] = [ent_list]
        else:
            par_e = {ck: ck for ck in ent_list}

            def _find_e(x):
                while par_e[x] != x:
                    par_e[x] = par_e[par_e[x]]
                    x = par_e[x]
                return x

            def _union_e(a, b):
                ra_e, rb_e = _find_e(a), _find_e(b)
                if ra_e != rb_e:
                    par_e[ra_e] = rb_e

            hq_anchor_e: Dict[str, Tuple[str, str]] = {}
            root_anchor_e: Dict[str, Tuple[str, str]] = {}
            cop_anchor_e: Dict[str, Tuple[str, str]] = {}
            for ck in ent_list:
                for chq in entity_hqs.get(ck, ()):
                    prev = hq_anchor_e.get(chq)
                    if prev is None:
                        hq_anchor_e[chq] = ck
                    else:
                        _union_e(prev, ck)
                for b_id in ent_bbl_map[ck]:
                    rr = find_r(bbl_to_root[b_id])
                    prev_r = root_anchor_e.get(rr)
                    if prev_r is None:
                        root_anchor_e[rr] = ck
                    else:
                        _union_e(prev_r, ck)
                for peer_h in entity_humans.get(ck, ()):
                    if peer_h == h_key or peer_h not in accepted_humans:
                        continue
                    # Do not let a widespread common name bridge two unrelated homonyms of `h_key`
                    if len(human_hqs_all.get(peer_h, ())) > 6 and len(human_entity_fanout.get(peer_h, ())) > 25:
                        continue
                    prev_p = cop_anchor_e.get(peer_h)
                    if prev_p is None:
                        cop_anchor_e[peer_h] = ck
                    else:
                        _union_e(prev_p, ck)

            grouped_e: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
            for ck in ent_list:
                grouped_e.setdefault(_find_e(ck), []).append(ck)
            person_subclusters = list(grouped_e.values())

        # Attach direct human/trustee parcels to their matching person sub-cluster
        sub_hqs_list = [
            {chq for ck in sub_ents for chq in entity_hqs.get(ck, ())}
            for sub_ents in person_subclusters
        ]
        sub_bbl_lists: List[List[str]] = [
            [b_id for ck in sub_ents for b_id in ent_bbl_map[ck]]
            for sub_ents in person_subclusters
        ]
        for b_dir, p_mail, _p_geo in human_direct_parcels.get(h_key, ()):
            matched_idx = None
            if p_mail:
                for s_idx, shqs in enumerate(sub_hqs_list):
                    if p_mail in shqs:
                        matched_idx = s_idx
                        break
            if matched_idx is None and len(person_subclusters) == 1:
                matched_idx = 0
            if matched_idx is not None:
                sub_bbl_lists[matched_idx].append(b_dir)

        for sub_bbls in sub_bbl_lists:
            roots = list({find_r(bbl_to_root[b_id]) for b_id in sub_bbls})
            if len(roots) < 2:
                continue
            for other in roots[1:]:
                if union_r(roots[0], other, require_hq_compat=True):
                    merges += 1

    # 3b. CT-Parity Same-Surname Co-Principal Bridges:
    # When >=2 accepted property-owning principals share an uncommon surname (len>=4, <=12 principals
    # in the jurisdiction) AND co-appear on an `oc_resolved` company, bridge their co-located portfolios.
    for ckey, hset in entity_humans.items():
        if not hset:
            continue
        by_surname: Dict[str, List[str]] = {}
        for hk in hset:
            if hk not in accepted_humans:
                continue
            base_hc = hk.split("||", 1)[0]
            surname = oc_canon_primary_surname.get(base_hc, "")
            if len(surname) >= 4 and len(oc_canon_surnames.get(surname) or ()) <= 12:
                by_surname.setdefault(surname, []).append(hk)
        ck_hqs = entity_hqs.get(ckey) or set()
        for s_hkeys in by_surname.values():
            uniq_hk = sorted(set(s_hkeys))
            if len(uniq_hk) < 2:
                continue
            # Only bridge entities of those family co-principals that share an HQ with `ckey` or
            # belong to globally unambiguous single-person names, preventing homonym spillover.
            co_bbls: List[str] = []
            for hk in uniq_hk:
                for ent_k, ent_bs in (human_entity_bbls.get(hk) or {}).items():
                    if ent_k == ckey or (ck_hqs and (entity_hqs.get(ent_k, set()) & ck_hqs)) or len(human_hqs_all.get(hk, ())) <= 3:
                        co_bbls.extend(ent_bs)
            roots = list({find_r(bbl_to_root[b_id]) for b_id in co_bbls})
            if len(roots) < 2:
                continue
            for other in roots[1:]:
                if union_r(roots[0], other, require_hq_compat=True):
                    merges += 1

    # 4. Corroborated corporate series & container-suffix siblings
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
            allow_series_merge = True
        elif len(exp_vars) == 1 and len(base_vars) >= 1:
            exp_geos = series_explicit_geos.get(bucket_key, set())
            b_geos = series_base_geos.get(bucket_key, set())
            if has_inst_kw or bool(exp_geos & b_geos):
                allow_series_merge = True
        elif len(exp_vars) == 0 and len(base_vars) >= 2 and "||" not in bucket_key and total_vars <= 8:
            b_geos = series_base_geos.get(bucket_key, set())
            if len(b_geos) >= 1:
                allow_series_merge = True

        if not allow_series_merge:
            continue
        roots = list({bbl_to_root[b] for b in bbls})
        for other in roots[1:]:
            if union_r(roots[0], other):
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

    _LEGAL_EXPANSION_SUFFIXES = (
        "LIMITED LIABILITY COMPANY",
        "CORPORATION",
        "INCORPORATED",
        "LIMITED PARTNERSHIP",
        "LIMITED LIABILITY PARTNERSHIP",
        "COMPANY",
        "LIMITED",
    )
    unsuffixed_expansions_by_bbl: Dict[str, Tuple[str, ...]] = {}

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
            fresh_coarse = normalize_mailing_address_coarse(
                strip_benign_care_of_prefix(row.get("mailing_address") or "")
            )
            coarse_by_bbl[bbl] = fresh_coarse or (row.get("mailing_address_coarse") or "")
            sale_by_bbl[bbl] = row.get("last_sale_date")
            lookup_norms.add(rec.owner_norm)
            if (
                len(rec.owner_norm) >= 8
                and len(rec.owner_norm.split()) >= 2
                and not _CORPORATE_SUFFIX_STRIP_RE.search(rec.owner_norm)
                and not is_generic_homonym_stem(rec.owner_norm)
            ):
                exps = tuple(f"{rec.owner_norm} {sfx}" for sfx in _LEGAL_EXPANSION_SUFFIXES)
                unsuffixed_expansions_by_bbl[bbl] = exps
                lookup_norms.update(exps)
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
        for exp_norm in unsuffixed_expansions_by_bbl.get(bbl, ()):
            pool.extend(candidates_by_norm.get(exp_norm, ()))
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
