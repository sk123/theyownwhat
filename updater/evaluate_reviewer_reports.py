#!/usr/bin/env python3
"""
updater/evaluate_reviewer_reports.py
====================================
Nightly evidence-based evaluator, rule generalizer, and audit-gated network
rebuilder for trusted-reviewer error reports (`network_error_reports`).

Core Anti-Weaponization & Epistemic Honesty Guarantee (`docs/ASSURANCE_POLICY.md`):
  - NEVER blindly honors a user report or deletes a landlord from a network.
  - Queries loaded public records (`businesses`, `principals`, `unique_principals`,
    `entity_networks`, `oc_companies`, `oc_officers`, `{jur}_properties`,
    `{jur}_networks`) to extract the exact structural bridge linking the flagged
    person/entity/address to the reported network and measures objective empirical
    signals (owning ratio, HQ/family/surname dispersion, multi-suite count,
    commercial intermediary roles, official non-stock/co-op/institutional entity
    type, or commercial nominee-manager fanout).
  - Applies a generalized `scope='global'` rule in `network_linking_rules` +
    `docs/benchmarks/learned_linking_rules.json` ONLY when empirical thresholds
    prove a real systemic linkage flaw (e.g., third-party law/CPA/RA office or
    email, multi-tenant office/UPS maildrop, common-name homonym collision,
    nominee-manager factory, or non-landlord institutional/co-op/placeholder).
  - Rejects reports where public records confirm a direct, cohesive landlord
    ownership link (`status='rejected_unverified'`), recording the exact public
    evidence so the daily digest explains why no change was made.
  - Flags ambiguous or stale-deed-only cases as `needs_human_review`.
  - Scans ALL 20 jurisdictions to discover every other jurisdiction impacted by
    the newly generalized rule, rebuilds affected jurisdictions through
    `scripts/audit_network_bridges.py` gates, and automatically rolls back via
    `deactivate_learned_rule()` if any regression gate fails.
"""

from __future__ import annotations

import argparse
import datetime as dt
from difflib import SequenceMatcher
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.linking_rules import (
    DEFAULT_RULES_JSON_PATH,
    deactivate_learned_rule,
    ensure_linking_rules_tables,
    reload_learned_rules,
    upsert_learned_rule,
)
from api.oc_linker import _COMMON_HOMONYM_SURNAMES
from api.reviewer_feedback_routes import (
    ensure_reviewer_feedback_tables,
    normalize_jurisdiction,
)
from api.shared_utils import (
    canonicalize_person_name,
    extract_owner_family_key,
    extract_postal_street_zip_key,
    get_email_match_key,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    load_registrar_address_substrings,
    looks_like_person_owner,
    normalize_business_name,
    normalize_mailing_address_coarse,
    normalize_person_name,
    strip_benign_care_of_prefix,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("reviewer-report-evaluator")

STATE_JURISDICTIONS = frozenset({
    "ma", "il", "ca", "mn", "oh", "mt", "fl", "ny", "va", "nc", "co", "wi", "ar", "vt",
})
CITY_JURISDICTIONS = frozenset({"dc", "baltimore", "detroit", "philadelphia"})
ALL_NON_CT_JURISDICTIONS = sorted(STATE_JURISDICTIONS | CITY_JURISDICTIONS | {"nj"})

PUBLIC_WEBMAIL_DOMAINS = frozenset({
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com",
    "me.com", "mac.com", "live.com", "msn.com", "comcast.net", "verizon.net",
    "sbcglobal.net", "att.net", "cox.net", "charter.net", "optonline.net",
    "protonmail.com", "proton.me", "ymail.com", "earthlink.net", "bellsouth.net",
})

_INTERMEDIARY_ADDR_MARKER_RE = re.compile(
    r"\b(?:LAW\s+(?:OFFICE|OFFICES|FIRM|GROUP)|ATTORNEYS?|ESQ|CPA|CPAS|ACCOUNTING|ACCOUNTANTS?|"
    r"REGISTERED\s+AGENT|STATUTORY\s+AGENT|CORPORATE\s+(?:SERVICES|FILINGS)|"
    r"UPS\s+STORE|POSTAL\s+ANNEX|MAIL\s+BOXES\s+ETC|PMB\s*#?\d+|"
    r"PROPERTY\s+MANAGEMENT|ESCROW|TITLE\s+(?:COMPANY|CO|INSURANCE|AGENCY)|1031\s+EXCHANGE)\b",
    re.IGNORECASE,
)

_INTERMEDIARY_EMAIL_RE = re.compile(
    r"(?:^|[._-])(?:cpa|cpas|acct|acctg|accounting|accountant|bookkeeping|taxprep|taxservice|"
    r"law|lawyer|lawfirm|lawoffice|attorney|esq|esquire|paralegal|annualreports?|corpfilings?|"
    r"businessfilings?|registeredagent|1031|escrow|title)(?:$|[._\-\d@])",
    re.IGNORECASE,
)

_INTERMEDIARY_ROLE_RE = re.compile(
    r"\b(?:REGISTERED\s+AGENT|STATUTORY\s+AGENT|RESIDENT\s+AGENT|AGENT\s+FOR\s+SERVICE|"
    r"INCORPORATOR|ORGANIZER|ATTORNEY|COUNSEL|ESQ|CPA|ACCOUNTANT|AUTHORIZED\s+REPRESENTATIVE|"
    r"FILER|SUBMITTER|PROCESS\s+AGENT|PROPERTY\s+MANAGER)\b",
    re.IGNORECASE,
)

_NON_LANDLORD_TYPE_RE = re.compile(
    r"\b(?:NON[\s-]?STOCK|NON[\s-]?PROFIT|CHARITABLE|RELIGIOUS|COOPERATIVE|CO-OP|"
    r"MUNICIPAL|GOVERNMENT|PUBLIC\s+AUTHORITY|HOUSING\s+AUTHORITY|SCHOOL|UNIVERSITY|"
    r"CONDOMINIUM|HOMEOWNERS|CHURCH|BENEVOLENT|FRATERNAL|CREDIT\s+UNION|BANKING)\b",
    re.IGNORECASE,
)

_NON_LANDLORD_NAME_RE = re.compile(
    r"\b(?:CO-?OP(?:ERATIVE)?|CONDOMINIUM\s+(?:ASSN|ASSOCIATION)|HOMEOWNERS\s+(?:ASSN|ASSOCIATION)|"
    r"CONDO\s+ASSN|PROPERTY\s+OWNERS\s+ASSN|CHURCH|PARISH|DIOCESE|SYNAGOGUE|MOSQUE|TEMPLE|"
    r"UNIVERSITY|COLLEGE|SCHOOL\s+DISTRICT|HOSPITAL|MEDICAL\s+CENTER|HEALTH\s+SYSTEM|"
    r"HOUSING\s+AUTHORITY|REDEVELOPMENT\s+AUTHORITY|CREDIT\s+UNION|NATIONAL\s+BANK|SAVINGS\s+BANK|"
    r"UTILITY|POWER\s+COMPANY|WATER\s+COMPANY|TELEPHONE|RAILROAD|TRANSIT\s+AUTHORITY|"
    r"LAW\s+(?:FIRM|OFFICE|OFFICES|GROUP)|ATTORNEYS\s+AT\s+LAW|HABITAT\s+FOR\s+HUMANITY|"
    r"CHARITABLE\s+FOUNDATION|COMMUNITY\s+LAND\s+TRUST)\b",
    re.IGNORECASE,
)

_PLACEHOLDER_OWNER_RE = re.compile(
    r"^(?:UNKNOWN|CURRENT\s+OWNER|OWNER\s+OF\s+RECORD|OCCUPANT|NO\s+NAME|NOT\s+AVAILABLE|"
    r"TRUST\s+AGREEMENT\s+DATED|DECLARATION\s+OF\s+TRUST|REVOCABLE\s+LIVING\s+TRUST|"
    r"LIVING\s+TRUST\s+DATED|FAMILY\s+TRUST\s+DATED|LAND\s+TRUST|TRUSTEE\s+UNDER|"
    r"AS\s+TRUSTEE\s+FOR|ESTATE\s+OF)\s*$",
    re.IGNORECASE,
)

_UNIT_EXTRACT_RE = re.compile(
    r"\b(?:STE|SUITE|APT|UNIT|FL|FLOOR|RM|ROOM|PMB|#)\s*([A-Z0-9-]+)\b",
    re.IGNORECASE,
)

_ZIP5_EXTRACT_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
_STREET_NUM_RE = re.compile(r"^(PO\s+BOX\s+\d+|\d+[A-Z]?)\s+(.+)$", re.IGNORECASE)
_DIRECTION_TOKEN_RE = re.compile(
    r"\b(NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|N|S|E|W)\b",
    re.IGNORECASE,
)
_HARD_NON_LANDLORD_ALLOWLIST_BLOCK_RE = re.compile(
    r"\b(?:BANK|CREDIT\s+UNION|MORTGAGE|SERVIC(?:ING|ER)|HOUSING\s+AUTHORITY|REDEVELOPMENT\s+AUTHORITY|"
    r"MUNICIPAL|CITY\s+OF|TOWN\s+OF|COUNTY\s+OF|STATE\s+OF|CO-?OP(?:ERATIVE)?|CONDOMINIUM|HOMEOWNERS|"
    r"LAW\s+(?:FIRM|OFFICE|OFFICES|GROUP)|ATTORNEYS?\s+AT\s+LAW|UTILITY|POWER\s+COMPANY|WATER\s+COMPANY|"
    r"TELEPHONE|RAILROAD|REGISTERED\s+AGENT|CORPORATE\s+SERVICES|1031\s+EXCHANGE|TITLE\s+INSURANCE|ESCROW)\b",
    re.IGNORECASE,
)
_LETTERED_AVE_RE = re.compile(r"\b(?:AVE|AVENUE)\s+([A-Z])\b", re.IGNORECASE)
_NUMBERED_HWY_RE = re.compile(r"\b(?:HIGHWAY|HWY|ROUTE|RT|RTE|COUNTY\s+ROAD|CR|STATE\s+ROAD|SR|INTERSTATE|US)\s+(\d+[A-Z]?)\b", re.IGNORECASE)
_CARE_OF_SPLIT_RE = re.compile(
    r"^\s*(?:C/O|CIO|CARE\s+OF|ATTN:?|ATTENTION:?|%)\s+([A-Z0-9 .&'/-]{2,42}?)\s*[,\s]+(\d+\s+.+|PO\s+BOX\s+\d+.*)$",
    re.IGNORECASE,
)
_LEGAL_SUFFIX_TAIL_RE = re.compile(
    r"\b(?:LLC|L\.L\.C\.|LIMITED\s+LIABILITY\s+COMPANY|INC|INCORPORATED|CORP|CORPORATION|"
    r"LP|L\.P\.|LIMITED\s+PARTNERSHIP|LLP|PLLC|LTD|LIMITED|CO|COMPANY)\b$",
    re.IGNORECASE,
)
_CANON_REAL_ESTATE_TERMS = frozenset({
    "MANAGEMENT", "PROPERTIES", "PROPERTY", "HOLDINGS", "HOLDING", "DEVELOPMENT",
    "INVESTMENTS", "INVESTMENT", "APARTMENTS", "APARTMENT", "ASSOCIATES", "ASSOCIATION",
    "ENTERPRISES", "PARTNERS", "PARTNERSHIP", "REALTY", "RESIDENTIAL", "HOUSING",
    "VENTURES", "CAPITAL", "BUILDERS", "EQUITIES", "EQUITY", "GROUP", "SERVICES",
    "CORPORATION", "INCORPORATED", "LIMITED", "COMPANY", "TOWERS", "COMMONS",
    "VILLAGE", "GARDENS", "HEIGHTS", "ESTATES", "PLAZA", "CENTER", "SQUARE",
})
_PLAUSIBLE_OFFICER_ROLE_TOKEN_RE = re.compile(
    r"^(?:MGR|MGRM|MGRMEM|MGMT|MNG|MNGR|MBR|MEM|MEMB|PRES|VP|EVP|SVP|AVP|SEC|SECR|SECY|"
    r"TREAS|TRS|TRST|TRSTE|TTEE|TRUSTEE|DIR|EXEC|CEO|CFO|COO|CIO|CTO|AUTH|AUTHP|SIGNER|"
    r"COMGR|GENPTR|GP|LP|PARTNER|PTR|OWNER|OWNR|PRIN|ADMIN|AGENT|AGT|OFFICER|OFF|"
    r"CHMN|CHAIR|MANAGING|MEMBER|MANAGER|DIRECTOR|PRESIDENT|SECRETARY|TREASURER)$",
    re.IGNORECASE,
)
_FOR_PROFIT_ENTITY_TYPE_RE = re.compile(
    r"\b(?:LLC|LIMITED\s+LIABILITY|PROFIT|STOCK|FOR[\s-]?PROFIT|BUSINESS\s+CORPORATION|"
    r"LIMITED\s+PARTNERSHIP|GENERAL\s+PARTNERSHIP|DOMESTIC\s+CORPORATION|FOREIGN\s+CORPORATION)\b",
    re.IGNORECASE,
)


def _db_url() -> str:
    raw = os.environ.get("DATABASE_URL") or "postgresql://user:password@127.0.0.1:5432/ctdata"
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


# ---------------------------------------------------------------------------
# Pure Deterministic Evidence Classifiers (Unit-Testable & Anti-Weaponization)
# ---------------------------------------------------------------------------

def evaluate_address_hub_evidence(stats: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Classify a candidate bridge address strictly from empirical DB counts.

    Returns `(verdict, rule_spec_or_None, rationale)` where verdict is one of:
      - `'verified'` -> clear third-party / multi-tenant / maildrop hub -> emit `third_party_hub_block`
      - `'rejected'` -> cohesive single-operator landlord HQ -> NEVER block
      - `'inconclusive'` -> ambiguous / insufficient evidence
    """
    coarse = str(stats.get("coarse_address") or "").strip().upper()
    if not coarse or len(coarse) < 6:
        return "inconclusive", None, "Connecting address could not be normalized to a specific street/PO key."

    total_biz = int(stats.get("total_businesses") or 0)
    owning_biz = int(stats.get("owning_businesses") or 0)
    owning_ratio = float(stats.get("owning_ratio") if stats.get("owning_ratio") is not None else (owning_biz / max(total_biz, 1)))
    distinct_families = int(stats.get("distinct_owner_families") or 0)
    distinct_surnames = int(stats.get("distinct_unrelated_surnames") or 0)
    distinct_suites = int(stats.get("distinct_suites") or 0)
    top1_share = float(stats.get("top1_family_share") if stats.get("top1_family_share") is not None else 1.0)
    top2_share = float(stats.get("top2_family_share") if stats.get("top2_family_share") is not None else 1.0)
    has_marker = bool(stats.get("has_intermediary_marker"))

    # Anti-weaponization guard FIRST: cohesive single-family landlord HQ is NEVER blocked
    if not has_marker and (
        (distinct_families <= 2 and total_biz < 15)
        or (top1_share >= 0.65 and distinct_suites < 4 and owning_ratio >= 0.25)
    ):
        return (
            "rejected",
            None,
            f"Address '{coarse}' is a concentrated landlord office ({owning_biz}/{total_biz} owning entities, "
            f"{distinct_families} owner family(ies), top-1 share {top1_share:.0%}, no 3rd-party RA/law/CPA markers).",
        )

    reasons: List[str] = []
    if total_biz >= 15 and owning_ratio < 0.15:
        reasons.append(f"non-real-estate filing hub ({owning_biz}/{total_biz} owning = {owning_ratio:.1%})")
    if distinct_families >= 8 and top2_share < 0.35:
        reasons.append(f"highly dispersed ownership ({distinct_families} families, top-2 share {top2_share:.0%})")
    if distinct_surnames >= 6 and top1_share < 0.35:
        reasons.append(f"diverse unrelated individual owners ({distinct_surnames} surnames, top-1 share {top1_share:.0%})")
    if distinct_suites >= 4 and distinct_families >= 4 and top1_share < 0.45:
        reasons.append(f"multi-suite office building ({distinct_suites} suites across {distinct_families} families, top-1 {top1_share:.0%})")
    if has_marker and distinct_families >= 3 and top1_share < 0.55:
        reasons.append(f"verified 3rd-party intermediary/maildrop marker across {distinct_families} families (top-1 {top1_share:.0%})")

    if reasons:
        return (
            "verified",
            {
                "rule_type": "third_party_hub_block",
                "rule_key": coarse,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed third-party / multi-tenant bridge address '{coarse}': " + "; ".join(reasons) + ".",
        )

    return (
        "inconclusive",
        None,
        f"Address '{coarse}' did not meet third-party hub thresholds "
        f"({owning_biz}/{total_biz} owning, {distinct_families} families, {distinct_suites} suites, top-1 {top1_share:.0%}).",
    )


def evaluate_email_hub_evidence(stats: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Classify a candidate bridge email / domain strictly from empirical DB dispersion."""
    raw_email = str(stats.get("email") or "").strip().lower()
    domain = str(stats.get("domain") or "").strip().lower()
    if not domain and "@" in raw_email:
        domain = raw_email.split("@", 1)[1]
    if not domain:
        return "inconclusive", None, "No valid email or domain found on connecting filings."

    is_webmail = domain in PUBLIC_WEBMAIL_DOMAINS
    total_biz = int(stats.get("total_businesses") or 0)
    distinct_hqs = int(stats.get("distinct_hqs") or 0)
    top2_hq_share = float(stats.get("top2_hq_share") if stats.get("top2_hq_share") is not None else 1.0)
    distinct_surnames = int(stats.get("distinct_surnames") or 0)
    top2_sur_share = float(stats.get("top2_surname_share") if stats.get("top2_surname_share") is not None else 1.0)
    is_prof = bool(stats.get("is_professional_intermediary") or _INTERMEDIARY_EMAIL_RE.search(raw_email or domain))

    # Anti-weaponization guard: concentrated single-landlord email/domain is NEVER blocked
    if not is_prof and top2_hq_share >= 0.65 and top2_sur_share >= 0.55:
        return (
            "rejected",
            None,
            f"Email/domain '{raw_email or domain}' is a concentrated operator contact "
            f"({total_biz} businesses, {distinct_hqs} HQs, top-2 HQ share {top2_hq_share:.0%}, top-2 surname share {top2_sur_share:.0%}).",
        )

    verified = False
    reason = ""
    if distinct_hqs >= 4 and top2_hq_share < 0.45 and distinct_surnames >= 4 and top2_sur_share < 0.40:
        verified = True
        reason = (
            f"dispersed across {distinct_hqs} HQs (top-2 {top2_hq_share:.0%}) and "
            f"{distinct_surnames} principal surnames (top-2 {top2_sur_share:.0%})"
        )
    elif is_prof and distinct_hqs >= 2 and distinct_surnames >= 2:
        verified = True
        reason = (
            f"professional law/CPA/filing intermediary contact spanning {distinct_hqs} HQs "
            f"and {distinct_surnames} surnames"
        )

    if verified:
        # Generalize custom firm domains to domain-level; restrict webmail to exact email!
        rule_key = raw_email if (is_webmail and "@" in raw_email) else domain
        return (
            "verified",
            {
                "rule_type": "third_party_email_block",
                "rule_key": rule_key,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed third-party email bridge '{rule_key}': {reason}.",
        )

    return (
        "inconclusive",
        None,
        f"Email '{raw_email or domain}' did not cross third-party intermediary dispersion thresholds "
        f"({total_biz} businesses, {distinct_hqs} HQs, {distinct_surnames} surnames).",
    )


def evaluate_principal_evidence(stats: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Distinguish a genuine single landlord (REJECT), a verified 3rd-party intermediary
    signer (third_party_principal_block), or a multi-person common-name homonym
    (ambiguous_homonym_block)."""
    raw_name = str(stats.get("principal_name") or "").strip()
    canon = canonicalize_person_name(raw_name)
    norm_upper = " ".join(normalize_person_name(raw_name).upper().split()) or " ".join(raw_name.upper().split())
    if not canon or len(canon.split()) < 2:
        return "inconclusive", None, "Principal name is missing or too short to classify."

    total_cos = int(stats.get("total_companies") or 0)
    distinct_hqs = int(stats.get("distinct_hqs") or 0)
    top2_hq_share = float(stats.get("top2_hq_share") if stats.get("top2_hq_share") is not None else 1.0)
    distinct_fams = int(stats.get("distinct_families") or 0)
    has_intermediary_role = bool(stats.get("has_intermediary_role"))
    toks = norm_upper.split()
    surname_is_common = bool(any(t in _COMMON_HOMONYM_SURNAMES for t in toks))

    # Anti-weaponization guard: concentrated single-HQ principal with no intermediary role is VALID!
    if not has_intermediary_role and (distinct_hqs <= 2 or top2_hq_share >= 0.65):
        return (
            "rejected",
            None,
            f"Public corporate filings confirm '{norm_upper}' is a concentrated principal "
            f"({total_cos} companies, {distinct_hqs} HQs, top-2 HQ share {top2_hq_share:.0%}) "
            f"with no third-party intermediary or homonym dispersion signal.",
        )

    if has_intermediary_role and distinct_fams >= 5 and distinct_hqs >= 4 and top2_hq_share < 0.40:
        return (
            "verified",
            {
                "rule_type": "third_party_principal_block",
                "rule_key": norm_upper,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed third-party intermediary principal '{norm_upper}' (attorney/CPA/RA/incorporator role "
            f"across {distinct_fams} unrelated families and {distinct_hqs} HQs, top-2 HQ share {top2_hq_share:.0%}).",
        )

    if (distinct_hqs >= 5 and top2_hq_share < 0.40 and distinct_fams >= 5) or (
        surname_is_common and distinct_hqs >= 3 and top2_hq_share < 0.50 and distinct_fams >= 3
    ):
        return (
            "verified",
            {
                "rule_type": "ambiguous_homonym_block",
                "rule_key": norm_upper,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed multi-person homonym collision on '{norm_upper}' "
            f"({total_cos} companies across {distinct_hqs} scattered HQs, top-2 HQ share {top2_hq_share:.0%}, "
            f"{distinct_fams} families; common surname={surname_is_common}). Quarantining bare-name merging globally.",
        )

    return (
        "inconclusive",
        None,
        f"Principal '{norm_upper}' has moderate dispersion ({total_cos} companies, {distinct_hqs} HQs, "
        f"top-2 share {top2_hq_share:.0%}) — requires human review before changing linking logic.",
    )


def evaluate_nominee_manager_evidence(stats: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Verify whether a corporate parent / manager entity is a commercial nominee-manager factory."""
    corp_name = " ".join(str(stats.get("manager_name") or "").upper().strip().split())
    if not corp_name:
        return "inconclusive", None, "No corporate manager entity identified."

    managed_cos = int(stats.get("managed_companies") or 0)
    distinct_fams = int(stats.get("distinct_families") or 0)
    distinct_hqs = int(stats.get("distinct_hqs") or 0)
    top2_hq_share = float(stats.get("top2_hq_share") if stats.get("top2_hq_share") is not None else 1.0)

    if distinct_hqs <= 2 or top2_hq_share >= 0.65:
        return (
            "rejected",
            None,
            f"Corporate manager '{corp_name}' operates from a cohesive HQ ({managed_cos} entities, "
            f"{distinct_hqs} HQs, top-2 share {top2_hq_share:.0%}) — appears to be a genuine holding/management parent.",
        )

    if managed_cos >= 6 and distinct_fams >= 6 and distinct_hqs >= 5 and top2_hq_share < 0.35:
        return (
            "verified",
            {
                "rule_type": "nominee_manager_block",
                "rule_key": normalize_business_name(corp_name) or corp_name,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed commercial nominee-manager factory '{corp_name}' managing {managed_cos} companies "
            f"across {distinct_fams} families and {distinct_hqs} scattered tax HQs (top-2 share {top2_hq_share:.0%}).",
        )

    return (
        "inconclusive",
        None,
        f"Corporate manager '{corp_name}' did not cross commercial nominee-factory thresholds "
        f"({managed_cos} companies, {distinct_hqs} HQs, top-2 share {top2_hq_share:.0%}).",
    )


def evaluate_non_landlord_entity_evidence(stats: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Verify whether an entity is empirically a non-landlord (co-op/HOA/nonprofit/bank/utility/law/placeholder)."""
    raw_name = " ".join(str(stats.get("entity_name") or "").upper().strip().split())
    entity_type = " ".join(str(stats.get("entity_type") or "").upper().strip().split())
    if not raw_name:
        return "inconclusive", None, "Entity name missing."

    if _PLACEHOLDER_OWNER_RE.match(raw_name):
        return (
            "verified",
            {
                "rule_type": "placeholder_owner",
                "rule_key": raw_name,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed generic placeholder owner string '{raw_name}'.",
        )

    if _NON_LANDLORD_TYPE_RE.search(entity_type) or _NON_LANDLORD_NAME_RE.search(raw_name) or is_excluded_third_party_entity(raw_name):
        norm_key = normalize_business_name(raw_name) or raw_name
        return (
            "verified",
            {
                "rule_type": "non_landlord_entity_block",
                "rule_key": norm_key,
                "rule_value": "",
                "scope": "global",
            },
            f"Confirmed non-landlord / institutional / co-op / intermediary entity '{raw_name}' "
            f"(public filing type: '{entity_type or 'name-verified'}').",
        )

    return (
        "rejected",
        None,
        f"Entity '{raw_name}' (type: '{entity_type or 'standard private entity'}') shows no public-record "
        f"nonprofit, co-op, HOA, government, utility, bank, or intermediary classification.",
    )


# ---------------------------------------------------------------------------
# DB Forensic Evidence Collectors (CT + All 19 Nationwide Jurisdictions)
# ---------------------------------------------------------------------------

def _jur_tables(jur: str) -> Tuple[str, str]:
    j = normalize_jurisdiction(jur)
    if j == "ny":
        return "nyc_properties", "nyc_networks"
    return f"{j}_properties", f"{j}_networks"


def _measure_address_across_db(conn, raw_address: str, jur: str) -> Dict[str, Any]:
    """Measure global + local dispersion metrics for a candidate connecting address."""
    cleaned = strip_benign_care_of_prefix(raw_address or "")
    coarse = normalize_mailing_address_coarse(cleaned)
    if not coarse:
        return {"coarse_address": ""}

    total_biz = 0
    owning_biz = 0
    families: Counter = Counter()
    surnames: Set[str] = set()
    suites: Set[str] = set()
    has_marker = bool(_INTERMEDIARY_ADDR_MARKER_RE.search(raw_address or ""))

    prefix_like = f"{coarse[:24]}%"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. CT SOTS businesses check (strongest signal for owning vs non-owning ratio!)
        try:
            cur.execute(
                """
                SELECT b.id, b.name,
                       COALESCE(b.mail_address, b.business_address, '') AS raw_addr,
                       EXISTS (SELECT 1 FROM properties p WHERE p.business_id = b.id) AS owns_prop
                FROM businesses b
                WHERE UPPER(COALESCE(b.mail_address, '')) LIKE %s
                   OR UPPER(COALESCE(b.business_address, '')) LIKE %s
                LIMIT 500
                """,
                (prefix_like, prefix_like),
            )
            for row in cur.fetchall():
                rc = normalize_mailing_address_coarse(strip_benign_care_of_prefix(row["raw_addr"]))
                if rc != coarse:
                    continue
                total_biz += 1
                if row["owns_prop"]:
                    owning_biz += 1
                nm = str(row["name"] or "")
                if _INTERMEDIARY_ADDR_MARKER_RE.search(nm):
                    has_marker = True
                fam = extract_owner_family_key(nm) or normalize_business_name(nm)
                if fam:
                    families[fam] += 1
                m_u = _UNIT_EXTRACT_RE.search(row["raw_addr"] or "")
                if m_u:
                    suites.add(m_u.group(1).upper())
        except Exception:
            conn.rollback()

        # 2. Local jurisdiction properties check (nationwide or CT)
        if jur != "ct":
            prop_tbl, _ = _jur_tables(jur)
            try:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{prop_tbl}",))
                if cur.fetchone()["ok"]:
                    cur.execute(
                        f"""
                        SELECT owner_name, mailing_address
                        FROM {prop_tbl}
                        WHERE mailing_address IS NOT NULL
                          AND UPPER(mailing_address) LIKE %s
                        LIMIT 1000
                        """,
                        (prefix_like,),
                    )
                    for row in cur.fetchall():
                        maddr = str(row["mailing_address"] or "")
                        rc = normalize_mailing_address_coarse(strip_benign_care_of_prefix(maddr))
                        if rc != coarse:
                            continue
                        total_biz += 1
                        owning_biz += 1
                        oname = str(row["owner_name"] or "")
                        if _INTERMEDIARY_ADDR_MARKER_RE.search(oname) or _INTERMEDIARY_ADDR_MARKER_RE.search(maddr):
                            has_marker = True
                        if looks_like_person_owner(oname):
                            toks = normalize_person_name(oname).split()
                            if toks:
                                surnames.add(toks[-1])
                            fam = canonicalize_person_name(oname)
                        else:
                            fam = extract_owner_family_key(oname) or normalize_business_name(oname)
                        if fam:
                            families[fam] += 1
                        m_u = _UNIT_EXTRACT_RE.search(maddr)
                        if m_u:
                            suites.add(m_u.group(1).upper())
            except Exception:
                conn.rollback()

        # 3. OpenCorporates registered/mailing address density check
        try:
            cur.execute(
                """
                SELECT name, registered_address_raw, mailing_address_raw
                FROM oc_companies
                WHERE registered_address_coarse = %s OR mailing_address_coarse = %s
                LIMIT 400
                """,
                (coarse, coarse),
            )
            for row in cur.fetchall():
                total_biz += 1
                nm = str(row["name"] or "")
                if _INTERMEDIARY_ADDR_MARKER_RE.search(nm):
                    has_marker = True
                fam = extract_owner_family_key(nm) or normalize_business_name(nm)
                if fam:
                    families[fam] += 1
                for ra in (row.get("registered_address_raw"), row.get("mailing_address_raw")):
                    if ra:
                        m_u = _UNIT_EXTRACT_RE.search(ra)
                        if m_u:
                            suites.add(m_u.group(1).upper())
        except Exception:
            conn.rollback()

    total_fams_count = sum(families.values())
    top1_share = (families.most_common(1)[0][1] / total_fams_count) if total_fams_count else 1.0
    top2_share = (sum(c for _, c in families.most_common(2)) / total_fams_count) if total_fams_count else 1.0

    return {
        "coarse_address": coarse,
        "total_businesses": total_biz,
        "owning_businesses": owning_biz,
        "owning_ratio": round(owning_biz / max(total_biz, 1), 4),
        "distinct_owner_families": len(families),
        "distinct_unrelated_surnames": len(surnames),
        "distinct_suites": len(suites),
        "top1_family_share": round(top1_share, 4),
        "top2_family_share": round(top2_share, 4),
        "has_intermediary_marker": has_marker,
    }


def _measure_email_across_db(conn, email_str: str) -> Dict[str, Any]:
    em = str(email_str or "").strip().lower()
    if "@" not in em:
        return {"email": em, "domain": ""}
    domain = em.split("@", 1)[1]
    is_webmail = domain in PUBLIC_WEBMAIL_DOMAINS

    hqs: Counter = Counter()
    surnames: Counter = Counter()
    total_biz = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            if is_webmail:
                cur.execute(
                    """
                    SELECT b.id, b.name, b.mail_address, b.business_address
                    FROM businesses b
                    WHERE LOWER(TRIM(b.business_email_address)) = %s
                    LIMIT 400
                    """,
                    (em,),
                )
            else:
                cur.execute(
                    """
                    SELECT b.id, b.name, b.mail_address, b.business_address
                    FROM businesses b
                    WHERE LOWER(TRIM(b.business_email_address)) LIKE %s
                    LIMIT 400
                    """,
                    (f"%@{domain}",),
                )
            biz_rows = cur.fetchall()
            biz_ids = [r["id"] for r in biz_rows]
            total_biz = len(biz_rows)
            for r in biz_rows:
                hq = normalize_mailing_address_coarse(r.get("mail_address") or r.get("business_address") or "")
                if hq:
                    hqs[hq] += 1
            if biz_ids:
                cur.execute(
                    """
                    SELECT DISTINCT p.name
                    FROM principals p
                    WHERE p.business_id = ANY(%s)
                    LIMIT 500
                    """,
                    (biz_ids,),
                )
                for pr in cur.fetchall():
                    toks = normalize_person_name(pr.get("name") or "").split()
                    if toks:
                        surnames[toks[-1]] += 1
        except Exception:
            conn.rollback()

    tot_hqs = sum(hqs.values())
    tot_sur = sum(surnames.values())
    return {
        "email": em,
        "domain": domain,
        "total_businesses": total_biz,
        "distinct_hqs": len(hqs),
        "top2_hq_share": round((sum(c for _, c in hqs.most_common(2)) / tot_hqs) if tot_hqs else 1.0, 4),
        "distinct_surnames": len(surnames),
        "top2_surname_share": round((sum(c for _, c in surnames.most_common(2)) / tot_sur) if tot_sur else 1.0, 4),
        "is_professional_intermediary": bool(_INTERMEDIARY_EMAIL_RE.search(em) or _INTERMEDIARY_EMAIL_RE.search(domain)),
    }


def _measure_principal_across_db(conn, principal_name: str, jur: str) -> Dict[str, Any]:
    """Measure a person's cross-company HQ dispersion, family fanout, and officer roles."""
    norm = normalize_person_name(principal_name)
    canon = canonicalize_person_name(principal_name)
    if not canon:
        return {"principal_name": principal_name}

    hqs: Counter = Counter()
    families: Set[str] = set()
    companies: Set[str] = set()
    has_intermediary_role = False

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. CT SOTS principals
        try:
            cur.execute(
                """
                SELECT p.business_id, p.title, b.name AS biz_name,
                       b.mail_address, b.business_address
                FROM principals p
                JOIN businesses b ON b.id = p.business_id
                WHERE UPPER(TRIM(p.name)) = UPPER(%s)
                   OR UPPER(TRIM(p.name)) = UPPER(%s)
                LIMIT 500
                """,
                (principal_name.strip(), norm),
            )
            for r in cur.fetchall():
                companies.add(f"ct:{r['business_id']}")
                if _INTERMEDIARY_ROLE_RE.search(str(r.get("title") or "")):
                    has_intermediary_role = True
                hq = normalize_mailing_address_coarse(r.get("mail_address") or r.get("business_address") or "")
                if hq:
                    hqs[hq] += 1
                fam = extract_owner_family_key(r.get("biz_name") or "")
                if fam:
                    families.add(fam)
        except Exception:
            conn.rollback()

        # 2. OpenCorporates officers nationwide
        try:
            cur.execute(
                """
                SELECT o.jurisdiction_code, o.company_number, o.position, o.role_class,
                       c.name AS co_name, c.registered_address_coarse, c.mailing_address_coarse
                FROM oc_officers o
                JOIN oc_companies c
                  ON c.jurisdiction_code = o.jurisdiction_code AND c.company_number = o.company_number
                WHERE o.name_norm = %s OR UPPER(TRIM(o.name)) = UPPER(%s)
                LIMIT 600
                """,
                (norm.upper(), principal_name.strip()),
            )
            for r in cur.fetchall():
                companies.add(f"{r['jurisdiction_code']}/{r['company_number']}")
                if _INTERMEDIARY_ROLE_RE.search(str(r.get("position") or "")) or str(r.get("role_class") or "") == "registered_agent":
                    has_intermediary_role = True
                hq = r.get("mailing_address_coarse") or r.get("registered_address_coarse") or ""
                if hq:
                    hqs[hq] += 1
                fam = extract_owner_family_key(r.get("co_name") or "")
                if fam:
                    families.add(fam)
        except Exception:
            conn.rollback()

    tot_hqs = sum(hqs.values())
    return {
        "principal_name": principal_name,
        "total_companies": len(companies),
        "distinct_hqs": len(hqs),
        "top2_hq_share": round((sum(c for _, c in hqs.most_common(2)) / tot_hqs) if tot_hqs else 1.0, 4),
        "distinct_families": len(families),
        "has_intermediary_role": has_intermediary_role,
    }


def _measure_nominee_manager_across_db(conn, corp_name: str) -> Dict[str, Any]:
    norm_b = normalize_business_name(corp_name) or corp_name.upper().strip()
    hqs: Counter = Counter()
    families: Set[str] = set()
    companies: Set[str] = set()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT o.jurisdiction_code, o.company_number, c.name AS co_name,
                       c.registered_address_coarse, c.mailing_address_coarse
                FROM oc_officers o
                JOIN oc_companies c
                  ON c.jurisdiction_code = o.jurisdiction_code AND c.company_number = o.company_number
                WHERE o.is_corporate_officer = TRUE
                  AND (o.name_norm = %s OR UPPER(TRIM(o.name)) = UPPER(%s))
                LIMIT 600
                """,
                (norm_b, corp_name.strip()),
            )
            for r in cur.fetchall():
                companies.add(f"{r['jurisdiction_code']}/{r['company_number']}")
                hq = r.get("mailing_address_coarse") or r.get("registered_address_coarse") or ""
                if hq:
                    hqs[hq] += 1
                fam = extract_owner_family_key(r.get("co_name") or "")
                if fam:
                    families.add(fam)
        except Exception:
            conn.rollback()
    tot_hqs = sum(hqs.values())
    return {
        "manager_name": corp_name,
        "managed_companies": len(companies),
        "distinct_families": len(families),
        "distinct_hqs": len(hqs),
        "top2_hq_share": round((sum(c for _, c in hqs.most_common(2)) / tot_hqs) if tot_hqs else 1.0, 4),
    }


def _find_connecting_bridges_for_report(conn, report: Dict[str, Any]) -> Dict[str, Any]:
    """Discover candidate addresses, emails, corporate managers, and entity metadata connecting
    the reported target (`target_value`) to the reported network (`network_id` / `network_name`)."""
    jur = normalize_jurisdiction(report.get("jurisdiction") or "ct")
    target_type = str(report.get("target_type") or "").lower()
    target_val = str(report.get("target_value") or "").strip()
    ctx = report.get("bridge_context") if isinstance(report.get("bridge_context"), dict) else {}

    candidate_addresses: Set[str] = set()
    candidate_emails: Set[str] = set()
    candidate_managers: Set[str] = set()
    entity_type = ""

    # Include any explicit bridge address/email captured by UI context
    for key in ("address", "mailing_address", "hq_address", "shared_address"):
        if ctx.get(key):
            candidate_addresses.add(str(ctx[key]))
    for addr in ctx.get("shared_addresses") or []:
        if addr:
            candidate_addresses.add(str(addr))
    if target_type == "address":
        candidate_addresses.add(target_val)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if jur == "ct":
            if target_type == "entity":
                try:
                    cur.execute(
                        """
                        SELECT b.id, b.name, b.business_type, b.business_email_address,
                               b.mail_address, b.business_address
                        FROM businesses b
                        WHERE UPPER(TRIM(b.name)) = UPPER(%s)
                           OR b.id = %s
                        LIMIT 5
                        """,
                        (target_val, str(report.get("target_id") or "")),
                    )
                    for row in cur.fetchall():
                        entity_type = str(row.get("business_type") or entity_type)
                        if row.get("business_email_address"):
                            candidate_emails.add(str(row["business_email_address"]))
                        for a in (row.get("mail_address"), row.get("business_address")):
                            if a:
                                candidate_addresses.add(str(a))
                        # Also pull corporate principals of this entity
                        cur.execute(
                            "SELECT name FROM principals WHERE business_id = %s LIMIT 25",
                            (row["id"],),
                        )
                        for pr in cur.fetchall():
                            pn = str(pr.get("name") or "").strip()
                            if pn and not looks_like_person_owner(pn):
                                candidate_managers.add(pn)
                except Exception:
                    conn.rollback()
            elif target_type == "principal":
                try:
                    cur.execute(
                        """
                        SELECT b.mail_address, b.business_address, b.business_email_address
                        FROM principals p
                        JOIN businesses b ON b.id = p.business_id
                        WHERE UPPER(TRIM(p.name)) = UPPER(%s)
                        LIMIT 40
                        """,
                        (target_val,),
                    )
                    for row in cur.fetchall():
                        if row.get("business_email_address"):
                            candidate_emails.add(str(row["business_email_address"]))
                        for a in (row.get("mail_address"), row.get("business_address")):
                            if a:
                                candidate_addresses.add(str(a))
                except Exception:
                    conn.rollback()
        else:
            prop_tbl, _ = _jur_tables(jur)
            try:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{prop_tbl}",))
                if cur.fetchone()["ok"] and target_type in ("entity", "property"):
                    if target_type == "entity":
                        cur.execute(
                            f"""
                            SELECT mailing_address, oc_jurisdiction_code, oc_company_number
                            FROM {prop_tbl}
                            WHERE UPPER(TRIM(owner_name)) = UPPER(%s)
                            LIMIT 20
                            """,
                            (target_val,),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT owner_name, mailing_address, oc_jurisdiction_code, oc_company_number
                            FROM {prop_tbl}
                            WHERE UPPER(TRIM(address)) = UPPER(%s) OR bbl = %s
                            LIMIT 10
                            """,
                            (target_val, str(report.get("target_id") or "")),
                        )
                    oc_keys = []
                    for row in cur.fetchall():
                        if row.get("mailing_address"):
                            candidate_addresses.add(str(row["mailing_address"]))
                        if row.get("oc_jurisdiction_code") and row.get("oc_company_number"):
                            oc_keys.append((row["oc_jurisdiction_code"], row["oc_company_number"]))
                    for oj, on in oc_keys[:5]:
                        cur.execute(
                            """
                            SELECT company_type, registered_address_raw, mailing_address_raw
                            FROM oc_companies
                            WHERE jurisdiction_code = %s AND company_number = %s
                            """,
                            (oj, on),
                        )
                        cr = cur.fetchone()
                        if cr:
                            entity_type = str(cr.get("company_type") or entity_type)
                            for a in (cr.get("registered_address_raw"), cr.get("mailing_address_raw")):
                                if a:
                                    candidate_addresses.add(str(a))
                        cur.execute(
                            """
                            SELECT name FROM oc_officers
                            WHERE jurisdiction_code = %s AND company_number = %s
                              AND is_corporate_officer = TRUE
                            LIMIT 10
                            """,
                            (oj, on),
                        )
                        for mo in cur.fetchall():
                            if mo.get("name"):
                                candidate_managers.add(str(mo["name"]))
            except Exception:
                conn.rollback()

    return {
        "candidate_addresses": sorted(candidate_addresses)[:12],
        "candidate_emails": sorted(candidate_emails)[:10],
        "candidate_managers": sorted(candidate_managers)[:10],
        "entity_type": entity_type,
    }


# ---------------------------------------------------------------------------
# Pure Organic Tweak Discovery for Missed Connections (Anti-Forced-Merge!)
# ---------------------------------------------------------------------------

def _extract_zip5(addr: str) -> str:
    matches = _ZIP5_EXTRACT_RE.findall(str(addr or ""))
    return matches[-1] if matches else ""


def _addresses_safe_to_alias(coarse_a: str, coarse_b: str, *, shared_corroboration: bool = False) -> bool:
    """Strict safety guard before emitting a ZIP-scoped `address_alias` rule.
    NEVER merges different street numbers, quadrants (NW vs NE), lettered avenues (E vs F),
    or numbered highways (HWY 2 vs 93)."""
    if not coarse_a or not coarse_b or coarse_a == coarse_b:
        return False
    ma = _STREET_NUM_RE.match(coarse_a)
    mb = _STREET_NUM_RE.match(coarse_b)
    if not ma or not mb:
        return False
    num_a, body_a = ma.group(1).upper().strip(), ma.group(2).upper().strip()
    num_b, body_b = mb.group(1).upper().strip(), mb.group(2).upper().strip()
    if num_a != num_b:
        return False
    # Never collapse PO BOX numbers with streets or differing quadrants/lettered avenues/highways
    if num_a.startswith("PO BOX"):
        return False
    # Never collapse differing directional/quadrant tokens (NW vs NE, NORTHWEST vs NORTHEAST, N vs S)
    dirs_a = {d.upper() for d in _DIRECTION_TOKEN_RE.findall(body_a)}
    dirs_b = {d.upper() for d in _DIRECTION_TOKEN_RE.findall(body_b)}
    if dirs_a != dirs_b and (dirs_a and dirs_b):
        return False
    la, lb = _LETTERED_AVE_RE.search(body_a), _LETTERED_AVE_RE.search(body_b)
    if la and lb and la.group(1) != lb.group(1):
        return False
    ha, hb = _NUMBERED_HWY_RE.search(body_a), _NUMBERED_HWY_RE.search(body_b)
    if ha and hb and ha.group(1) != hb.group(1):
        return False

    sim = SequenceMatcher(None, body_a, body_b).ratio()
    min_sim = 0.65 if shared_corroboration else 0.78
    return sim >= min_sim


def _is_plausible_abbrev(short_tok: str, long_tok: str) -> bool:
    """Return True if `short_tok` is a clean, unambiguous abbreviation/typo of `long_tok`."""
    s = short_tok.upper().strip()
    l = long_tok.upper().strip()
    if not (s.isalpha() and l.isalpha()) or s == l or len(s) < 2 or len(l) < 4:
        return False
    if l not in _CANON_REAL_ESTATE_TERMS:
        return False
    if len(s) >= len(l):
        # Allow single-character transposition/typo (e.g. MANAGMENT -> MANAGEMENT)
        return SequenceMatcher(None, s, l).ratio() >= 0.80
    # Prefix (e.g. PROP -> PROPERTIES, DEV -> DEVELOPMENT, HLDG -> HOLDINGS)
    if len(s) >= 3 and l.startswith(s):
        return True
    # Consonant-skeleton subsequence starting with the exact same initial letter (e.g. MGMT -> MANAGEMENT, HLDGS -> HOLDINGS)
    if len(s) >= 3 and s[0] == l[0]:
        it = iter(l)
        if all(ch in it for ch in s):
            return True
    return SequenceMatcher(None, s, l).ratio() >= 0.75


def discover_missed_connection_tweaks(
    side_a: Dict[str, Any],
    side_b: Dict[str, Any],
    *,
    blocked_addresses: Sequence[str] = (),
    address_stats_fn: Optional[Callable[[str], Dict[str, Any]]] = None,
    email_stats_fn: Optional[Callable[[str], Dict[str, Any]]] = None,
    has_external_evidence_url: bool = False,
) -> Dict[str, Any]:
    """Forensically inspect public-record profiles of Side A and Side B to find GENERALIZED,
    ORGANIC builder tweaks (`ra_allowlist`, `benign_care_of_token`, `address_alias`,
    `biz_abbrev`, `officer_role_suffix`, `email_allowlist`, `entity_allowlist`) that make
    the connection happen naturally during normal network building across all 20 jurisdictions.

    NEVER invents hardcoded forced merges when no underlying public-record corroboration exists!
    """
    entities_a = [str(x).strip() for x in (side_a.get("entities") or []) if x]
    entities_b = [str(x).strip() for x in (side_b.get("entities") or []) if x]
    etypes_a = {str(k).upper().strip(): str(v) for k, v in (side_a.get("entity_types") or {}).items()}
    etypes_b = {str(k).upper().strip(): str(v) for k, v in (side_b.get("entity_types") or {}).items()}
    addrs_a = [str(x).strip() for x in (side_a.get("raw_addresses") or []) if x]
    addrs_b = [str(x).strip() for x in (side_b.get("raw_addresses") or []) if x]
    emails_a = [str(x).strip().lower() for x in (side_a.get("emails") or []) if x and "@" in str(x)]
    emails_b = [str(x).strip().lower() for x in (side_b.get("emails") or []) if x and "@" in str(x)]
    prins_a = [str(x).strip() for x in (side_a.get("principals") or []) if x]
    prins_b = [str(x).strip() for x in (side_b.get("principals") or []) if x]

    blocked_upper = {str(a).upper().strip() for a in blocked_addresses if a}
    verified_rules: List[Dict[str, Any]] = []
    rationales: List[str] = []
    checks: List[Dict[str, Any]] = []

    coarse_a_map = {}
    for ra in addrs_a:
        c = normalize_mailing_address_coarse(strip_benign_care_of_prefix(ra))
        if c:
            coarse_a_map.setdefault(c, []).append(ra)
    coarse_b_map = {}
    for rb in addrs_b:
        c = normalize_mailing_address_coarse(strip_benign_care_of_prefix(rb))
        if c:
            coarse_b_map.setdefault(c, []).append(rb)

    zips_a = {_extract_zip5(a) for a in addrs_a if _extract_zip5(a)}
    zips_b = {_extract_zip5(b) for b in addrs_b if _extract_zip5(b)}
    shared_zips = zips_a & zips_b

    prins_a_canon = {canonicalize_person_name(p) for p in prins_a if canonicalize_person_name(p)}
    prins_b_canon = {canonicalize_person_name(p) for p in prins_b if canonicalize_person_name(p)}
    shared_prins = prins_a_canon & prins_b_canon
    surnames_a = {normalize_person_name(p).split()[-1] for p in prins_a if normalize_person_name(p).split()}
    surnames_b = {normalize_person_name(p).split()[-1] for p in prins_b if normalize_person_name(p).split()}
    shared_surnames = surnames_a & surnames_b

    fams_a = {extract_owner_family_key(e) for e in entities_a if extract_owner_family_key(e)}
    fams_b = {extract_owner_family_key(e) for e in entities_b if extract_owner_family_key(e)}
    shared_fams = fams_a & fams_b
    shared_corroboration = bool(shared_prins or shared_surnames or shared_fams)

    # --- Detector 1: Over-blocked Cohesive Landlord HQ (`ra_allowlist`) ---
    shared_coarse = set(coarse_a_map.keys()) & set(coarse_b_map.keys())
    for c_addr in sorted(shared_coarse):
        sample_raw = (coarse_a_map[c_addr] + coarse_b_map[c_addr])[0]
        matched_blocked = [
            b for b in sorted(blocked_upper)
            if b == c_addr or (len(b) >= 8 and b in c_addr)
        ]
        is_blocked = bool(matched_blocked) or is_excluded_third_party_address(sample_raw)
        if not is_blocked:
            continue
        stats = address_stats_fn(c_addr) if address_stats_fn else {}
        owning_ratio = float(stats.get("owning_ratio") or 0.0)
        distinct_fams = int(stats.get("distinct_owner_families") or 999)
        distinct_suites = int(stats.get("distinct_suites") or 999)
        top1_share = float(stats.get("top1_family_share") or 0.0)
        has_marker = bool(stats.get("has_intermediary_marker"))
        if (
            stats
            and not has_marker
            and owning_ratio >= 0.45
            and distinct_suites < 4
            and (top1_share >= 0.55 or distinct_fams <= 3)
        ):
            # Allowlist the exact matched blocked substring(s) (so `substrings -= allow_expanded`
            # removes the blocking rule) as well as the full coarse key when distinct.
            keys_to_allow = matched_blocked or [c_addr]
            for allow_k in keys_to_allow:
                verified_rules.append({
                    "rule_type": "ra_allowlist",
                    "rule_key": allow_k,
                    "rule_value": "",
                    "scope": "global",
                })
            rat = (
                f"Both sides share cohesive landlord HQ '{c_addr}' that was over-blocked "
                f"({owning_ratio:.0%} owning, {distinct_fams} family(ies), top-1 share {top1_share:.0%}, "
                f"no RA/law/CPA markers). Adding global `ra_allowlist` entry."
            )
            rationales.append(rat)
            checks.append({"detector": "ra_allowlist", "verdict": "verified", "address": c_addr, "keys": keys_to_allow, "stats": stats})
        else:
            checks.append({"detector": "ra_allowlist", "verdict": "rejected_still_hub", "address": c_addr, "stats": stats})

    # --- Detector 2: Benign C/O Prefix Blocking Shared Address (`benign_care_of_token`) ---
    all_side_words = {
        w
        for nm in (entities_a + entities_b + prins_a + prins_b)
        for w in re.findall(r"[A-Z]{3,}", nm.upper())
        if w not in {"LLC", "INC", "CORP", "THE", "AND", "PROPERTIES", "REALTY", "HOLDINGS", "GROUP"}
    }
    for side_addrs, other_coarse_set in ((addrs_a, set(coarse_b_map.keys())), (addrs_b, set(coarse_a_map.keys()))):
        for raw_addr in side_addrs:
            m = _CARE_OF_SPLIT_RE.match(raw_addr)
            if not m:
                continue
            co_token = " ".join(m.group(1).upper().strip().split())
            remainder = m.group(2).strip()
            rem_coarse = normalize_mailing_address_coarse(remainder)
            if not rem_coarse or rem_coarse not in other_coarse_set:
                continue
            if _INTERMEDIARY_ADDR_MARKER_RE.search(co_token) or is_excluded_third_party_entity(co_token):
                checks.append({"detector": "benign_care_of_token", "verdict": "rejected_3p_co", "token": co_token})
                continue
            # Require token to relate to the operator's own name/family (proving internal in-house C/O)
            tok_words = set(re.findall(r"[A-Z]{3,}", co_token))
            if tok_words & all_side_words:
                verified_rules.append({
                    "rule_type": "benign_care_of_token",
                    "rule_key": co_token,
                    "rule_value": "",
                    "scope": "global",
                })
                rat = (
                    f"Stripping operator-owned C/O prefix '{co_token}' reveals shared HQ '{rem_coarse}'. "
                    f"Adding global `benign_care_of_token` rule so similar C/O filings normalize organically."
                )
                rationales.append(rat)
                checks.append({"detector": "benign_care_of_token", "verdict": "verified", "token": co_token, "address": rem_coarse})

    # --- Detector 3: ZIP-Scoped Address Variant / Typo (`address_alias`) ---
    for ra in addrs_a:
        za = _extract_zip5(ra)
        ca = normalize_mailing_address_coarse(strip_benign_care_of_prefix(ra))
        if not za or not ca:
            continue
        for rb in addrs_b:
            zb = _extract_zip5(rb)
            cb = normalize_mailing_address_coarse(strip_benign_care_of_prefix(rb))
            if not zb or za != zb or not cb or ca == cb:
                continue
            if _addresses_safe_to_alias(ca, cb, shared_corroboration=shared_corroboration):
                # Canonicalize the shorter/misspelled variant -> cleaner/longer form
                canonical, variant = (ca, cb) if len(ca) >= len(cb) else (cb, ca)
                alias_key = f"{variant}||{za}"
                verified_rules.append({
                    "rule_type": "address_alias",
                    "rule_key": alias_key,
                    "rule_value": canonical,
                    "scope": "global",
                })
                rat = (
                    f"Same street number & ZIP ({za}) address variant detected: '{variant}' -> '{canonical}'. "
                    f"Adding ZIP-scoped `address_alias` rule so both sides unify organically."
                )
                rationales.append(rat)
                checks.append({"detector": "address_alias", "verdict": "verified", "alias_key": alias_key, "canonical": canonical})
                break

    # --- Detector 4: Unrecognized Business Abbreviation / Typo (`biz_abbrev`) ---
    if shared_zips or shared_corroboration or shared_coarse:
        for ea in entities_a:
            na = _LEGAL_SUFFIX_TAIL_RE.sub("", normalize_business_name(ea)).strip()
            ta = na.split()
            if len(ta) < 2:
                continue
            for eb in entities_b:
                nb = _LEGAL_SUFFIX_TAIL_RE.sub("", normalize_business_name(eb)).strip()
                tb = nb.split()
                if len(tb) != len(ta) or ta == tb:
                    continue
                diffs = [(x, y) for x, y in zip(ta, tb) if x != y]
                if len(diffs) != 1:
                    continue
                tok_a, tok_b = diffs[0]
                if _is_plausible_abbrev(tok_a, tok_b):
                    short_t, long_t = tok_a, tok_b
                elif _is_plausible_abbrev(tok_b, tok_a):
                    short_t, long_t = tok_b, tok_a
                else:
                    continue
                verified_rules.append({
                    "rule_type": "biz_abbrev",
                    "rule_key": short_t,
                    "rule_value": long_t,
                    "scope": "global",
                })
                rat = (
                    f"Corroborated business-name abbreviation mismatch between '{ea}' and '{eb}': "
                    f"'{short_t}' -> '{long_t}'. Adding global `biz_abbrev` rule across all jurisdictions."
                )
                rationales.append(rat)
                checks.append({"detector": "biz_abbrev", "verdict": "verified", "short": short_t, "long": long_t})

    # --- Detector 5: Unrecognized Officer Role Suffix (`officer_role_suffix`) ---
    for pa in prins_a:
        na_toks = normalize_person_name(pa).split()
        if len(na_toks) < 2:
            continue
        for pb in prins_b:
            nb_toks = normalize_person_name(pb).split()
            if len(nb_toks) < 2 or na_toks == nb_toks:
                continue
            longer, shorter = (na_toks, nb_toks) if len(na_toks) == len(nb_toks) + 1 else (
                (nb_toks, na_toks) if len(nb_toks) == len(na_toks) + 1 else (None, None)
            )
            if not longer or longer[:-1] != shorter:
                continue
            extra = longer[-1]
            if _PLAUSIBLE_OFFICER_ROLE_TOKEN_RE.match(extra):
                verified_rules.append({
                    "rule_type": "officer_role_suffix",
                    "rule_key": extra.upper(),
                    "rule_value": "",
                    "scope": "global",
                })
                rat = (
                    f"Principal '{pa}' vs '{pb}' differed only by unstripped officer role suffix '{extra}'. "
                    f"Adding global `officer_role_suffix` rule so filings with '{extra}' align everywhere."
                )
                rationales.append(rat)
                checks.append({"detector": "officer_role_suffix", "verdict": "verified", "suffix": extra.upper()})

    # --- Detector 6: Over-Blocked Cohesive Custom Email Domain (`email_allowlist`) ---
    domains_a = {e.split("@", 1)[1] for e in emails_a if e.split("@", 1)[1] not in PUBLIC_WEBMAIL_DOMAINS}
    domains_b = {e.split("@", 1)[1] for e in emails_b if e.split("@", 1)[1] not in PUBLIC_WEBMAIL_DOMAINS}
    for dom in sorted(domains_a & domains_b):
        sample_em = next((e for e in emails_a + emails_b if e.endswith("@" + dom)), f"info@{dom}")
        if get_email_match_key(sample_em, {}) is not None:
            continue
        if _INTERMEDIARY_EMAIL_RE.search(dom):
            checks.append({"detector": "email_allowlist", "verdict": "rejected_intermediary_domain", "domain": dom})
            continue
        e_stats = email_stats_fn(sample_em) if email_stats_fn else {}
        top2_hq = float(e_stats.get("top2_hq_share") or 0.0)
        top2_sur = float(e_stats.get("top2_surname_share") or 0.0)
        if e_stats and top2_hq >= 0.65 and top2_sur >= 0.55 and not e_stats.get("is_professional_intermediary"):
            verified_rules.append({
                "rule_type": "email_allowlist",
                "rule_key": dom,
                "rule_value": "",
                "scope": "global",
            })
            rat = (
                f"Both sides share cohesive custom email domain '{dom}' (top-2 HQ share {top2_hq:.0%}, "
                f"top-2 surname share {top2_sur:.0%}). Adding global `email_allowlist` entry."
            )
            rationales.append(rat)
            checks.append({"detector": "email_allowlist", "verdict": "verified", "domain": dom, "stats": e_stats})

    # --- Detector 7: False-Positive Institutional Entity Exclusion (`entity_allowlist`) ---
    has_organic_link_signal = bool(shared_coarse or shared_prins or (domains_a & domains_b))
    if has_organic_link_signal:
        for ent_list, etypes in ((entities_a, etypes_a), (entities_b, etypes_b)):
            for ent in ent_list:
                if not is_excluded_third_party_entity(ent):
                    continue
                et = etypes.get(ent.upper().strip(), "")
                if _NON_LANDLORD_TYPE_RE.search(et) or _HARD_NON_LANDLORD_ALLOWLIST_BLOCK_RE.search(ent):
                    checks.append({"detector": "entity_allowlist", "verdict": "rejected_genuine_non_landlord", "entity": ent, "type": et})
                    continue
                if _FOR_PROFIT_ENTITY_TYPE_RE.search(et):
                    norm_ent = normalize_business_name(ent) or ent.upper().strip()
                    verified_rules.append({
                        "rule_type": "entity_allowlist",
                        "rule_key": norm_ent,
                        "rule_value": "",
                        "scope": "global",
                    })
                    rat = (
                        f"Private for-profit entity '{ent}' (filing type: '{et}') shares verified ownership signals "
                        f"with the network but tripped institutional exclusion. Adding `entity_allowlist` entry."
                    )
                    rationales.append(rat)
                    checks.append({"detector": "entity_allowlist", "verdict": "verified", "entity": norm_ent, "type": et})

    # Deduplicate verified rules
    dedup_rules: List[Dict[str, Any]] = []
    seen_rk: Set[Tuple[str, str, str]] = set()
    for r in verified_rules:
        k = (r["rule_type"], r["rule_key"], r.get("scope", "global"))
        if k not in seen_rk:
            seen_rk.add(k)
            dedup_rules.append(r)

    if dedup_rules:
        return {
            "status": "verified_applied",
            "rules": dedup_rules,
            "rationale": " ".join(rationales),
            "evidence": {"checks": checks, "shared_zips": sorted(shared_zips), "shared_principals": sorted(shared_prins)},
        }

    # No organic builder tweak verified from loaded public records -> NEVER force a hardcoded merge!
    has_loaded_records = bool(entities_a or addrs_a or prins_a) and bool(entities_b or addrs_b or prins_b)
    if has_external_evidence_url or not has_loaded_records:
        return {
            "status": "needs_human_review",
            "rules": [],
            "rationale": (
                "No organic normalization or allowlist tweak in loaded public filings bridges these two records yet "
                "(no hardcoded merge applied). Escalated for human review / external-source inspection."
            ),
            "evidence": {"checks": checks, "has_loaded_records": has_loaded_records},
        }

    return {
        "status": "rejected_unverified",
        "rules": [],
        "rationale": (
            "No linking change applied — loaded public filings for both sides show no corroborating shared "
            "address, email, principal, abbreviation variant, or over-blocked HQ (anti-fabrication safeguard)."
        ),
        "evidence": {"checks": checks, "has_loaded_records": has_loaded_records},
    }


def _collect_side_profile(
    conn,
    jur: str,
    *,
    target_type: str,
    target_value: str,
    target_id: str = "",
    network_id: str = "",
    network_key: str = "",
    bridge_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Pull entities, entity types, raw mailing/business/OC addresses, emails, and principals
    for one side of a reported missed connection from loaded PostgreSQL tables."""
    entities: Set[str] = set()
    entity_types: Dict[str, str] = {}
    raw_addresses: Set[str] = set()
    emails: Set[str] = set()
    principals: Set[str] = set()

    ctx = bridge_context if isinstance(bridge_context, dict) else {}
    for k in ("address", "mailing_address", "hq_address", "shared_address"):
        if ctx.get(k):
            raw_addresses.add(str(ctx[k]))
    for a in ctx.get("shared_addresses") or []:
        if a:
            raw_addresses.add(str(a))
    for e in (ctx.get("linked_entities") or []) + (ctx.get("operating_entities") or []):
        if e:
            entities.add(str(e))
    for p in (ctx.get("key_principals") or []) + (ctx.get("key_officers") or []):
        if p:
            principals.add(str(p))

    t_val = str(target_value or "").strip()
    t_type = str(target_type or "").lower()
    if t_type in ("entity", "network", "network_merge") and t_val:
        entities.add(t_val)
    elif t_type == "principal" and t_val:
        principals.add(t_val)
    elif t_type == "address" and t_val:
        raw_addresses.add(t_val)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if jur == "ct":
            biz_ids: Set[str] = set()
            if network_id and str(network_id).isdigit():
                try:
                    cur.execute(
                        "SELECT entity_id FROM entity_networks WHERE network_id = %s AND entity_type = 'business' LIMIT 60",
                        (int(network_id),),
                    )
                    biz_ids.update(str(r["entity_id"]) for r in cur.fetchall() if r.get("entity_id"))
                except Exception:
                    conn.rollback()
            if not biz_ids and t_val:
                try:
                    if t_type == "principal":
                        cur.execute(
                            "SELECT business_id FROM principals WHERE UPPER(TRIM(name)) = UPPER(%s) LIMIT 40",
                            (t_val,),
                        )
                        biz_ids.update(str(r["business_id"]) for r in cur.fetchall() if r.get("business_id"))
                    elif t_type == "property":
                        cur.execute(
                            "SELECT business_id, owner, mailing_address FROM properties WHERE UPPER(TRIM(address)) = UPPER(%s) OR id::text = %s LIMIT 15",
                            (t_val, target_id),
                        )
                        for r in cur.fetchall():
                            if r.get("business_id"):
                                biz_ids.add(str(r["business_id"]))
                            if r.get("owner"):
                                entities.add(str(r["owner"]))
                            if r.get("mailing_address"):
                                raw_addresses.add(str(r["mailing_address"]))
                    else:
                        cur.execute(
                            "SELECT id FROM businesses WHERE UPPER(TRIM(name)) = UPPER(%s) OR id = %s LIMIT 15",
                            (t_val, target_id),
                        )
                        biz_ids.update(str(r["id"]) for r in cur.fetchall() if r.get("id"))
                except Exception:
                    conn.rollback()

            if biz_ids:
                bid_list = sorted(biz_ids)[:60]
                try:
                    cur.execute(
                        """
                        SELECT id, name, business_type, business_email_address,
                               mail_address, mail_city, mail_state, mail_zip,
                               business_address, business_city, business_state, business_zip
                        FROM businesses
                        WHERE id = ANY(%s)
                        """,
                        (bid_list,),
                    )
                    for r in cur.fetchall():
                        nm = str(r.get("name") or "").strip()
                        if nm:
                            entities.add(nm)
                            if r.get("business_type"):
                                entity_types[nm.upper()] = str(r["business_type"])
                        if r.get("business_email_address"):
                            emails.add(str(r["business_email_address"]).strip().lower())
                        for addr_parts in (
                            (r.get("mail_address"), r.get("mail_city"), r.get("mail_state"), r.get("mail_zip")),
                            (r.get("business_address"), r.get("business_city"), r.get("business_state"), r.get("business_zip")),
                        ):
                            full_a = ", ".join(str(x).strip() for x in addr_parts if x and str(x).strip())
                            if full_a:
                                raw_addresses.add(full_a)
                    cur.execute(
                        "SELECT DISTINCT name FROM principals WHERE business_id = ANY(%s) LIMIT 80",
                        (bid_list,),
                    )
                    for pr in cur.fetchall():
                        if pr.get("name"):
                            principals.add(str(pr["name"]).strip())
                except Exception:
                    conn.rollback()
        else:
            prop_tbl, _ = _jur_tables(jur)
            try:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{prop_tbl}",))
                if cur.fetchone()["ok"]:
                    rows = []
                    if network_key:
                        cur.execute(
                            f"SELECT owner_name, mailing_address, oc_jurisdiction_code, oc_company_number FROM {prop_tbl} WHERE network_key = %s LIMIT 80",
                            (network_key,),
                        )
                        rows = cur.fetchall()
                    elif network_id:
                        cur.execute(
                            f"SELECT owner_name, mailing_address, oc_jurisdiction_code, oc_company_number FROM {prop_tbl} WHERE network_id::text = %s LIMIT 80",
                            (str(network_id),),
                        )
                        rows = cur.fetchall()
                    if not rows and t_val:
                        if t_type == "property":
                            cur.execute(
                                f"SELECT owner_name, mailing_address, oc_jurisdiction_code, oc_company_number FROM {prop_tbl} WHERE UPPER(TRIM(address)) = UPPER(%s) OR bbl = %s LIMIT 25",
                                (t_val, target_id),
                            )
                            rows = cur.fetchall()
                        elif t_type == "principal":
                            cur.execute(
                                """
                                SELECT o.jurisdiction_code, o.company_number, o.name AS officer_name,
                                       c.name AS co_name, c.company_type, c.registered_address_raw, c.mailing_address_raw
                                FROM oc_officers o
                                JOIN oc_companies c ON c.jurisdiction_code = o.jurisdiction_code AND c.company_number = o.company_number
                                WHERE o.name_norm = %s OR UPPER(TRIM(o.name)) = UPPER(%s)
                                LIMIT 40
                                """,
                                (normalize_person_name(t_val).upper(), t_val),
                            )
                            for oc_r in cur.fetchall():
                                if oc_r.get("officer_name"):
                                    principals.add(str(oc_r["officer_name"]))
                                if oc_r.get("co_name"):
                                    entities.add(str(oc_r["co_name"]))
                                    if oc_r.get("company_type"):
                                        entity_types[str(oc_r["co_name"]).upper()] = str(oc_r["company_type"])
                                for a in (oc_r.get("mailing_address_raw"), oc_r.get("registered_address_raw")):
                                    if a:
                                        raw_addresses.add(str(a))
                        else:
                            cur.execute(
                                f"SELECT owner_name, mailing_address, oc_jurisdiction_code, oc_company_number FROM {prop_tbl} WHERE UPPER(TRIM(owner_name)) = UPPER(%s) LIMIT 40",
                                (t_val,),
                            )
                            rows = cur.fetchall()

                    oc_keys: List[Tuple[str, str]] = []
                    for r in rows:
                        if r.get("owner_name"):
                            entities.add(str(r["owner_name"]).strip())
                        if r.get("mailing_address"):
                            raw_addresses.add(str(r["mailing_address"]).strip())
                        if r.get("oc_jurisdiction_code") and r.get("oc_company_number"):
                            oc_keys.append((r["oc_jurisdiction_code"], r["oc_company_number"]))
                    for oj, on in oc_keys[:20]:
                        cur.execute(
                            "SELECT name, company_type, registered_address_raw, mailing_address_raw FROM oc_companies WHERE jurisdiction_code = %s AND company_number = %s",
                            (oj, on),
                        )
                        cr = cur.fetchone()
                        if cr:
                            if cr.get("name"):
                                entities.add(str(cr["name"]).strip())
                                if cr.get("company_type"):
                                    entity_types[str(cr["name"]).strip().upper()] = str(cr["company_type"])
                            for a in (cr.get("mailing_address_raw"), cr.get("registered_address_raw")):
                                if a:
                                    raw_addresses.add(str(a).strip())
                        cur.execute(
                            "SELECT name FROM oc_officers WHERE jurisdiction_code = %s AND company_number = %s AND COALESCE(is_current, TRUE) = TRUE LIMIT 20",
                            (oj, on),
                        )
                        for o_row in cur.fetchall():
                            if o_row.get("name"):
                                principals.add(str(o_row["name"]).strip())
            except Exception:
                conn.rollback()

    return {
        "entities": sorted(entities)[:60],
        "entity_types": entity_types,
        "raw_addresses": sorted(raw_addresses)[:60],
        "emails": sorted(emails)[:30],
        "principals": sorted(principals)[:60],
    }


def evaluate_single_report(conn, report: Dict[str, Any]) -> Dict[str, Any]:
    """Forensically evaluate one pending report against loaded public records.

    Handles both `false_positive` (erroneous inclusion) and `missed_connection`
    (organic builder tweak discovery) reports with full anti-weaponization guards.
    """
    jur = normalize_jurisdiction(report.get("jurisdiction") or "ct")
    report_kind = str(report.get("report_kind") or "false_positive").strip().lower()

    if report_kind == "missed_connection":
        ctx = report.get("bridge_context") if isinstance(report.get("bridge_context"), dict) else {}
        side_a = _collect_side_profile(
            conn,
            jur,
            target_type=str(report.get("target_type") or "network"),
            target_value=str(report.get("target_value") or report.get("network_name") or ""),
            target_id=str(report.get("target_id") or ""),
            network_id=str(report.get("network_id") or ""),
            network_key=str(report.get("network_key") or ""),
            bridge_context=ctx,
        )
        side_b = _collect_side_profile(
            conn,
            jur,
            target_type=str(report.get("related_target_type") or "entity"),
            target_value=str(report.get("related_target_value") or ""),
            target_id=str(report.get("related_target_id") or ""),
            bridge_context={"address": ctx.get("related_address", "")} if ctx.get("related_address") else {},
        )
        blocked_addrs = load_registrar_address_substrings(conn)
        return discover_missed_connection_tweaks(
            side_a,
            side_b,
            blocked_addresses=blocked_addrs,
            address_stats_fn=lambda addr: _measure_address_across_db(conn, addr, jur),
            email_stats_fn=lambda em: _measure_email_across_db(conn, em),
            has_external_evidence_url=bool(str(report.get("evidence_url") or "").strip()),
        )

    target_type = str(report.get("target_type") or "").lower()
    target_val = str(report.get("target_value") or "").strip()
    reason_code = str(report.get("reason_code") or "other").lower()

    discovered = _find_connecting_bridges_for_report(conn, report)
    verified_rules: List[Dict[str, Any]] = []
    rationales: List[str] = []
    rejection_notes: List[str] = []
    evidence_log: Dict[str, Any] = {"discovered": discovered, "checks": []}

    # 1. Check non-landlord / placeholder entity when entity is flagged
    if target_type == "entity" and reason_code in ("non_landlord_entity", "wrong_entity", "other"):
        v, rspec, rat = evaluate_non_landlord_entity_evidence(
            {"entity_name": target_val, "entity_type": discovered.get("entity_type") or ""}
        )
        evidence_log["checks"].append({"check": "non_landlord_entity", "verdict": v, "rationale": rat})
        if v == "verified" and rspec:
            verified_rules.append(rspec)
            rationales.append(rat)
        elif reason_code == "non_landlord_entity" and v == "rejected":
            rejection_notes.append(rat)

    # 2. Check principal homonym / 3rd-party intermediary when principal is flagged
    if target_type == "principal":
        p_stats = _measure_principal_across_db(conn, target_val, jur)
        v, rspec, rat = evaluate_principal_evidence(p_stats)
        evidence_log["checks"].append({"check": "principal_dispersion", "verdict": v, "stats": p_stats, "rationale": rat})
        if v == "verified" and rspec:
            verified_rules.append(rspec)
            rationales.append(rat)
        elif v == "rejected":
            rejection_notes.append(rat)

    # 3. Check commercial nominee-manager factories connecting the target
    if target_type == "entity" and reason_code == "nominee_manager_factory":
        discovered["candidate_managers"].insert(0, target_val)
    for mgr in discovered.get("candidate_managers") or []:
        m_stats = _measure_nominee_manager_across_db(conn, mgr)
        v, rspec, rat = evaluate_nominee_manager_evidence(m_stats)
        evidence_log["checks"].append({"check": "nominee_manager", "manager": mgr, "verdict": v, "stats": m_stats, "rationale": rat})
        if v == "verified" and rspec:
            verified_rules.append(rspec)
            rationales.append(rat)

    # 4. Check connecting addresses for third-party / multi-tenant / maildrop hubs
    seen_coarse: Set[str] = set()
    for raw_addr in discovered.get("candidate_addresses") or []:
        a_stats = _measure_address_across_db(conn, raw_addr, jur)
        c_key = a_stats.get("coarse_address") or ""
        if not c_key or c_key in seen_coarse:
            continue
        seen_coarse.add(c_key)
        v, rspec, rat = evaluate_address_hub_evidence(a_stats)
        evidence_log["checks"].append({"check": "address_hub", "verdict": v, "stats": a_stats, "rationale": rat})
        if v == "verified" and rspec:
            verified_rules.append(rspec)
            rationales.append(rat)
        elif v == "rejected":
            rejection_notes.append(rat)

    # 5. Check connecting emails/domains for third-party CPA/law/filing hubs
    seen_emails: Set[str] = set()
    for em in discovered.get("candidate_emails") or []:
        em_l = em.strip().lower()
        if not em_l or em_l in seen_emails:
            continue
        seen_emails.add(em_l)
        e_stats = _measure_email_across_db(conn, em_l)
        v, rspec, rat = evaluate_email_hub_evidence(e_stats)
        evidence_log["checks"].append({"check": "email_hub", "verdict": v, "stats": e_stats, "rationale": rat})
        if v == "verified" and rspec:
            verified_rules.append(rspec)
            rationales.append(rat)
        elif v == "rejected":
            rejection_notes.append(rat)

    # Deduplicate verified rules by (rule_type, rule_key, scope)
    dedup_rules: List[Dict[str, Any]] = []
    seen_rk: Set[Tuple[str, str, str]] = set()
    for r in verified_rules:
        k = (r["rule_type"], r["rule_key"], r.get("scope", "global"))
        if k not in seen_rk:
            seen_rk.add(k)
            dedup_rules.append(r)

    if dedup_rules:
        return {
            "status": "verified_applied",
            "rules": dedup_rules,
            "rationale": " ".join(rationales),
            "evidence": evidence_log,
        }

    # If the user flagged a stale deed / wrong label, route to human review rather than rejecting outright
    if reason_code in ("stale_or_sold_property", "wrong_label") or target_type == "label":
        return {
            "status": "needs_human_review",
            "rules": [],
            "rationale": (
                f"Flagged '{target_val}' as {reason_code}; public-record hub/homonym rules do not resolve "
                f"label or deed-freshness edits automatically. Escalated for maintainer review."
            ),
            "evidence": evidence_log,
        }

    # Anti-weaponization rejection when every structural check confirmed a valid cohesive landlord link
    if rejection_notes:
        return {
            "status": "rejected_unverified",
            "rules": [],
            "rationale": (
                "No linking change applied — loaded public records indicate a cohesive, legitimate ownership link "
                "(anti-weaponization safeguard): " + " ".join(rejection_notes[:3])
            ),
            "evidence": evidence_log,
        }

    return {
        "status": "needs_human_review",
        "rules": [],
        "rationale": (
            f"Automated public-record inspection for '{target_val}' in {jur.upper()} was inconclusive "
            f"(no bridge crossed systemic intermediary/homonym thresholds). Queued for maintainer review."
        ),
        "evidence": evidence_log,
    }


def find_impacted_jurisdictions(conn, rules: Sequence[Dict[str, Any]], origin_jur: str) -> List[str]:
    """Scan ALL 20 jurisdictions to find every dataset that contains entities/addresses/principals
    affected by the newly generalized global rules so system-wide fixes spread automatically."""
    impacted: Set[str] = {normalize_jurisdiction(origin_jur)}
    if not rules:
        return sorted(impacted)

    with conn.cursor() as cur:
        for rule in rules:
            rt = rule.get("rule_type")
            rk = str(rule.get("rule_key") or "").strip().upper()
            if not rk:
                continue

            if rt in ("third_party_hub_block", "ra_allowlist", "address_alias"):
                addr_lookup = rk.split("||", 1)[0]
                like_pat = f"{addr_lookup[:22]}%"
                # Check CT
                try:
                    cur.execute(
                        "SELECT EXISTS(SELECT 1 FROM businesses WHERE UPPER(COALESCE(mail_address,'')) LIKE %s OR UPPER(COALESCE(business_address,'')) LIKE %s LIMIT 1)",
                        (like_pat, like_pat),
                    )
                    if cur.fetchone()[0]:
                        impacted.add("ct")
                except Exception:
                    conn.rollback()
                # Check OC companies by state prefix -> maps directly to nationwide jurisdictions!
                try:
                    cur.execute(
                        "SELECT DISTINCT jurisdiction_code FROM oc_companies WHERE registered_address_coarse = %s OR mailing_address_coarse = %s LIMIT 50",
                        (addr_lookup, addr_lookup),
                    )
                    for (jcode,) in cur.fetchall():
                        st = str(jcode or "").replace("us_", "").lower()
                        if st in STATE_JURISDICTIONS or st in ("nj", "dc", "ct"):
                            impacted.add(st)
                        elif st == "pa":
                            impacted.add("philadelphia")
                        elif st == "md":
                            impacted.add("baltimore")
                        elif st == "mi":
                            impacted.add("detroit")
                except Exception:
                    conn.rollback()

            elif rt in ("third_party_email_block", "email_allowlist"):
                impacted.add("ct")

            elif rt in ("ambiguous_homonym_block", "third_party_principal_block", "nominee_manager_block"):
                try:
                    cur.execute(
                        "SELECT DISTINCT jurisdiction_code FROM oc_officers WHERE name_norm = %s LIMIT 50",
                        (rk,),
                    )
                    for (jcode,) in cur.fetchall():
                        st = str(jcode or "").replace("us_", "").lower()
                        if st in STATE_JURISDICTIONS or st in ("nj", "dc", "ct"):
                            impacted.add(st)
                        elif st == "pa":
                            impacted.add("philadelphia")
                        elif st == "md":
                            impacted.add("baltimore")
                        elif st == "mi":
                            impacted.add("detroit")
                except Exception:
                    conn.rollback()

            elif rt in ("non_landlord_entity_block", "placeholder_owner", "entity_allowlist"):
                try:
                    cur.execute(
                        "SELECT DISTINCT jurisdiction_code FROM oc_companies WHERE name_norm = %s LIMIT 50",
                        (rk,),
                    )
                    for (jcode,) in cur.fetchall():
                        st = str(jcode or "").replace("us_", "").lower()
                        if st in STATE_JURISDICTIONS or st in ("nj", "dc", "ct"):
                            impacted.add(st)
                        elif st == "pa":
                            impacted.add("philadelphia")
                        elif st == "md":
                            impacted.add("baltimore")
                        elif st == "mi":
                            impacted.add("detroit")
                except Exception:
                    conn.rollback()

            elif rt in ("biz_abbrev", "benign_care_of_token", "officer_role_suffix"):
                # Global normalizers apply to every future build across all 20 jurisdictions;
                # always rebuild origin immediately and also check CT if the token appears there.
                try:
                    if rt == "biz_abbrev":
                        cur.execute(
                            "SELECT EXISTS(SELECT 1 FROM businesses WHERE UPPER(name) LIKE %s LIMIT 1)",
                            (f"% {rk} %",),
                        )
                    elif rt == "officer_role_suffix":
                        cur.execute(
                            "SELECT EXISTS(SELECT 1 FROM principals WHERE UPPER(name) LIKE %s LIMIT 1)",
                            (f"% {rk}",),
                        )
                    else:
                        cur.execute(
                            "SELECT EXISTS(SELECT 1 FROM businesses WHERE UPPER(COALESCE(mail_address,'')) LIKE %s LIMIT 1)",
                            (f"%{rk}%",),
                        )
                    if cur.fetchone()[0]:
                        impacted.add("ct")
                except Exception:
                    conn.rollback()

    return sorted(impacted)


def _snapshot_network_metrics(conn, jur: str, network_id: str, network_name: str) -> Dict[str, Any]:
    j = normalize_jurisdiction(jur)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            if j == "ct":
                if network_id and str(network_id).isdigit():
                    cur.execute(
                        """
                        SELECT id, property_count, business_count, principal_count
                        FROM networks WHERE id = %s
                        """,
                        (int(network_id),),
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)
                return {"jurisdiction": "ct", "network_id": network_id}
            else:
                _, net_tbl = _jur_tables(j)
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{net_tbl}",))
                if cur.fetchone()["ok"] and network_id:
                    cur.execute(
                        f"SELECT id, display_name, property_count, entity_count FROM {net_tbl} WHERE id::text = %s LIMIT 1",
                        (str(network_id),),
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)
        except Exception:
            conn.rollback()
    return {"jurisdiction": j, "network_id": network_id, "network_name": network_name}


def rebuild_jurisdictions(jurisdictions: Sequence[str], *, run_audit: bool = True) -> Dict[str, Any]:
    """Rebuild affected jurisdictions using existing shadow/hardened pipelines + audit gates."""
    jurs = sorted({normalize_jurisdiction(j) for j in jurisdictions if j})
    results: Dict[str, Any] = {}
    env = os.environ.copy()
    env["PYTHONPATH"] = REPO_ROOT
    env["DATABASE_URL"] = _db_url()

    states = [j for j in jurs if j in STATE_JURISDICTIONS]
    cities = [j for j in jurs if j in CITY_JURISDICTIONS]

    if "ct" in jurs:
        t0 = time.time()
        cmd = [sys.executable, os.path.join(REPO_ROOT, "scripts", "safe_network_refresh.py"), "--skip-linking"]
        res = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=2400)
        ok = res.returncode == 0
        if ok:
            subprocess.run(
                [sys.executable, os.path.join(REPO_ROOT, "api", "generate_insights.py")],
                cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=1200,
            )
        results["ct"] = {
            "ok": ok,
            "returncode": res.returncode,
            "seconds": round(time.time() - t0, 1),
            "tail": (res.stdout or res.stderr or "")[-400:],
        }

    if "nj" in jurs:
        t0 = time.time()
        res = subprocess.run(
            [sys.executable, "-m", "nj.build_nj_networks"],
            cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=1800,
        )
        results["nj"] = {
            "ok": res.returncode == 0,
            "returncode": res.returncode,
            "seconds": round(time.time() - t0, 1),
            "tail": (res.stdout or res.stderr or "")[-400:],
        }

    if states or cities:
        t0 = time.time()
        cmd = [
            sys.executable,
            os.path.join(REPO_ROOT, "scripts", "rebuild_hardened_networks.py"),
            "--skip-ra-refresh",
            "--skip-oc-relink",
            "--states", *states,
            "--cities", *cities,
        ]
        res = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=3600)
        ok = res.returncode == 0
        for j in states + cities:
            results[j] = {
                "ok": ok,
                "returncode": res.returncode,
                "seconds": round(time.time() - t0, 1),
                "tail": (res.stdout or res.stderr or "")[-400:],
            }

    if run_audit and jurs:
        audit_cmd = [
            sys.executable,
            os.path.join(REPO_ROOT, "scripts", "audit_network_bridges.py"),
            "--jurisdictions", *jurs,
        ]
        ares = subprocess.run(audit_cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=600)
        results["_audit"] = {
            "ok": ares.returncode == 0,
            "returncode": ares.returncode,
            "tail": (ares.stdout or ares.stderr or "")[-500:],
        }

    return results


def process_pending_reports(
    conn,
    *,
    dry_run: bool = False,
    skip_rebuild: bool = False,
    limit: int = 50,
    json_path: str = DEFAULT_RULES_JSON_PATH,
) -> Dict[str, Any]:
    """Main nightly workflow: evaluate all `pending` reports, persist verified global rules,
    rebuild impacted jurisdictions with post-build audit gates, and auto-rollback on regression."""
    ensure_reviewer_feedback_tables(conn)
    ensure_linking_rules_tables(conn)
    reload_learned_rules(conn, json_path=json_path)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM network_error_reports
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        pending = cur.fetchall()

    if not pending:
        logger.info("No pending reviewer feedback reports to evaluate.")
        return {"processed": 0, "verified": 0, "rejected": 0, "needs_review": 0, "rebuilt": []}

    logger.info("Evaluating %d pending reviewer feedback report(s)...", len(pending))
    verified_reports: List[Tuple[Dict[str, Any], Dict[str, Any], List[str], Dict[str, Any]]] = []
    all_impacted_jurs: Set[str] = set()
    counts = {"processed": len(pending), "verified": 0, "rejected": 0, "needs_review": 0}

    for rep in pending:
        rep_id = int(rep["id"])
        jur = normalize_jurisdiction(rep["jurisdiction"])
        before_metrics = _snapshot_network_metrics(conn, jur, rep.get("network_id") or "", rep.get("network_name") or "")
        outcome = evaluate_single_report(conn, rep)
        status = outcome["status"]

        if status == "verified_applied":
            applied_rules: List[Dict[str, Any]] = []
            if not dry_run:
                for rspec in outcome["rules"]:
                    upsert_learned_rule(
                        conn,
                        rspec["rule_type"],
                        rspec["rule_key"],
                        rspec.get("rule_value", ""),
                        scope=rspec.get("scope", "global"),
                        discovered_in=f"reviewer_report:{rep_id}:{jur}",
                        evidence={
                            "report_id": rep_id,
                            "jurisdiction": jur,
                            "target_type": rep.get("target_type"),
                            "target_value": rep.get("target_value"),
                            "rationale": outcome["rationale"],
                        },
                        json_path=json_path,
                    )
                    applied_rules.append(rspec)
            else:
                applied_rules = list(outcome["rules"])

            impacted = find_impacted_jurisdictions(conn, applied_rules, jur)
            all_impacted_jurs.update(impacted)
            verified_reports.append((rep, outcome, impacted, before_metrics))
            counts["verified"] += 1

            if not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET status = 'verified_applied',
                            evaluation_summary = %s::jsonb,
                            applied_rules = %s::jsonb,
                            affected_jurisdictions = %s::jsonb,
                            rebuild_summary = %s::jsonb,
                            evaluated_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            json.dumps({"rationale": outcome["rationale"], "evidence": outcome["evidence"]}, default=str),
                            json.dumps(applied_rules),
                            json.dumps(impacted),
                            json.dumps({"before": before_metrics, "rebuild": "pending"}),
                            rep_id,
                        ),
                    )
                conn.commit()
        else:
            if status == "rejected_unverified":
                counts["rejected"] += 1
            else:
                counts["needs_review"] += 1
            if not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET status = %s,
                            evaluation_summary = %s::jsonb,
                            evaluated_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            status,
                            json.dumps({"rationale": outcome["rationale"], "evidence": outcome["evidence"]}, default=str),
                            rep_id,
                        ),
                    )
                conn.commit()
        logger.info("  Report #%d [%s] %s='%s' -> %s", rep_id, jur.upper(), rep["target_type"], rep["target_value"], status)

    # Rebuild impacted jurisdictions once in a single clean batch and verify post-build audit gate
    rebuild_results: Dict[str, Any] = {}
    if verified_reports and not dry_run and not skip_rebuild:
        jurs_to_rebuild = sorted(all_impacted_jurs)
        logger.info("Rebuilding %d jurisdiction(s) impacted by verified generalized rules: %s", len(jurs_to_rebuild), jurs_to_rebuild)
        rebuild_results = rebuild_jurisdictions(jurs_to_rebuild, run_audit=True)
        audit_ok = bool((rebuild_results.get("_audit") or {}).get("ok", True))

        if not audit_ok:
            logger.error("Post-rebuild audit gate FAILED — rolling back newly applied reviewer rules!")
            for rep, outcome, impacted, before_metrics in verified_reports:
                for rspec in outcome["rules"]:
                    deactivate_learned_rule(
                        conn,
                        rspec["rule_type"],
                        rspec["rule_key"],
                        scope=rspec.get("scope", "global"),
                        json_path=json_path,
                    )
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET status = 'needs_human_review',
                            rebuild_summary = %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            json.dumps({"before": before_metrics, "rolled_back": True, "audit": rebuild_results.get("_audit")}, default=str),
                            int(rep["id"]),
                        ),
                    )
                conn.commit()
            # Re-run rebuild to restore clean pre-change live state
            rebuild_jurisdictions(jurs_to_rebuild, run_audit=False)
        else:
            for rep, outcome, impacted, before_metrics in verified_reports:
                jur = normalize_jurisdiction(rep["jurisdiction"])
                after_metrics = _snapshot_network_metrics(conn, jur, rep.get("network_id") or "", rep.get("network_name") or "")
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET rebuild_summary = %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            json.dumps(
                                {
                                    "before": before_metrics,
                                    "after": after_metrics,
                                    "rebuilt_jurisdictions": impacted,
                                    "audit_passed": True,
                                },
                                default=str,
                            ),
                            int(rep["id"]),
                        ),
                    )
                conn.commit()

    counts["rebuilt"] = sorted(all_impacted_jurs)
    counts["rebuild_results"] = rebuild_results
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Nightly evidence-based reviewer feedback evaluator & generalizer")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate reports without writing rules or rebuilding.")
    parser.add_argument("--skip-rebuild", action="store_true", help="Persist rules without triggering jurisdiction rebuilds.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--rebuild-only", action="store_true", help="Run rebuild only (used by 1-click revert endpoint).")
    parser.add_argument("--jurisdictions", type=str, default="", help="Comma-separated jurisdictions for --rebuild-only.")
    parser.add_argument("--revert-report-id", type=int, default=0, help="Report ID whose revert_rebuild_status to update.")
    args = parser.parse_args()

    if args.rebuild_only:
        jurs = [j.strip() for j in args.jurisdictions.split(",") if j.strip()]
        res = rebuild_jurisdictions(jurs, run_audit=False)
        all_ok = all(v.get("ok", True) for k, v in res.items() if not k.startswith("_"))
        if args.revert_report_id:
            conn = psycopg2.connect(_db_url())
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE network_error_reports
                        SET revert_rebuild_status = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        ("completed" if all_ok else "failed", int(args.revert_report_id)),
                    )
                    cur.execute(
                        """
                        UPDATE network_error_report_reverts
                        SET rebuild_status = %s
                        WHERE report_id = %s
                        """,
                        ("completed" if all_ok else "failed", int(args.revert_report_id)),
                    )
                conn.commit()
            finally:
                conn.close()
        logger.info("Rebuild-only finished for %s (all_ok=%s).", jurs, all_ok)
        return 0 if all_ok else 1

    conn = psycopg2.connect(_db_url())
    try:
        summary = process_pending_reports(
            conn,
            dry_run=args.dry_run,
            skip_rebuild=args.skip_rebuild,
            limit=args.limit,
        )
        logger.info("Nightly reviewer report evaluation complete: %s", json.dumps(summary, default=str))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
