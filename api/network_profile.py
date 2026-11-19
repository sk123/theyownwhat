"""
api/network_profile.py
======================
Deterministic, 100% source-backed operator & business profiling for ownership networks.

Epistemic honesty invariant (`docs/ASSURANCE_POLICY.md`):
- NEVER invent, guess, or hardcode brand/operator names.
- Every returned `operator_name`, `operating_entities`, `primary_hq_address`,
  `key_officers`, and `sample_entities` value MUST come verbatim from loaded
  public records (`oc_companies`, `oc_officers`, `{jur}_networks.member_names`,
  `{jur}_networks.member_addresses`, or `connection_signals`).
- Every derived operator label carries an explicit `operator_source` and
  `operator_basis` explaining which public records produced it.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from api.shared_utils import (
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    looks_like_person_owner,
)

logger = logging.getLogger("network-profile")

CITY_TO_OC_JUR: Dict[str, str] = {
    "ma": "us_ma",
    "boston": "us_ma",
    "ca": "us_ca",
    "ny": "us_ny",
    "nyc": "us_ny",
    "fl": "us_fl",
    "miami": "us_fl",
    "il": "us_il",
    "chicago": "us_il",
    "mn": "us_mn",
    "minneapolis": "us_mn",
    "nj": "us_nj",
    "dc": "us_dc",
    "baltimore": "us_md",
    "detroit": "us_mi",
    "philadelphia": "us_pa",
    "co": "us_co",
    "mt": "us_mt",
    "nc": "us_nc",
    "oh": "us_oh",
    "va": "us_va",
    "vt": "us_vt",
    "wi": "us_wi",
    "ar": "us_ar",
    "ct": "us_ct",
}

_OPERATING_KEYWORDS_RE = re.compile(
    r"\b("
    r"COMPANIES|COMPANY|CO\.?|MANAGEMENT|MGMT|DEVELOPMENT|REDEVELOPMENT|GROUP|"
    r"HOLDINGS|HOLDING|REALTY|PROPERTIES|PROPERTY|CAPITAL|RESIDENTIAL|"
    r"PARTNERS|ENTERPRISES|COMMUNITIES|HOMES|CORPORATION|CORP|MANAGER|"
    r"EQUITIES|EQUITY|VENTURES|ASSETS|ASSET|ADVISORS|INVESTMENTS|FINANCE|"
    r"ASSOCIATES|HOUSING|BUILDERS|CONSTRUCTION|SERVICES|SERVICE|REAL\s+ESTATE"
    r")\b",
    re.IGNORECASE,
)

_FLAGSHIP_OPERATING_RE = re.compile(
    r"\b("
    r"COMPANIES|COMPANY|CO\.?|MANAGEMENT|DEVELOPMENT|GROUP|HOLDINGS|REALTY|"
    r"PROPERTIES|CAPITAL|RESIDENTIAL|PARTNERS|ENTERPRISES|COMMUNITIES|"
    r"HOMES|CORPORATION|FINANCE|EQUITIES|ADVISORS"
    r")\b",
    re.IGNORECASE,
)

_SPV_LIKE_RE = re.compile(
    r"(?:^\s*\d|"
    r"\b(?:LIMITED\s+PARTNERSHIP|APARTMENTS|APARTMENT|APTS|CONDOMINIUM|CONDO|"
    r"NOMINEE\s+TRUST|REVOCABLE\s+TRUST|LIVING\s+TRUST|FAMILY\s+TRUST|"
    r"BORROWER|SPE|PHASE\s+[A-Z0-9-]+|BLOCK\s+[A-Z0-9-]+|LOT\s+[A-Z0-9-]+|PARCEL\s+[A-Z0-9-]+|"
    r"GP\s+LLC|GP\s+INC|GP,\s*INC|"
    r"LP\s+LLC|FUNDING\s+[IVX0-9]+|SERIES\s+[A-Z0-9]+|JV-?\d+|"
    r"LOFTS|TOWERS|VILLAGE|COMMONS|PLAZA|MANOR|TERRACE|GARDENS|ESTATES|"
    r"STREET|ST|AVENUE|AVE|ROAD|RD|BOULEVARD|BLVD|DRIVE|DR|LANE|LN|PLACE|PL|SQUARE|SQ|PARKWAY|PKWY|HWY|"
    r"TAX\s+DEPT|ASSET\s+COMPANY\s+\d+|20\d{2}-\d+)\b|"
    r"\b(?:TS|TRS|TRST|TRSTS|TR|ETAL|LL)\s*$|"
    r"\b(?:I|II|III|IV|V|VI)\s+(?:[A-Z]{2,4}\s+)?(?:LLC|LP|INC|CORP)\b|"
    r"\b\d{1,4}\s*,?\s*(?:LLC|LP|INC|CORP)\b|"
    r"\b[A-Z]{2,4}\s+[A-Z]{1,3}\d+\s+(?:LLC|LP|INC|CORP)\b)",
    re.IGNORECASE,
)

_NON_OPERATING_CHARITABLE_RE = re.compile(
    r"\b(FOUNDATION|CHARITABLE|SCHOLARSHIP|ENDOWMENT|CHURCH|MINISTRIES|"
    r"SYNAGOGUE|MOSQUE|PARISH|DIOCESE|UNIVERSITY|COLLEGE|HOSPITAL|"
    r"FELLOWSHIP|ALUMNI|CEMETERY)\b",
    re.IGNORECASE,
)

_NONPROFIT_JV_RE = re.compile(
    r"\b(HOUSING\s+CORPORATION|ECONOMIC\s+DEVELOPMENT|NEIGHBORHOOD\s+DEVELOPMENT|"
    r"COMMUNITY\s+DEVELOPMENT|SENIORLIFE|SENIOR\s+HOUSING|ELDERLY\s+HOUSING|"
    r"CDC|URBAN\s+AFFAIRS)\b",
    re.IGNORECASE,
)

_RA_OR_SYNTHETIC_ADDR_RE = re.compile(
    r"\b(NEW\s+CASTLE|WILMINGTON,\s*DE|DOVER,\s*DE|LEWES,\s*DE|"
    r"CORPORATION\s+SERVICE|CT\s+CORPORATION|C\s*T\s+CORP|CSC\b|"
    r"REGISTERED\s+AGENTS|NORTHWEST\s+REGISTERED|INCORP\s+SERVICES|"
    r"LEGALZOOM|COGENCY\s+GLOBAL|CAPITOL\s+SERVICES|NATIONAL\s+REGISTERED|"
    r"UNITED\s+AGENT|PARACORP|CORPORATE\s+CREATIONS|RESIDENT\s+AGENTS|"
    r"UNDELIVERABLE|99999|0000\s+UNDELIVERABLE|NO\s+ADDRESS|UNKNOWN)\b",
    re.IGNORECASE,
)

_CARE_OF_EXTRACT_RE = re.compile(
    r"(?:C/O|C\\O|ATTN:?)\s+([A-Za-z][A-Za-z0-9 .,&'/-]{2,64}?)(?:,|\||$)",
    re.IGNORECASE,
)

_CARE_OF_NOISE_RE = re.compile(
    r"^(?:US|USA|U\.?S\.?A?\.?|MANAGEMENT\s+OFFICE|PROPERTY\s+MANAGEMENT|"
    r"TAX\s+DEP(?:T|ARTMENT)?|PM\s+DEPT(?:\s*#?\d+)?|ACCOUNTS?\s+PAYABLE|"
    r"ACCOUNTING(?:\s+DEPT)?|FINANCE\s+DEPT|LEGAL\s+DEPT|OWNER|LANDLORD|"
    r"MANAGER|OFFICE|PRESIDENT|TREASURER|SECRETARY|CONTROLLER|PRINCIPAL|"
    r"RESIDENT\s+AGENT|REGISTERED\s+AGENT|CORPORATION\s+SERVICE\s+CO.*|"
    r"CT\s+CORPORATION.*|CSC.*|NATIONAL\s+REGISTERED.*|INCORP.*|LEGALZOOM.*|"
    r"THE\s+CORPORATION\s+TRUST.*|UNITED\s+STATES\s+CORPORATION.*)$",
    re.IGNORECASE,
)

_LEADING_WORD_NUMBERS = {
    "ONE": "1",
    "TWO": "2",
    "THREE": "3",
    "FOUR": "4",
    "FIVE": "5",
    "SIX": "6",
    "SEVEN": "7",
    "EIGHT": "8",
    "NINE": "9",
    "TEN": "10",
}

_BRAND_STOPWORDS = frozenset({
    "LLC", "INC", "CORP", "CORPORATION", "LTD", "LIMITED", "LP", "LLP", "PLLC",
    "THE", "AND", "FOR", "WITH", "FROM", "REALTY", "RLTY", "TRUST", "TRUSTEE",
    "TRUSTEES", "NOMINEE", "REVOCABLE", "IRREVOCABLE", "LIVING", "FAMILY",
    "PROPERTIES", "PROPERTY", "PROP", "HOLDINGS", "HOLDING", "MANAGEMENT", "MGMT",
    "DEVELOPMENT", "REDEVELOPMENT", "PARTNERS", "PARTNERSHIP", "GROUP", "COMPANY",
    "COMPANIES", "ASSOCIATES", "ASSOC", "ESTATE", "ESTATES", "REAL", "CAPITAL",
    "INVESTMENTS", "INVESTMENT", "ENTERPRISES", "VENTURES", "HOUSING", "APARTMENTS",
    "APARTMENT", "APTS", "STREET", "AVE", "AVENUE", "ROAD", "DRIVE", "LANE",
    "COURT", "PLACE", "SQUARE", "PARK", "PARKWAY", "PKWY", "BLVD", "BOULEVARD",
    "HILL", "HILLS", "NORTH", "SOUTH", "EAST", "WEST", "CENTRAL", "NEW", "OLD",
    "ENGLAND", "MASS", "MASSACHUSETTS", "BOSTON", "CAMBRIDGE", "SOMERVILLE",
    "BROOKLINE", "ALLSTON", "BRIGHTON", "DORCHESTER", "ROXBURY", "LOWELL",
    "WORCESTER", "SPRINGFIELD", "QUINCY", "NEWTON", "CHELSEA", "REVERE",
    "EVERETT", "LYNN", "MALDEN", "MEDFORD", "YORK", "BROOKLYN", "BRONX",
    "QUEENS", "MANHATTAN", "STATEN", "ISLAND", "CALIFORNIA", "ANGELES",
    "FRANCISCO", "DIEGO", "FLORIDA", "MIAMI", "ILLINOIS", "CHICAGO",
    "CONNECTICUT", "HARTFORD", "MINNESOTA", "MINNEAPOLIS", "PHILADELPHIA",
    "BALTIMORE", "DETROIT", "WASHINGTON", "CONDO", "CONDOMINIUM", "CENTER",
    "CENTRE", "COMMONS", "VILLAGE", "TOWER", "TOWERS", "PLAZA", "BUILDING",
    "BUILDINGS", "MGR", "MANAGER", "MANAGING", "MEMBER", "GENERAL", "PARTNER",
    "OWNER", "OWNERS", "HOMES", "HOME", "RESIDENTIAL", "COMMUNITIES", "COMMUNITY",
    "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN",
    "PHASE", "MILL", "MILLS", "HOUSE", "LOFTS", "LAND", "FUND", "FUNDING",
    "ASSET", "ASSETS", "SERVICES", "SERVICE", "EQUITY", "EQUITIES", "URBAN",
    "METRO", "METROPOLITAN", "AMERICAN", "AMERICA", "NATIONAL", "FIRST", "SECOND",
    "THIRD", "ETAL", "TRS", "TRST", "TRSTS", "ACQUISITIONS", "ACQUISITION",
    "BORROWER", "SPE", "UNIT", "UNITS", "MAIN", "BROADWAY", "BEACON",
    "COMMONWEALTH", "TERRACE", "GARDENS", "MANOR", "HEIGHTS", "VIEW", "COURT",
    "GREEN", "BAY", "RIVER", "LAKE", "VALLEY", "CREEK", "WOOD", "WOODS",
    "SPRING", "FIELD", "PORT", "POINT", "HARBOR", "BEACH", "CITY", "TOWN",
    "STATE", "COUNTY", "USA", "UNITED", "STATES", "CORP", "INC", "CO",
    "COMMERCIAL", "MULTI", "MULTIFAMILY", "SENIOR", "ELDERLY", "AFFORDABLE",
    "PRESERVATION", "RENTAL", "RENTALS", "LEASING", "LEASE", "OFFICE",
    "SFR", "REIT", "TAX", "DEPT", "DEPARTMENT", "SUB", "PORTFOLIO", "OPERATING",
    "FRAMINGHAM", "WOBURN", "BELMONT", "BRAINTREE", "DARTMOUTH", "PLYMOUTH",
    "WESTBOROUGH", "ARLINGTON", "NEEDHAM", "WATERTOWN", "WALTHAM", "WELLESLEY",
    "LEXINGTON", "DEDHAM", "ANDOVER", "BEVERLY", "PEABODY", "SALEM", "FALL",
    "LAWRENCE", "HAVERHILL", "BROCKTON", "TAUNTON", "FITCHBURG", "GARDNER",
    "FRANKLIN", "WESTWOOD", "SCITUATE", "SWAMPSCOTT", "MILFORD", "NATICK",
    "ACTON", "WAYLAND", "WINTHROP", "MELROSE", "CHESTNUT",
})

_SUFFIX_STRIP_RE = re.compile(
    r"(?:,?\s+(?:A\s+[A-Z]+\s+)?(?:L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|"
    r"CORPORATION|LTD\.?|LIMITED\s+PARTNERSHIP|LIMITED\s+LIABILITY\s+COMPANY|"
    r"L\.?P\.?|L\.?L\.?P\.?|CO\.?|P\.?C\.?|MASS\s+LLC))+\s*$",
    re.IGNORECASE,
)


def _canonical_org_key(name: str) -> str:
    """Suffix-stripped uppercase core key for deduplicating org variants."""
    s = re.sub(r"[^A-Za-z0-9&\s]+", " ", (name or "").upper())
    s = re.sub(r"\s+", " ", s).strip()
    if s.startswith("THE "):
        s = s[4:].strip()
    prev = None
    while prev != s:
        prev = s
        s = _SUFFIX_STRIP_RE.sub("", s).strip()
    return s


def _extract_brand_tokens(text: str) -> List[str]:
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", (text or "").upper())
    tokens = []
    for tok in cleaned.split():
        if len(tok) < 3 or tok.isdigit():
            continue
        if tok in _BRAND_STOPWORDS:
            continue
        tokens.append(tok)
    return tokens


def _token_matches_stem(cand_token: str, ref_token: str) -> bool:
    """True if two brand tokens are identical or share a strong >=4-char prefix (e.g. WINN -> WINNCOMPANIES)."""
    if cand_token == ref_token:
        return True
    if len(ref_token) >= 4 and cand_token.startswith(ref_token):
        return True
    if len(cand_token) >= 4 and ref_token.startswith(cand_token):
        return True
    return False


def _extract_person_surnames(names: Iterable[str]) -> set[str]:
    surnames: set[str] = set()
    suffixes = {"JR", "SR", "II", "III", "IV", "MD", "ESQ", "PHD", "TR", "TRS", "TRST", "TRUSTEE"}
    for raw in names:
        if not raw:
            continue
        text = str(raw).strip().upper()
        if not text:
            continue
        if "," in text:
            # LAST, FIRST format
            parts = [p.strip() for p in text.split(",") if p.strip()]
            toks = re.findall(r"[A-Z]{2,}", parts[0]) if parts else []
            if toks:
                cand = toks[-1]
                if len(cand) >= 4 and cand not in _BRAND_STOPWORDS and cand not in suffixes:
                    surnames.add(cand)
            continue
        toks = [t for t in re.findall(r"[A-Z]{2,}", text) if t not in suffixes]
        if len(toks) >= 2:
            last = toks[-1]
            if len(last) >= 4 and last not in _BRAND_STOPWORDS:
                surnames.add(last)
    return surnames


def _normalize_officer_variants(names: Iterable[str]) -> List[str]:
    """Return both full normalized name AND middle-initial-stripped FIRST LAST for OC index lookup."""
    variants: List[str] = []
    seen: set[str] = set()
    suffixes = {"JR", "SR", "II", "III", "IV", "MD", "ESQ", "TR", "TRS", "TRST", "TRUSTEE"}
    for raw in names:
        if not raw:
            continue
        s = str(raw).strip().upper()
        if "," in s and len(s.split(",")) == 2:
            last, first = [p.strip() for p in s.split(",", 1)]
            s = f"{first} {last}"
        toks = [t for t in re.findall(r"[A-Z]+", s) if t not in suffixes]
        if len(toks) < 2:
            continue
        full = " ".join(toks)
        if full not in seen:
            seen.add(full)
            variants.append(full)
        # Middle-initial / middle-name stripped variant (matches CA oc_officers name_norm)
        short = f"{toks[0]} {toks[-1]}"
        if short not in seen:
            seen.add(short)
            variants.append(short)
    return variants


def hq_street_zip_key(address: Optional[str]) -> Optional[str]:
    """Extract coarse normalized `NUM STREET|ZIP5` key for physical HQ matching, ignoring RA stubs."""
    if not address:
        return None
    raw = str(address).strip()
    if not raw or _RA_OR_SYNTHETIC_ADDR_RE.search(raw):
        return None
    # Strip leading C/O or ATTN clause up to first comma or street number
    cleaned = re.sub(
        r"^(?:C/O|C\\O|ATTN:?)\s+[^,0-9]{1,50},?\s*",
        "",
        raw,
        flags=re.IGNORECASE,
    ).strip()
    cleaned = re.sub(r"^C/O\s+USA?\]?,?\s*", "", cleaned, flags=re.IGNORECASE).strip()
    if not cleaned or re.match(r"^P\.?\s*O\.?\s*BOX\b", cleaned, re.IGNORECASE):
        return None

    # Extract 5-digit ZIP (prefer last 5-digit sequence)
    zips = re.findall(r"\b(\d{5})(?:-\d{4})?\b", cleaned)
    zip5 = zips[-1] if zips else ""
    if zip5 == "99999":
        return None

    # Normalize leading spelled-out street numbers (e.g. ONE WASHINGTON MALL -> 1 WASHINGTON MALL)
    first_seg = cleaned.split(",")[0].strip().upper()
    words = re.findall(r"[A-Z0-9]+", first_seg)
    if not words:
        return None
    if words[0] in _LEADING_WORD_NUMBERS:
        words[0] = _LEADING_WORD_NUMBERS[words[0]]
    if not words[0].isdigit():
        # Try finding embedded street number after a C/O org without comma
        num_idx = next((i for i, w in enumerate(words) if w.isdigit()), None)
        if num_idx is None or num_idx + 1 >= len(words):
            return None
        words = words[num_idx:]
    if len(words) < 2:
        return None

    street_num = words[0].lstrip("0") or "0"
    if street_num == "0":
        return None
    # Skip directional prefix (N/S/E/W) to get primary street token
    idx = 1
    if idx < len(words) - 1 and words[idx] in {"N", "S", "E", "W", "NORTH", "SOUTH", "EAST", "WEST"}:
        idx += 1
    street_word = words[idx]
    if len(street_word) < 2:
        return None
    if not zip5:
        return None
    return f"{street_num} {street_word}|{zip5}"


def clean_display_hq(address: Optional[str]) -> Optional[str]:
    """Clean assessor/registry boilerplate (`C/O US,`, `C/O USA,`, `-0000`, double commas) from an HQ address."""
    if not address:
        return None
    s = str(address).strip()
    if not s or _RA_OR_SYNTHETIC_ADDR_RE.search(s):
        return None
    # Strip C/O US / C/O USA prefixes
    s = re.sub(r"^(?:C/O|C\\O)\s+U\.?S\.?A?\.?\]?,?\s*", "", s, flags=re.IGNORECASE).strip()
    # Strip embedded ", C/O <ORG>," so HQ stays a clean street address
    s = re.sub(r",?\s*(?:C/O|C\\O|ATTN:?)\s+[^,]{2,60}(?=,)", "", s, flags=re.IGNORECASE)
    # Strip leading "C/O <ORG>, " or "C/O <ORG> " before a street number
    s = re.sub(r"^(?:C/O|C\\O|ATTN:?)\s+[^,0-9]{2,55},?\s+(?=(?:\d|ONE\b|TWO\b|THREE\b))", "", s, flags=re.IGNORECASE)
    # Remove duplicate suite tokens like "SUITE 110, SUITE 110" and "-0000" ZIP4 suffixes
    s = re.sub(r"\b(\d{5})-0000\b", r"\1", s)
    s = re.sub(r"\b(SUITE\s+[A-Z0-9-]+),\s*\1\b", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r",\s*,+", ", ", s)
    s = re.sub(r"\s+", " ", s).strip(" ,;")
    if len(s) < 8:
        return None
    if not re.search(r"\b(?:\d{1,6}|ONE|TWO|THREE)\b", s, re.IGNORECASE):
        return None
    return s


def extract_care_of_orgs(addresses: Iterable[str]) -> Counter:
    """Extract cleaned `C/O <ORG>` / `ATTN: <ORG>` operating org names from raw addresses."""
    counts: Counter = Counter()
    for raw in addresses:
        if not raw:
            continue
        text = str(raw)
        for m in _CARE_OF_EXTRACT_RE.finditer(text):
            org = m.group(1).strip(" .,-;:")
            # Trim trailing street address if no comma separated org from street number
            org = re.sub(r"\s+\d{1,6}\s+[A-Za-z].*$", "", org).strip(" .,-;:")
            org = re.sub(r"\s+", " ", org)
            if len(org) < 3 or len(org) > 58:
                continue
            if _CARE_OF_NOISE_RE.match(org):
                continue
            if org[0].isdigit():
                continue
            if is_excluded_third_party_entity(org):
                continue
            # Skip individual human names in C/O unless they carry a corporate keyword
            if looks_like_person_owner(org) and not _OPERATING_KEYWORDS_RE.search(org):
                continue
            counts[org] += 1
    return counts


def _dedupe_human_officers(
    primary_human_name: Optional[str],
    source_human_names: Sequence[str],
    preferred_tokens: Sequence[str] = (),
    officer_scores: Optional[Dict[str, float]] = None,
    max_officers: int = 4,
) -> Tuple[List[str], int]:
    """
    Deduplicate human officer variants (e.g. 'LEONARD J SAMIA' / 'SAMIA, LEONARD J.')
    and rank the most representative people across the entire network (by entity frequency,
    managing/operating-company officer roles, and operator-surname corroboration).
    """
    by_canon: Dict[str, str] = {}
    order: List[str] = []
    suffixes = {"JR", "SR", "II", "III", "IV", "MD", "ESQ", "TR", "TRS", "TRST", "TRUSTEE"}

    def _canon_for(raw_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not raw_name:
            return None, None
        cleaned = " ".join(str(raw_name).strip().split())
        if not cleaned or len(cleaned) < 3 or "+" in cleaned or "&" in cleaned or "/" in cleaned:
            return None, None
        if is_excluded_third_party_entity(cleaned):
            return None, None
        if _OPERATING_KEYWORDS_RE.search(cleaned) or re.search(r"\b(LLC|INC|CORP|LP|GP|HOLDINGS|HOLDIMGS|SFR|TRUST)\b", cleaned, re.IGNORECASE):
            return None, None
        # Strip leading TRUSTEE / TR prefixes and collapse duplicated full names ("David Veneziano David Veneziano")
        cleaned = re.sub(r"^(?:TRUSTEE|TRST|TRS|TR)\s+", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"^([A-Za-z][A-Za-z .'-]{3,}?)\s+\1$", r"\1", cleaned, flags=re.IGNORECASE).strip()
        disp = cleaned
        if "," in disp and len(disp.split(",")) == 2:
            last, first = [p.strip() for p in disp.split(",", 1)]
            if first and last and not re.search(r"\b(JR|SR|II|III|IV|ESQ|TR|TRS)\b", first, re.IGNORECASE):
                disp = f"{first} {last}"
        toks = [t for t in re.findall(r"[A-Z]+", disp.upper()) if t not in suffixes]
        if len(toks) < 2 or len(toks) > 4:
            return None, None
        # Collapse duplicated trailing surname like "ALEXANDRA SAMIA SOLIA SOLIA"
        if len(toks) >= 3 and toks[-1] == toks[-2]:
            disp = re.sub(rf"\s+{re.escape(toks[-1])}\s*$", "", disp, flags=re.IGNORECASE).strip()
            toks = toks[:-1]
        # Collapse reversed LAST FIRST duplicates (e.g. "WILDER BENJAMIN" == "BENJAMIN J WILDER")
        pair = sorted((toks[0], toks[-1]))
        return f"{pair[0]}|{pair[1]}", disp

    def _add(raw_name: Optional[str]) -> None:
        canon, disp = _canon_for(raw_name)
        if not canon or not disp:
            return
        if canon not in by_canon:
            by_canon[canon] = disp
            order.append(canon)
        else:
            existing = by_canon[canon]
            if len(disp) > len(existing) and "," not in disp and "  " not in disp:
                by_canon[canon] = disp

    if primary_human_name:
        _add(primary_human_name)
    for nm in source_human_names or ():
        _add(nm)
    for raw_nm in (officer_scores or {}).keys():
        _add(raw_nm)

    if not order:
        return [], 0

    canon_scores: Dict[str, float] = defaultdict(float)
    for raw_nm, val in (officer_scores or {}).items():
        canon, _ = _canon_for(raw_nm)
        if canon:
            canon_scores[canon] += float(val or 0.0)

    pref_set = {t.upper() for t in preferred_tokens if len(t) >= 4}
    primary_canon, _ = _canon_for(primary_human_name) if primary_human_name else (None, None)

    def _rank_key(canon_key: str) -> Tuple[float, int]:
        score = canon_scores.get(canon_key, 0.0)
        surname = canon_key.split("|")[-1]
        if pref_set and any(_token_matches_stem(pt, surname) for pt in pref_set):
            score += 12.0
        # Slight tie-breaker for existing primary_human_name when scores are equal or absent
        if primary_canon and canon_key == primary_canon:
            score += 1.5
        return (-score, order.index(canon_key))

    ranked_canons = sorted(order, key=_rank_key)
    return [by_canon[c] for c in ranked_canons[:max_officers]], len(order)


def _pick_sample_entities(
    member_names: Sequence[str],
    exclude_keys: set[str],
    display_name: Optional[str] = None,
    limit: int = 3,
) -> List[str]:
    """Select up to `limit` informative affiliated entity names (preferring operating/named LLCs over street SPVs)."""
    seen_keys = set(exclude_keys)
    candidates: List[Tuple[int, int, str]] = []
    for raw in member_names or ():
        if not raw:
            continue
        nm = " ".join(str(raw).strip().split())
        if not nm or looks_like_person_owner(nm) or is_excluded_third_party_entity(nm):
            continue
        key = _canonical_org_key(nm)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        is_spv = bool(_SPV_LIKE_RE.search(nm))
        has_op = bool(_OPERATING_KEYWORDS_RE.search(nm))
        is_disp = bool(display_name and _canonical_org_key(display_name) == key)
        # Prefer named operating entities (tier 0), then display_name (tier 1), then cleaner LLCs (tier 2)
        tier = 0 if (has_op and not is_spv) else (1 if is_disp else (2 if not is_spv else 3))
        candidates.append((tier, len(nm), nm))

    candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    return [c[2] for c in candidates[:limit]]


def derive_network_business_profile(
    display_name: Optional[str],
    member_names: Sequence[str],
    member_addresses: Sequence[str],
    connection_signals: Optional[Dict[str, Any]] = None,
    oc_enrichment: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deterministically derive a source-backed operator/business profile for a network.

    `oc_enrichment` (optional, populated via `fetch_batch_network_oc_profiles`) may contain:
      - `corp_parents`: list[tuple[str, int]]       # (corporate_parent_name, managed_company_count)
      - `oc_care_of`: list[tuple[str, int]]         # (care_of_org_name, filing_count)
      - `oc_hq_addresses`: list[tuple[str, int]]    # (raw_address, company_count)
      - `hq_operating_cos`: list[dict]              # [{name, shared_officers, shares_hq, care_of_org}]
    """
    signals = connection_signals or {}
    enrich = oc_enrichment or {}
    members = [str(m).strip() for m in (member_names or ()) if m and str(m).strip()]
    addrs = [str(a).strip() for a in (member_addresses or ()) if a and str(a).strip()]

    primary_human = signals.get("primary_human_name")
    source_humans = [str(h).strip() for h in (signals.get("source_human_names") or ()) if h]
    if not primary_human and display_name and looks_like_person_owner(display_name):
        primary_human = display_name

    corp_members = [m for m in members if not looks_like_person_owner(m)]
    business_count = len(corp_members)
    if business_count == 0 and display_name and not looks_like_person_owner(display_name):
        business_count = 1

    # 1. Pick cleanest verified physical HQ address (OC registered HQ first, then member_addresses)
    primary_hq_address: Optional[str] = None
    hq_entity_count: int = 0
    hq_keys: set[str] = set()

    oc_hq_groups: Dict[str, Tuple[int, str]] = {}
    for raw_addr, cnt in (enrich.get("oc_hq_addresses") or ()):
        cleaned = clean_display_hq(raw_addr)
        if not cleaned:
            continue
        k = hq_street_zip_key(raw_addr) or cleaned.upper()
        if hq_street_zip_key(raw_addr):
            hq_keys.add(hq_street_zip_key(raw_addr))
        prev_cnt, prev_addr = oc_hq_groups.get(k, (0, cleaned))
        # Prefer fuller/cleaner variant when merging same street+ZIP
        best_addr = prev_addr if (prev_cnt >= cnt and len(prev_addr) >= len(cleaned)) else cleaned
        oc_hq_groups[k] = (prev_cnt + int(cnt or 1), best_addr)

    if oc_hq_groups:
        ranked_hqs = sorted(oc_hq_groups.values(), key=lambda x: (-x[0], len(x[1])))
        hq_entity_count, primary_hq_address = ranked_hqs[0]
    else:
        # Fall back to cleanest non-RA mailing address from member_addresses
        addr_groups: Dict[str, Tuple[int, str]] = {}
        for raw_addr in addrs:
            cleaned = clean_display_hq(raw_addr)
            if not cleaned:
                continue
            k = hq_street_zip_key(raw_addr) or cleaned.upper()
            if hq_street_zip_key(raw_addr):
                hq_keys.add(hq_street_zip_key(raw_addr))
            prev_cnt, prev_addr = addr_groups.get(k, (0, cleaned))
            best_addr = cleaned if len(cleaned) > len(prev_addr) else prev_addr
            addr_groups[k] = (prev_cnt + 1, best_addr)
        if addr_groups:
            ranked_addrs = sorted(addr_groups.values(), key=lambda x: (-x[0], len(x[1])))
            _, primary_hq_address = ranked_addrs[0]

    # 2. Compute portfolio-wide brand token frequency across member entities & corporate parents
    token_entity_sets: Dict[str, set[str]] = defaultdict(set)
    for m in corp_members:
        m_key = _canonical_org_key(m)
        if not m_key:
            continue
        for tok in set(_extract_brand_tokens(m)):
            token_entity_sets[tok].add(m_key)

    corp_parents_raw: List[Tuple[str, int]] = [
        (str(nm).strip(), int(cnt or 1))
        for nm, cnt in (enrich.get("corp_parents") or ())
        if nm and str(nm).strip() and not is_excluded_third_party_entity(str(nm))
    ]
    for cp_name, _ in corp_parents_raw:
        cp_key = _canonical_org_key(cp_name)
        if cp_key:
            for tok in set(_extract_brand_tokens(cp_name)):
                token_entity_sets[tok].add(cp_key)

    brand_token_freq: Dict[str, int] = {tok: len(keys) for tok, keys in token_entity_sets.items()}
    principal_surnames = _extract_person_surnames(
        ([primary_human] if primary_human else []) + source_humans[:12]
    )

    # 3. Aggregate C/O orgs (from OC corporate filings + member_addresses)
    care_of_counts: Counter = Counter()
    for org, cnt in (enrich.get("oc_care_of") or ()):
        if org and not is_excluded_third_party_entity(org):
            care_of_counts[org] += int(cnt or 1)
    # Include C/O orgs from member_addresses with lower weight if not already present
    member_co_counts = extract_care_of_orgs(addrs)
    for org, cnt in member_co_counts.items():
        care_of_counts[org] += min(int(cnt), 3)

    # Also include C/O orgs from verified HQ-matched operating companies (e.g. C/O WINNCOMPANIES)
    hq_operating_cos: List[Dict[str, Any]] = list(enrich.get("hq_operating_cos") or ())
    hq_umbrella_co_keys: set[str] = set()
    for op_co in hq_operating_cos:
        co_org = op_co.get("care_of_org")
        if co_org and not is_excluded_third_party_entity(co_org):
            care_of_counts[co_org] += 2
            ck = _canonical_org_key(co_org)
            if ck:
                hq_umbrella_co_keys.add(ck)

    # 4. Build deduplicated candidate pool across all 4 source-backed signals
    candidates: Dict[str, Dict[str, Any]] = {}

    def _ensure_cand(raw_name: str) -> Optional[Dict[str, Any]]:
        nm = " ".join(str(raw_name).strip().split())
        if not nm or len(nm) < 3:
            return None
        if looks_like_person_owner(nm) or is_excluded_third_party_entity(nm):
            return None
        if _NON_OPERATING_CHARITABLE_RE.search(nm):
            return None
        key = _canonical_org_key(nm)
        if not key or len(key) < 2:
            return None
        rec = candidates.get(key)
        if rec is None:
            rec = {
                "key": key,
                "name": nm,
                "corp_parent_count": 0,
                "care_of_count": 0,
                "hq_op_shared_officers": 0,
                "hq_op_shares_hq": False,
                "in_members": False,
                "is_display_name": False,
            }
            candidates[key] = rec
        else:
            # Prefer a cleaner official form (e.g. with Inc./LLC if not overly long, without ALL-CAPS jitter)
            if len(nm) > len(rec["name"]) and len(nm) <= len(rec["name"]) + 10 and _FLAGSHIP_OPERATING_RE.search(nm):
                rec["name"] = nm
        return rec

    for cp_name, cnt in corp_parents_raw:
        rec = _ensure_cand(cp_name)
        if rec is not None:
            rec["corp_parent_count"] += cnt

    for co_org, cnt in care_of_counts.items():
        rec = _ensure_cand(co_org)
        if rec is not None:
            rec["care_of_count"] += cnt

    for op_co in hq_operating_cos:
        op_name = op_co.get("name")
        if not op_name:
            continue
        rec = _ensure_cand(op_name)
        if rec is not None:
            rec["hq_op_shared_officers"] = max(
                rec["hq_op_shared_officers"], int(op_co.get("shared_officers") or 1)
            )
            if op_co.get("shares_hq"):
                rec["hq_op_shares_hq"] = True
            if op_co.get("is_primary_hq"):
                rec["hq_op_primary_hq"] = True

    # Add flagship / non-SPV member entities (and display_name itself if corporate)
    disp_key = _canonical_org_key(display_name) if display_name and not looks_like_person_owner(display_name) else ""
    for m in corp_members:
        if not _SPV_LIKE_RE.search(m) or (disp_key and _canonical_org_key(m) == disp_key):
            rec = _ensure_cand(m)
            if rec is not None:
                rec["in_members"] = True
                if disp_key and rec["key"] == disp_key:
                    rec["is_display_name"] = True

    if display_name and not looks_like_person_owner(display_name):
        # Never treat a bare 2-word non-corporate string (e.g. NYC HPD "BROWN GLEN") as a corporate operator
        has_corp_marker = bool(
            _OPERATING_KEYWORDS_RE.search(display_name)
            or re.search(r"\b(LLC|L\.L\.C|INC|CORP|LP|L\.P|LLP|LTD|TRUST|CO|FUND|REIT|ASSOCIATION|HDFC)\b", display_name, re.IGNORECASE)
        )
        if has_corp_marker or len(display_name.split()) >= 3:
            rec = _ensure_cand(display_name)
            if rec is not None:
                rec["is_display_name"] = True
                rec["in_members"] = True

    # 5. Score every candidate deterministically using corroborated multi-signal evidence
    scored: List[Tuple[float, Dict[str, Any], int, bool]] = []
    for rec in candidates.values():
        nm = rec["name"]
        cand_tokens = _extract_brand_tokens(nm)
        is_spv = bool(_SPV_LIKE_RE.search(nm))
        has_op_kw = bool(_OPERATING_KEYWORDS_RE.search(nm))
        has_flagship_kw = bool(_FLAGSHIP_OPERATING_RE.search(nm))
        is_np_jv = bool(_NONPROFIT_JV_RE.search(nm))

        # Count distinct member/parent entities corroborated by this candidate's brand tokens
        # Prefer full brand words (>=4 chars or with vowels) over 3-letter SPV initials like LJS / FKH / HPA
        stem_hits = 0
        matched_stem = ""
        for ct in cand_tokens:
            is_short_abbrev = len(ct) <= 3
            for ref_tok, ref_cnt in brand_token_freq.items():
                if _token_matches_stem(ct, ref_tok):
                    eff_cnt = min(ref_cnt, 6) if is_short_abbrev else ref_cnt
                    if eff_cnt > stem_hits:
                        stem_hits = eff_cnt
                        matched_stem = ref_tok

        surname_hit = any(
            _token_matches_stem(ct, sn)
            for ct in cand_tokens
            for sn in principal_surnames
        )

        score = 0.0
        if rec["corp_parent_count"] > 0:
            score += 6.0 * min(rec["corp_parent_count"], 40)
            if rec["corp_parent_count"] >= 3:
                score += 18.0

        if rec["care_of_count"] > 0:
            score += 5.0 * min(rec["care_of_count"], 18)
            # Strong corroboration bonus when C/O org matches portfolio entities or principal surname
            if stem_hits >= 2 or surname_hit or rec["corp_parent_count"] >= 2:
                score += 34.0
                if rec["key"] in hq_umbrella_co_keys:
                    score += 56.0
            elif rec["care_of_count"] >= 3:
                score += 10.0

        if rec["hq_op_shares_hq"] or rec["hq_op_shared_officers"] >= 2:
            score += 24.0 if rec["hq_op_shares_hq"] else 16.0
            score += 12.0 * min(max(rec["hq_op_shared_officers"] - 1, 0), 4)
            # Extra credit when this operating company is registered at the network's #1 HQ
            if rec.get("hq_op_primary_hq"):
                score += 14.0
            if surname_hit:
                score += 24.0
            if stem_hits >= 2:
                score += 18.0
            elif stem_hits >= 1:
                score += 10.0
            if has_flagship_kw:
                score += 8.0

        if not is_spv and stem_hits >= 2:
            score += 3.5 * min(stem_hits, 35)
            if surname_hit:
                score += 16.0
        elif not is_spv and surname_hit and has_op_kw:
            score += 18.0

        if rec["in_members"] and not is_spv:
            score += 10.0
            if has_flagship_kw:
                score += 6.0

        if rec["is_display_name"]:
            # Clean flagship display_name (e.g., Pulte Homes of New England LLC) gets a healthy baseline
            score += 20.0 if (not is_spv and has_op_kw) else (8.0 if not is_spv else 0.0)

        # Penalize project SPVs, single-property LPs, and short cryptic acronyms
        if is_spv or re.search(r"\b\d{5}\b", nm):
            score -= 36.0
        if len(rec["key"].replace(" ", "")) <= 4 and not has_op_kw and stem_hits < 3:
            score -= 20.0
        if is_np_jv and stem_hits < 5:
            score -= 14.0
        # Prefer flagship parent company nouns (COMPANIES, GROUP, MANAGEMENT, DEVELOPMENT, HOLDINGS)
        if not is_spv and re.search(r"\b(COMPANIES|COMPANY|GROUP|MANAGEMENT|DEVELOPMENT|RESIDENTIAL)\b", nm, re.IGNORECASE):
            score += 8.0
        # Slight preference for concise, flagship operator names over verbose subsidiary names
        if len(nm) > 42:
            score -= 4.0

        rec["matched_stem"] = matched_stem
        rec["stem_hits"] = stem_hits
        rec["surname_hit"] = surname_hit
        rec["is_spv"] = is_spv
        rec["score"] = round(score, 2)
        scored.append((score, rec, stem_hits, surname_hit))

    scored.sort(
        key=lambda item: (
            -item[0],
            -item[2],
            -item[1]["corp_parent_count"],
            -item[1]["care_of_count"],
            len(item[1]["name"]),
            item[1]["name"],
        )
    )

    operator_name: Optional[str] = None
    operator_source: Optional[str] = None
    operator_basis: Optional[str] = None

    if scored:
        top_score, top_rec, top_stem_hits, top_surname_hit = scored[0]
        # Only override a person-anchored network (like NYC HPD "BROWN GLEN") if we have strong corporate evidence
        is_person_only_display = bool(display_name and looks_like_person_owner(display_name))
        threshold = 22.0 if is_person_only_display else 10.0
        if top_score >= threshold and not (top_rec["is_spv"] and not top_rec["is_display_name"]):
            operator_name = top_rec["name"]
            basis_parts: List[str] = []
            if top_rec["corp_parent_count"] >= 2:
                operator_source = "corporate_parent"
                basis_parts.append(
                    f"Managing parent of {top_rec['corp_parent_count']} linked entities"
                )
            elif top_rec["care_of_count"] >= 2 and (top_stem_hits >= 2 or top_surname_hit or top_rec["care_of_count"] >= 3):
                operator_source = "corporate_care_of"
                basis_parts.append(f"Listed C/O on {top_rec['care_of_count']} filings")
            elif top_rec["hq_op_shares_hq"] or top_rec["hq_op_shared_officers"] >= 2:
                operator_source = "hq_operating_company"
                if top_rec["hq_op_shares_hq"]:
                    basis_parts.append("Active operating company at shared HQ")
                else:
                    basis_parts.append(
                        f"Active company shared by {top_rec['hq_op_shared_officers']} listed officers"
                    )
            elif top_rec["in_members"] and not top_rec["is_display_name"]:
                operator_source = "portfolio_entity"
                basis_parts.append("Operating entity in portfolio filings")
            else:
                operator_source = "deed_owner"
                basis_parts.append("Primary deed-listed entity")

            if top_stem_hits >= 3 and top_rec.get("matched_stem"):
                stem_title = top_rec["matched_stem"].title()
                basis_parts.append(f"{top_stem_hits} {stem_title}-named entities")
            elif top_rec["care_of_count"] >= 2 and operator_source != "corporate_care_of":
                basis_parts.append(f"C/O on {top_rec['care_of_count']} filings")

            operator_basis = " · ".join(basis_parts)

    # 6. Collect top secondary operating / managing entities (deduplicating numbered series like "SARV 2" / "SARV 3")
    op_key = _canonical_org_key(operator_name) if operator_name else ""
    op_series_key = re.sub(r"\b(?:\d{1,4}|20\d{2}-\d+|I|II|III|IV|V|VI)\b", "", op_key).strip()
    operating_entities: List[str] = []
    seen_op_keys = {op_key} if op_key else set()
    seen_series_keys = {op_series_key} if op_series_key else set()
    for sc, rec, _, _ in scored[1:]:
        if len(operating_entities) >= 3:
            break
        series_k = re.sub(r"\s+", " ", re.sub(r"\b(?:\d{1,4}|20\d{2}-\d+|I|II|III|IV|V|VI)\b", "", rec["key"])).strip()
        if rec["key"] in seen_op_keys or (series_k and series_k in seen_series_keys) or rec["is_spv"]:
            continue
        if sc < 14.0:
            continue
        if (
            rec["corp_parent_count"] >= 2
            or rec["hq_op_shares_hq"]
            or rec["hq_op_shared_officers"] >= 2
            or rec["care_of_count"] >= 2
            or (rec["in_members"] and _FLAGSHIP_OPERATING_RE.search(rec["name"]))
        ):
            seen_op_keys.add(rec["key"])
            if series_k:
                seen_series_keys.add(series_k)
            operating_entities.append(rec["name"])

    # 7. Deduplicated representative key officers ranked by network-wide salience
    #    (direct entity frequency + officer presence on verified operating companies + operator surname)
    op_tokens = _extract_brand_tokens(operator_name or "")
    officer_scores: Dict[str, float] = defaultdict(float)
    for off_nm, cnt in (enrich.get("direct_officer_counts") or ()):
        if off_nm:
            officer_scores[str(off_nm)] += 2.0 * min(float(cnt or 0), 50.0)
    for op_co in hq_operating_cos:
        op_co_toks = _extract_brand_tokens(op_co.get("name") or "")
        matches_winner = bool(
            op_tokens
            and any(_token_matches_stem(ct, wt) for ct in op_co_toks for wt in op_tokens)
        )
        weight = 6.0 if (matches_winner or op_co.get("shares_hq")) else 2.0
        for off_nm in (op_co.get("officers") or ()):
            if off_nm:
                officer_scores[str(off_nm)] += weight

    key_officers, officer_count = _dedupe_human_officers(
        primary_human,
        source_humans,
        preferred_tokens=op_tokens,
        officer_scores=officer_scores,
        max_officers=4,
    )
    representative_human_name = key_officers[0] if key_officers else primary_human

    # 8. Sample affiliated property-holding entities (excluding duplicates of operator/operating_entities)
    sample_entities = _pick_sample_entities(
        members, exclude_keys=seen_op_keys, display_name=display_name, limit=3
    )

    return {
        "operator_name": operator_name,
        "operator_source": operator_source,
        "operator_basis": operator_basis,
        "operating_entities": operating_entities,
        "primary_hq_address": primary_hq_address,
        "hq_entity_count": hq_entity_count if hq_entity_count >= 2 else None,
        "business_count": business_count,
        "representative_human_name": representative_human_name,
        "key_officers": key_officers,
        "officer_count": officer_count,
        "sample_entities": sample_entities,
    }


def fetch_batch_network_oc_profiles(
    query_fn: Callable[..., List[Dict[str, Any]]],
    city: str,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch OpenCorporates enrichment (corporate parents, C/O lines, verified HQs, and
    HQ-matched active operating companies) for a batch of network rows using at most
    3 fast indexed SQL queries. Safe fallback to {} on any missing table/index.
    """
    if not rows:
        return {}

    oc_jur = CITY_TO_OC_JUR.get((city or "").lower())
    net_to_cids: Dict[str, List[Tuple[str, str]]] = {}
    all_jurs: List[str] = []
    all_nums: List[str] = []
    cid_to_netkeys: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for row in rows:
        nk = row.get("network_key")
        if not nk:
            continue
        raw_cids = list(row.get("oc_company_ids") or ())
        sigs = row.get("connection_signals") or {}
        if isinstance(sigs, dict):
            raw_cids.extend(sigs.get("oc_matched_companies") or ())
            raw_cids.extend(sigs.get("oc_company_ids") or ())
        seen_local: set[Tuple[str, str]] = set()
        for cid in raw_cids:
            if not isinstance(cid, str) or "/" not in cid:
                continue
            j, n = cid.split("/", 1)
            j, n = j.strip(), n.strip()
            if not j or not n or (j, n) in seen_local:
                continue
            seen_local.add((j, n))
            if len(seen_local) > 250:
                break
            net_to_cids.setdefault(nk, []).append((j, n))
            if (j, n) not in cid_to_netkeys:
                all_jurs.append(j)
                all_nums.append(n)
            cid_to_netkeys[(j, n)].append(nk)

    enrichment: Dict[str, Dict[str, Any]] = {
        row["network_key"]: {
            "corp_parents": [],
            "direct_officer_counts": [],
            "oc_care_of": [],
            "oc_hq_addresses": [],
            "hq_operating_cos": [],
        }
        for row in rows
        if row.get("network_key")
    }

    net_hq_keys: Dict[str, set[str]] = defaultdict(set)
    for row in rows:
        nk = row.get("network_key")
        if not nk:
            continue
        for addr in (row.get("member_addresses") or ())[:60]:
            k = hq_street_zip_key(addr)
            if k:
                net_hq_keys[nk].add(k)

    # Query A & B: Batch lookup of matched oc_companies (HQs & C/O lines) and corporate_parent officers
    if all_jurs:
        try:
            co_rows = query_fn(
                """
                SELECT
                    c.jurisdiction_code,
                    c.company_number,
                    c.registered_address_raw,
                    c.mailing_address_raw
                FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                JOIN oc_companies c
                  ON c.jurisdiction_code = k.j
                 AND c.company_number = k.n
                """,
                (all_jurs, all_nums),
            )
            net_addr_counts: Dict[str, Counter] = defaultdict(Counter)
            net_raw_addrs: Dict[str, List[str]] = defaultdict(list)
            for cr in co_rows:
                pair = (cr["jurisdiction_code"], cr["company_number"])
                nks = cid_to_netkeys.get(pair) or ()
                reg_a = (cr.get("registered_address_raw") or "").strip()
                mail_a = (cr.get("mailing_address_raw") or "").strip()
                best_a = reg_a or mail_a
                for nk in nks:
                    if best_a:
                        net_addr_counts[nk][best_a] += 1
                        k = hq_street_zip_key(best_a)
                        if k:
                            net_hq_keys[nk].add(k)
                    if reg_a:
                        net_raw_addrs[nk].append(reg_a)
                    if mail_a and mail_a != reg_a:
                        net_raw_addrs[nk].append(mail_a)

            for nk, cnts in net_addr_counts.items():
                enrichment[nk]["oc_hq_addresses"] = cnts.most_common(12)
                # Record the #1 most frequent physical HQ key per network
                best_k = None
                for raw_a, _c in cnts.most_common(12):
                    k = hq_street_zip_key(raw_a)
                    if k:
                        best_k = k
                        break
                if best_k:
                    enrichment[nk]["primary_hq_key"] = best_k
            for nk, raw_list in net_raw_addrs.items():
                co_cnts = extract_care_of_orgs(raw_list)
                if co_cnts:
                    enrichment[nk]["oc_care_of"] = co_cnts.most_common(10)
        except Exception as exc:
            logger.debug("Batch oc_companies enrichment skipped for %s: %s", city, exc)

        try:
            cp_rows = query_fn(
                """
                SELECT
                    o.jurisdiction_code,
                    o.company_number,
                    o.role_class,
                    o.name,
                    o.name_norm
                FROM unnest(%s::text[], %s::text[]) AS k(j, n)
                JOIN oc_officers o
                  ON o.jurisdiction_code = k.j
                 AND o.company_number = k.n
                WHERE o.role_class IN ('corporate_parent', 'beneficial_principal')
                  AND o.name IS NOT NULL
                  AND o.name <> ''
                """,
                (all_jurs, all_nums),
            )
            net_cp_counts: Dict[str, Dict[str, set[Tuple[str, str]]]] = defaultdict(lambda: defaultdict(set))
            net_bp_counts: Dict[str, Dict[str, set[Tuple[str, str]]]] = defaultdict(lambda: defaultdict(set))
            bp_display_pref: Dict[str, str] = {}
            for cr in cp_rows:
                pair = (cr["jurisdiction_code"], cr["company_number"])
                raw_name = (cr.get("name") or "").strip()
                if not raw_name:
                    continue
                role_cls = cr.get("role_class")
                if role_cls == "corporate_parent":
                    for nk in cid_to_netkeys.get(pair) or ():
                        net_cp_counts[nk][raw_name].add(pair)
                elif role_cls == "beneficial_principal":
                    norm_key = (cr.get("name_norm") or raw_name).strip().upper()
                    if norm_key not in bp_display_pref or len(raw_name) > len(bp_display_pref[norm_key]):
                        bp_display_pref[norm_key] = raw_name
                    for nk in cid_to_netkeys.get(pair) or ():
                        net_bp_counts[nk][norm_key].add(pair)

            for nk, name_map in net_cp_counts.items():
                ranked_cp = sorted(
                    ((nm, len(cids)) for nm, cids in name_map.items()),
                    key=lambda x: (-x[1], len(x[0]), x[0]),
                )
                enrichment[nk]["corp_parents"] = ranked_cp[:15]
            for nk, bp_map in net_bp_counts.items():
                ranked_bp = sorted(
                    ((bp_display_pref.get(kn, kn), len(cids)) for kn, cids in bp_map.items()),
                    key=lambda x: (-x[1], x[0]),
                )
                enrichment[nk]["direct_officer_counts"] = ranked_bp[:15]
        except Exception as exc:
            logger.debug("Batch oc_officers enrichment skipped for %s: %s", city, exc)

    # Query C: Active in-state operating/management companies linked to each network's top beneficial principals
    # Guarded strictly by shared physical HQ OR (>= 2 shared network officers + brand/surname match)
    if oc_jur:
        officer_to_nets: Dict[str, List[str]] = defaultdict(list)
        net_surnames: Dict[str, set[str]] = {}
        net_member_tokens: Dict[str, set[str]] = {}
        all_officer_norms: List[str] = []
        seen_norms: set[str] = set()

        for row in rows:
            nk = row.get("network_key")
            if not nk:
                continue
            members = row.get("member_names") or ()
            if len(members) < 2:
                continue
            sigs = row.get("connection_signals") or {}
            if not isinstance(sigs, dict):
                continue
            prim = sigs.get("primary_human_name")
            src_h = list(sigs.get("source_human_names") or ())
            # Take top 6 unique human principals (including any whose surname matches a recurring member stem)
            m_toks: Counter = Counter()
            for m in members:
                for tok in set(_extract_brand_tokens(m)):
                    m_toks[tok] += 1
            net_member_tokens[nk] = {t for t, c in m_toks.items() if c >= 1}
            top_stems = {t for t, c in m_toks.items() if c >= 2}

            selected_humans: List[str] = []
            # 1. Start with top officers ranked by direct entity count across the network (e.g. Stephen Whalen 33, Fred Starikov 32!)
            for direct_nm, _cnt in (enrichment[nk].get("direct_officer_counts") or ())[:4]:
                if direct_nm and direct_nm not in selected_humans:
                    selected_humans.append(direct_nm)
            # 2. Add stored primary human
            if prim and prim not in selected_humans:
                selected_humans.append(prim)
            # 3. Add officers whose surname matches a recurring portfolio brand stem (e.g. Gilbert J. Winn!)
            for h in src_h[:35]:
                if len(selected_humans) >= 9:
                    break
                h_surnames = _extract_person_surnames([h])
                if h_surnames & top_stems and h not in selected_humans:
                    selected_humans.append(h)
            # 4. Fill remaining slots with top source_human_names
            for h in src_h[:15]:
                if len(selected_humans) >= 9:
                    break
                if h not in selected_humans:
                    selected_humans.append(h)

            if not selected_humans:
                continue
            net_surnames[nk] = _extract_person_surnames(selected_humans)
            for norm in _normalize_officer_variants(selected_humans):
                officer_to_nets[norm].append(nk)
                if norm not in seen_norms:
                    seen_norms.add(norm)
                    all_officer_norms.append(norm)

        if all_officer_norms:
            try:
                op_rows = query_fn(
                    """
                    SELECT
                        o.name_norm,
                        c.company_number,
                        c.name AS company_name,
                        c.registered_address_raw,
                        c.mailing_address_raw
                    FROM oc_officers o
                    JOIN oc_companies c
                      ON c.jurisdiction_code = o.jurisdiction_code
                     AND c.company_number = o.company_number
                    WHERE o.jurisdiction_code = %s
                      AND o.name_norm = ANY(%s)
                      AND o.role_class = 'beneficial_principal'
                      AND c.is_active = TRUE
                      AND c.name !~ '^[0-9]'
                      AND c.name ~* '(COMPANIES|COMPANY|& CO|CO\.|MANAGEMENT|DEVELOPMENT|GROUP|HOLDINGS|REALTY|PROPERTIES|CAPITAL|RESIDENTIAL|PARTNERS|ENTERPRISES|COMMUNITIES|HOMES|MANAGER|FINANCE|EQUITIES)'
                    LIMIT 2500
                    """,
                    (oc_jur, all_officer_norms[:140]),
                )
                # Group by (network_key, company_number)
                net_op_map: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
                for r in op_rows:
                    cname = (r.get("company_name") or "").strip()
                    if not cname or _NON_OPERATING_CHARITABLE_RE.search(cname) or is_excluded_third_party_entity(cname):
                        continue
                    cnum = r["company_number"]
                    onorm = r["name_norm"]
                    reg_a = (r.get("registered_address_raw") or "").strip()
                    mail_a = (r.get("mailing_address_raw") or "").strip()
                    co_hq_keys = {k for k in (hq_street_zip_key(reg_a), hq_street_zip_key(mail_a)) if k}
                    co_orgs = extract_care_of_orgs([reg_a, mail_a])
                    top_co_org = co_orgs.most_common(1)[0][0] if co_orgs else None

                    for nk in officer_to_nets.get(onorm) or ():
                        entry = net_op_map[nk].get(cnum)
                        if entry is None:
                            prim_k = enrichment[nk].get("primary_hq_key")
                            entry = {
                                "name": cname,
                                "officers": set(),
                                "shares_hq": bool(co_hq_keys & net_hq_keys.get(nk, set())),
                                "is_primary_hq": bool(prim_k and prim_k in co_hq_keys),
                                "care_of_org": top_co_org,
                            }
                            net_op_map[nk][cnum] = entry
                        entry["officers"].add(onorm)

                for nk, comp_dict in net_op_map.items():
                    surnames = net_surnames.get(nk, set())
                    m_toks = net_member_tokens.get(nk, set())
                    verified: List[Dict[str, Any]] = []
                    for entry in comp_dict.values():
                        n_off = len(entry["officers"])
                        c_toks = _extract_brand_tokens(entry["name"])
                        matches_surname = any(
                            _token_matches_stem(ct, sn) for ct in c_toks for sn in surnames
                        )
                        matches_member = any(
                            _token_matches_stem(ct, mt) for ct in c_toks for mt in m_toks
                        )
                        # Strict epistemic guard: ONLY accept if HQ-verified OR (>=2 officers + brand/surname match)
                        if entry["shares_hq"] or (n_off >= 2 and (matches_surname or matches_member)):
                            verified.append(
                                {
                                    "name": entry["name"],
                                    "shared_officers": n_off,
                                    "officers": sorted(entry["officers"]),
                                    "shares_hq": entry["shares_hq"],
                                    "is_primary_hq": entry["is_primary_hq"],
                                    "care_of_org": entry["care_of_org"],
                                }
                            )
                    verified.sort(
                        key=lambda x: (
                            0 if x["is_primary_hq"] else (1 if x["shares_hq"] else 2),
                            -x["shared_officers"],
                            len(x["name"]),
                        )
                    )
                    enrichment[nk]["hq_operating_cos"] = verified[:20]
            except Exception as exc:
                logger.debug("Batch principal operating-company lookup skipped for %s: %s", city, exc)

    return enrichment
