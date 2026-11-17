# shared_utils.py
import re
from functools import lru_cache
from typing import Set

def normalize_business_name(name: str) -> str:
    """Canonical function to normalize business names for matching."""
    if not name: return ''
    normalized = name.upper().strip()
    normalized = re.sub(r"[,.'`\"]", '', normalized)

    # Standardize conjunctions
    normalized = normalized.replace('&', ' AND ')
    normalized = normalized.replace('+', ' AND ')

    # Standardize common abbreviations for better cross-matching
    # Using \b for word boundaries to avoid partial matches
    abbrevs = {
        'CO': 'COMPANY',
        'CORP': 'CORPORATION',
        'INC': 'INCORPORATED',
        'ASSOC': 'ASSOCIATION',
        'ASSOCIATES': 'ASSOCIATION',
        'ASSOCIATED': 'ASSOCIATION',
        'ASSC': 'ASSOCIATION',
        'ASSN': 'ASSOCIATION',
        'MGMT': 'MANAGEMENT',
        'MGT': 'MANAGEMENT',
        'PROP': 'PROPERTIES',
        'PROPS': 'PROPERTIES',
        'PROPERTY': 'PROPERTIES',
        'SVCS': 'SERVICES',
        'DEVL': 'DEVELOPMENT',
        'DEV': 'DEVELOPMENT',
        'SYS': 'SYSTEM',
        'SYST': 'SYSTEM',
        'SYSTS': 'SYSTEM',
        'HLDG': 'HOLDING',
        'HLDGS': 'HOLDINGS',
        'BLDG': 'BUILDING',
        'CTR': 'CENTER',
        'CNTR': 'CENTER',
    }
    for abbr, full in abbrevs.items():
        normalized = re.sub(rf"\b{abbr}\b", full, normalized)

    # Clean up non-alphanumeric (except space and hyphen)
    normalized = re.sub(r"[^A-Z0-9\s-]", '', normalized)
    normalized = re.sub(r"\s+", ' ', normalized).strip()
    return normalized

def normalize_person_name(name: str) -> str:
    """Canonical function to normalize person names for matching."""
    if not name: return ''
    normalized = name.upper().strip()

    # Swap commas: "LAST, FIRST MIDDLE" -> "FIRST MIDDLE LAST"
    if ',' in normalized:
        parts_comma = normalized.split(',', 1)
        if len(parts_comma) == 2:
            last = parts_comma[0].strip()
            first_mid = parts_comma[1].strip()
            normalized = f"{first_mid} {last}"

    # 0. Pre-strip noise and handle joint names
    # If name is "KAZEROUNIAN KAZEM &", remove the trailing &
    normalized = re.sub(r'[&/]\s*$', '', normalized).strip()
    # If name contains " & ", " AND ", or " / ", we take the first part for normalization
    # but the matching logic in variations will handle permutations.
    # Actually, for the principal list, we want to split them.
    # For now, let's just clean common joint markers to a space or split.
    normalized = normalized.replace(' & ', ' ')
    normalized = normalized.replace(' / ', ' ')
    normalized = normalized.replace(' AND ', ' ')

    # 1. Contextual typo corrections for the Gurevitch network
    is_gurevitch = any(x in normalized for x in ['GUREVITCH', 'GURAVITCH', 'GUREVICH', 'GUREVITH', 'GUTVITCH', 'GUREVITOH', 'GURVITCH'])
    is_edelkopf = any(x in normalized for x in ['EDELKOPF', 'EDELKOPH'])

    if is_gurevitch:
        g_typos = {
            'MENACHERM': 'MENACHEM',
            'MENAHEM': 'MENACHEM',
            'MENACHER': 'MENACHEM',
            'MANACHEM': 'MENACHEM',
            'GURAVITCH': 'GUREVITCH',
            'GUREVICH': 'GUREVITCH',
            'GUREVITOH': 'GUREVITCH',
            'GUREVITH': 'GUREVITCH',
            'GUTVITCH': 'GUREVITCH',
            'GURVITCH': 'GUREVITCH',
        }
        for typo, correction in g_typos.items():
            normalized = re.sub(rf"\b{typo}\b", correction, normalized)

    if is_edelkopf:
        e_typos = {'EDELKOPH': 'EDELKOPF'}
        for typo, correction in e_typos.items():
            normalized = re.sub(rf"\b{typo}\b", correction, normalized)

    # Standard punctuation and whitespace cleanup
    normalized = re.sub(r"[,.'`\"]", '', normalized)
    normalized = re.sub(r"\s+", ' ', normalized).strip()

    # Remove standard suffixes
    for pattern in PERSON_SUFFIX_PATTERNS:
        normalized = pattern.sub('', normalized)

    # Remove standalone initials after suffix cleanup. This covers both
    # FIRST M LAST and assessor-style LAST FIRST M owner names.
    parts = normalized.split()
    if len(parts) > 2:
        non_initial_parts = [part for part in parts if len(part) > 1]
        if len(non_initial_parts) >= 2:
            normalized = " ".join(non_initial_parts)

    return normalized.strip()


BUSINESS_ENTITY_TERMS = {
    'APARTMENT', 'APARTMENTS', 'ASSOC', 'ASSOCIATES', 'ASSOCIATION',
    'AUTHORITY', 'BANK', 'BOARD', 'CENTER', 'CHURCH', 'CLUB', 'COM', 'COMMUNITY',
    'COMMONWEALTH', 'CONDOMINIUM', 'CONDO', 'COOPERATIVE', 'COUNCIL', 'COUNTY',
    'CO', 'COMPANY', 'CORP', 'CORPORATION', 'DEVELOPMENT', 'ENTERPRISE',
    'ENTERPRISES', 'ESTATE', 'FOUNDATION', 'FUND', 'GOVERNMENT', 'GROUP',
    'HOSPITAL', 'HOUSING', 'HOLDING', 'HOLDINGS', 'INC',
    'INCORPORATED', 'INVESTMENT', 'INVESTMENTS', 'LLC', 'LLP', 'LP', 'LPS', 'LTD',
    'MANAGEMENT', 'MINISTRY', 'MUNICIPAL', 'MUSEUM', 'PARTNER', 'PARTNERS',
    'PARTNERSHIP', 'PROJECT', 'PROPERTIES', 'PROPERTY', 'REALTY', 'REAL',
    'REDEVELOPMENT', 'SCHOOL', 'SERVICES',
    'SOCIETY', 'STATE', 'TRUST', 'UNIVERSITY', 'VENTURES'
}


def looks_like_business_name(name: str) -> bool:
    """Return true when an owner string should keep business-style normalization."""
    if not name:
        return False
    cleaned = normalize_business_name(name)
    return any(part in BUSINESS_ENTITY_TERMS for part in cleaned.split())


PERSON_OWNER_ROLE_TERMS = {'TR', 'TRS', 'TS', 'TRUSTEE', 'TRUSTEES'}


def looks_like_person_owner(name: str) -> bool:
    """Conservatively identify a human directly named in an ownership source."""
    if not name:
        return False
    cleaned = normalize_business_name(name)
    if not cleaned or any(char.isdigit() for char in cleaned):
        return False
    parts = cleaned.split()
    organization_fragments = (
        'PROPERT', 'APARTMENT', 'COMMONWEALTH', 'DISTRICT', 'DEPARTMENT',
        'HOUSING', 'CONDOMIN', 'COOPERAT', 'ASSOCIAT', 'FOUNDATION', 'BANK',
        'LIMITED', ' TOWER', 'CONVENT', 'BOSTON BAY', 'PORTFOLIO', 'INCORP',
        'LLLP', ' SFR ', 'HEALTH SYSTEM', 'NEIGHBOR', 'HOPE BAY',
        'RIVERWAY MOSAIC',
    )
    if any(fragment in cleaned for fragment in organization_fragments):
        return False
    if any(part in BUSINESS_ENTITY_TERMS for part in parts):
        return False
    core_parts = [part for part in parts if part not in PERSON_OWNER_ROLE_TERMS]
    if not 2 <= len(core_parts) <= 5:
        return False
    return all(re.fullmatch(r"[A-Z][A-Z-]*", part) for part in core_parts)


def normalize_owner_name(name: str) -> str:
    """Normalize property owner/co-owner names for mixed business/person matching."""
    if not name:
        return ''
    return normalize_business_name(name) if looks_like_business_name(name) else normalize_person_name(name)

def canonicalize_person_name(name: str) -> str:
    """
    Creates a word-sorted version of a name to treat "LAST FIRST"
    the same as "FIRST LAST" during network building/graph matching.
    """
    norm = normalize_person_name(name)
    if not norm: return ""
    parts = sorted(norm.split())
    return " ".join(parts)

def canonicalize_business_name(name: str) -> str:
    """
    Strips suffixes and word-sorts for robust business matching.
    e.g. 'THE ACME CORP' -> 'ACME'
    """
    # 1. Start with variations which already gives us the most-stripped version
    variations = sorted(list(get_name_variations(name, 'business')), key=len)
    if not variations: return ""

    # 2. Take the shortest variation (most stripped)
    base = variations[0]

    # 3. Strip 'THE' specifically if it's a prefix
    base = re.sub(r'^THE\s+', '', base)

    # 4. Word sort
    parts = sorted(base.split())
    return " ".join(parts)

# List of suffixes to remove, kept outside the function for clarity
BUSINESS_SUFFIXES_TO_REMOVE = [
    'LIMITED LIABILITY COMPANY', 'LIMITED LIABILITY PARTNERSHIP',
    'PROFESSIONAL LIMITED LIABILITY COMPANY', 'LIMITED PARTNERSHIP',
    'INCORPORATED', 'CORPORATION', 'MANAGEMENT', 'PROPERTIES',
    'INVESTMENTS', 'PARTNERSHIP', 'COMPANY', 'LIMITED', 'PARTNERS',
    'ASSOCIATION', 'ASSOCIATES', 'ASSOCIATED', 'REALTY', 'GROUP',
    'TRUST', 'ESTATE', 'HOLDINGS', 'VENTURES', 'SYSTEM', 'SYSTEMS',
    'ENTERPRISES', 'SERVICES', 'DEVELOPMENT', 'REAL ESTATE',
    'L L C', 'L L P', 'L P', 'LLC', 'LLP', 'LTD', 'INC', 'CORP', 'LP', 'CO',
    'ASSC', 'ASSOC', 'ASSN', 'MGMT', 'MGT', 'PROP', 'PROPS'
]

BUSINESS_SUFFIX_PATTERNS = [re.compile(r"\s+" + re.escape(suffix) + r"$") for suffix in BUSINESS_SUFFIXES_TO_REMOVE]

PERSON_SUFFIXES = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS']
PERSON_SUFFIX_PATTERNS = [re.compile(r"\s+" + re.escape(suffix) + r"$") for suffix in PERSON_SUFFIXES]

def get_name_variations(name: str, entity_type: str) -> Set[str]:
    """
    Canonical function to generate a robust set of name variations for searching.
    """
    if not name:
        return set()

    variations = set()

    if entity_type == 'business':
        # --- NEW LOGIC: Iteratively strip suffixes to find more potential matches ---
        normed = normalize_business_name(name)
        if not normed: return set()

        current_name = normed
        variations.add(current_name)

        changed = True
        while changed:
            changed = False
            for pattern in BUSINESS_SUFFIX_PATTERNS:
                if pattern.search(current_name):
                    current_name = pattern.sub('', current_name).strip()
                    variations.add(current_name)
                    changed = True
                    break # Restart with the new shorter name

    elif entity_type == 'principal':
        # Handle joint names like "KAZEM KAZEROUNIAN & SALMUN KAZEROUNIAN"
        # We split by common markers
        raw_names = re.split(r'\s+(?:&|AND|/)\s+', name.upper())
        for rn in raw_names:
            normalized = normalize_person_name(rn)
            if not normalized: continue
            variations.add(normalized)

            parts = normalized.split()
            if len(parts) >= 2:
                # Permutation: LAST FIRST -> FIRST LAST
                variations.add(f"{parts[-1]} {' '.join(parts[:-1])}")
                # Permutation: FIRST LAST -> LAST FIRST
                variations.add(f"{' '.join(parts[1:])} {parts[0]}")

                # Initials: LAST, F
                variations.add(f"{parts[-1]}, {parts[0][0]}")
                variations.add(f"{parts[0]}, {parts[-1][0]}")

    return {v for v in variations if v}


ADDRESS_PLACEHOLDER_VALUES = {
    'NO INFORMATION PROVIDED', 'NOT PROVIDED', 'NONE', 'N/A', 'NA',
    'UNKNOWN', 'NO ADDRESS', 'ADDRESS NOT PROVIDED', 'NOT APPLICABLE',
    'NULL', 'CONNECTICUT', 'CT', 'USA', 'UNITED STATES',
}

SHARED_ADDRESS_IGNORE_VALUES = {
    # Registered-agent/service addresses that collapse unrelated businesses.
    # The authoritative, editable list lives in the address_match_rules table;
    # these are a hardcoded fail-safe for when that table is unavailable.
    '2389 MAIN STREET GLASTONBURY',
    '2 CORPORATE DRIVE SHELTON',
    '2 CORPORATE DR SHELTON',
}

# Two-letter USPS state/territory codes. Needed so that a trailing state token
# is not mistaken for a street suffix: 'CT' is both Connecticut and Court, and
# 'FL' is both Florida and Floor. The state is peeled off the end of the string
# before suffix expansion so both readings stay correct.
US_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI',
    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN',
    'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH',
    'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA',
    'WV', 'WI', 'WY', 'PR', 'VI', 'GU', 'AS', 'MP',
}

# Street-type abbreviations expanded to a single canonical spelling.
STREET_SUFFIX_MAP = {
    'ST': 'STREET', 'STR': 'STREET', 'STS': 'STREET',
    'RD': 'ROAD', 'RDS': 'ROAD',
    'AVE': 'AVENUE', 'AV': 'AVENUE', 'AVEN': 'AVENUE',
    'DR': 'DRIVE', 'DRV': 'DRIVE',
    'LN': 'LANE',
    'BLVD': 'BOULEVARD', 'BLV': 'BOULEVARD', 'BVD': 'BOULEVARD',
    'PL': 'PLACE',
    'CT': 'COURT',
    'TER': 'TERRACE', 'TERR': 'TERRACE',
    'CIR': 'CIRCLE',
    'PKWY': 'PARKWAY', 'PKY': 'PARKWAY', 'PWY': 'PARKWAY',
    'HWY': 'HIGHWAY',
    'SQ': 'SQUARE',
    'TPKE': 'TURNPIKE', 'TPK': 'TURNPIKE',
    'PLZ': 'PLAZA',
    'TRL': 'TRAIL',
    'EXT': 'EXTENSION',
    'XING': 'CROSSING',
    'HTS': 'HEIGHTS',
    'MT': 'MOUNT',
}

# Canonical spellings, used to recognise a street-type token that is already
# expanded (e.g. detecting the '103' in '399 WHALLEY AVENUE 103 NEW HAVEN').
STREET_SUFFIX_WORDS = set(STREET_SUFFIX_MAP.values()) | {
    'STREET', 'ROAD', 'AVENUE', 'DRIVE', 'LANE', 'BOULEVARD', 'PLACE',
    'COURT', 'TERRACE', 'CIRCLE', 'PARKWAY', 'HIGHWAY', 'SQUARE',
    'TURNPIKE', 'PLAZA', 'TRAIL', 'EXTENSION', 'WAY', 'ROW', 'PIKE',
}

DIRECTIONAL_MAP = {
    'N': 'NORTH', 'S': 'SOUTH', 'E': 'EAST', 'W': 'WEST',
    'NE': 'NORTHEAST', 'NW': 'NORTHWEST', 'SE': 'SOUTHEAST', 'SW': 'SOUTHWEST',
}

# Secondary-unit designators. 'FL' is included but is only consumed when an
# actual unit token follows it, so a trailing state 'FL' is never eaten.
UNIT_MARKER_PATTERN = (
    r'SUITE|STE|UNIT|APARTMENT|APT|ROOM|RM|FLOOR|FLR|FL|BLDG|BUILDING|DEPT|DEPARTMENT|OFC|OFFICE'
)


def is_placeholder_address(address: str) -> bool:
    """Return true for blank CSV fragments and source placeholder addresses."""
    if not address:
        return True

    cleaned = str(address).upper().strip()
    if not cleaned:
        return True

    compact = re.sub(r'[\s,.;:_-]+', '', cleaned)
    if not compact:
        return True
    if re.fullmatch(r'0+', compact):
        return True

    alpha_only = re.sub(r'[^A-Z]', '', cleaned)
    if alpha_only in {
        'NOINFORMATIONPROVIDED', 'NOTPROVIDED', 'NONE', 'NA',
        'UNKNOWN', 'NOADDRESS', 'ADDRESSNOTPROVIDED',
        'NOTAPPLICABLE', 'NULL',
    }:
        return True

    normalized_words = re.sub(r"[,.'`\"]", '', cleaned)
    normalized_words = re.sub(r'\s+', ' ', normalized_words).strip()
    return normalized_words in ADDRESS_PLACEHOLDER_VALUES


# Junk values for an individual city/state/zip column. Deliberately narrower
# than ADDRESS_PLACEHOLDER_VALUES, which covers whole-address fields.
COMPONENT_JUNK_VALUES = {
    'NO INFORMATION PROVIDED', 'NOT PROVIDED', 'NONE', 'N A', 'NA', 'UNKNOWN',
    'NULL', 'NOT APPLICABLE', 'N', 'A', '0', '00000',
}


def compose_address(street: str, city: str = None, state: str = None, zipcode: str = None) -> str:
    """
    Join address components that arrive in separate DB columns into one string.

    Source tables are inconsistent: `businesses.mail_address` carries the whole
    address inline (comma-mangled), while `businesses.business_address` is
    street-only with the city/state/zip in sibling columns. Feeding only the
    street to the normalizer drops the city, which merges e.g. '70 West Main
    Street' in East Islip with '70 West Main Street' in Babylon. Components
    already present in the street line are not appended twice.
    """
    street_text = (str(street) if street is not None else '').strip()
    if not street_text or is_placeholder_address(street_text):
        return ''

    parts = [street_text]
    upper_street = re.sub(r'[^A-Z0-9 ]', ' ', street_text.upper())
    upper_street = re.sub(r'\s+', ' ', upper_street)

    for idx_c, component in enumerate((city, state, zipcode)):
        text = (str(component) if component is not None else '').strip()
        if not text:
            continue
        # Clean float-formatted ZIP codes (e.g. '11211.0' -> '11211', '6830.0' -> '06830')
        if idx_c == 2:
            text = re.sub(r'^(\d{4,5})\.0+$', lambda m: m.group(1).zfill(5), text)
        token = re.sub(r'[^A-Z0-9 ]', ' ', text.upper())
        token = re.sub(r'\s+', ' ', token).strip()
        # Only reject true junk here. is_placeholder_address() must not be used:
        # it classifies a bare 'CT'/'CONNECTICUT' as a placeholder, which is right
        # for a whole address field but would drop the state from every
        # Connecticut address and leave CT keys shaped differently from NY ones.
        if not token or token in COMPONENT_JUNK_VALUES:
            continue
        # Skip a component the street line already carries.
        if re.search(rf'\b{re.escape(token)}\b', upper_street):
            continue
        parts.append(text)
        upper_street = f"{upper_street} {token}"

    return ' '.join(parts)


def _split_state_and_zip(norm: str):
    """Peel a trailing state code and ZIP off a cleaned address body.

    Returns (body, state, zipcode). Doing this before street-suffix expansion is
    what keeps 'CT' (Connecticut vs Court) and 'FL' (Florida vs Floor) correct.
    """
    state = ''
    zipcode = ''

    # ZIP may sit at the end, or just before a stray state code.
    match = re.search(r'\b(\d{5})\s*$', norm)
    if match:
        zipcode = match.group(1)
        norm = norm[: match.start()].strip()

    match = re.search(r'\b([A-Z]{2})\s*$', norm)
    if match and match.group(1) in US_STATE_CODES:
        state = match.group(1)
        norm = norm[: match.start()].strip()

        # Some sources render 'CITY CT 06103 USA' -> after USA removal the ZIP
        # can end up trailing the state; check once more.
        if not zipcode:
            match = re.search(r'\b(\d{5})\s*$', norm)
            if match:
                zipcode = match.group(1)
                norm = norm[: match.start()].strip()

    return norm.strip(), state, zipcode


def _expand_address_tokens(body: str) -> str:
    """Expand directional and street-type abbreviations token by token."""
    tokens = body.split()
    if not tokens:
        return ''

    out = []
    for idx, token in enumerate(tokens):
        # Never expand the leading house number position.
        if token in DIRECTIONAL_MAP and idx > 0:
            out.append(DIRECTIONAL_MAP[token])
            continue
        if token in STREET_SUFFIX_MAP and idx > 0:
            out.append(STREET_SUFFIX_MAP[token])
            continue
        out.append(token)
    return ' '.join(out)


def normalize_mailing_address(address: str) -> str:
    """
    Standardizes a mailing address to detect hidden links, keeping the unit.
    e.g. '123 Main St, Suite 400' -> '123 MAIN STREET #400'
    """
    if is_placeholder_address(address):
        return ""

    # 1. Uppercase and basic cleanup (fix pandas float ZIPs like '11211.0' or '6830.0' before dot removal)
    norm = str(address).upper().strip()
    norm = re.sub(
        r'\b(\d{4,5})\.0+(?=\s*(?:$|,|\bUSA\b|\bUS\b|\bUNITED STATES\b))',
        lambda m: m.group(1).zfill(5),
        norm,
    )
    norm = re.sub(r"[.,'`\"]", "", norm)  # Remove punctuation
    norm = re.sub(r"\s+", " ", norm)      # Collapse spaces

    # 2. Drop country noise and normalize zip+4 before the tail is parsed
    norm = re.sub(r'\b(?:UNITED STATES OF AMERICA|UNITED STATES|USA|US)\b', ' ', norm)
    norm = re.sub(r'\b(\d{5})-\d{4}\b', r'\1', norm)
    norm = re.sub(r"\s+", " ", norm).strip()

    # 3. Peel the trailing state/ZIP off BEFORE expanding street suffixes, so a
    #    state 'CT'/'FL' is never rewritten into COURT/#floor (and vice versa).
    body, state, zipcode = _split_state_and_zip(norm)

    # 4. PO Box canonicalization
    body = re.sub(r"\b(?:P\s*O\s*BOX|POBOX|POST\s+OFFICE\s+BOX|P\s*O\s*B)\b", "PO BOX", body)

    # 5. Standardize Unit/Suite/Floor. Do NOT strip them here, or we merge
    #    unrelated tenants of the same skyscraper.
    #    5a. Ordinal floors: '18TH FLOOR' / '2ND FL' -> '#18' / '#2'
    body = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\s+(?:FLOOR|FLR|FL)\b', r'#\1', body)
    #    5b. Marker + token. The lookahead requires a real short/numeric unit token to follow,
    #        so 'OFC, PHILADELPHIA' never eats the city name and bare 'FLOOR' leaves no '#'.
    body = re.sub(
        rf"\b(?:{UNIT_MARKER_PATTERN})\b[.\s#]*(?=(?:\d+[A-Z0-9-]*|[A-Z]{{1,3}}\d*[A-Z]?)\b)",
        "#",
        body,
    )
    #    5c. Any marker with nothing after it (or standalone OFC/OFFICE before city) is noise; drop it.
    body = re.sub(rf"\b(?:{UNIT_MARKER_PATTERN})\b[.\s#]*$", "", body)
    body = re.sub(r"\b(?:OFC|OFFICE)\b", " ", body)

    # 6. Expand directionals and street types on the body only
    body = _expand_address_tokens(body)

    # Ensure a space before '#', then collapse whitespace
    body = re.sub(r"([^\s])#", r"\1 #", body)
    body = re.sub(r"\s+", " ", body).strip()

    if not body:
        return ""

    norm = " ".join(part for part in (body, state, zipcode) if part)

    # Blacklist generic placeholders
    if norm in ADDRESS_PLACEHOLDER_VALUES:
        return ""

    # Check for empty numeric placeholders like "0" or "000"
    if re.match(r'^0+$', norm):
        return ""

    # Minimum content: must contain at least one letter and be > 4 chars.
    # This catches blank CSV fields like ", , , , ," which collapse to nothing.
    if len(norm) < 5 or not re.search(r'[A-Z]', norm):
        return ""

    return norm

def normalize_mailing_address_coarse(address: str) -> str:
    """
    Building-level address normalization for network discovery.
    Strips suite/unit numbers and normalizes ordinals/directionals so that
    businesses at the same physical building are matched together.
    e.g. '399 Whalley Ave Suite 103, New Haven, CT 06511-1631' and
         '399 Whalley Ave, New Haven, CT 06511' both normalize to the same key,
    while '1650 East 15 Street' and '1650 East 18 Street' stay distinct.
    """
    # Start with the fine-grained normalization
    norm = normalize_mailing_address(address)
    if not norm:
        return ""

    # 1. Strip suite/unit/apt/floor designators and their numbers
    norm = re.sub(r'\s*#\s*[A-Z0-9][A-Z0-9-]*', ' ', norm)
    norm = norm.replace('#', ' ')

    is_po_box = bool(re.match(r'^PO BOX \d+\b', norm))

    # 2. Strip the trailing ZIP; suites in one building carry different ZIP+4s
    #    and sometimes different ZIPs, so the ZIP is not part of the key.
    norm = re.sub(r'\s+\d{5}\b\s*$', '', norm)

    # 3. Normalize street ordinals so '15 STREET' and '15TH ST' agree.
    norm = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', norm)

    # 4. Strip a stray unit number left behind by CSV field splits, e.g.
    #    '399 WHALLEY AVENUE 103 NEW HAVEN' -> '399 WHALLEY AVENUE NEW HAVEN'.
    #    Only fires when the number directly follows a street-type word, so
    #    numbered streets ('1650 EAST 15 STREET') keep their ordinal.
    if not is_po_box:
        suffixes = '|'.join(sorted(STREET_SUFFIX_WORDS))
        norm = re.sub(
            rf'\b({suffixes})\b(?:\s+(?:{UNIT_MARKER_PATTERN})\b)?(?:\s+\d{{1,5}}[A-Z]?\b)?',
            r'\1',
            norm,
        )

    # 5. Collapse whitespace and clean up
    norm = re.sub(r'\s+', ' ', norm).strip()

    # 6. Final minimum-content check: must have a street number + name
    if len(norm) < 5 or not re.search(r'\d.*[A-Z]', norm):
        return ""

    return norm


def first_normalized_mailing_address(*addresses: str, coarse: bool = False) -> str:
    """Return the first usable normalized address from ordered source fields."""
    normalizer = normalize_mailing_address_coarse if coarse else normalize_mailing_address
    for address in addresses:
        norm = normalizer(address)
        if norm:
            return norm
    return ""


# Rules whose substrings describe a *shape* of address rather than a specific
# registered-agent office. They are already covered by is_placeholder_address
# and by the shared-address frequency cap, and applying them as blanket bans
# would discard legitimate evidence (a landlord using one PO Box for every LLC).
GENERIC_ADDRESS_RULE_SUBSTRINGS = {
    'NO INFORMATION PROVIDED', 'NONE', 'N/A', 'NOT APPLICABLE', 'UNKNOWN',
    'NOT PROVIDED', 'PO BOX', 'C/O', 'CARE OF', 'C.O.', 'C O',
}


def load_registrar_address_substrings(conn) -> frozenset:
    """Load registered-agent address substrings from the address_match_rules table.

    Returns normalized-uppercase substrings to be matched against a normalized
    address. Generic shape rules (see GENERIC_ADDRESS_RULE_SUBSTRINGS) are
    excluded; they are handled by placeholder detection and the frequency cap.
    """
    substrings = set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT substring FROM address_match_rules WHERE match_type = 'registrar'"
            )
            for (value,) in cur:
                if not value:
                    continue
                cleaned = str(value).upper().strip()
                if not cleaned or cleaned in GENERIC_ADDRESS_RULE_SUBSTRINGS:
                    continue
                # Match rules against normalized addresses, so normalize the
                # rule the same way ('80 STATE ST' -> '80 STATE STREET').
                expanded = _expand_address_tokens(re.sub(r"[.,'`\"]", '', cleaned))
                substrings.add(cleaned)
                if expanded:
                    substrings.add(expanded)
    except Exception:
        # Table missing or unreadable — fall back to the hardcoded set only.
        return frozenset()
    return frozenset(substrings)


@lru_cache(maxsize=8)
def _registrar_pattern(registrar_substrings: frozenset):
    """Compile the rule set into one alphanumeric-bounded alternation.

    Plain `in` matching is wrong here: '42 BROADWAY' is a substring of
    '142 BROADWAY', and '80 STATE STREET' of '1080 STATE STREET', so a rule for
    one registered-agent office would silently block unrelated buildings on the
    same street. The lookarounds require the match to start and end on a token
    boundary rather than mid-number.
    """
    if not registrar_substrings:
        return None
    alternation = '|'.join(
        re.escape(sub) for sub in sorted(registrar_substrings, key=len, reverse=True)
    )
    return re.compile(rf'(?<![A-Z0-9])(?:{alternation})(?![A-Z0-9])')


def is_registrar_address(normalized_address: str, registrar_substrings) -> bool:
    """True when a normalized address matches a known registered-agent office."""
    if not normalized_address:
        return True
    pattern = _registrar_pattern(frozenset(registrar_substrings or ()))
    if pattern is None:
        return False
    return bool(pattern.search(normalized_address.upper()))


def is_ignored_shared_address(address: str) -> bool:
    if not address:
        return True
    raw_norm = str(address).upper().strip()
    norm = normalize_mailing_address_coarse(address) or raw_norm
    return is_placeholder_address(raw_norm) or norm in SHARED_ADDRESS_IGNORE_VALUES

def extract_base_address(address: str) -> str:
    """
    Standardizes an address and strips unit/apt/suite for grouping into buildings.
    Matches the logic in main.py but refined for shared use.
    """
    if not address: return ""

    # 1. Basic normalization (but keep it close to raw for display/grouping consistency)
    addr = address.strip()

    # 2. Strip unit info
    # Use \b (word bound) or (?=\d) (followed by digit) to avoid partial matches
    # on street names (e.g. 'FL' matching 'FLORENCE').
    # regex matches: (comma or space) + (keyword) + (boundary/digit) + space? + (alphanumeric/dash) until end

    pattern = r'(?:,|\s+)\s*(?:(?:UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|RM|ROOM)(?:\b|(?=\d))|#)\s*[\w\d-]+$'

    clean = re.sub(pattern, '', addr, flags=re.IGNORECASE).strip()

    # Remove trailing comma if unit was after a comma
    if clean.endswith(','):
        clean = clean[:-1].strip()

    return clean

def is_likely_street_address(addr: str) -> bool:
    """
    Heuristic: Valid street addresses usually start with a digit (house number)
    AND have at least one text part (street name).
    Avoids grouping outliers like '93' or '0'.
    """
    if not addr: return False
    addr = addr.strip()
    if not addr or not addr[0].isdigit():
        return False

    parts = addr.split()
    if len(parts) < 2:
        return False

    return True

PROFESSIONAL_AGENT_EMAIL_DOMAINS = {
    "pullcom.com", "shipmangoodwin.com", "daypitney.com", "hinckleyallen.com",
    "wiggin.com", "cohenandwolf.com", "updikepc.com", "halloransage.com",
    "berchemmoses.com", "brodywilkinson.com", "reidandriege.com",
    "neubertpepe.com", "bendett-mchugh.com", "susmanmillian.com",
    # Commercial registered agents & formation services
    "cscglobal.com", "cscinfo.com", "wolterskluwer.com", "ctcorporation.com",
    "legalinc.com", "legalzoom.com", "zenbusiness.com", "incfile.com",
    "northwestregisteredagent.com", "registeredagentsinc.com", "incorp.com",
    "cogencyglobal.com", "paracorp.com", "harborcompliance.com", "vcorpservices.com",
    # Banks, mortgage servicers & tax-escrow processors
    "citi.com", "citigroup.com", "wellsfargo.com", "jpmchase.com", "chase.com",
    "bankofamerica.com", "bofa.com", "usbank.com", "wilmingtontrust.com",
    "wsfsbank.com", "bnymellon.com", "db.com", "pnc.com", "mtb.com", "truist.com",
    "td.com", "citizensbank.com", "53.com", "key.com", "keybank.com",
    "mrcooper.com", "nationstarmtg.com", "shellpointmtg.com", "newrez.com",
    "spservicing.com", "ocwen.com", "onitygroup.com", "mortgagefamily.com",
    "fayservicing.com", "cenlar.com", "loancare.com", "pennymac.com",
    "carringtonms.com", "corelogic.com", "lereta.com", "ryan.com",
}

PROFESSIONAL_AGENT_DOMAIN_RE = re.compile(
    r"(?:"
    r"(?<!de)(?<!out)law(?!n|rence|lor|s?softball)"
    r"|legal|esq(?=\.|uire|$|-)|attorneys?"
    r"|cpas?(?:\b|llc|pc|firm|group|co|ct|ny|us|\d|\.)"
    r"|(?<!un)accounting|acct"
    r"|title(?:co|company|corp|group|services|agency|ins|insurance)|escrow"
    r"|\.(?:law|legal|cpa|attorney|lawyer)$"
    r")",
    re.IGNORECASE,
)


def get_email_match_key(email: str, email_rules: dict) -> str | None:
    """Classifies an email and returns a matching key based on the 3-category logic."""
    if not email or '@' not in email: return None
    email = email.lower().strip()
    try:
        _, domain = email.split('@', 1)
    except ValueError:
        return None

    # Check rules
    rule = email_rules.get(domain)
    if rule == 'registrar': return None
    if rule == 'custom': return domain

    # If the domain is in the rules as 'public', we return the FULL EMAIL
    # so that individuals are NOT merged (unique identity).
    if rule == 'public':
        return email

    # Exclude third-party law firms, legal filing services, and CPA/accounting
    # firms so attorney/accountant emails on SOS filings do not merge unrelated
    # landlord portfolios.
    if domain in PROFESSIONAL_AGENT_EMAIL_DOMAINS or PROFESSIONAL_AGENT_DOMAIN_RE.search(domain):
        return None

    # Default logic for undefined domains:
    # If it's a domain with multiple dots (except common ones like .co.uk),
    # or if it looks very specialized, it's likely a custom business domain.
    # For now, we return the domain to group everyone on that domain.
    return domain


# ---------------------------------------------------------------------------
# Canonical Cross-Jurisdiction 3rd-Party Entity & Address Exclusions
# ---------------------------------------------------------------------------
# Street-type lookahead prevents street-named private LLCs (e.g. '1628 CHURCH ST LLC',
# '1120 COLLEGE AVE HDFC', '3024 CHURCH AVENUE LLC') from matching institutional
# keywords like CHURCH, COLLEGE, UNIVERSITY, or TEMPLE.
_STREET_FOLLOWER_LOOKAHEAD = (
    r"(?!\s+(?:AVE|AVENUE|ST\b(?!\s+[A-Z]{3})|STREET|RD|ROAD|PL|PLACE|BLVD|BOULEVARD|"
    r"DR|DRIVE|LN|LANE|WAY|CT|COURT|TER|TERRACE|PKWY|PARKWAY)\b)"
)

THIRD_PARTY_ENTITY_REGEX_BODY = (
    # 1. Commercial, National & Savings Banks, GSEs & Credit Unions
    r"\b(?:CITIBANK|CITIGROUP|CITIMORTGAGE|CITIFINANCIAL|"
    r"WELLS\s+FARGO|JPMORGAN|J\.?\s*P\.?\s+MORGAN\s+CHASE|CHASE\s+BANK|CHASEBANK|CHASE\s+MANHATTAN|"
    r"BANK\s+OF\s+AMERICA|LASALLE\s+BANK|COUNTRYWIDE\s+HOME\s+LOANS?|INDYMAC|"
    r"U\.?\s*S\.?\s*BANK|US\s+BANK|"
    r"WILMINGTON\s+SAVINGS|WILMINGTON\s+(?:SAV[A-Z]*\s+)?FUND(?:\s+SOCIETY)?|WILMINGTON\s+TRUST|"
    r"BANK\s+OF\s+NEW\s+YORK|BANK\s+OF\s+NY|BNY\s+MELLON|BANKERS\s+TRUST\s+CO(?:MPANY)?|"
    r"DEUTSCHE\s+BANK|HSBC\s+BANK|HSBC\s+MORTGAGE|HSBC\s+USA|"
    r"PNC\s+BANK|M\s*&\s*T\s+BANK|MANUFACTURERS\s+(?:AND|&)\s+TRADERS\s+TRUST|"
    r"TRUIST\s+BANK|SUNTRUST\s+BANK|SUNTRUST\s+MORTGAGE|BB&T\s+BANK|BRANCH\s+BANKING\s+(?:AND|&)\s+TRUST|"
    r"TD\s+BANK|CITIZENS\s+BANK|FIFTH\s+THIRD\s+BANK|REGIONS\s+BANK|"
    r"SANTANDER\s+BANK|SOVEREIGN\s+BANK|KEYBANK|FLAGSTAR\s+BANK|"
    r"COMERICA\s+BANK|HUNTINGTON\s+NATIONAL\s+BANK|INDUSTRIAL\s+BANK|"
    r"WEBSTER\s+BANK|LIBERTY\s+BANK|ION\s+BANK|PEOPLE.?S\s+UNITED\s+BANK|"
    r"EASTERN\s+BANK|ROCKLAND\s+TRUST|BERKSHIRE\s+BANK|VALLEY\s+NATIONAL\s+BANK|"
    r"CAPITAL\s+ONE\s*,?\s*N\.?\s*A|ALLY\s+BANK|BARCLAYS\s+BANK|MIDFIRST\s+BANK|"
    r"WASHINGTON\s+MUTUAL|WACHOVIA\s+BANK|BMO\s+HARRIS\s+BANK|ASSOCIATED\s+BANK|"
    r"FIRST\s+NATIONAL\s+BANK|OLD\s+NATIONAL\s+BANK|OLD\s+SECOND\s+NATIONAL|USAA\s+FEDERAL\s+SAVINGS|NAVY\s+FEDERAL|"
    r"MARQUETTE\s+BANK|DEVON\s+BANK|AMALGAMATED\s+BANK|PARKWAY\s+BANK|FIRST\s+MIDWEST\s+BANK|"
    r"NORTH\s+COMMUNITY\s+BANK|BRIDGEVIEW\s+BANK|ALBANY\s+BANK|TCF\s+BANK|BYLINE\s+BANK|"
    r"WINTRUST\s+BANK|BEVERLY\s+BANK|COLE\s+TAYLOR\s+BANK|COSMOPOLITAN\s+BANK|"
    r"STANDARD\s+BANK\s+(?:AND|&)\s+TRUST|BANK\s*FINANCIAL|"
    r"GOLDMAN\s+SACHS|MTGLQ\s+INVESTORS|MCLP\s+ASSET\s+COMPANY|DLJ\s+MORTGAGE|"
    r"FANNIE\s+MAE|FREDDIE\s+MAC|GINNIE\s+MAE|HOME\s*PATH\s+FIN(?:ANCIAL)?|HOMEPATH|"
    r"FEDERAL\s+NATIONAL\s+MORTGAGE|FEDERAL\s+HOME\s+LOAN|GOVERNMENT\s+NATIONAL\s+MORTGAGE|"
    r"MORTGAGE\s+ELECTRONIC\s+REGISTRATION|MERSCORP)\b|"
    r"\b(?:NATIONAL|SAVINGS|FEDERAL\s+SAVINGS|COOPERATIVE|MUTUAL\s+SAVINGS)\s+BANK\b|"
    r"\bBANK\s*,?\s*(?:N\.?\s*A\.?|NATIONAL\s+ASS(?:OCIATION|OC|N)|NATL\s+ASS(?:OC|N)|FSB|F\.S\.B\.|USA|TRUST\s+CO|AS\s+TRUSTEE)\b|"
    r"\bSAV(?:INGS?|NGS?)\s+FUND\s+(?:SOCIETY|SOC|SOCI)\b|"
    r"\bCREDIT\s+UNION\b|"
    # 2. Securitization, RMBS, IRA Custodian, Land-Trust Custodian & Foreclosure Trustees
    r"\b(?:CHICAGO\s+TITLE\s+(?:LAND\s+TR(?:UST|U)?|(?:AND|&)?\s*TRUST)|CHGO\s+TITLE\s+LAND\s+TR(?:UST|U)?|"
    r"CHICAGO\s+LAND\s+(?:AND|&)\s+TITLE|THE\s+LAND\s+TRUST\s+(?:COMPANY|CO|AS\s+TR(?:USTEE|US)?)|ATG\s+TRUST\s+CO)\b|"
    r"\bTRUST\s*,?\s*(?:NATIONAL\s+ASS(?:OCIATION|OC|N)|NATL\s+ASS(?:OC|N)|N\.?\s*A\.?)\b|"
    r"\b(?:EQUITY|STRATA|MIDLAND|HORIZON|PENSCO|MILLENNIUM|KINGDOM|MADISON|QUEST|PACIFIC\s+PREMIER)\s+TRUST\s+CO(?:MPANY)?\b|"
    r"\b(?:ENTRUST\s+GROUP|ENTRUST\s+ADMINISTRATION)\b|"
    r"\bTRUST\s+COMPANY\b.*\b(?:TRUSTEES?|CUST(?:ODIAN)?)\b|"
    r"\bTRUST\s+CO\b.*\bCUST(?:ODIAN)?\b|"
    r"\b(?:NATIONAL\s+ASS(?:OCIATION|OC|N)|NATL\s+ASS(?:OC|N)|N\.?\s*A\.?)\s+(?:AS\s+)?(?:INDENTURE\s+|SUCCESSOR\s+)?(?:TRUSTEE|TR)\b|"
    r"\bINDENTURE\s+TRUSTEE\b|"
    r"\bMORTGAGE\s+(?:LOAN|ASSET|PASS[- ]THROUGH|SECURITIES|BACKED)\s+TRUST\b|"
    r"\b(?:LEGAL\s+TITLE|VENDEE\s+MORTGAGE)\s+TRUST\b|"
    r"\bRESIDENTIAL\s+(?:MORTGAGE\s+AGGREGATION|CREDIT\s+OPPORTUNITIES)\s+TRUST\b|"
    r"\bASSET[- ]BACKED\s+(?:CERTIFICATES|SECURITIES|PASS[- ]THROUGH)\b|"
    r"\bPASS[- ]THROUGH\s+CERTIFICATES\b|"
    r"\bSECURITIZATION\s+TRUST\b|"
    # 3. Mortgage Servicers, Reverse-Mortgage Servicers, Tax Processors & REO Vendors
    r"\b(?:NATIONSTAR|MR\.?\s+COOPER|SHELLPOINT|NEWREZ|"
    r"SELECT\s+PORTFOLIO\s+SERVICING|OCWEN|ONITY|PHH\s+MORTGAGE|"
    r"FAY\s+SERVICING|CENLAR|LOANCARE|PENNYMAC|CARRINGTON\s+MORTGAGE|"
    r"ROCKET\s+MORTGAGE|QUICKEN\s+LOANS|FREEDOM\s+MORTGAGE|"
    r"LAKEVIEW\s+LOAN\s+SERVICING|CALIBER\s+HOME\s+LOANS|ROUNDPOINT\s+MORTGAGE|"
    r"SPECIALIZED\s+LOAN\s+SERVICING|RUSHMORE\s+LOAN|SELENE\s+FINANCE|"
    r"GREGORY\s+FUNDING|BSI\s+FINANCIAL|DOVENMUEHLE|SERVICEMAC|"
    r"DITECH\s+FINANCIAL|GREEN\s+TREE\s+SERVICING|OCWEN\s+LOAN|"
    r"BAYVIEW\s+LOAN\s+SERVICING|COMMUNITY\s+LOAN\s+SERVICING|LITTON\s+LOAN|"
    r"SAXON\s+MORTGAGE|AURORA\s+LOAN\s+SERVICES|EMC\s+MORTGAGE|AMERIHOME\s+MORTGAGE|"
    r"LOANDEPOT(?:COM)?|ROCKETMORTGAGE|FREEDOMMORTGAGE|MRCOOPER|SHELLPOINTMTG|"
    r"CELINK|LONGBRIDGE\s+FINANCIAL|REVERSE\s+MORTGAGE\s+FUNDING|"
    r"CHAMPION\s+MORTGAGE|FINANCE\s+OF\s+AMERICA\s+REVERSE|"
    r"CORELOGIC|LERETA|SAFEGUARD\s+PROPERTIES|GUARDIAN\s+ASSET\s+MANAGEMENT|"
    r"REO\s+MANAGEMENT\s+SOLUTIONS|ALTISOURCE|CYPREXX|H\s*&\s*R\s+BLOCK)\b|"
    r"\b(?:MORTGAGE|LOAN)\s+SERVICING\b|"
    # 4. Commercial Registered Agents & Corporate Formation Services
    r"\b(?:CORPORATION\s+SERVICE\s+COMPANY|CSC\s+LAWYERS|CSC\s+ENTITY|"
    r"C\.?\s*T\.?\s+CORPORATION\s+SYSTEM|LEGALINC|LEGALZOOM|ZENBUSINESS|INCFILE|BIZFILINGS|"
    r"NORTHWEST\s+REGISTERED\s+AGENT|REGISTERED\s+AGENTS\s+INC|"
    r"NATIONAL\s+REGISTERED\s+AGENTS|UNITED\s+STATES\s+CORPORATION\s+AGENTS|"
    r"INCORP\s+SERVICES|COGENCY\s+GLOBAL|PARACORP|HARBOR\s+COMPLIANCE|"
    r"CORPORATE\s+CREATIONS|CAPITOL\s+CORPORATE\s+SERVICES|CAPITOL\s+SERVICES\s+INC|"
    r"VCORP\s+SERVICES|WOLTERS\s+KLUWER|INCORPORATING\s+SERVICES\s+LTD|"
    r"REGISTERED\s+AGENT\s+SOLUTIONS|UNITED\s+AGENT\s+GROUP|PRIME\s+CORPORATE\s+SERVICES|"
    r"NATIONAL\s+CORPORATE\s+RESEARCH|SPIEGEL\s*&\s*UTRERA|BLUMBERGEXCELSIOR)\b|"
    r"^(?:C\s*/?\s*O\s+)?C\.?\s*T\.?\s+CORPORATION\b|"
    # 5. Government, Housing Authorities/Commissions, Land Banks, Transit, Utilities, Universities, Hospitals & Religious
    r"\b(?:UNITED\s+STATES\s+OF\s+AMERICA|U\.?\s*S\.?\s+GOVERNMENT|US\s+GOVERNMENT|"
    r"SECRETARY\s+OF\s+HOUSING|DEPT\s+OF\s+HOUSING\s+AND\s+URBAN|DEPARTMENT\s+OF\s+HOUSING\s+AND\s+URBAN|"
    r"SECRETARY\s+OF\s+VETERANS|DEPT\s+OF\s+VETERANS\s+AFFAIRS|"
    r"DISTRICT\s+OF\s+COLUMBIA|NYC\s+HPD|NYCHA|NEW\s+YORK\s+CITY\s+HOUSING|"
    r"DEPARTMENT\s+OF\s+HOUSING\s+PRESERVATION|DEPT\s+OF\s+HOUSING\s+PRESERVATION|"
    r"DAMP/?TIL|DAMPTIL|NEIGHBORHOOD\s+RESTORE|NEIGHBORHOOD\s+RENEWAL|"
    r"RESTORING\s+COMMUNITIES|RESTORING\s+URBAN\s+NEIGHBORHOODS|PRESERVING\s+CITY\s+NEIGHBORHOODS|"
    r"CONNECTICUT\s+STATE|NEW\s+YORK\s+STATE|"
    r"HOUSING\s+AUTH(?:ORITY)?|CHICAGO\s+HSING\s+AUTH(?:ORTI|ORITY)?|HOUSING\s+COMMISSION|PUBLIC\s+HOUSING\s+(?:AUTH|ADMIN|COMM|AGENCY|CORP)|MINNEAPOLIS\s+PUBLIC\s+HOUSING|MPHA|PHADC|NRHA|RRHA|PRHA|ARHA|CRHA|"
    r"LAND\s+BANK|REDEVELOPM?E?NT\s+AUTH(?:ORITY|OR)?|REDVLPMNT\s+AUTH(?:ORITY|OR)?|REDEVLPMNT\s+AUTH(?:ORITY|OR)?|REDEVELOPMENT\s+COMMISSION|VIRGINIA\s+BEACH\s+COMMUNITY\s+ETC|"
    r"HIGHWAY\s+COMM(?:ISSION)?|STATE\s+HWY\s+COMM(?:ISSION)?|(?:STATE|ARKANSAS\s+STATE)\s+HIGHWAY|STATE\s+HIGHWAY\s+ADMIN(?:ISTRATION)?|VDOT|FORESTRY\s+COMM(?:ISSION)?|GAME\s+(?:AND|&)\s+FISH|CORPS\s+OF\s+ENGINEERS|"
    r"BOARD\s+OF\s+(?:COUNTY\s+)?SUPERVISORS|BD\s+(?:OF\s+)?CO(?:UNTY)?\s+SUPERVISORS|SANITARY\s+DIST(?:RICT)?|"
    r"DEPT\s+OF\s+HUD|HUD\s+PROP(?:ER?TY)?\s+DISP|GOVERNMENTAUCTION|WITHHELD\s+AT\s+REQUEST(?:\s+OF\s+OWNER)?|"
    r"BOARD\s+OF\s+ED(?:UC(?:ATION)?)?|SCHOOL\s+BOARD|SCHOOL\s+DISTRICT|PUBLIC\s+SCHOOLS|"
    r"CHICAGO\s+PARK\s+DIST(?:RICT)?|PARK\s+DISTRICT|PUB(?:LIC)?\s+BLDG\s+COMM(?:ISSION)?|PUBLIC\s+BUILDING\s+COMMISSION|"
    r"MEDICAL\s+DIST(?:RICT)?\.?\s+COM|METRO\s+PIER\s+(?:AND|&)\s+EXPO|WESTSIDE\s+HEALTH\s+AUTHOR|"
    r"PARKS\s+(?:AND|&)\s+RECREATION|DEPT\s+OF\s+TRANSPORTATION|DEPARTMENT\s+OF\s+TRANSPORTATION|"
    r"(?:IL|ILLINOIS|US|U\.?\s*S\.?)\s+DEPT\s+(?:OF\s+)?(?:TRANSPORTATION|HOUSING)|COMM\s+OF\s+MASS|"
    r"UNITED\s+STATES\s+POSTAL\s+SERVICE|USPS|"
    r"WMATA|WASHINGTON\s+METROPOLITAN\s+AREA\s+TRANSIT|SEPTA|MBTA|NJ\s+TRANSIT|NEW\s+JERSEY\s+TRANSIT|"
    r"MASS(?:ACHUSETTS)?\s+BAY\s+TRANS(?:P(?:ORTATION)?)?\s+AUTH(?:ORITY)?|TRANSIT\s+ADMIN(?:ISTRATION)?|MDOT|RTA\s*/\s*METRA|"
    r"AMTRAK|NATIONAL\s+RAILROAD|RAILROAD\s+PASSENGER|RAILROAD\s+MUSEUM|CSX\s+TRANSPORTATION|NORFOLK\s+SOUTHERN|CONRAIL|METRO[- ]NORTH|CENTRAL\s+LINES\s+LLC|BOSTON\s*(?:&|AND)\s*MAINE\s+CORP|"
    r"TRANSIT\s+AUTHOR(?:ITY)?|TRANSPORTATION\s+AUTH(?:ORITY)?|TRANSIT\s+AGENCY|PORT\s+AUTHORITY|TURNPIKE\s+AUTH(?:ORITY|OR)?|HIGHWAY\s+AUTHORITY|CONVENTION\s+CENTER\s+AUTH(?:ORITY)?|"
    r"INTERNATIONAL\s+BRIDGE\s+CO|RAILROAD\s+COMPANY|RAILWAY\s+COMPANY|"
    r"POTOMAC\s+ELECTRIC\s+POWER|PEPCO|BALTIMORE\s+GAS|BALTO\.?\s+GAS|GREEN\s+MOUNTAIN\s+POWER|"
    r"EVERSOURCE|BOSTON\s+EDISON|WESTERN\s+MASS(?:ACHUSETTS)?\s+ELECTRIC|FITCHBURG\s+GAS\s*(?:\+|AND|&)\s*ELECTRIC|"
    r"UNITED\s+ILLUMINATING|CONSOLIDATED\s+EDISON|CON\s+EDISON|"
    r"NATIONAL\s+GRID|NIAGARA\s+MOHAWK|DTE\s+ENERGY|DTE\s+ELECTRIC|DETROIT\s+EDISON|"
    r"CONSUMERS\s+ENERGY|COMMONWEALTH\s+EDISON|XCEL\s+ENERGY|NORTHERN\s+STATES\s+POWER|"
    r"FLORIDA\s+POWER|PECO\s+ENERGY|PUBLIC\s+SERVICE\s+ELECTRIC|PSE&G|"
    r"WASHINGTON\s+GAS\s+LIGHT|AQUARION|WATER\s+AUTHORITY|SEWER\s+AUTHORITY|WATER\s+(?:AND|&|\+)\s+SEWER\s+COMM(?:ISSION)?|CONSERVATION\s+COMM(?:ISSION)?|"
    r"ELECTRIC\s+COMPANY|POWER\s+COMPANY|POWER\s+CORP(?:ORATION)?|LIGHT\s+AND\s+POWER|GAS\s+(?:AND|&)\s+ELECTRIC|"
    r"MARATHON\s+PETROLEUM|SUNOCO|GLOBAL\s+MONTELLO|ALLIANCE\s+ENERGY|"
    r"WEYERHAEUSER|DELTIC\s+TIMBER|POTLATCH\s+(?:CORP|FOREST|TIMBER)|GREEN\s+BAY\s+PACKAGING|"
    r"(?:LESTER|TATE)\s+LUMBER|ROANOKE\s+TIMBERLANDS|FOREST\s+INVEST(?:MENT)?\s+REIT|TIMBER\s+OPPORTUNIT(?:Y|IES)|CF\s+HIGHLANDS|"
    r"CNX\s+GAS|PENN\s+VIRGINIA\s+OPERATING|BLUE\s+ROCK\s+RESOURCES|DOMINION\s+ENERGY|VIRGINIA\s+ELECTRIC\s+(?:AND|&)\s+POWER|APPALACHIAN\s+POWER|"
    r"COLONIAL\s+WILLIAMSBURG\s+FOUNDATION|ATLANTIC\s+SHORES\s+COOPERATIVE|"
    r"P\.?O\.?A\.?|BVPOA|FFBCCI|H\.?O\.?A\.?|(?:(?:PROPERTY|HOME|UNIT|RESORT|VILLAGE|CONDO(?:MINIUM)?|TOWNHOUSE|TOWNHOME|LOT)\s+)?OWNERS?\s+ASS(?:OC|OCIATION|N)|HOME\s*OWNERS?\s+ASS(?:OC|OCIATION|N)|PARKS\s*(?:&|AND)\s*REC(?:REATION)?\s+ASS(?:OC|OCIATION|N)|COMMUNITY\s+CLUB|"
    r"CONDO(?:MINIUM)?\s+(?:OWNERS?\s+)?ASS(?:OC|OCIATION|N)|COMMUNITY\s+(?:CONDO(?:MINIUM)?\s+)?ASS(?:OC|OCIATION|N)|LEISURE\s+VIL(?:LAGE)?\s+[A-Z]?\s*ASS(?:OC|OCIATION|N)|THE\s+VILLAGES\s+ASS(?:OC|OCIATION|N)|"
    r"INSTITUTE\s+OF\s+TECHNOLOGY|POLYTECHNIC\s+INSTITUTE|"
    r"SEMINARY|GENERAL\s+HOSPITAL|HOSPITAL\s+CORP|"
    r"HEALTH\s+SYSTEMS?|MEDICAL\s+CENTER|MEDICAL\s+SYSTEM|JOHNS\s+HOPKINS|"
    r"ARCHDIOCESE|DIOCESE|ARCHBISHOP|CATHOLIC\s+BISHOP|ROMAN\s+CATH(?:OLIC)?|ARCH\s+OF\s+BOS|CARMELITE\s+CONVENT|CATHEDRAL|SYNAGOGUE|SALVATION\s+ARMY|ARMY\s+SALVATION)\b|"
    rf"\b(?:UNIVERSITY|COLLEGE|CHURCH|TEMPLE|HOSPITAL){_STREET_FOLLOWER_LOOKAHEAD}\b|"
    r"\bUNIV\s+OF\s+[A-Z]|"
    r"^(?:RAILROAD|MASSACHUSETTS\s+BAY|U\.?\s*S\.?\s*A?\.?|US\s+US|HUD|CEMETERY\s*\.?)$|\bCOA$|"
    r"\b[A-Z][A-Z\s.-]{1,28},\s*(?:TOWN|CITY|COUNTY|COMMONWEALTH|STATE|BOROUGH|VILLAGE)\s+O(?:F)?\b|"
    r"\b(?:STATE|COMM?O?N?W(?:EA)?LTH|CITY|TOWN|BOROUGH|VILLAGE|TOWNSHIP|COUNTY)\s+OF\s+[A-Z]|"
    r"\b(?:STATE|COMM?O?N?W(?:EA)?LTH|CITY|TOWN|COUNTY)\s+OF\s*$|"
    r"\bMAYOR\s+(?:AND|&)\s+CITY\s+COUNCIL\b|"
    r"\b(?:MIAMI[- ]DADE|WAYNE|HENNEPIN|COOK|SUFFOLK|NASSAU|WESTCHESTER|BALTIMORE)\s+COUNTY\b"
)

THIRD_PARTY_ENTITY_RE = re.compile(THIRD_PARTY_ENTITY_REGEX_BODY, re.IGNORECASE)

# PostgreSQL ARE (~* / !~*) uses \y for word boundaries instead of \b (which is backspace in POSIX ARE),
# and (?n) so ^ / $ anchors match individual newline-delimited identity strings in CONCAT_WS(E'\n', ...).
THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN = "(?n)" + THIRD_PARTY_ENTITY_REGEX_BODY.replace(r"\b", r"\y")


def is_excluded_third_party_entity(name: str | None) -> bool:
    """Return True if `name` is a non-landlord 3rd party (bank, mortgage/RMBS trustee,
    loan servicer, tax processor, commercial registered agent, utility, transit/railroad,
    government/housing authority, university, hospital, or religious institution) that
    must never anchor or bridge landlord networks.
    """
    if not name:
        return False
    cleaned = " ".join(str(name).upper().strip().split())
    if not cleaned:
        return False
    return bool(THIRD_PARTY_ENTITY_RE.search(cleaned))


# Curated street-address stems of major national mortgage servicers, tax-escrow
# processors, REO preservation vendors, and commercial registered agents.
KNOWN_THIRD_PARTY_HUB_SUBSTRINGS = frozenset({
    # Mortgage loan servicers & foreclosure/REO vendors
    "55 BEATTIE PLACE", "55 BEATTIE PL",
    "75 BEATTIE PLACE", "75 BEATTIE PL",
    "8950 CYPRESS WATERS",
    "3501 OLYMPUS BOULEVARD", "3501 OLYMPUS BLVD",
    "3217 SOUTH DECKER LAKE", "3217 DECKER LAKE", "3217 S DECKER LAKE",
    "1661 WORTHINGTON ROAD", "1661 WORTHINGTON RD",
    "5720 PREMIER PARK DRIVE", "5720 PREMIER PARK DR",
    "425 SOUTH FINANCIAL PLACE", "425 S FINANCIAL PL",
    "5426 BAY CENTER DRIVE", "5426 BAY CENTER DR",
    "3476 STATE VIEW BOULEVARD", "3476 STATE VIEW BLVD",
    "1 HOME CAMPUS",
    "4101 WISEMAN BOULEVARD", "4101 WISEMAN BLVD",
    "9726 OLD BAILES ROAD", "9726 OLD BAILES RD",
    "13801 WIRELESS WAY",
    "3900 CAPITAL CITY BOULEVARD", "3900 CAPITAL CITY BLVD",
    "2300 EAST LINCOLN HIGHWAY", "2300 E LINCOLN HWY", "1 OXFORD VAL",
    "14405 WALTERS ROAD", "14405 WALTERS RD",
    "4600 SOUTH ULSTER STREET", "4600 S ULSTER ST",
    # Property-tax & escrow processors
    "1 CORELOGIC WAY",
    "3001 HACKBERRY ROAD", "3001 HACKBERRY RD",
    "4 FIRST AMERICAN WAY",
    "1123 SOUTH PARKVIEW DRIVE", "1123 S PARKVIEW DR",
    "901 CORPORATE CENTER DRIVE", "901 CORPORATE CENTER DR",
    # Commercial registered-agent & corporate service hubs
    "5000 THAYER CENTER", "5000 THAYER CTR",
    "251 LITTLE FALLS DRIVE", "251 LITTLE FALLS DR",
    "1209 ORANGE STREET", "1209 ORANGE ST", "1209 NORTH ORANGE STREET",
    "1201 ORANGE STREET", "1201 ORANGE ST",
    "8 THE GREEN",
    "651 NORTH BROAD STREET", "651 N BROAD ST",
    "160 GREENTREE DRIVE", "160 GREENTREE DR",
    "850 NEW BURTON ROAD", "850 NEW BURTON RD",
    "1013 CENTRE ROAD", "1013 CENTRE RD",
    "2711 CENTERVILLE ROAD", "2711 CENTERVILLE RD",
    "615 SOUTH DUPONT HIGHWAY", "615 S DUPONT HWY",
    "30 NORTH GOULD STREET", "30 N GOULD ST",
    "1621 CENTRAL AVENUE", "1621 CENTRAL AVE",
    "1712 PIONEER AVENUE", "1712 PIONEER AVE",
    "7901 4TH STREET NORTH", "7901 4TH ST N",
    "1967 WEHRLE DRIVE", "1967 WEHRLE DR",
    "80 STATE STREET", "80 STATE ST",
    "90 STATE STREET", "90 STATE ST",
    "418 BROADWAY",
    "28 LIBERTY STREET", "28 LIBERTY ST",
    # Known UPS Store / PMB / lender / IRA custodian / 3rd-party management hubs uncovered in live city audits
    "1 EQUITY WAY",
    "7901 FISH POND ROAD", "7901 FISH POND RD",
    "1524 BROOKHOLLOW DRIVE", "1524 BROOKHOLLOW DR",
    "7900 EAST UNION AVENUE", "7900 E UNION AVE",
    "23636 MICHIGAN AVENUE", "23636 MICHIGAN AVE",
    "23875 MICHIGAN AVENUE", "23875 MICHIGAN AVE",
    "8401 GREENSBORO DRIVE", "8401 GREENSBORO DR",
    "57 WEST TIMONIUM ROAD", "57 W TIMONIUM RD",
    # Commercial property-tax representation firms & triple-net (NNN) retail lessee tax departments
    "2424 RIDGE ROAD", "2424 RIDGE RD",
    "6920 POINTE INVERNESS",
    "PO BOX 80615",
    "15 WEST 6TH STREET", "15 W 6TH ST",
    "10950 GRANDVIEW DRIVE", "10950 GRANDVIEW DR",
    "3200 HACKBERRY ROAD", "3200 HACKBERRY RD",
    "1722 ROUTH STREET", "1722 ROUTH ST",
    "PO BOX 711 DALLAS", "711 PO BOX DALLAS", "PO BOX 711 CORPORATE",
    "500 VOLVO PARKWAY", "500 VOLVO PKWY",
    "2202 NORTH WEST SHORE", "2202 N WEST SHORE",
    "1 HORMEL PLACE", "1 HORMEL PL",
    "5010 CELL TOWER",
})

_PMB_MAILBOX_RE = re.compile(r"\bPMB\s*#?\s*[A-Z0-9]+\b", re.IGNORECASE)
_CARE_OF_LEAD_RE = re.compile(
    r"^\s*(?:C\s*/?\s*O|CIO|CARE\s+OF|ATTN\.?:?|ATTENTION:?|%)(?:\s|$)",
    re.IGNORECASE,
)
_INLINE_CARE_OF_PREFIX_RE = re.compile(
    r"^\s*(?:C\s*/?\s*O|CIO|CARE\s+OF|ATTN\.?:?|ATTENTION:?|%)\s+"
    r"([^,\d][^,]*?(?:,\s*(?:LLC|L\.L\.C\.?|INC\.?|CORP\.?|LP|L\.P\.?|LLP|PC|P\.C\.?|PLLC))?)"
    r"\s*(?:,\s*|\s+)(?=\d+\s+[A-Z0-9]|P\.?\s*O\.?\s*BOX\b|\d+\s+P\.?\s*O\.?\s*BOX\b)",
    re.IGNORECASE,
)
_THIRD_PARTY_CARE_OF_AGENT_RE = re.compile(
    r"\b(?:ESQ|ESQUIRE|LAW\b|ATTORNEYS?|COUNSEL|C\.?P\.?A\.?|ACCOUNTING|ACCOUNTANTS?|"
    r"REGISTERED\s+AGENT|RYAN\s*,?\s*LLC|BADEN\s+TAX|CHARTER\s+CENTRAL|KE\s+ANDREWS|DUCHARME|PARADIGM\s+TAX|"
    r"TAX\s+(?:SERV(?:ICE|ICES)?|MANAG(?:EMENT|ER|ERS)|CONSULT(?:ING|ANTS)?|ADVISORS?)|ESCROW|"
    r"FAMILY\s+DOLLAR|DOLLAR\s+TREE|DOLLAR\s+GENERAL|7[- ]?ELEVEN|WALGREENS|CVS\s+PHARMACY|CHIPOTLE|MCDONALD'?S|AUTOZONE|O'?REILLY\s+AUTO|"
    r"PROPERTY\s+MANAG(?:EMENT|ER|ERS)|PROP\s+MGMT|REALTY\s+MANAG(?:EMENT|ER))\b",
    re.IGNORECASE,
)
_HAS_STREET_OR_POBOX_RE = re.compile(
    r"(?:^\s*\d+[A-Z0-9-]*\s+[A-Z0-9]|\bP\.?\s*O\.?\s*BOX\s+[A-Z0-9]+|\b\d+\s+P\.?\s*O\.?\s*BOX\b|\bR(?:URAL\s+)?R(?:OUTE)?\s*\d+|\bH(?:IGH)?W(?:A)?Y\s+\d+|\bONE\s+[A-Z]+\s+(?:PLAZA|CENTER|WAY|PLACE|SQUARE|DRIVE|BOULEVARD|STREET|AVENUE)\b)",
    re.IGNORECASE,
)


_LEADING_RECIPIENT_ADDR_RE = re.compile(
    r"^\s*([^,\d][^,]{2,80}?(?:,\s*(?:LLC|L\.L\.C\.?|INC\.?|CORP\.?|LP|L\.P\.?|LLP|LTD\.?|PC|P\.C\.?|PLLC))?)"
    r"\s*,\s*(?=(?:\d+\s+[A-Z0-9]|P\.?\s*O\.?\s*BOX\b))",
    re.IGNORECASE,
)


def strip_benign_care_of_prefix(raw_addr: str | None) -> str:
    """Strip a leading 'C/O <Parent Company>,' or '<Recipient LLC>, <Street>' prefix when
    the prefix is NOT a 3rd-party agent/bank/law/CPA firm, so 'C/O INVITATION HOMES, 2525 W FRYE RD',
    'BALTIMORE EXCEL 01, LLC, 829 N 29TH ST, PHILADELPHIA, PA', and '2525 W FRYE RD' normalize
    to the same HQ address. If the C/O or recipient IS a 3rd-party intermediary, leave it intact
    so `is_excluded_third_party_address` rejects it.
    """
    if not raw_addr:
        return ""
    s = str(raw_addr).strip()
    m = _INLINE_CARE_OF_PREFIX_RE.match(s)
    if m:
        target = m.group(1).strip()
        if is_excluded_third_party_entity(target) or _THIRD_PARTY_CARE_OF_AGENT_RE.search(target):
            return s
        rest = s[m.end():].strip()
        return rest if rest else s
    m_rec = _LEADING_RECIPIENT_ADDR_RE.match(s)
    if m_rec:
        target = m_rec.group(1).strip()
        if is_excluded_third_party_entity(target) or _THIRD_PARTY_CARE_OF_AGENT_RE.search(target):
            return s
        rest = s[m_rec.end():].strip()
        return rest if rest else s
    return s


def is_excluded_third_party_address(
    raw_addr: str | None,
    normalized_addr: str | None = None,
    registrar_substrings=(),
) -> bool:
    """Return True if a mailing/business address is a commercial registered agent,
    virtual mailbox (PMB), mortgage-servicer/tax-escrow hub, or 3rd-party C/O line
    that must never bridge unrelated property owners.
    """
    raw_text = str(raw_addr or "").strip()
    norm_text = str(normalized_addr or "").strip().upper()
    if not raw_text and not norm_text:
        return True

    if _PMB_MAILBOX_RE.search(raw_text) or _PMB_MAILBOX_RE.search(norm_text):
        return True

    m = _INLINE_CARE_OF_PREFIX_RE.match(raw_text) or _LEADING_RECIPIENT_ADDR_RE.match(raw_text)
    if m:
        target = m.group(1).strip()
        if is_excluded_third_party_entity(target) or _THIRD_PARTY_CARE_OF_AGENT_RE.search(target):
            return True

    # If the raw address itself starts with C/O / ATTN and has no street remainder
    if _CARE_OF_LEAD_RE.match(raw_text) and not m:
        return True

    combined_rules = frozenset(registrar_substrings or ()) | KNOWN_THIRD_PARTY_HUB_SUBSTRINGS
    check_target = norm_text or normalize_mailing_address_coarse(raw_text) or raw_text.upper()
    if not _HAS_STREET_OR_POBOX_RE.search(check_target):
        return True
    if is_registrar_address(check_target, combined_rules):
        return True
    if raw_text and is_registrar_address(raw_text.upper(), combined_rules):
        return True

    return False


_BIZ_SUFFIX_STRIP_RE = re.compile(
    r"(?:\s*\b(?:LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|LP|L\.P|LLP|LTD|LIMITED|CO|COMPANY|HDFC)\b)+$",
    re.IGNORECASE,
)
_SERIES_TOKEN_ATOM = (
    r"(?:[0-9]{1,3}[A-Z]?(?:-[A-Z0-9]+)?|[SBO][0-9]{1,2}|[IVXLC]{1,5}(?:-[A-Z0-9]+)?|[A-Z](?:-[A-Z0-9]+)?)"
)
_SERIES_DESIGNATOR_TAIL_RE = re.compile(
    r"(?:"
    r"(?:\s+(?:PROPCO|PORTFOLIO|FUND|HOLDINGS?|SERIES|PHASE|TRANCHE|BORROWER|PLEDGOR|EQUITY\s+OWNER))?"
    rf"(?:\s+(?:SERIES|NO|NUM)?\s*{_SERIES_TOKEN_ATOM})+"
    r"|(?:\s+(?:PROPCO|PORTFOLIO|TRANCHE|BORROWER|PLEDGOR|EQUITY\s+OWNER))+"
    r")+$"
)


def extract_owner_family_key(raw_owner: str | None) -> str:
    """Normalize an owner name into a coarse 'owner family' stem so numbered corporate
    series from one operator (e.g. 'BALTIMORE EXCEL 01 LLC'..'BALTIMORE EXCEL S11 LLC',
    'BALTIMORE SFR PORTFOLIO 1A, LLC'..'BALTIMORE SFR PORTFOLIO VI, LLC',
    'IH2 PROPERTY FLORIDA LP'..'IH6 PROPERTY FLORIDA LP', 'FKH SFR PROPCO B-HLD LP')
    count as one owner family, while distinct unrelated LLCs ('DEERA LLC', 'ZAHAV VENTURES LLC',
    '1811 DUKELAND LLC', '2119 ASHBURTON LLC') remain distinct.
    """
    if not raw_owner:
        return ""
    # Separate glued suffixes like '06LLC' -> '06 LLC' before normalizing
    pre = re.sub(r"(\d)(LLC|LP|INC|CORP)\b", r"\1 \2", str(raw_owner), flags=re.IGNORECASE)
    # Preserve natural word order BEFORE stripping trailing numbered series tokens!
    ordered = normalize_business_name(pre)
    if not ordered:
        return ""
    ordered = _BIZ_SUFFIX_STRIP_RE.sub("", ordered).strip()
    collapsed = re.sub(r"\b([A-Z]{2,5})\d{1,2}\b", r"\1", ordered)
    stripped = _SERIES_DESIGNATOR_TAIL_RE.sub("", collapsed).strip()
    if stripped and len(stripped) >= 4 and not stripped.isdigit():
        return stripped
    return collapsed


def extract_corroborated_series_spv_key(raw_owner: str | None) -> str:
    """Return a strict multi-word corporate-series stem ONLY when `raw_owner` is an
    unambiguous numbered/lettered corporate series SPV (e.g. 'BALTIMORE SFR PORTFOLIO 1A, LLC',
    'BALTIMORE SFR PORTFOLIO III, LLC', 'BALTIMORE EXCEL 02 LLC', 'PROGRESS RESIDENTIAL BORROWER 7 LLC'),
    returning '' for street-named LLCs ('1811 DUKELAND LLC', '123 MAIN ST 1 LLC'),
    single-word generic stems ('APEX 1 LLC'), or non-series entities ('DEERA LLC').
    """
    if not raw_owner:
        return ""
    pre = re.sub(r"(\d)(LLC|LP|INC|CORP)\b", r"\1 \2", str(raw_owner), flags=re.IGNORECASE)
    if not looks_like_business_name(pre) or is_excluded_third_party_entity(pre):
        return ""
    ordered = normalize_business_name(pre)
    if not ordered or re.match(r"^\d", ordered):
        return ""
    base = _BIZ_SUFFIX_STRIP_RE.sub("", ordered).strip()
    if not base:
        return ""
    fam = extract_owner_family_key(raw_owner)
    if not fam or fam == base:
        return ""
    words = fam.split()
    if len(words) < 2 or len(fam) < 8:
        return ""
    if not any(len(w) >= 4 and w.isalpha() for w in words):
        return ""
    if words[-1] in STREET_SUFFIX_WORDS or words[-1] in STREET_SUFFIX_MAP:
        return ""
    return fam
