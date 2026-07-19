# shared_utils.py
import re
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
    '2389 MAIN STREET GLASTONBURY',
    '2 CORPORATE DRIVE SHELTON',
    '2 CORPORATE DR SHELTON',
}


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


def normalize_mailing_address(address: str) -> str:
    """
    Standardizes a mailing address to detect hidden links.
    e.g. '123 Main St, Suite 400' -> '123 MAIN STREET' (stripping suite for base building match)
    """
    if is_placeholder_address(address):
        return ""

    # 1. Uppercase and basic cleanup
    norm = address.upper().strip()
    norm = re.sub(r"[.,'`\"]", "", norm) # Remove punctuation
    norm = re.sub(r"\s+", " ", norm)      # Collapse spaces

    # 2. Expand common suffixes for better matching
    replacements = [
        (r"\bST\b", "STREET"), (r"\bRD\b", "ROAD"), (r"\bAVE\b", "AVENUE"),
        (r"\bDR\b", "DRIVE"), (r"\bLN\b", "LANE"), (r"\bBLVD\b", "BOULEVARD"),
        (r"\bPL\b", "PLACE"),
        (r"\bCT\b(?!\s+(?:(?:UNITED STATES|USA)\s+)?\d{5}(?:-\d{4})?\b)", "COURT"),
        (r"\bTER\b", "TERRACE"),
        (r"\bP\s*O\s*BOX\b", "PO BOX"),
    ]
    for pattern, repl in replacements:
        norm = re.sub(pattern, repl, norm)

    # 3. Standardize Unit/Suite (Do NOT strip them, or we merge skyscrapers)
    # We convert common prefixes to a standard '#' symbol to catch "Suite 100" == "Ste 100"
    # But we preserve the number "100" so "Suite 100" != "Suite 200"

    # Replace variations with '#'
    norm = re.sub(r"\b(?:SUITE|STE|UNIT|APT|RM|ROOM|FL|FLOOR)\b[\.\s]*", "#", norm)

    # Ensure space before '#'
    norm = re.sub(r"([^\s])#", r"\1 #", norm)

    # Remove any double spaces created
    norm = re.sub(r"\s+", " ", norm).strip()

    # Blacklist generic placeholders
    if norm in ADDRESS_PLACEHOLDER_VALUES:
        return ""

    # Check for empty numeric placeholders like "0" or "000"
    if re.match(r'^0+$', norm):
        return ""

    # Minimum content: must contain at least one letter and be > 4 chars
    # This catches blank CSV fields like ", , , , ," which collapse to empty/short strings
    if len(norm) < 5 or not re.search(r'[A-Z]', norm):
        return ""

    # Strip zip+4 suffixes (06511-1631 -> 06511)
    norm = re.sub(r'(\d{5})-\d{4}\b', r'\1', norm)

    return norm

def normalize_mailing_address_coarse(address: str) -> str:
    """
    Building-level address normalization for network discovery.
    Strips suite/unit numbers, normalizes PO Box variations, and removes zip+4
    so that businesses at the same physical building are matched together.
    e.g. '399 Whalley Ave Suite 103, New Haven, CT 06511-1631' and
         '399 Whalley Ave, New Haven, CT 06511' both normalize to the same key.
    """
    # Start with the fine-grained normalization
    norm = normalize_mailing_address(address)
    if not norm:
        return ""

    # 1. Strip suite/unit/apt/floor designators and their numbers
    norm = re.sub(r'\s*#\s*[\w\d-]+', '', norm)

    is_po_box = bool(re.match(r'^PO BOX \d+\b', norm))

    # 3. Strip trailing zip code entirely for address matching
    norm = re.sub(r'\s+\d{5}$', '', norm)

    # 4. Strip state/country noise tokens
    for token in ['UNITED STATES', 'CONNECTICUT', 'USA', 'CT', 'COURT']:
        norm = re.sub(rf'\b{re.escape(token)}\b', '', norm)

    # 5. Strip standalone short unit numbers (from CSV field splits like ",103,")
    # Match: street name (has letters) then a space then just digits (1-4 chars) then space or end
    # e.g. "399 WHALLEY AVENUE 103 NEW HAVEN" -> "399 WHALLEY AVENUE NEW HAVEN"
    if not is_po_box:
        norm = re.sub(r'(\b[A-Z]+\b)\s+\d{1,4}\s+', r'\1 ', norm)

    # 6. Collapse whitespace and clean up
    norm = re.sub(r'\s+', ' ', norm).strip()

    # 7. Final minimum-content check: must have a street number + name (at least 5 chars with letters)
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

    # If the domain is marked as 'registrar', we ignore it entirely.
    if rule == 'registrar':
        return None

    # Default logic for undefined domains:
    # If it's a domain with multiple dots (except common ones like .co.uk),
    # or if it looks very specialized, it's likely a custom business domain.
    # For now, we return the domain to group everyone on that domain.
    return domain
