# shared_utils.py
import re
from typing import Set

def normalize_business_name(name: str) -> str:
    """Canonical function to normalize business names for matching."""
    if not name: return ''
    normalized = name.upper().strip()
    normalized = re.sub(r"[,.'`\"]", '', normalized)
    normalized = normalized.replace('&', 'AND')
    # --- CHANGE: Kept hyphens as they are common in business names ---
    normalized = re.sub(r"[^A-Z0-9\s-]", '', normalized) 
    normalized = re.sub(r"\s+", ' ', normalized).strip()
    return normalized

def normalize_person_name(name: str) -> str:
    """Canonical function to normalize person names for matching."""
    if not name: return ''
    normalized = name.upper().strip()
    
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

    # 2. Generalizable Cleaning
    # Remove middle initials: "MENACHEM M GUREVITCH" -> "MENACHEM GUREVITCH"
    parts = normalized.split()
    if len(parts) > 2:
        new_parts = []
        for i, part in enumerate(parts):
            clean_part = re.sub(r"\.", "", part)
            if len(clean_part) == 1 and 0 < i < len(parts) - 1:
                continue
            new_parts.append(part)
        normalized = " ".join(new_parts)

    # Standard punctuation and whitespace cleanup
    normalized = re.sub(r"[,.'`\"]", '', normalized)
    normalized = re.sub(r"\s+", ' ', normalized).strip()
    
    # Remove standard suffixes
    for pattern in PERSON_SUFFIX_PATTERNS:
        normalized = pattern.sub('', normalized)
        
    return normalized.strip()

def canonicalize_person_name(name: str) -> str:
    """
    Creates a word-sorted version of a name to treat "LAST FIRST" 
    the same as "FIRST LAST" during network building/graph matching.
    """
    norm = normalize_person_name(name)
    if not norm: return ""
    parts = sorted(norm.split())
    return " ".join(parts)

# List of suffixes to remove, kept outside the function for clarity
BUSINESS_SUFFIXES_TO_REMOVE = [
    'LIMITED LIABILITY COMPANY', 'LIMITED LIABILITY PARTNERSHIP', 
    'PROFESSIONAL LIMITED LIABILITY COMPANY', 'LIMITED PARTNERSHIP',
    'INCORPORATED', 'CORPORATION', 'MANAGEMENT', 'PROPERTIES', 
    'INVESTMENTS', 'PARTNERSHIP', 'COMPANY', 'LIMITED', 'PARTNERS',
    'REALTY', 'GROUP', 'TRUST', 'ESTATE', 'HOLDINGS', 'VENTURES',
    'ENTERPRISES', 'SERVICES', 'DEVELOPMENT', 'REAL ESTATE',
    'L L C', 'L L P', 'L P', 'LLC', 'LLP', 'LTD', 'INC', 'CORP', 'LP', 'CO'
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

    variations = {name.upper()}
    
    if entity_type == 'business':
        # --- NEW LOGIC: Iteratively strip suffixes to find more potential matches ---
        current_name = normalize_business_name(name)
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

def normalize_mailing_address(address: str) -> str:
    """
    Standardizes a mailing address to detect hidden links.
    e.g. '123 Main St, Suite 400' -> '123 MAIN STREET' (stripping suite for base building match)
    """
    if not address: return ""
    
    # 1. Uppercase and basic cleanup
    norm = address.upper().strip()
    norm = re.sub(r"[.,'`\"]", "", norm) # Remove punctuation
    norm = re.sub(r"\s+", " ", norm)      # Collapse spaces
    
    # 2. Expand common suffixes for better matching
    replacements = [
        (r"\bST\b", "STREET"), (r"\bRD\b", "ROAD"), (r"\bAVE\b", "AVENUE"),
        (r"\bDR\b", "DRIVE"), (r"\bLN\b", "LANE"), (r"\bBLVD\b", "BOULEVARD"),
        (r"\bPL\b", "PLACE"), (r"\bCT\b", "COURT"), (r"\bTER\b", "TERRACE"),
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

    return norm

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