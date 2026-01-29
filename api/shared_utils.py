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
    
    # 1. Contextual typo corrections for the Gurevitch network
    # We only apply these if the name contains GUREVITCH (or a common typo of it)
    is_gurevitch = any(x in normalized for x in ['GUREVITCH', 'GURAVITCH', 'GUREVICH', 'GUREVITH', 'GUTVITCH'])
    
    if is_gurevitch:
        g_typos = {
            'MENACHERM': 'MENACHEM',
            'MENAHEM': 'MENACHEM',
            'MENACHER': 'MENACHEM',
            'GURAVITCH': 'GUREVITCH',
            'GUREVICH': 'GUREVITCH', 
            'GUREVITH': 'GUREVITCH',
            'GUTVITCH': 'GUREVITCH',
        }
        for typo, correction in g_typos.items():
            normalized = re.sub(rf"\b{typo}\b", correction, normalized)

    # 2. Generalizable Cleaning (Apply to ALL)
    
    # Remove middle initials: "MENACHEM M GUREVITCH" -> "MENACHEM GUREVITCH"
    parts = normalized.split()
    if len(parts) > 2:
        new_parts = []
        for i, part in enumerate(parts):
            # If it's a single letter (with or without a dot) and not the first or last word
            # e.g. "M." or "M"
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
        # Normalizing the name removes commas and standardizes spacing
        normalized = normalize_person_name(name)
        variations.add(normalized)
        
        parts = normalized.split()
        
        # --- NEW: More robust permutation logic ---
        # If a name has at least two parts, generate both possible orders
        if len(parts) >= 2:
            # Assumes the first part is FIRST and last part is LAST
            first_last_order = f"{parts[0]} {' '.join(parts[1:])}"
            variations.add(first_last_order)
            
            # Assumes the first part is LAST and rest is FIRST/MIDDLE
            last_first_order = f"{' '.join(parts[1:])} {parts[0]}"
            variations.add(last_first_order)

            # Also create common "LAST, F" variation from both interpretations
            variations.add(f"{parts[-1]}, {parts[0][0]}") # FIRST LAST -> LAST, F
            variations.add(f"{parts[0]}, {parts[1][0]}") # LAST FIRST -> LAST, F

    return {v for v in variations if v} # Return a cleaned set of non-empty variations

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
    
    # Default behavior for unknown:
    # If the user provided a robust list of "public" providers, we should check it.
    # Logic: If domain is in rules as 'public', exact match.
    # If domain is NOT in rules, assume custom? Or assume public?
    # User said: "if the email is gmail... match using full email address. if custom domain... match domain".
    
    # We will assume the caller passes a rules dict populated with public domains marked as 'public'
    if rule == 'public':
        return email
    
    # If not in rules at all, assume it might be custom?
    # Or safest: use full email unless explicitly known as custom.
    # But user implied: "if custom... match domain".
    # We need a list of public domains.
    # If we don't have it, we default to full email match to be safe.
    
    return email 