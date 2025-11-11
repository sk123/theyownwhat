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
    normalized = re.sub(r"[,.'`\"]", '', normalized)
    normalized = re.sub(r"\s+", ' ', normalized).strip()
    person_suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS']
    for suffix in person_suffixes:
        pattern = re.compile(r"\s+" + re.escape(suffix) + r"$")
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
            for suffix in BUSINESS_SUFFIXES_TO_REMOVE:
                pattern = re.compile(r"\s+" + re.escape(suffix) + r"$")
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