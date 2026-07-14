
import requests
import time
import logging
import re
from difflib import SequenceMatcher

logger = logging.getLogger("geocoder-utils")

NOMINATIM_LOCAL_URL = "http://ctdata_nominatim:8080/search"
NOMINATIM_PUBLIC_URL = "https://nominatim.openstreetmap.org/search"
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
USER_AGENT = "TheyOwnWhatApp/1.0"

CT_LAT_MIN, CT_LAT_MAX = 40.8, 42.3
CT_LON_MIN, CT_LON_MAX = -73.9, -71.6
US_LAT_MIN, US_LAT_MAX = 24.0, 50.0
US_LON_MIN, US_LON_MAX = -125.0, -66.0


def is_valid_coordinate(lat, lon, state="CT"):
    """Reject sentinel, non-US, and out-of-state geocoder results."""
    try:
        if lat is None or lon is None:
            return False
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return False

    if (lat_f, lon_f) in {(0.0, 0.0), (-1.0, -1.0)}:
        return False
    if not (US_LAT_MIN <= lat_f <= US_LAT_MAX and US_LON_MIN <= lon_f <= US_LON_MAX):
        return False
    if str(state or "").upper() == "CT":
        return CT_LAT_MIN <= lat_f <= CT_LAT_MAX and CT_LON_MIN <= lon_f <= CT_LON_MAX
    return True


_STREET_SUFFIXES = {
    "ALY", "ALLEY", "AV", "AVE", "AVENUE", "BLVD", "BOULEVARD", "CIR", "CIRCLE",
    "COURT", "CT", "DR", "DRIVE", "HWY", "HIGHWAY", "LANE", "LN", "LOOP",
    "PKWY", "PARKWAY", "PL", "PLACE", "PLZ", "PLAZA", "RD", "ROAD", "ROW",
    "ST", "STREET", "TER", "TERRACE", "TRL", "TRAIL", "WAY",
}
_DIRECTION_MAP = {
    "N": "NORTH",
    "NO": "NORTH",
    "S": "SOUTH",
    "SO": "SOUTH",
    "E": "EAST",
    "W": "WEST",
}
_TOKEN_MAP = {
    **_DIRECTION_MAP,
    "RT": "ROUTE",
    "RTE": "ROUTE",
    "HWY": "HIGHWAY",
    "AV": "AVENUE",
    "LA": "LANE",
    "PT": "POINT",
    "RDG": "RIDGE",
    "VL": "VILLAGE",
    "VLG": "VILLAGE",
    "BUS": "BUSINESS",
    "GDN": "GARDEN",
    "GDNS": "GARDEN",
    "GARDENS": "GARDEN",
    "1ST": "1ST",
    "FIRST": "1ST",
    "2ND": "2ND",
    "SECOND": "2ND",
    "3RD": "3RD",
    "THIRD": "3RD",
    "4TH": "4TH",
    "FOURTH": "4TH",
    "5TH": "5TH",
    "FIFTH": "5TH",
    "6TH": "6TH",
    "SIXTH": "6TH",
    "7TH": "7TH",
    "SEVENTH": "7TH",
    "8TH": "8TH",
    "EIGHTH": "8TH",
    "9TH": "9TH",
    "NINTH": "9TH",
    "10TH": "10TH",
    "TENTH": "10TH",
}
_UNIT_TOKENS = {"APT", "APARTMENT", "BLDG", "FL", "FLOOR", "NO", "STE", "SUITE", "UNIT"}


def _street_part(address):
    parts = [part.strip().upper() for part in str(address or "").split(",") if part.strip()]
    if not parts:
        return ""
    # Nominatim often returns "10, Howe Street, New Haven..." instead of
    # "10 Howe Street, New Haven...".
    if len(parts) > 1 and re.fullmatch(r"\d+[A-Z]?(?:\s*(?:;|\+|-|/|\.|&|\s)\s*\d+[A-Z]?)*", parts[0]):
        number_parts = []
        idx = 0
        while idx < len(parts) and re.fullmatch(r"\d+[A-Z]?(?:\s*(?:;|\+|-|/|\.|&|\s)\s*\d+[A-Z]?)*", parts[idx]):
            number_parts.append(parts[idx])
            idx += 1
        if idx < len(parts):
            return f"{' '.join(number_parts)} {parts[idx]}"
        return " ".join(number_parts)
    # It can also prefix a point-of-interest name:
    # "Hospital Name, 1450, Chapel Street, New Haven...".
    if len(parts) > 2 and not re.search(r"\d", parts[0]) and re.fullmatch(r"\d+[A-Z]?", parts[1]):
        number_parts = []
        idx = 1
        while idx < len(parts) and re.fullmatch(r"\d+[A-Z]?", parts[idx]):
            number_parts.append(parts[idx])
            idx += 1
        if idx < len(parts):
            return f"{' '.join(number_parts)} {parts[idx]}"
        return " ".join(number_parts)
    return parts[0]


def _house_candidates(address):
    street = _street_part(address).lstrip("#").strip()
    prefix = re.match(r"^\s*([0-9;+\-.\s/&]+)", street)
    if not prefix:
        return set()

    prefix_text = prefix.group(1)
    # Fractions like "30 1/2 Colony" identify 30, not 1. Slash-separated
    # multi-addresses like "916/922 Shippan" should keep both candidates.
    if "/" in prefix_text:
        nums = [int(value) for value in re.findall(r"\d+", prefix_text)]
        if len(nums) > 1 and all(value >= 10 for value in nums):
            return set(nums)
        return {nums[0]} if nums else set()

    candidates = set()
    range_match = re.search(r"\b(\d+)\s*-\s*(\d+)\b", prefix_text)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        candidates.update({start, end})
        if len(range_match.group(2)) <= 2:
            candidates.add(int(f"{range_match.group(1)}{range_match.group(2).zfill(2)}"))

    for value in re.findall(r"\d+", prefix_text):
        candidates.add(int(value))
    return candidates


def _all_numbers(address):
    return {int(value) for value in re.findall(r"\d+", _street_part(address))}


def _house_range(address):
    street = _street_part(address).lstrip("#").strip()
    range_match = re.search(r"\b(\d+)\s*-\s*(\d+)\b", street)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        return min(start, end), max(start, end)
    match = re.search(r"\b(\d+)", street)
    if match:
        num = int(match.group(1))
        return num, num
    return None


def _tokenize_street(address):
    street = _street_part(address).strip()
    if street.startswith("#"):
        street = street[1:].strip()
    else:
        street = re.sub(r"#.*$", "", street)
    street = re.sub(r"[^A-Z0-9\s]", " ", street)
    raw_tokens = street.split()
    house = _house_range(address)
    house_candidates = _house_candidates(address)
    seen_street_token = False
    tokens = []
    for idx, raw in enumerate(raw_tokens):
        if raw.isdigit():
            continue
        keep_short_token = False
        if raw == "SO" and idx + 1 < len(raw_tokens) and raw_tokens[idx + 1] == "JO":
            token = raw
        elif raw == "S" and idx > 0 and idx + 1 < len(raw_tokens) and raw_tokens[idx - 1] == "RAM" and raw_tokens[idx + 1] == "GATE":
            token = raw
            keep_short_token = True
        else:
            token = _TOKEN_MAP.get(raw, raw)
        if token in _UNIT_TOKENS:
            break
        keep_single_letter_street = (
            len(token) == 1
            and idx > 0
            and _TOKEN_MAP.get(raw_tokens[idx - 1], raw_tokens[idx - 1]) in {"AVENUE", "AVE", "AV"}
        )
        if len(token) <= 1 and not keep_short_token and not keep_single_letter_street:
            continue
        tokens.append(token)
        if not raw.isdigit():
            seen_street_token = True
    return tokens


def _street_tokens(address):
    tokens = _tokenize_street(address)
    core = [token for token in tokens if token not in _STREET_SUFFIXES]
    return set(core or tokens)


def _street_token_list(address):
    tokens = _tokenize_street(address)
    core = [token for token in tokens if token not in _STREET_SUFFIXES]
    return core or tokens


def _route_numbers(address):
    street = _street_part(address)
    route_matches = re.findall(r"\b(?:STATE\s+)?(?:ROUTE|RTE|RT|HWY|HIGHWAY)\s+(\d+)\b", street)
    return {int(value) for value in route_matches}


def _house_numbers_compatible(input_address, matched_address):
    input_range = _house_range(input_address)
    matched_range = _house_range(matched_address)
    input_candidates = _house_candidates(input_address)
    matched_candidates = _house_candidates(matched_address)
    input_all_numbers = _all_numbers(input_address)
    matched_all_numbers = _all_numbers(matched_address)

    if not input_range or not matched_range:
        return True
    if input_candidates and matched_candidates and input_candidates & matched_candidates:
        return True
    if input_all_numbers and matched_candidates and input_all_numbers & matched_candidates:
        return True
    if matched_all_numbers and input_candidates and matched_all_numbers & input_candidates:
        return True

    matched_start, matched_end = matched_range
    input_start, input_end = input_range
    if input_start <= matched_start <= input_end or input_start <= matched_end <= input_end:
        return True
    return any(matched_start <= value <= matched_end for value in input_candidates)


def _street_names_overlap(input_address, matched_address):
    input_list = _street_token_list(input_address)
    matched_list = _street_token_list(matched_address)
    input_tokens = set(input_list)
    matched_tokens = set(matched_list)
    if not input_tokens or not matched_tokens:
        return False
    if input_tokens & matched_tokens:
        return True

    input_join = "".join(input_list)
    matched_join = "".join(matched_list)
    if input_join and matched_join and (input_join == matched_join or input_join in matched_join or matched_join in input_join):
        return True

    input_routes = _route_numbers(input_address)
    matched_routes = _route_numbers(matched_address)
    if input_routes and matched_routes and input_routes & matched_routes:
        return True

    return any(
        len(a) >= 5 and len(b) >= 5 and SequenceMatcher(None, a, b).ratio() >= 0.82
        for a in input_tokens
        for b in matched_tokens
    )


def is_geocode_match_credible(input_address, matched_address):
    """Require the geocoder's normalized street to resemble the requested street."""
    if not input_address or not matched_address:
        return False

    if not _house_numbers_compatible(input_address, matched_address):
        return False

    return _street_names_overlap(input_address, matched_address)

def geocode_census(address):
    """Primary: US Census Bureau (Fast, Parallel, Free)"""
    try:
        params = {
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json"
        }
        resp = requests.get(CENSUS_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        matches = data.get("result", {}).get("addressMatches", [])
        if matches:
            coords = matches[0].get("coordinates", {})
            return float(coords.get("y")), float(coords.get("x")), matches[0].get("matchedAddress") # Lat, Lon, Norm
            
        return None, None, None
    except Exception as e:
        logger.warning(f"Census User Geocode Error: {e}")
        return None, None, None

def geocode_nominatim(address):
    """Secondary: Nominatim (Slow, Rate Limited)"""
    # 1. Try Local First
    try:
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": USER_AGENT}
        
        resp = requests.get(NOMINATIM_LOCAL_URL, params=params, headers=headers, timeout=2)
        resp.raise_for_status()
        data = resp.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon']), data[0].get('display_name')
    except Exception:
        # 2. Fallback to Public with Strict Rate Limiting
        try:
            time.sleep(1.1) # Nominatim policy: 1 request/s
            resp = requests.get(NOMINATIM_PUBLIC_URL, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data:
                return float(data[0]['lat']), float(data[0]['lon']), data[0].get('display_name')
        except Exception as e:
            logger.error(f"Nominatim Geocoding error for {address}: {e}")
            
    return None, None, None
