
import requests
import time
import logging

logger = logging.getLogger("geocoder-utils")

NOMINATIM_LOCAL_URL = "http://localhost:8080/search"
NOMINATIM_PUBLIC_URL = "https://nominatim.openstreetmap.org/search"
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
USER_AGENT = "TheyOwnWhatApp/1.0"

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
