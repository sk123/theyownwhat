
import requests
import time
import logging

logger = logging.getLogger("geocoder-utils")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
USER_AGENT = "TheyOwnWhatApp/1.0"
RATE_LIMIT_DELAY = 1.1

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
            return float(coords.get("y")), float(coords.get("x")) # Lat, Lon
            
        return None, None
    except Exception as e:
        logger.warning(f"Census User Geocode Error: {e}")
        return None, None

def geocode_nominatim(address):
    """Secondary: Nominatim (Slow, Rate Limited, Rate Limited)"""
    try:
        # Strict rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }
        headers = {"User-Agent": USER_AGENT}
        
        resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        
        data = resp.json()
        if data and len(data) > 0:
            return float(data[0]['lat']), float(data[0]['lon'])
        
        return None, None
    except Exception as e:
        logger.error(f"Nominatim Geocoding error for {address}: {e}")
        return None, None
