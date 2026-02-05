import requests
import unittest
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_headers(url):
    """
    Checks the Last-Modified header of a URL.
    Returns datetime object or None.
    """
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        # response.raise_for_status() # Some sites might return 405 Method Not Allowed for HEAD, try GET?
        
        if response.status_code == 405:
            response = requests.get(url, stream=True, timeout=10)
            response.close() # Close immediately
            
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            return parsedate_to_datetime(last_modified)
            
        # Fallback: ETag?
        etag = response.headers.get('ETag')
        if etag:
            # Etag doesn't give us a date, but it gives us a Change Signal.
            # For this script we want a Date if possible, or we just return "Unknown"
            return None 
            
    except Exception as e:
        logger.warning(f"Failed to check headers for {url}: {e}")
        return None
    
    return None

class TestQuickCheck(unittest.TestCase):
    def test_ct_geodata(self):
        # Known stable URL
        url = "https://geodata.ct.gov/api/download/v1/items/82a733423a244c43a9d4bf552954cea9/csv?layers=0"
        dt = check_headers(url)
        print(f"CT Geodata Last Modified: {dt}")
        self.assertIsNotNone(dt) # Usually this server behaves well
        
    def test_vision_appraisal_landing(self):
        # Vision usually doesn't give Last-Modified on the dynamic ASPX pages.
        # But maybe static assets?
        url = "https://gis.vgsi.com/hamdenct/"
        dt = check_headers(url)
        print(f"Vision Hamden Last Modified: {dt}")
        # Expecting None likely, but good to verify
        
if __name__ == '__main__':
    unittest.main()
