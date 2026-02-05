
import sys
import os
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

# Mock dependencies
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.pool'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()
sys.modules['fastapi'] = MagicMock()
sys.modules['fastapi.middleware.cors'] = MagicMock()
sys.modules['fastapi.responses'] = MagicMock()
sys.modules['fastapi.staticfiles'] = MagicMock()
sys.modules['api.shared_utils'] = MagicMock()
sys.modules['api.geocoding_utils'] = MagicMock()

# Add project root to path
sys.path.append('/home/sk/dev/theyownwhat')
# Mock os.makedirs to prevent PermissionError
os.makedirs = MagicMock()

# Import the function to test
from api.main import get_completeness_report

# Mock the database connection and cursor
mock_conn = MagicMock()
mock_cursor = MagicMock()
mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

# Helper to run the test
def test_completeness_report_cache():
    print("Testing cached report with timezone-aware datetime...")
    
    # Setup mock return value for cache hit
    # Use a time from 10 minutes ago, ensuring it is timezone aware (as DB would return)
    created_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    mock_cursor.fetchone.return_value = {
        'value': {'status': 'ok'},
        'created_at': created_at
    }
    
    try:
        result = get_completeness_report(mock_conn)
        print("Success! Result:", result)
    except Exception as e:
        print(f"FAILED: {e}")
        # traceback
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_completeness_report_cache()
