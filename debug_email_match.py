import psycopg2
import os
from api.shared_utils import get_email_match_key
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def test_email_matching():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # 1. Fetch rules
    cursor.execute("SELECT domain, match_type FROM email_match_rules")
    email_rules = {row['domain']: row['match_type'] for row in cursor}
    print(f"Loaded {len(email_rules)} rules.")
    if 'cscinfo.com' in email_rules:
        print(f"DEBUG: cscinfo.com rule = '{email_rules['cscinfo.com']}'")
    else:
        print("DEBUG: cscinfo.com NOT found in rules!")

    # 2. Fetch problematic emails
    target_emails = [
        'Compliancemail@cscinfo.com', 
        'annualreports@cscglobal.com',
        'tom@dinardoent.com' # Control (maybe valid)
    ]
    
    print("\n--- Testing Specific Emails ---")
    for email in target_emails:
        key = get_email_match_key(email, email_rules)
        print(f"Email: '{email}' -> Key: '{key}' (Should be None for CSC)")

    # 3. Fetch from DB to check for hidden chars
    print("\n--- Testing DB Rows ---")
    cursor.execute("SELECT business_email_address FROM businesses WHERE business_email_address ILIKE '%cscinfo.com%' LIMIT 5")
    for row in cursor:
        raw_email = row['business_email_address']
        key = get_email_match_key(raw_email, email_rules)
        print(f"Row: '{raw_email}' -> Key: '{key}'")
        if key:
            print(f"  -> FAILURE! Domain extracted: '{raw_email.split('@')[1] if '@' in raw_email else 'N/A'}'")
            print(f"  -> Rule lookup: {email_rules.get(raw_email.split('@')[1].lower().strip())}")

if __name__ == "__main__":
    test_email_matching()
