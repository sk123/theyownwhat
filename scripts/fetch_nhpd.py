import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

NHPD_USER = os.getenv("NHPD_USER")
NHPD_PASS = os.getenv("NHPD_PASS")
DOWNLOAD_DIR = "/app/data"
OUTPUT_FILE = os.path.join(DOWNLOAD_DIR, "nhpd_ct.xlsx")

def fetch_nhpd_data():
    if not NHPD_USER or not NHPD_PASS:
        print("❌ NHPD_USER or NHPD_PASS missing in .env")
        return

    login_url = "https://nhpd.preservationdatabase.org/Account/Login"
    download_post_url = "https://nhpd.preservationdatabase.org/Report/Download"
    
    # We use a session to maintain cookies
    session = requests.Session()
    
    print(f"🚀 Logging in to NHPD as {NHPD_USER}...")
    
    # Get the login page to retrieve any CSRF tokens if necessary (common in ASP.NET)
    response = session.get(login_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the verification token (ASP.NET Anti-Forgery Token)
    verification_token = soup.find('input', {'name': '__RequestVerificationToken'})
    
    login_data = {
        'Email': NHPD_USER,
        'Password': NHPD_PASS,
        'RememberMe': 'false'
    }
    
    if verification_token:
        login_data['__RequestVerificationToken'] = verification_token['value']
        print("✅ Anti-Forgery Token found.")

    # Log in
    login_response = session.post(login_url, data=login_data)
    
    if "Log Off" not in login_response.text and "Main" not in login_response.url:
        print("❌ Login failed. Check credentials.")
        # print(login_response.text[:500]) # Log first part of response for debugging
        return
    
    print("✅ Login successful.")

    # The download logic identified by the browser agent:
    # POST to /Report/Download with downloadFileId=2157 (Active and Inconclusive Properties CT)
    # The button click sends this ID.
    
    print("🚚 Requesting CT report (ID: 2157)...")
    
    download_data = {
        'downloadFileId': '2157' # Inconclusive and Active Properties CT
    }
    
    # Note: We need to handle the download as a stream
    try:
        download_response = session.post(download_post_url, data=download_data, stream=True)
        download_response.raise_for_status()
        
        # Check if we actually got a file and not an error page
        content_type = download_response.headers.get('Content-Type', '')
        if 'html' in content_type:
             print("❌ Received HTML instead of a file. The download request might have failed or session expired.")
             return

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        with open(OUTPUT_FILE, 'wb') as f:
            for chunk in download_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"✅ Successfully downloaded NHPD data to {OUTPUT_FILE}")
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error during download: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    fetch_nhpd_data()
