
import requests
from bs4 import BeautifulSoup
import re

def debug_hartford(acc_num):
    session = requests.Session()
    base_url = "http://assessor1.hartford.gov"
    
    # Init session
    session.get(f"{base_url}/search.asp")
    
    # Search to set context (using account number search if possible)
    # Actually, hartford_enrichment.py uses SearchParcel. Let's try that.
    # Parcel ID: 135-384-211 for 25458?
    
    # Search by Account Number
    payload = {
        "SearchAccountNumber": acc_num,
        "SearchSubmitted": "yes",
        "cmdGo": "Go"
    }
    session.post(f"{base_url}/SearchResults.asp", data=payload)
    
    resp = session.get(f"{base_url}/summary-bottom.asp?AccountNumber={acc_num}")
    
    soup = BeautifulSoup(resp.text, 'lxml')
    print(soup.prettify()[:2000])

if __name__ == "__main__":
    debug_hartford("25458")
