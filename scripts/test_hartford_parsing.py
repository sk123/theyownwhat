
import requests
import re
from bs4 import BeautifulSoup
import time

def test_hartford_parsing(acc_num):
    session = requests.Session()
    session.get("http://assessor1.hartford.gov/search.asp")
    
    # Use account number search directly if possible?
    # Actually, hartford_enrichment uses SearchParcel.
    # Let's try SearchParcel with '248-557-141'
    payload = {
        "SearchParcel": "248-557-141",
        "SearchSubmitted": "yes",
        "cmdGo": "Go"
    }
    session.post("http://assessor1.hartford.gov/SearchResults.asp", data=payload)
    
    res = session.get(f"http://assessor1.hartford.gov/summary-bottom.asp?AccountNumber={acc_num}")
    soup = BeautifulSoup(res.text, 'lxml')
    
    # Try to find narrative
    narrative = soup.find(string=re.compile("This property contains", re.IGNORECASE))
    if narrative:
        print("FOUND NARRATIVE")
        parent = narrative.find_parent()
        text = parent.get_text(" ", strip=True)
        print(f"TEXT: {text}")
        
        yb_match = re.search(r'built about\s+(\d{4})', text, re.IGNORECASE)
        if yb_match: print(f"YEAR BUILT: {yb_match.group(1)}")
        
        rooms_match = re.search(r'(\d+)\s+total room', text, re.IGNORECASE)
        if rooms_match: print(f"ROOMS: {rooms_match.group(1)}")

        beds_match = re.search(r'(\d+)\s+total bedroom', text, re.IGNORECASE)
        if beds_match: print(f"BEDS: {beds_match.group(1)}")
        
        baths_match = re.search(r'(\d+)\s+total bath', text, re.IGNORECASE)
        if baths_match: print(f"BATHS: {baths_match.group(1)}")
    else:
        print("NARRATIVE NOT FOUND")
        # Print first bits of soup
        print(soup.prettify()[:1000])

if __name__ == "__main__":
    test_hartford_parsing("25458")
