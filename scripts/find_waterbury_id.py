
import requests
from bs4 import BeautifulSoup
import re

# Modified to search by ID/MBL
def find_waterbury_id(test_id):
    url = "https://www.propertyrecordcards.com/SearchMaster.aspx?towncode=151"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    s = requests.Session()
    resp = s.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'lxml')
    
    viewstate = soup.find(id="__VIEWSTATE")['value']
    viewstate_gen = soup.find(id="__VIEWSTATEGENERATOR")['value']
    event_validation = soup.find(id="__EVENTVALIDATION")['value']
    
    # Test Name field
    print(f"Testing Name: {test_id}")
    payload = {
        '__VIEWSTATE': viewstate,
        '__VIEWSTATEGENERATOR': viewstate_gen,
        '__EVENTVALIDATION': event_validation,
        'ctl00$MainContent$tbPropertySearchName': test_id,
        'ctl00$MainContent$btnPropertySearch': 'Search'
    }
    resp = s.post(url, data=payload, headers=headers)
    with open("waterbury_debug.html", "w") as f:
        f.write(resp.text)

    if "propertyresults.aspx" in resp.text:
        print(f"SUCCESS MATCH for Name: {test_id}")
        result_links = re.findall(r'propertyresults.aspx\?towncode=151&uniqueid=[^"]+', resp.text)
        for link in result_links:
            print(f"FOUND LINK: {link}")
        return
        
    print(f"Failed to find match for {test_id}. Check waterbury_debug.html")


if __name__ == "__main__":
    find_waterbury_id("FILIPPONE")
