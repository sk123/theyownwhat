import requests

BASE_URL = "https://newbritain.mapxpress.net"

def check_url_param():
    session = requests.Session()
    session.verify = False
    
    # Test parid
    url_parid = f"{BASE_URL}/PAGES/detail.asp?parid=1791"
    resp1 = session.get(url_parid)
    print(f"parid=1791 -> Status {resp1.status_code}, Length {len(resp1.content)}")
    
    # Test UNIQUE_ID
    url_unique = f"{BASE_URL}/PAGES/detail.asp?UNIQUE_ID=1791"
    resp2 = session.get(url_unique)
    print(f"UNIQUE_ID=1791 -> Status {resp2.status_code}, Length {len(resp2.content)}")

if __name__ == "__main__":
    check_url_param()
