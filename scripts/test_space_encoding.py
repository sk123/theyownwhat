
import requests
from bs4 import BeautifulSoup

def test():
    # URL with literal space
    url = "https://gis.vgsi.com/winchesterct/Streets.aspx?Name=MAIN ST"
    print(f"Fetching: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        print(f"Status Code: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        selector = "a[href*='.aspx?pid='], a[href*='.aspx?acct=']"
        prop_links = soup.select(selector)
        
        print(f"Found {len(prop_links)} property links.")
        
        if len(prop_links) == 0:
            print("Title of page:", soup.title.string.strip() if soup.title else "No Title")
            # Check if it's the street list page
            if "Street Listing" in response.text:
                print("Content seems to be Street Listing page.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
