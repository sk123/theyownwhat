
import requests

URL = "https://www.actdatascout.com/RealProperty/Connecticut/Norwalk"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def test():
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=10)
        print(f"Status: {resp.status_code}")
        print("Cookies:", resp.cookies.get_dict())
        if resp.status_code == 200:
            # print(resp.text[:500])
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'html.parser')
            print("Hidden Inputs:")
            for inp in soup.find_all('input', type='hidden'):
                print(f"  {inp.get('name', inp.get('id'))} = {inp.get('value')}")
        else:
            print("Failed.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
