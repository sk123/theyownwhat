import requests

url = "https://dataviz1.dc.gov/t/OCTO/views/DOBPublicDashboard/ViolationsAbatementLVT?%3AshowAppBanner=false&%3Adisplay_count=n&%3AshowVizHome=n&%3Aorigin=viz_share_link&%3Aembed=yes&%3Atoolbar=no"
session = requests.Session()
r = session.get(url)
print("Status:", r.status_code)
print("History:", r.history)
print("Response headers:", r.headers)
print("Response start:")
print(r.text[:500])
