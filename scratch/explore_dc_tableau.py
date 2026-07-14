from tableauscraper import TableauScraper as TS
import sys

url = "https://dataviz1.dc.gov/t/OCTO/views/DOBPublicDashboard/ViolationsAbatementLVT?%3AshowAppBanner=false&%3Adisplay_count=n&%3AshowVizHome=n&%3Aorigin=viz_share_link&%3Aembed=yes&%3Atoolbar=no"
ts = TS()
try:
    ts.loads(url)
except Exception as e:
    print(f"Error loading: {e}", file=sys.stderr)
    sys.exit(1)

dashboard = ts.getDashboard()
print("Worksheets available:")
for t in dashboard.worksheets:
    print(f"- {t.name}")
    try:
        df = t.data
        print(f"  Shape: {df.shape}")
        if not df.empty:
            print("  Columns:", list(df.columns))
            print("  First 3 rows:")
            print(df.head(3))
    except Exception as ex:
        print(f"  Error getting data: {ex}")
