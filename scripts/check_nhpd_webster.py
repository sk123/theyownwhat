
import pandas as pd
import os

DATA_FILE = "data/Active and Inconclusive Properties.xlsx"
if not os.path.exists(DATA_FILE):
    DATA_FILE = "/app/data/Active and Inconclusive Properties.xlsx"

try:
    df = pd.read_excel(DATA_FILE)
    if 'State' in df.columns:
        df = df[df['State'] == 'CT']
    
    matches = df[df['PropertyAddress'].astype(str).str.contains("WEBSTER", case=False, na=False)]
    print(matches[['PropertyAddress', 'City', 'TotalUnits', 'NHPDPropertyID']].to_string())
except Exception as e:
    print(f"Error: {e}")
