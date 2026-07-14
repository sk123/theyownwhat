import pandas as pd

src = "/run/user/1000/.flatpak/com.google.Chrome/tmp/playwright-artifacts-pEzMW6/857422a8-025d-496c-9cd9-9693043ae9e0"
dst = "/home/sk/dev/theyownwhat/data/dc_violations.csv"

try:
    df = pd.read_csv(src, sep='\t', encoding='utf-16')
    print("Columns:", list(df.columns))
    print("Shape:", df.shape)
    print("First 5 rows:")
    print(df.head(5))
    df.to_csv(dst, index=False, encoding='utf-8')
    print("Saved to", dst)
except Exception as e:
    print("Error:", e)
