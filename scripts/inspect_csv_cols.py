import pandas as pd
try:
    df = pd.read_csv('/tmp/ct_geodata.csv', nrows=1)
    print("COLUMNS:", df.columns.tolist())
except Exception as e:
    print("ERROR:", e)
