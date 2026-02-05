import sqlite3

try:
    conn = sqlite3.connect('api/properties.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables found:", [t[0] for t in tables])
    conn.close()
except Exception as e:
    print(e)
