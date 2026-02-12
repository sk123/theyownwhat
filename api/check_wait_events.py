import psycopg2, os
try:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute("SELECT pid, state, wait_event_type, wait_event, query_start, query FROM pg_stat_activity WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%'")
    rows = cur.fetchall()
    print(f"Active Queries: {len(rows)}")
    for r in rows:
        print(r)
except Exception as e:
    print(f"Error: {e}")
