import os
import sys
import psycopg2
from api.db import get_db_connection

def check_locks():
    # get_db_connection is a generator
    gen = get_db_connection()
    conn = next(gen)
    cur = conn.cursor()
    try:
        print("--- Active Queries ---")
        cur.execute("""
            SELECT pid, state, age(clock_timestamp(), query_start) as duration, query 
            FROM pg_stat_activity 
            WHERE state != 'idle' AND pid != pg_backend_pid()
            ORDER BY duration DESC;
        """)
        for row in cur.fetchall():
            print(f"PID: {row[0]}, State: {row[1]}, Duration: {row[2]}")
            print(f"Query: {row[3]}\n")

        print("\n--- Blocking Queries ---")
        cur.execute("""
            SELECT blocked_locks.pid AS blocked_pid,
                   blocked_activity.usename AS blocked_user,
                   blocking_locks.pid AS blocking_pid,
                   blocking_activity.usename AS blocking_user,
                   blocked_activity.query AS blocked_statement,
                   blocking_activity.query AS blocking_statement
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
            WHERE NOT blocked_locks.granted;
        """)
        blocks = cur.fetchall()
        if not blocks:
            print("No blocking queries found.")
        else:
            for row in blocks:
                print(f"Blocked PID: {row[0]} (User: {row[1]}) blocked by PID: {row[2]} (User: {row[3]})")
                print(f"Blocked Query: {row[4]}")
                print(f"Blocking Query: {row[5]}")

    except Exception as e:
        print(f"Error checking locks: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_locks()
