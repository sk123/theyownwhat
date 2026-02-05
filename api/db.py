import os
import time
import psycopg2
from psycopg2 import pool
from fastapi import HTTPException
from typing import Optional
import logging

logger = logging.getLogger("they-own-what")

# Global DB Pool
db_pool: Optional[pool.SimpleConnectionPool] = None

def init_db_pool():
    global db_pool
    if db_pool is None:
        DATABASE_URL = os.environ.get("DATABASE_URL")
        retries = 60
        while retries > 0:
            try:
                db_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 40,
                    dsn=DATABASE_URL
                )
                logger.info("Database connection pool created successfully.")
                break
            except psycopg2.OperationalError as e:
                retries -= 1
                logger.warning(f"DB not ready; retrying... ({retries} left). Error: {e}")
                time.sleep(2)
        
        if db_pool is None:
            # Fatal error if we can't connect
            raise Exception("Could not connect to DB after retries.")

def get_db_connection():
    if db_pool is None:
        init_db_pool()
        
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database connection unavailable")
        
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)
