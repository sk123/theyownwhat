import os
import argparse
import psycopg2
import logging
from dotenv import load_dotenv

# --- Configuration & Setup ---
# Load environment variables from a .env file if it exists
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def clear_cache(scope: str):
    """
    Connects to the database and clears the report_cache table based on the scope.

    Args:
        scope (str): Determines which reports to clear.
                     'today' clears reports created on the current day.
                     'all' clears the entire cache.
    """
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable not set. Please configure it.")
        return

    conn = None
    try:
        logger.info(f"Connecting to the database...")
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cursor:
            if scope == 'today':
                logger.info("🔥 Clearing cached reports created today...")
                # The WHERE clause filters for records where the 'created_at' timestamp
                # is on or after the beginning of the current day.
                cursor.execute("DELETE FROM report_cache WHERE created_at >= current_date;")
                deleted_count = cursor.rowcount
                logger.info(f"✅ Successfully deleted {deleted_count} of today's reports.")

            elif scope == 'all':
                logger.warning("🔥 Clearing ALL cached reports...")
                # TRUNCATE is faster than DELETE for clearing an entire table.
                cursor.execute("TRUNCATE TABLE report_cache;")
                logger.info("✅ Successfully cleared all reports from the cache.")
            
            conn.commit()

    except psycopg2.Error as e:
        logger.error(f"❌ Database operation failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

def main():
    """
    Parses command-line arguments and calls the clear_cache function.
    """
    parser = argparse.ArgumentParser(
        description="Utility to clear cached AI reports from the database.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        'scope',
        choices=['today', 'all'],
        help="""Specify the scope of the cache clearing operation:
'today' - Deletes only the reports generated today.
'all'   - Deletes all reports in the cache."""
    )

    args = parser.parse_args()
    
    # Add a confirmation step for the 'all' option to prevent accidental data loss.
    if args.scope == 'all':
        confirm = input("Are you sure you want to delete ALL cached reports? This cannot be undone. (yes/no): ")
        if confirm.lower() != 'yes':
            logger.info("Operation cancelled by user.")
            return
            
    clear_cache(args.scope)

if __name__ == "__main__":
    main()
