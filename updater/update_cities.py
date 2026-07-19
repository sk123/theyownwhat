import os
import sys
import logging
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from api.populate_cities import populate_city, get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CITIES = ["dc", "baltimore", "boston", "detroit", "philadelphia", "chicago", "miami", "minneapolis"]

def run_update(full=False):
    if full:
        logger.info("Starting FULL multi-city property ingestion & network rebuilding (D.C., Baltimore, Boston, Detroit, Philadelphia, Chicago, Miami, Minneapolis)")
        try:
            conn = get_connection()
            try:
                for city in CITIES:
                    populate_city(conn, city)
                logger.info("✓ Full multi-city sync and network rebuild completed successfully.")
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error during full multi-city update: {e}")
    else:
        logger.info("Skipping daily multi-city update: no real incremental source is configured.")
        logger.info("Run with --full to rebuild D.C., Baltimore, and Boston from public source data.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-city property update runner")
    parser.add_argument("--full", action="store_true", help="Perform full ingestion and network rebuild")
    args = parser.parse_args()
    run_update(full=args.full)
