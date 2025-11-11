#!/usr/bin/env python3
"""
analyze_unlinked_properties_fast.py

Analyzes properties not linked to any business networks by performing a high-performance,
database-native fuzzy string match against the businesses table.

*** PREREQUISITES ***
This script requires the pg_trgm PostgreSQL extension to be enabled and GIN indexes
to be created on the relevant columns. Run the following commands on your database once:

1. CREATE EXTENSION IF NOT EXISTS pg_trgm;
2. CREATE INDEX IF NOT EXISTS idx_gin_properties_owner ON properties USING gin (owner gin_trgm_ops);
3. CREATE INDEX IF NOT EXISTS idx_gin_businesses_name ON businesses USING gin (name gin_trgm_ops);
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import argparse
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('unlinked_business_analysis_fast.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        raise

def find_unlinked_business_properties(conn, min_similarity=0.3, limit=None):
    """
    Finds unlinked properties with potential business matches using a database-native
    fuzzy search with pg_trgm.
    """
    logger.info(f"Finding unlinked properties with similarity >= {min_similarity}...")

    # This single query replaces the entire N x M loop from the original script.
    # It uses a window function to rank matches and get the top 3 for each property.
    query = """
        WITH ranked_matches AS (
            SELECT
                p.id AS property_id,
                p.owner,
                p.location,
                p.property_city,
                p.assessed_value,
                p.sale_date,
                b.id AS business_id,
                b.name AS business_name,
                similarity(p.owner, b.name) AS similarity,
                -- Rank matches for each property by similarity score
                ROW_NUMBER() OVER(PARTITION BY p.id ORDER BY similarity(p.owner, b.name) DESC) as rn
            FROM
                properties p
            JOIN businesses b ON p.owner %% b.name
            WHERE
                p.network_id IS NULL
                AND p.owner IS NOT NULL AND p.owner != ''
                AND b.name IS NOT NULL AND b.name != ''
                -- Further filter results by the similarity threshold
                AND similarity(p.owner, b.name) >= %(min_similarity)s
        )
        SELECT *
        FROM ranked_matches
        WHERE rn <= 3 -- Get only the top 3 matches per property
        ORDER BY property_id, similarity DESC
    """
    
    params = {'min_similarity': min_similarity}

    if limit:
        query += " LIMIT %(limit)s"
        params['limit'] = limit

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, params)  # This line will now work
        results = cursor.fetchall()

    logger.info(f"Query returned {len(results)} potential property-business matches.")

    # --- Group the flat results into the nested structure for output ---
    grouped_properties = defaultdict(lambda: {'property': None, 'matches': []})

    for row in results:
        prop_id = row['property_id']
        
        # Store property details only once
        if not grouped_properties[prop_id]['property']:
            grouped_properties[prop_id]['property'] = {
                'id': prop_id,
                'owner': row['owner'],
                'location': row['location'],
                'property_city': row['property_city'],
                'assessed_value': row['assessed_value'],
                'sale_date': row['sale_date']
            }
        
        # Append all matches
        grouped_properties[prop_id]['matches'].append({
            'business_id': row['business_id'],
            'business_name': row['business_name'],
            'similarity': row['similarity']
        })
    
    # Convert defaultdict to a simple list for the output function
    final_list = list(grouped_properties.values())
    logger.info(f"Found {len(final_list)} unique unlinked properties with potential matches.")
    return final_list

def output_analysis_results(business_properties, output_file=None):
    """
    Output the analysis results to both log and optionally a file.
    (This function is identical to the one in the original script)
    """
    logger.info("="*80)
    logger.info("UNLINKED BUSINESS PROPERTIES ANALYSIS RESULTS")
    logger.info("="*80)

    if not business_properties:
        logger.info("No unlinked business properties with potential matches found.")
        return

    # Group by similarity ranges for summary
    high_similarity = [bp for bp in business_properties if bp['matches'][0]['similarity'] >= 0.7]
    medium_similarity = [bp for bp in business_properties if 0.5 <= bp['matches'][0]['similarity'] < 0.7]
    low_similarity = [bp for bp in business_properties if bp['matches'][0]['similarity'] < 0.5]

    logger.info("SUMMARY:")
    logger.info(f"  High similarity matches (≥0.7): {len(high_similarity)}")
    logger.info(f"  Medium similarity matches (0.5-0.69): {len(medium_similarity)}")
    logger.info(f"  Low similarity matches (<0.5): {len(low_similarity)}")
    logger.info(f"  Total unique properties found: {len(business_properties)}")
    logger.info("")

    output_lines = []
    for i, bp in enumerate(business_properties, 1):
        prop = bp['property']
        matches = bp['matches']

        lines = [
            f"Property #{i}:",
            f"  Property ID: {prop['id']}",
            f"  Owner: {prop['owner']}",
            f"  Location: {prop['location'] or 'N/A'}",
            f"  Assessed Value: ${prop['assessed_value']:,.2f}" if prop['assessed_value'] else "  Assessed Value: N/A",
            "  Potential Business Matches:"
        ]

        for j, match in enumerate(matches, 1):
            lines.append(f"    {j}. {match['business_name']} (ID: {match['business_id']}) - Similarity: {match['similarity']:.3f}")
        
        lines.append("")
        for line in lines:
            logger.info(line)
        output_lines.extend(lines)

    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("UNLINKED BUSINESS PROPERTIES ANALYSIS RESULTS\n")
                f.write("="*80 + "\n\n")
                # ... (rest of file writing is the same)
                f.write(f"Total properties analyzed: {len(business_properties)}\n\n")
                for line in output_lines:
                    f.write(line + "\n")
            logger.info(f"Detailed results written to: {output_file}")
        except Exception as e:
            logger.error(f"Failed to write output file {output_file}: {e}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Analyze unlinked properties using fast, database-native fuzzy matching.'
    )
    parser.add_argument(
        '--min-similarity',
        type=float,
        default=0.3,
        help='Minimum similarity score for business name matching (default: 0.3)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit the total number of property-match pairs returned (for testing)'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        help='Write detailed results to this file'
    )

    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        business_properties = find_unlinked_business_properties(
            conn,
            min_similarity=args.min_similarity,
            limit=args.limit
        )
        output_analysis_results(business_properties, args.output_file)
        logger.info("Analysis complete!")

    except Exception as e:
        logger.error(f"An error occurred during analysis: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()