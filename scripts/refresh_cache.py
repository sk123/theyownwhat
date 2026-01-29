
import os
import psycopg2
import json
import logging
import traceback
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("they-own-what")

DATABASE_URL = os.environ.get("DATABASE_URL")

def _calculate_and_cache_insights(cursor, town_col, town_filter):
    town_where_clause = ""
    params = []
    if town_col and town_filter:
        town_where_clause = f"WHERE p.{town_col} = %s"
        params.append(town_filter)

    query = f"""
        WITH property_to_network AS (
            SELECT p.id as property_id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
            WHERE p.business_id IS NOT NULL
            
            UNION
            
            SELECT p.id, en.network_id
            FROM properties p
            JOIN principals pr ON p.principal_id = pr.id::text
            JOIN entity_networks en ON pr.name_c = en.entity_id AND en.entity_type = 'principal'
            WHERE p.principal_id IS NOT NULL

            UNION

            SELECT p.id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.owner_norm = en.entity_id AND en.entity_type = 'principal'
            WHERE p.owner_norm IS NOT NULL
        ),
        top_networks AS (
            SELECT
                ptn.network_id,
                COUNT(DISTINCT ptn.property_id) as property_count,
                COALESCE(SUM(p.assessed_value), 0) as total_assessed_value,
                COALESCE(SUM(p.appraised_value), 0) as total_appraised_value,
                (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = ptn.network_id AND en.entity_type = 'business') as business_count
            FROM property_to_network ptn
            JOIN properties p ON ptn.property_id = p.id
            {town_where_clause}
            GROUP BY ptn.network_id
            HAVING COUNT(DISTINCT ptn.property_id) > 0
            ORDER BY property_count DESC
            LIMIT 50
        ),
        network_display_entity AS (
            SELECT DISTINCT ON (tn.network_id)
                tn.network_id,
                tn.property_count,
                tn.total_assessed_value,
                tn.total_appraised_value,
                tn.business_count,
                en.entity_id,
                en.entity_type,
                en.entity_name,
                (
                    SELECT COUNT(p_inner.id)
                    FROM properties p_inner
                    WHERE (p_inner.business_id::text = en.entity_id AND en.entity_type = 'business')
                       OR (p_inner.principal_id IN (SELECT id::text FROM principals pr WHERE pr.name_c = en.entity_id) AND en.entity_type = 'principal')
                       OR (p_inner.business_id IN (SELECT business_id FROM principals pr WHERE pr.name_c = en.entity_id AND pr.business_id IS NOT NULL) AND en.entity_type = 'principal')
                       OR (p_inner.owner_norm = en.entity_id AND en.entity_type = 'principal')
                ) as entity_property_count
            FROM top_networks tn
            JOIN entity_networks en ON tn.network_id = en.network_id
            ORDER BY tn.network_id, 
                     CASE WHEN en.entity_type = 'principal' THEN 0 ELSE 1 END,
                     CASE 
                        WHEN en.entity_name ILIKE '%%%% LLC' THEN 2 
                        WHEN en.entity_name ILIKE '%%%% INC%%' THEN 2 
                        WHEN en.entity_name ILIKE '%%%% CORP%%' THEN 2 
                        WHEN en.entity_name ILIKE '%%%% LTD%%' THEN 2 
                        ELSE 0 
                     END,
                     entity_property_count DESC
        ),
        controlling_business AS (
            SELECT DISTINCT ON (tn.network_id)
                tn.network_id,
                en.entity_name as business_name,
                en.entity_id as business_id
            FROM top_networks tn
            JOIN entity_networks en ON tn.network_id = en.network_id
            WHERE en.entity_type = 'business'
            ORDER BY tn.network_id, 
                (SELECT COUNT(*) FROM properties p WHERE p.business_id::text = en.entity_id) DESC
        )
        SELECT
            nde.entity_id,
            nde.entity_name,
            nde.entity_type,
            nde.property_count as value,
            nde.total_assessed_value,
            nde.total_appraised_value,
            nde.business_count,
            nde.network_id,
            cb.business_name as controlling_business_name,
            cb.business_id as controlling_business_id
        FROM network_display_entity nde
        LEFT JOIN controlling_business cb ON nde.network_id = cb.network_id
        ORDER BY nde.property_count DESC
    """
    print("Executing query...")
    cursor.execute(query, params)
    raw_networks = cursor.fetchall()
    print(f"Raw networks found: {len(raw_networks)}")
    
    merged_networks = []
    seen_keys = {} 
    
    for net in raw_networks:
        c_id = net.get('controlling_business_id')
        unique_key = c_id if c_id else f"ent_{net['entity_id']}"
        network = dict(net)
        
        if unique_key in seen_keys:
            existing_idx = seen_keys[unique_key]
            existing_net = merged_networks[existing_idx]
            if existing_net['entity_type'] == 'business' and network['entity_type'] == 'principal':
                existing_net['entity_name'] = network['entity_name']
                existing_net['entity_type'] = 'principal'
                existing_net['entity_id'] = network['entity_id']
            elif existing_net['entity_type'] == 'principal' and network['entity_type'] == 'principal':
                 if network['entity_name'] not in existing_net['entity_name']:
                     if len(existing_net['entity_name']) < 60: 
                        existing_net['entity_name'] += f" & {network['entity_name']}"
            
            if network['value'] > existing_net['value']:
                existing_net['value'] = network['value']
                existing_net['total_assessed_value'] = network['total_assessed_value']
                existing_net['total_appraised_value'] = network['total_appraised_value']
            
            existing_net['business_count'] = max(existing_net.get('business_count', 0), network.get('business_count', 0))
            continue
        
        seen_keys[unique_key] = len(merged_networks)
        merged_networks.append(network)
    
    print(f"Merged networks count: {len(merged_networks)}")
    final_networks = merged_networks[:10]
    result = []
    
    for network in final_networks:
        cursor.execute("""
            SELECT name, state FROM (
                SELECT DISTINCT ON(pr.name_c)
                    pr.name_c as name,
                    pr.state,
                    COUNT(*) as link_count
                FROM entity_networks en
                JOIN principals pr ON en.entity_id = pr.business_id
                WHERE en.network_id = %s AND en.entity_type = 'business' AND pr.name_c IS NOT NULL
                GROUP BY pr.name_c, pr.state
                ORDER BY pr.name_c, link_count DESC
            ) as distinct_principals
            ORDER BY link_count DESC
            LIMIT 3;
        """, (network['network_id'],))
        network['principals'] = cursor.fetchall()
        
        cursor.execute("""
            SELECT entity_name as name
            FROM entity_networks
            WHERE network_id = %s AND entity_type = 'business'
            ORDER BY entity_name
            LIMIT 5;
        """, (network['network_id'],))
        network['representative_entities'] = cursor.fetchall()
        result.append(network)
        
    return result

def run():
    print("Connecting to DB...")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            insights_by_municipality = {}
            
            print("Calculating STATEWIDE...")
            insights_by_municipality['STATEWIDE'] = _calculate_and_cache_insights(cursor, None, None)
            
            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    return super().default(obj)

            insights_json = json.dumps(insights_by_municipality, cls=DateTimeEncoder)
            
            print("Updating kv_cache...")
            cursor.execute("""
                INSERT INTO kv_cache (key, value) VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now()
            """, ('insights', insights_json))
            
            conn.commit()
            print("✅ Insights refreshed.")
            
            if insights_by_municipality['STATEWIDE']:
                top = insights_by_municipality['STATEWIDE'][0]
                print(f"Name: {top['entity_name']}")
                print(f"Type: {top['entity_type']}")
                print(f"Count: {top['value']}")
            else:
                print("No statewide networks found!")

    except Exception:
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    run()
