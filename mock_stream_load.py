import os
import psycopg2
import json
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # We simulate the parts of stream_load_network in api/main.py
        # Let's get network_ids = [414]
        network_ids = [414]
        
        # Get businesses
        cursor.execute(
            "SELECT entity_id AS id, entity_name AS name FROM entity_networks "
            "WHERE network_id = ANY(%s) AND entity_type = 'business'",
            (network_ids,)
        )
        businesses = cursor.fetchall()
        
        # Get principals
        cursor.execute(
            "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
            "FROM entity_networks "
            "WHERE network_id = ANY(%s) AND entity_type = 'principal'",
            (network_ids,)
        )
        principals_in_network = cursor.fetchall()
        
        print(f"Loaded {len(businesses)} businesses and {len(principals_in_network)} principals.")
        
        # Let's see the principal details consolidation
        principal_names = {p['principal_name'] for p in principals_in_network if p.get('principal_name')}
        merged_principals = {}
        if principal_names:
            cursor.execute(
                "SELECT * FROM principals WHERE name_c = ANY(%s)",
                (list(principal_names),)
            )
            all_principal_records = cursor.fetchall()
            for record in all_principal_records:
                name_c = record.get('name_c')
                if not name_c: continue
                if name_c not in merged_principals:
                    merged_principals[name_c] = record
                else:
                    for key, value in record.items():
                        if merged_principals[name_c].get(key) is None and value is not None:
                            merged_principals[name_c][key] = value

        entities_dict = {}
        links = {"business_to_principal": [], "principal_to_business": []}

        for b in businesses:
            b_key = f"business_{b['id']}"
            entities_dict[b_key] = {
                "id": b["id"],
                "name": b["name"],
                "type": "business",
                "connections": [],
            }

        def _principal_key(name: str) -> str:
            from shared_utils import canonicalize_person_name
            return f"principal_{canonicalize_person_name(name)}"

        for pr in principals_in_network:
            principal_name = pr.get("principal_name") or pr["principal_id"]
            p_key = _principal_key(principal_name)
            details = merged_principals.get(principal_name, {"name_c": principal_name})
            entities_dict[p_key] = {
                "id": pr["principal_id"],
                "name": principal_name,
                "type": "principal",
                "details": details,
                "connections": [],
            }

        _canonical_to_pkey = {}
        _entity_id_to_pkey = {}
        for pr in principals_in_network:
            principal_name = pr.get("principal_name") or pr["principal_id"]
            p_key = _principal_key(principal_name)
            if p_key in entities_dict:
                from shared_utils import canonicalize_person_name
                canon = canonicalize_person_name(principal_name)
                _canonical_to_pkey[canon] = p_key
                _entity_id_to_pkey[str(pr["principal_id"])] = p_key

        if businesses:
            biz_id_list = [b["id"] for b in businesses]
            principal_entity_ids = [str(pr["principal_id"]) for pr in principals_in_network]
            
            # Fallback name-based matching
            cursor.execute(
                "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                "FROM principals WHERE business_id = ANY(%s)",
                (biz_id_list,)
            )
            fallback_count = 0
            seen_links = set()
            for r in cursor.fetchall():
                if not r.get("pname"): continue
                b_key = f"business_{r['business_id']}"
                if b_key not in entities_dict: continue
                p_key = _principal_key(r["pname"])
                if p_key not in entities_dict:
                    from shared_utils import canonicalize_person_name
                    canon = canonicalize_person_name(r["pname"])
                    p_key = _canonical_to_pkey.get(canon)
                if p_key and p_key in entities_dict:
                    link_pair = (b_key, p_key)
                    if link_pair not in seen_links:
                        seen_links.add(link_pair)
                        links["business_to_principal"].append({"source": b_key, "target": p_key})
                        links["principal_to_business"].append({"source": p_key, "target": b_key})
                        fallback_count += 1
                        
            print(f"Fallback links count: {fallback_count}")
            # Print David Mack links if any
            for l in links["business_to_principal"]:
                if "DAVID MACK" in l["target"]:
                    print("Found David Mack link:", l)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
