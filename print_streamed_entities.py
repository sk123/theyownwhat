import json

def main():
    with open('/tmp/stream_414.json', 'r') as f:
        for line in f:
            if not line.strip(): continue
            chunk = json.loads(line)
            if chunk.get('type') == 'entities':
                entities = chunk.get('data', {}).get('entities', [])
                principals = [e for e in entities if e.get('type') == 'principal']
                businesses = [e for e in entities if e.get('type') == 'business']
                print(f"Total principals in stream: {len(principals)}")
                print(f"Total businesses in stream: {len(businesses)}")
                
                print("\n=== All principals in stream ===")
                for p in principals:
                    print(p['id'], p['name'])
                    
                print("\n=== Let's check David Mack specifically ===")
                dm = [p for p in principals if 'MACK' in p['name'].upper()]
                for p in dm:
                    print(p)

if __name__ == "__main__":
    main()
