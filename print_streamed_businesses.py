import json

def main():
    with open('/tmp/stream_414.json', 'r') as f:
        for line in f:
            if not line.strip(): continue
            chunk = json.loads(line)
            if chunk.get('type') == 'entities':
                entities = chunk.get('data', {}).get('entities', [])
                businesses = [e for e in entities if e.get('type') == 'business']
                for b in businesses:
                    print(b['id'], b['name'])

if __name__ == "__main__":
    main()
