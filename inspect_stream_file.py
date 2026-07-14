import json

def main():
    with open('/tmp/stream_414.json', 'r') as f:
        for line in f:
            if not line.strip(): continue
            chunk = json.loads(line)
            if chunk.get('type') == 'entities':
                data = chunk.get('data', {})
                entities = data.get('entities', [])
                links = data.get('links', {})
                print(f"Number of entities in stream: {len(entities)}")
                print("Links keys in stream:", links.keys())
                for k, v in links.items():
                    print(f"  - {k}: {len(v)} links")
                    if len(v) > 0:
                        print(f"    Sample: {v[:2]}")

if __name__ == "__main__":
    main()
