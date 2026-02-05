
import os

filepath = "/home/sk/dev/theyownwhat/api/main.py"
with open(filepath, "r") as f:
    lines = f.readlines()

new_lines = []
skip = False
added = False

for i, line in enumerate(lines):
    if '# Hartford Image Proxy' in line:
        skip = True
        if not added:
            new_lines.append("# ------------------------------------------------------------\n")
            new_lines.append("# Hartford Image Proxy\n")
            new_lines.append("# ------------------------------------------------------------\n")
            new_lines.append("@app.get(\"/api/hartford/image/{account_number}\")\n")
            new_lines.append("async def proxy_hartford_image(account_number: str):\n")
            new_lines.append("    \"\"\"\n")
            new_lines.append("    Hartford's portal requires a session to view images.\n")
            new_lines.append("    This proxy establishes a session and returns the image byte stream.\n")
            new_lines.append("    \"\"\"\n")
            new_lines.append("    base_url = \"https://assessor1.hartford.gov\"\n")
            new_lines.append("    search_page = f\"{base_url}/search-middle-ns.asp\"\n")
            new_lines.append("    image_url = f\"{base_url}/showimage.asp?AccountNumber={account_number}&Width=500\"\n")
            new_lines.append("    \n")
            new_lines.append("    headers = {\n")
            new_lines.append("        \"User-Agent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36\",\n")
            new_lines.append("        \"Referer\": f\"{base_url}/Summary.asp?AccountNumber={account_number}\"\n")
            new_lines.append("    }\n")
            new_lines.append("    \n")
            new_lines.append("    try:\n")
            new_lines.append("        session = requests.Session()\n")
            new_lines.append("        session.headers.update(headers)\n")
            new_lines.append("        # 1. Hit Search Page to set session cookies\n")
            new_lines.append("        session.get(search_page, timeout=10, verify=False)\n")
            new_lines.append("        # 2. Hit Summary Page to ensure session context for this specific account\n")
            new_lines.append("        session.get(f\"{base_url}/Summary.asp?AccountNumber={account_number}\", timeout=10, verify=False)\n")
            new_lines.append("        # 3. Finally, fetch the image\n")
            new_lines.append("        resp = session.get(image_url, timeout=10, stream=True, verify=False)\n")
            new_lines.append("        \n")
            new_lines.append("        if resp.status_code != 200:\n")
            new_lines.append("            logger.error(f\"Failed to fetch image for {account_number}: {resp.status_code}\")\n")
            new_lines.append("            raise HTTPException(status_code=resp.status_code, detail=\"Failed to fetch image from Hartford portal\")\n")
            new_lines.append("            \n")
            new_lines.append("        return StreamingResponse(\n")
            new_lines.append("            resp.iter_content(chunk_size=1024),\n")
            new_lines.append("            media_type=resp.headers.get(\"Content-Type\", \"image/jpeg\")\n")
            new_lines.append("        )\n")
            new_lines.append("    except Exception as e:\n")
            new_lines.append("        logger.error(f\"Hartford proxy error for {account_number}: {e}\")\n")
            new_lines.append("        raise HTTPException(status_code=500, detail=str(e))\n\n")
            new_lines.append("@app.get(\"/api/health\")\n")
            new_lines.append("def health_check():\n")
            new_lines.append("    # Check if OpenAI key is present and NOT the placeholder\n")
            added = True
        continue
    
    if skip and 'async def login(' in line:
        skip = False
    
    if not skip:
        new_lines.append(line)

with open(filepath, "w") as f:
    f.writelines(new_lines)

print("Repair complete.")
