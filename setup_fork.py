
import os

target_file = '../theyownwhat-tt/docker-compose.yml'

with open(target_file, 'r') as f:
    lines = f.readlines()

new_lines = []
in_db_section = False

for line in lines:
    # Detect start of ctdata_db service (indent 2 usually)
    if line.strip().startswith('ctdata_db:'):
        in_db_section = True
        continue
    
    if in_db_section:
        # If we hit another service (indent 2) or global key (indent 0)
        # Assuming 2 spaces indent for services. 
        # If line starts with 2 spaces and not empty/comment, it could be property.
        # But if it starts with 2 spaces and keys like properties, it's inside db.
        # If it starts with 2 spaces and is a key like 'networks:', it's next.
        # Wait, services are indented by 2 spaces?
        # Top level keys: services, networks, volumes.
        # Service keys: indented by 2 spaces.
        
        # Check specific indentation.
        # If line is less intended than 4 spaces (properties) and not empty, it might be end.
        
        # Actually, easiest way: Just skip specific range? No, file might change.
        
        # YAML structure:
        # services:
        #   srv1:
        #     ...
        #   ctdata_db:
        
        # If we see a line with same or less indentation (<= 2 spaces) that is NOT a comment and NOT empty, 
        # that marks end of section.
        # Except "  ctdata_db:" itself has 2 spaces.
        if len(line) - len(line.lstrip()) <= 2 and line.strip() and not line.strip().startswith('#'):
             in_db_section = False
        else:
            continue # Skip lines inside db section

    # Replacements
    if 'container_name: ctdata_api' in line:
        line = line.replace('ctdata_api', 'ctdata_api_tt')
    if 'container_name: ctdata_nginx' in line:
        line = line.replace('ctdata_nginx', 'ctdata_nginx_tt')
    if 'container_name: ctdata_updater' in line:
        line = line.replace('ctdata_updater', 'ctdata_updater_tt')
    
    if '"8000:8000"' in line:
        line = line.replace('"8000:8000"', '"8001:8000"')
    
    if '"6262:80"' in line:
        line = line.replace('"6262:80"', '"6263:80"')

    new_lines.append(line)

with open(target_file, 'w') as f:
    f.writelines(new_lines)

print("Modified docker-compose.yml")
