import os
import sys
import logging

# Set python path to include api
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api'))

from safe_network_refresh import run_refresh

if __name__ == "__main__":
    os.environ['DATABASE_URL'] = "postgresql://user:password@localhost:5432/ctdata"
    print("🚀 Running safe_network_refresh manually...")
    success = run_refresh(dry_run=True, skip_linking=True) # dry_run = True first to verify!
    print("Success:", success)
