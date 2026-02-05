try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    print("psycopg2 is available!")
except ImportError:
    print("psycopg2 NOT FOUND")

import os
print(f"Current Environment: {os.environ.get('VIRTUAL_ENV', 'None')}")
import sys
print(f"Python Executable: {sys.executable}")
print(f"Path: {sys.path}")
