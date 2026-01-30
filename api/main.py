import os
import re
import sys
import time
import json
import logging
import threading
import requests
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_batch

from fastapi import FastAPI, Depends, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
try:
    from starlette.middleware.sessions import SessionMiddleware
    from authlib.integrations.starlette_client import OAuth
    from starlette.middleware.sessions import SessionMiddleware
    from authlib.integrations.starlette_client import OAuth
    # Default to True if deps exist, but allow env var override
    TOOLBOX_ENABLED = os.environ.get("TOOLBOX_ENABLED", "true").lower() == "true"
except ImportError:
    TOOLBOX_ENABLED = False
    SessionMiddleware = None
    OAuth = None
from pydantic import BaseModel

# Optional OpenAI import (AI report). App still runs without it.
try:
    import openai  # type: ignore
except Exception:  # pragma: no cover
    openai = None  # type: ignore

# ------------------------------------------------------------
# App / Config
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("they-own-what")

DATABASE_URL = os.environ.get("DATABASE_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Lock file path (same as in build_networks.py)
LOCK_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'maintenance.lock')

SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY")  # reserved for future use

if openai and OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

# Auth Config
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
# A random secret for sessions
SESSION_SECRET_KEY = os.environ.get("SESSION_SECRET_KEY", "a-very-secret-key-change-this-in-prod")

app = FastAPI(title="they own WHAT?? API")

@app.get("/api/system/status")
def get_system_status():
    """Checks if the system is in maintenance mode (rebuilding networks)."""
    is_maintenance = os.path.exists(LOCK_FILE_PATH)
    return {"maintenance": is_maintenance}


# OAuth setup
if TOOLBOX_ENABLED:
    oauth = OAuth()
    oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )
else:
    oauth = None

if TOOLBOX_ENABLED:
    app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Mount static files for scraped images
# Use relative path so it works locally and in container (WORKDIR /app)
os.makedirs("api/static", exist_ok=True)
app.mount("/api/static", StaticFiles(directory="api/static"), name="static")

@app.get("/api/health")
def health_check():
    # Check if OpenAI key is present and NOT the placeholder
    ai_key = os.environ.get("OPENAI_API_KEY", "")
    ai_enabled = bool(ai_key and "REPLACE_WITH_API_KEY" not in ai_key)
    return {
        "status": "ok", 
        "timestamp": time.time(), 
        "ai_enabled": ai_enabled,
        "toolbox_enabled": TOOLBOX_ENABLED
    }

# ------------------------------------------------------------
# AUTH ROUTES
# ------------------------------------------------------------
@app.get("/api/auth/login")
async def login(request: Request):
    # Support mock login for development
    if os.environ.get("USE_MOCK_AUTH", "true") == "true":
         return RedirectResponse(url="/api/auth/mock-login")
    
    redirect_uri = request.url_for('auth_callback')
    return await oauth.google.authorize_redirect(request, redirect_uri)

@app.get("/api/auth/mock-login")
async def mock_login(request: Request):
    # Upsert a mock user
    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (google_id, email, full_name, picture_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (google_id) DO UPDATE SET
                    full_name = EXCLUDED.full_name
                RETURNING id;
            """, ("mock_id_123", "organizer@example.com", "Mock Organizer", "https://api.dicebear.com/7.x/avataaars/svg?seed=mock"))
            db_user_id = cur.fetchone()[0]
            conn.commit()
        db_pool.putconn(conn)

    request.session['user'] = {
        'id': db_user_id,
        'email': "organizer@example.com",
        'name': "Mock Organizer",
        'picture': "https://api.dicebear.com/7.x/avataaars/svg?seed=mock"
    }
    return RedirectResponse(url="/")

@app.get("/api/auth/callback")
async def auth_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        raise HTTPException(status_code=400, detail="Authentication failed")
        
    user_info = token.get('userinfo')
    if not user_info:
        raise HTTPException(status_code=400, detail="No user info returned from Google")

    # Upsert user in database
    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (google_id, email, full_name, picture_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (google_id) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    picture_url = EXCLUDED.picture_url
                RETURNING id;
            """, (user_info['sub'], user_info['email'], user_info['name'], user_info.get('picture')))
            db_user_id = cur.fetchone()[0]
            conn.commit()
        db_pool.putconn(conn)

    # Store user info in session
    request.session['user'] = {
        'id': db_user_id,
        'email': user_info['email'],
        'name': user_info['name'],
        'picture': user_info.get('picture')
    }
    
    # Redirect back to frontend
    return RedirectResponse(url="/")

@app.get("/api/auth/logout")
async def logout(request: Request):
    request.session.pop('user', None)
    return RedirectResponse(url="/")

@app.get("/api/auth/me")
async def get_me(request: Request):
    if not TOOLBOX_ENABLED:
        return {"authenticated": False}
    user = request.session.get('user')
    if not user:
        return {"authenticated": False}
    return {"authenticated": True, "user": user}

# ------------------------------------------------------------
# TOOLBOX / GROUP ROUTES
# ------------------------------------------------------------
class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

@app.post("/api/groups")
async def create_group(group: GroupCreate, request: Request):
    if not TOOLBOX_ENABLED:
        raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
        
    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            # 1. Create group
            cur.execute("""
                INSERT INTO groups (name, description, created_by)
                VALUES (%s, %s, %s) RETURNING id;
            """, (group.name, group.description, user['id']))
            group_id = cur.fetchone()[0]
            
            # 2. Add creator as owner
            cur.execute("""
                INSERT INTO group_members (group_id, user_id, role)
                VALUES (%s, %s, 'owner');
            """, (group_id, user['id']))
            
            conn.commit()
        db_pool.putconn(conn)
        
    return {"status": "success", "group_id": group_id}

@app.get("/api/groups")
async def list_groups(request: Request):
    if not TOOLBOX_ENABLED:
        raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
        
    with db_pool.getconn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT g.*, gm.role,
                       (SELECT COUNT(*) FROM group_properties gp WHERE gp.group_id = g.id) as property_count,
                       (SELECT COUNT(*) FROM group_members gmem WHERE gmem.group_id = g.id) as member_count
                FROM groups g
                JOIN group_members gm ON g.id = gm.group_id
                WHERE gm.user_id = %s
            """, (user['id'],))
            groups = cur.fetchall()
        db_pool.putconn(conn)
        
    return groups

@app.post("/api/groups/{group_id}/properties")
async def add_property_to_group(group_id: int, payload: Dict[str, Any], request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
        
    property_id = payload.get('property_id')
    item_id = payload.get('item_id')
    item_type = payload.get('item_type') 

    if not property_id and not item_id:
        raise HTTPException(status_code=400, detail="property_id or item_id is required")

    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            # Check membership
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
            role = cur.fetchone()
            if not role:
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Not a member of this group")
            
            # 1. FETCH CANDIDATES
            candidates = []
            if property_id or item_type == 'address':
                target_id = property_id or item_id
                cur.execute("SELECT normalized_address, property_city, location FROM properties WHERE id = %s", (target_id,))
                target = cur.fetchone()
                if target:
                    norm, city, loc = target
                    cur.execute("""
                        SELECT id, property_city, normalized_address, location 
                        FROM properties 
                        WHERE property_city = %s AND ((normalized_address IS NOT NULL AND normalized_address = %s) OR (normalized_address IS NULL AND location = %s))
                    """, (city, norm, loc))
                    candidates = cur.fetchall()
            elif item_type == 'owner':
                cur.execute("SELECT id, property_city, normalized_address, location FROM properties WHERE owner = %s OR co_owner = %s", (item_id, item_id))
                candidates = cur.fetchall()
            elif item_type == 'business':
                cur.execute("SELECT id, property_city, normalized_address, location FROM properties WHERE business_id = %s", (item_id,))
                candidates = cur.fetchall()

            # 2. GROUP BY ADDRESS (Smart Grouping)
            grouped = {}
            for c in candidates:
                pid, city, norm, loc = c
                # Key for complex: City + Address
                # Use normalized address if avail, else location
                addr_key = norm if norm else loc
                if not addr_key: addr_key = "Unknown Address"
                if not city: city = "Unknown City"
                
                key = (city, addr_key)
                if key not in grouped: grouped[key] = []
                grouped[key].append(pid)
            
            # 3. CREATE COMPLEXES & INSERT
            for (city, addr), pids in grouped.items():
                if not pids: continue
                
                # Check/Create Complex
                cur.execute("SELECT id FROM group_complexes WHERE group_id = %s AND name = %s AND municipality = %s", (group_id, addr, city))
                complex_row = cur.fetchone()
                complex_id = None
                
                if complex_row:
                    complex_id = complex_row[0]
                else:
                    # Auto-Create Complex
                    cur.execute("""
                        INSERT INTO group_complexes (group_id, name, municipality, color)
                        VALUES (%s, %s, %s, 'blue')
                        RETURNING id
                    """, (group_id, addr, city))
                    complex_id = cur.fetchone()[0]
                
                # Bulk Insert Properties
                # Use execute_values or loop? Loop is fine for reasonable batches.
                # ON CONFLICT: If property already in group, we DO update complex_id if it was null? 
                # Or just ignore? User request: "associate properties... smartly".
                # If I already have it unassigned, and I search & add it, I probably expect it to move to the complex.
                # So DO UPDATE SET complex_id.
                for pid in pids:
                    cur.execute("""
                        INSERT INTO group_properties (group_id, property_id, added_by, complex_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (group_id, property_id) 
                        DO UPDATE SET complex_id = EXCLUDED.complex_id
                    """, (group_id, pid, user['id'], complex_id))

            conn.commit()
        db_pool.putconn(conn)
        
    return {"status": "success", "added_count": len(candidates)}

@app.get("/api/groups/{group_id}/properties")
async def list_group_properties(group_id: int, request: Request):
    if not TOOLBOX_ENABLED:
        raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
        
    with db_pool.getconn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check membership
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
            role = cur.fetchone()
            if not role:
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Not a member of this group")

            # 1. Fetch Real Properties
            cur.execute("""
                SELECT p.*, 
                       gp.complex_id, 
                       gp.custom_unit, 
                       gp.custom_address,
                       (SELECT COUNT(*) FROM property_notes pn WHERE pn.property_id = p.id AND pn.group_id = %s) as notes_count,
                       (SELECT COUNT(*) FROM property_photos pp WHERE pp.property_id = p.id AND pp.group_id = %s) as photos_count,
                       (SELECT COUNT(*) FROM property_tags pt WHERE pt.property_id = p.id AND pt.group_id = %s) as tags_count
                FROM properties p
                JOIN group_properties gp ON p.id = gp.property_id
                WHERE gp.group_id = %s
                ORDER BY gp.added_at DESC
            """, (group_id, group_id, group_id, group_id))
            real_props = cur.fetchall()
            
            # 2. Fetch Custom Units (Separate Table)
            cur.execute("""
                SELECT id, group_id, complex_id, name, created_at
                FROM group_custom_units
                WHERE group_id = %s
                ORDER BY created_at DESC
            """, (group_id,))
            custom_rows = cur.fetchall()

            # 3. Merge
            properties = []
            for p in real_props:
                # Stringify decimals
                if p.get('assessed_value'): p['assessed_value'] = str(p['assessed_value'])
                if p.get('appraised_value'): p['appraised_value'] = str(p['appraised_value'])
                if p.get('sale_amount'): p['sale_amount'] = str(p['sale_amount'])
                if p.get('sale_date'): p['sale_date'] = str(p['sale_date'])
                p['is_custom'] = False
                properties.append(p)
            
            for c in custom_rows:
                properties.append({
                    "id": -c['id'], # Negative ID for custom units
                    "gp_id": None, # Not in group_properties
                    "complex_id": c['complex_id'],
                    "address": c['name'], # Use name as address
                    "location": c['name'],
                    "city": "Custom Unit",
                    "zip": "",
                    "is_custom": True,
                    "notes_count": 0,
                    "photos_count": 0,
                    "tags_count": 0,
                    "custom_unit": None,
                    "custom_address": None
                })
                
        db_pool.putconn(conn)
        
    return properties

@app.post("/api/groups/{group_id}/properties/custom")
async def create_custom_unit(group_id: int, payload: Dict[str, Any], request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    name = payload.get('name')
    complex_id = payload.get('complex_id')
    if not name or not complex_id: raise HTTPException(status_code=400, detail="Missing name or complex_id")

    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            # Check membership
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
            if not cur.fetchone():
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Not a member")
            
            cur.execute("""
                INSERT INTO group_custom_units (group_id, complex_id, name, created_by)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (group_id, complex_id, name, user['id']))
            new_id = cur.fetchone()[0]
            conn.commit()
    return {"status": "created", "id": -new_id}

@app.delete("/api/groups/{group_id}/properties/{gp_id}")
async def delete_item_from_group(group_id: int, gp_id: int, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    with cursor_context() as cur:
        # Check membership
        cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="Not a member")
        
        if gp_id > 0:
            # Real Property: remove from group_properties by property_id
            cur.execute("DELETE FROM group_properties WHERE property_id = %s AND group_id = %s", (gp_id, group_id))
        else:
            # Custom Unit: delete from group_custom_units
            custom_id = abs(gp_id)
            cur.execute("DELETE FROM group_custom_units WHERE id = %s AND group_id = %s", (custom_id, group_id))
                
    return {"status": "deleted"}

@app.get("/api/groups/{group_id}/members")
async def list_group_members(group_id: int, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    with db_pool.getconn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
            role = cur.fetchone()
            if not role:
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Not a member of this group")

            cur.execute("""
                SELECT u.id, u.full_name, u.email, u.picture_url, gm.role, gm.joined_at
                FROM group_members gm
                JOIN users u ON gm.user_id = u.id
                WHERE gm.group_id = %s
                ORDER BY u.full_name ASC
            """, (group_id,))
            members = cur.fetchall()
            for m in members:
                if m.get('joined_at'): m['joined_at'] = str(m['joined_at'])
        db_pool.putconn(conn)
    return members

@app.post("/api/groups/{group_id}/members")
async def add_group_member(group_id: int, payload: Dict[str, Any], request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    email = payload.get('email')
    role = payload.get('role', 'member')
    if not email: raise HTTPException(status_code=400, detail="Email is required")

    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
            my_role = cur.fetchone()
            if not my_role or my_role[0] != 'organizer':
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Only organizers can add members")

            cur.execute("SELECT id FROM users WHERE email = %s", (email,))
            target_user = cur.fetchone()
            if not target_user:
                db_pool.putconn(conn)
                raise HTTPException(status_code=404, detail=f"User with email {email} not found.")

            cur.execute("""
                INSERT INTO group_members (group_id, user_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (group_id, user_id) DO UPDATE SET role = EXCLUDED.role
            """, (group_id, target_user[0], role))
            conn.commit()
        db_pool.putconn(conn)
    return {"status": "success"}

@app.patch("/api/groups/{group_id}/members/{user_id}")
async def update_group_member(group_id: int, user_id: int, payload: Dict[str, Any], request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    current_user = request.session.get('user')
    if not current_user: raise HTTPException(status_code=401, detail="Authentication required")
    
    role = payload.get('role')
    if not role: raise HTTPException(status_code=400, detail="Role is required")

    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, current_user['id']))
            my_role = cur.fetchone()
            if not my_role or my_role[0] != 'organizer':
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Only organizers can update roles")

            cur.execute("UPDATE group_members SET role = %s WHERE group_id = %s AND user_id = %s", (role, group_id, user_id))
            conn.commit()
        db_pool.putconn(conn)
    return {"status": "success"}

@app.delete("/api/groups/{group_id}/members/{user_id}")
async def remove_group_member(group_id: int, user_id: int, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    current_user = request.session.get('user')
    if not current_user: raise HTTPException(status_code=401, detail="Authentication required")

    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, current_user['id']))
            my_role = cur.fetchone()
            
            if not my_role:
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Not a member of this group")
                
            if current_user['id'] != user_id and my_role[0] != 'organizer':
                db_pool.putconn(conn)
                raise HTTPException(status_code=403, detail="Only organizers can remove other members")

            cur.execute("DELETE FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user_id))
            conn.commit()
        db_pool.putconn(conn)
    return {"status": "success"}

@app.get("/api/users/search")
async def search_users(email: str, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    if not request.session.get('user'): raise HTTPException(status_code=401, detail="Authentication required")
    
    with db_pool.getconn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, full_name, email, picture_url FROM users WHERE email ILIKE %s LIMIT 10", (f"%{email}%",))
            users = cur.fetchall()
        db_pool.putconn(conn)
    return users


# ------------------------------------------------------------
# AI REPORTING ROUTE
# ------------------------------------------------------------
class ReportRequest(BaseModel):
    context: Dict[str, Any]
    prompt_config: Optional[str] = "investigative"

@app.post("/api/ai/report")
async def generate_ai_report(req: ReportRequest, request: Request):
    """
    Generate an 'investigative journalist' summary of a portfolio.
    Input: 'context' dict with stats, cities, top violations, etc.
    Output: text summary.
    """
    if not openai:
        raise HTTPException(status_code=400, detail="AI features not configured (missing openai lib)")
    if not OPENAI_API_KEY or "REPLACE_WITH" in OPENAI_API_KEY:
        # Return a mock report for demo purposes if key is missing? 
        # Or just specific error. Let's return error but 400 to avoid Nginx HTML.
        raise HTTPException(status_code=400, detail="AI features not configured (missing OPENAI_API_KEY)")

    # Verify Auth (optional, but recommended for cost)
    if TOOLBOX_ENABLED:
        user = request.session.get('user')
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required for AI features")

    try:
        # Construct a prompt from the context
        owner_name = req.context.get('name', 'Unknown Entity')
        prop_count = req.context.get('property_count', 0)
        total_val = req.context.get('total_value', 0)
        top_city = req.context.get('top_city', 'Unknown')
        
        system_prompt = (
            "You are an investigative housing journalist. You write short, punchy, cynical summaries "
            "about landlord portfolios. Focus on scale, consolidation, and potential monopolization. "
            "Do not be polite. Be objective but sharp."
        )
        
        user_prompt = (
            f"Write a 1-paragraph summary (max 300 words) for a landlord named '{owner_name}'.\n"
            f"They own {prop_count} properties in Connecticut, mostly in {top_city}.\n"
            f"Total assessed portfolio value is roughly {total_val}.\n"
            "Highlight the scale of their operation. If they have > 50 properties, mention they are a major player."
        )

        # OpenAI v1.0+ Client
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=250
        )
        
        summary = response.choices[0].message.content.strip()
        return {"report": summary}

    except Exception as e:
        logger.error(f"OpenAI Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

db_pool: Optional[pool.SimpleConnectionPool] = None


# ------------------------------------------------------------
# Helpers (make available to all endpoints)
# ------------------------------------------------------------
_PERSON_SUFFIXES = {'JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'}
import decimal


def _extract_street_address(address: str) -> str:
    """
    Experimental: Remove unit/apartment numbers to get the 'base' street address.
    e.g. '123 MAIN ST UNIT 4' -> '123 MAIN ST'
    """
    if not address:
        return ""
    
    # Check for unit keywords. 
    # Use \b (word bound) or (?=\d) (followed by digit) to avoid partial matches 
    # on street names (e.g. 'FL' matching 'FLORENCE').
    # regex matches: space + (keyword) + (boundary/digit) + space? + (alphanumeric/dash) until end
    
    pattern = r'(?:,|\s+)\s*(?:(?:UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|RM|ROOM)(?:\b|(?=\d))|#)\s*[\w\d-]+$'
    
    clean = re.sub(pattern, '', address, flags=re.IGNORECASE).strip()
    return clean

def is_likely_street_address(addr: str) -> bool:
    """
    Heuristic: Valid street addresses usually start with a digit (house number)
    AND have at least one text part (street name).
    Avoids grouping outliers like '93' or '0'.
    """
    if not addr: return False
    addr = addr.strip()
    if not addr[0].isdigit():
        return False
    
    parts = addr.split()
    if len(parts) < 2:
        return False
        
    return True


def get_property_subsidies(cursor, property_id: int) -> List[Dict[str, Any]]:
    """Fetch subsidies for a specific property."""
    cursor.execute("""
        SELECT program_name, subsidy_type, units_subsidized, expiry_date, source_url 
        FROM property_subsidies 
        WHERE property_id = %s
    """, (property_id,))
    return [dict(row) for row in cursor.fetchall()]

def shape_property_row(p: dict, subsidies: List[dict] = None) -> dict:
    """Normalize a property DB row into the shape the frontend expects."""
    # Only use normalized_address if it looks like an address (starts with digit).
    # Otherwise it might be a POI name from geocoding (e.g. 'Clifford Beers') which breaks grouping.
    norm_addr = p.get("normalized_address")
    if norm_addr and not is_likely_street_address(norm_addr):
        norm_addr = None

    return {
        "id": p.get("id"),
        "address": p.get("location") or "",
        "city": p.get("property_city") or "",
        "owner": p.get("owner") or "",
        "assessed_value": (
            f"${int(p['assessed_value']):,}" if p.get("assessed_value") is not None else None
        ),
        "appraised_value": (
            f"${int(p['appraised_value']):,}" if p.get("appraised_value") is not None else None
        ),
        "unit": p.get("unit"),
        "number_of_units": p.get("number_of_units"),
        "latitude": float(p['latitude']) if p.get("latitude") is not None else None,
        "longitude": float(p['longitude']) if p.get("longitude") is not None else None,
        "normalized_address": norm_addr,
        "complex_name": p.get("complex_name"),
        "management_company": p.get("management_company"),
        "subsidies": subsidies or [],
        "details": p,  # keep full row for drill-down
    }


def group_properties_into_complexes(properties: List[dict]) -> List[dict]:
    """
    Group properties by street address into complexes.
    - Main row shows street address with count of units
    - Sub-rows show individual units with their owners
    
    Uses the 'location' and 'property_city' fields to group properties.
    """
    from collections import defaultdict
    
    # Group by (location, city) tuple
    complexes_map = defaultdict(list)
    
    for prop in properties:
        # Use normalized_address if available AND valid, otherwise location + city
        raw_norm = (prop.get("normalized_address") or "").strip()
        if raw_norm and not is_likely_street_address(raw_norm):
            raw_norm = ""
            
        location = (prop.get("location") or "").strip()
        city = (prop.get("property_city") or "").strip()
        
        # Priority to normalized address, but fall back to raw location
        # CRITICAL CHANGE: Always strip unit numbers for grouping purposes
        raw_grouping_str = raw_norm
        if not raw_grouping_str:
             if is_likely_street_address(location):
                 raw_grouping_str = location
        
        if not raw_grouping_str:
            continue
            
        base_address = _extract_street_address(raw_grouping_str)
        grouping_key = (base_address, city)
        
        complexes_map[grouping_key].append(prop)
    
    # Fetch management info for all grouping keys in one go if possible
    # For now, we'll do individual lookups or a batch lookup if we have the keys
    group_keys = list(complexes_map.keys())
    mgt_info = {}
    if group_keys:
        try:
            with cursor_context() as cur:
                # Use a tuple for the where clause: ((addr1, city1), (addr2, city2), ...)
                # PostgreSQL supports this syntax: WHERE (street_address, city) IN (('addr1', 'city1'), ...)
                placeholders = []
                params = []
                for addr, city in group_keys:
                    placeholders.append("(%s, %s)")
                    params.extend([addr, city])
                
                if placeholders:
                    query = f"SELECT street_address, city, management_name, official_url, phone FROM complex_management WHERE (street_address, city) IN ({', '.join(placeholders)})"
                    cur.execute(query, params)
                    for r in cur.fetchall():
                        mgt_info[(r['street_address'], r['city'])] = r
        except Exception as e:
            logger.warning(f"Failed to fetch management info: {e}")

    # Build result with complexes
    result = []
    
    # Pre-fetch subsidies for all properties in this batch
    all_property_ids = [p['id'] for units in complexes_map.values() for p in units]
    subsidies_map = defaultdict(list)
    if all_property_ids:
        try:
            with cursor_context() as cur:
                cur.execute("""
                    SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                    FROM property_subsidies
                    WHERE property_id = ANY(%s)
                """, (all_property_ids,))
                for row in cur.fetchall():
                    subsidies_map[row['property_id']].append(dict(row))
        except Exception as e:
            logger.warning(f"Failed to fetch subsidies: {e}")

    for (street_address, city), units in sorted(complexes_map.items()):
        mgt = mgt_info.get((street_address, city))
        
        # Aggregate complex level info from first unit (NHPD data usually consistent for complex)
        complex_name = next((u.get('complex_name') for u in units if u.get('complex_name')), None)
        management_co = next((u.get('management_company') for u in units if u.get('management_company')), None)
        
        if len(units) > 1:
            # This is a complex - create parent row with children
            total_assessed = sum(
                (p.get("assessed_value") or 0) for p in units
            )
            
            display_addr = street_address
            
            # Aggregate subsidies for the complex
            complex_subsidies = []
            seen_subsidy_keys = set()
            for u in units:
                for s in subsidies_map.get(u['id'], []):
                    key = (s['program_name'], s['subsidy_type'], s['expiry_date'])
                    if key not in seen_subsidy_keys:
                        complex_subsidies.append(s)
                        seen_subsidy_keys.add(key)
            
            complex_row = {
                "id": f"complex_{hash((street_address, city))}",
                "address": display_addr,
                "city": city,
                "owner": f"{units[0].get('owner', 'Multiple')} (+{len(units)-1} others)",
                "assessed_value": f"${int(total_assessed):,}" if total_assessed else None,
                "unit_count": sum(u.get("number_of_units") or 1 for u in units),
                "is_complex": True,
                "units": [u for u in units], 
                "latitude": float(units[0]['latitude']) if units[0].get("latitude") is not None else None,
                "longitude": float(units[0]['longitude']) if units[0].get("longitude") is not None else None,
                "normalized_address": display_addr,
                "complex_name": complex_name,
                "management_company": management_co,
                "subsidies": complex_subsidies,
                "management_info": {
                    "name": mgt['management_name'] if mgt else management_co, # prefer scraped mgt if available, else NHPD
                    "url": mgt['official_url'] if mgt else None,
                    "phone": mgt['phone'] if mgt else None
                }
            }
            # Deep shape the units
            complex_row["units"] = [shape_property_row(u, subsidies_map.get(u['id'], [])) for u in units]
            result.append(complex_row)
        else:
            # Single property, no grouping needed
            # Pass subsidies for this property
            res = shape_property_row(units[0], subsidies_map.get(units[0]['id'], []))
            if mgt:
                res["management_info"] = {
                    "name": mgt['management_name'],
                    "url": mgt['official_url'],
                    "phone": mgt['phone']
                }
            result.append(res)
    
    return result



def json_converter(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)

def normalize_person_name_py(name: str) -> str:
    """Robust normalization for FIRST LAST vs LAST, FIRST, suffixes, punctuation."""
    if not name:
        return ''
    n = name.upper().strip()
    n = re.sub(r"[`\"'.]", "", n)         # remove quotes/periods
    n = re.sub(r"\s+", " ", n).strip()    # collapse whitespace
    parts = n.split()
    if parts and parts[-1] in _PERSON_SUFFIXES:
        parts = parts[:-1]
    n = " ".join(parts)
    if ',' in n:
        m = re.match(r"^\s*([^,]+)\s*,\s*([A-Z0-9\- ]+)", n)
        if m:
            n = f"{m.group(2).strip()} {m.group(1).strip()}"
    
    # Specific typo fixes
    n = n.replace("GUREVITOH", "GUREVITCH")
    n = n.replace("MANACHEM", "MENACHEM")
    n = n.replace("MENACHERM", "MENACHEM")
    n = n.replace("MENAHEM", "MENACHEM")
    n = n.replace("GURAVITCH", "GUREVITCH")
    
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    
    # Middle Initial Stripping Strategy:
    # Only strip single-letter middle parts if the First and Last parts are robust (>1 char).
    # This protects short business names like "A B LLC".
    parts = n.split()
    if len(parts) >= 3:
        # Check if first and last name are likely real names (>1 char)
        if len(parts[0]) > 1 and len(parts[-1]) > 1:
            # Filter out single-letter middle tokens
            middle = parts[1:-1]
            # Keep middle tokens only if length > 1
            middle_robust = [p for p in middle if len(p) > 1]
            n = " ".join([parts[0]] + middle_robust + [parts[-1]])

    return n

def get_name_variations(name: str, entity_type: str) -> Set[str]:
    """Small set of useful variants (principal vs business)."""
    vars_: Set[str] = set()
    if not name:
        return vars_
    u = name.upper().strip()
    vars_.add(u)

    if entity_type == "principal":
        n = normalize_person_name_py(name)
        if n:
            vars_.add(n)
        tokens = n.split()
        if len(tokens) >= 2:
            vars_.add(f"{tokens[-1]} {tokens[0]}")  # LAST FIRST
    elif entity_type == "business":
        no_punct = re.sub(r"[^\w\s&]", " ", u)
        no_punct = re.sub(r"\s+", " ", no_punct).strip()
        vars_.add(no_punct)
        if '&' in u:
            vars_.add(u.replace('&', 'AND'))
        if ' AND ' in u:
            vars_.add(u.replace(' AND ', '&'))
        # strip common suffixes (iterate until none)
        suffixes = [
            'LIMITED LIABILITY COMPANY','LIMITED LIABILITY PARTNERSHIP',
            'PROFESSIONAL LIMITED LIABILITY COMPANY','LIMITED PARTNERSHIP',
            'INCORPORATED','CORPORATION','L L C','L L P','L P',
            'LLC','LLP','LTD','INC','CORP','LP','CO'
        ]
        n = u
        changed = True
        while changed:
            changed = False
            for s in suffixes:
                pat = re.compile(r"\s+" + re.escape(s) + r"$")
                if pat.search(n):
                    n = pat.sub('', n).strip()
                    changed = True
                    break
        if n:
            vars_.add(n)
    return {v for v in vars_ if v}

def find_properties_for_entity(cursor, entity_name: str, entity_type: str) -> List[Dict[str, Any]]:
    """Robust match on normalized owner/co-owner for principal or business name."""
    if not entity_name:
        return []
    et = "principal" if entity_type in ("owner", "principal") else "business"
    vars_ = get_name_variations(entity_name, et)
    norm_variants = list({normalize_person_name_py(v) for v in vars_ if v})
    if not norm_variants:
        return []
    cursor.execute(
        """
        SELECT *
        FROM properties
        WHERE owner_norm = ANY(%s) OR co_owner_norm = ANY(%s)
        """,
        (norm_variants, norm_variants)
    )
    return cursor.fetchall()

def _ndjson(obj: dict) -> bytes:
    return (json.dumps(obj, default=str) + "\n").encode("utf-8")


# ------------------------------------------------------------
# DB bootstrap (idempotent)
# ------------------------------------------------------------
DDL_NORMALIZE_FUNCTION = r"""
CREATE OR REPLACE FUNCTION normalize_person_name(input_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    n TEXT := COALESCE(input_name, '');
BEGIN
    n := UPPER(n);
    n := regexp_replace(n, '[`"''.]', '', 'g');
    n := regexp_replace(n, '\s+(JR|SR|III|IV|II|ESQ|MD|PHD|DDS)$', '', 'g');
    n := regexp_replace(n, '\s+', ' ', 'g');
    n := trim(n);
    IF position(',' IN n) > 0 THEN
        n := regexp_replace(n, '^\s*([^,]+)\s*,\s*([A-Z0-9\- ]+).*$','\2 \1');
        n := regexp_replace(n, '\s+', ' ', 'g');
        n := trim(n);
    END IF;
    RETURN n;
END;
$$;
"""

DDL_ADD_OWNER_NORM = """
ALTER TABLE properties ADD COLUMN IF NOT EXISTS owner_norm TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS co_owner_norm TEXT;
"""

DDL_INDEXES = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_properties_owner_gin
    ON properties USING gin (owner gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_location_gin
    ON properties USING gin (location gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_owner_norm_gin
    ON properties USING gin (owner_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_properties_co_owner_norm_gin
    ON properties USING gin (co_owner_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_businesses_name_gin
    ON businesses USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_principals_name_c_gin
    ON principals USING gin (name_c gin_trgm_ops);
"""

DDL_OWNERSHIP_TABLES = """
CREATE TABLE IF NOT EXISTS ownership_networks (
    id SERIAL PRIMARY KEY,
    root_entity_id TEXT NOT NULL,
    root_entity_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ownership_links (
    network_id INTEGER REFERENCES ownership_networks(id) ON DELETE CASCADE,
    from_entity TEXT NOT NULL,
    to_entity TEXT NOT NULL,
    link_type TEXT NOT NULL,
    PRIMARY KEY (network_id, from_entity, to_entity)
);
"""

DDL_ADD_GEO_COLUMNS = """
ALTER TABLE properties ADD COLUMN IF NOT EXISTS latitude NUMERIC;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS longitude NUMERIC;
"""

# Cached AI reports
DDL_AI_REPORTS = """
CREATE TABLE IF NOT EXISTS ai_reports (
    id SERIAL PRIMARY KEY,
    entity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    report_date DATE NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    sources JSONB,
    created_at TIMESTAMP DEFAULT now(),
    UNIQUE (entity, entity_type, report_date)
);
CREATE INDEX IF NOT EXISTS idx_ai_reports_entity
    ON ai_reports(entity, entity_type, report_date);
"""

# Simple Key-Value store for caching complex objects like insights
DDL_KV_CACHE = """
CREATE TABLE IF NOT EXISTS kv_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def backfill_owner_norm_columns(conn) -> None:
    with conn.cursor() as c:
        logger.info("Backfilling owner_norm / co_owner_norm where NULL...")
        c.execute("""
            UPDATE properties
            SET owner_norm = normalize_person_name(owner)
            WHERE owner IS NOT NULL AND owner_norm IS NULL
        """)
        logger.info("Rows updated (owner_norm): %s", c.rowcount)
        c.execute("""
            UPDATE properties
            SET co_owner_norm = normalize_person_name(co_owner)
            WHERE co_owner IS NOT NULL AND co_owner_norm IS NULL
        """)
        logger.info("Rows updated (co_owner_norm): %s", c.rowcount)
    conn.commit()


@contextmanager
def cursor_context():
    """Context manager for getting a cursor from the pool and ensuring the connection is returned."""
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool not initialized")
    conn = db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)

@app.on_event("startup")
def startup_event():
    global db_pool
    retries = 60
    while retries > 0:
        try:
            db_pool = pool.SimpleConnectionPool(1, 40, dsn=DATABASE_URL)
            break
        except psycopg2.OperationalError as e:
            retries -= 1
            logger.warning(f"DB not ready; retrying... ({retries} left). Error: {e}")
            time.sleep(5)

    if db_pool is None:
        logger.error("Could not connect to DB after retries. Exiting.")
        sys.exit(1)

    conn = db_pool.getconn()
    try:
        with conn.cursor() as c:
            # c.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            # # c.execute("DROP FUNCTION IF EXISTS normalize_person_name(TEXT)")
            # c.execute(DDL_NORMALIZE_FUNCTION)
            # c.execute(DDL_ADD_OWNER_NORM)
            # c.execute(DDL_ADD_GEO_COLUMNS)
            # c.execute(DDL_OWNERSHIP_TABLES)
            # c.execute(DDL_AI_REPORTS)
            # c.execute(DDL_KV_CACHE)
            # c.execute(DDL_INDEXES)
            # V3 Schema Updates
            # 1. Custom Unit Columns
            c.execute("ALTER TABLE group_properties ADD COLUMN IF NOT EXISTS custom_unit TEXT;")
            c.execute("ALTER TABLE group_properties ADD COLUMN IF NOT EXISTS custom_address TEXT;")
            
            # 2. Photos Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS property_photos (
                    id SERIAL PRIMARY KEY,
                    group_id INT NOT NULL,
                    property_id INT NOT NULL,
                    url TEXT NOT NULL,
                    caption TEXT,
                    uploaded_by INT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)

            # 3. Assignees Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS group_property_assignees (
                    id SERIAL PRIMARY KEY,
                    group_id INT NOT NULL,
                    property_id INT NOT NULL,
                    user_id INT NOT NULL,
                    assigned_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(group_id, property_id, user_id)
                );
            """)

            # 4. Playground Features: Custom Units (Separate Table)
            c.execute("""
                CREATE TABLE IF NOT EXISTS group_custom_units (
                    id SERIAL PRIMARY KEY,
                    group_id INT NOT NULL,
                    complex_id INT, -- Can be NULL if unassigned
                    name TEXT NOT NULL,
                    description TEXT,
                    created_by INT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            c.execute("ALTER TABLE group_properties ADD COLUMN IF NOT EXISTS is_custom BOOLEAN DEFAULT FALSE;")
            c.execute("ALTER TABLE group_properties ADD COLUMN IF NOT EXISTS custom_name TEXT;")
        conn.commit()
        # backfill_owner_norm_columns(conn) # Commented out to prevent startup block
        logger.info("✅ Startup DB bootstrap completed.")
        
        logger.info("Triggering initial insights cache refresh in the background...")
        thread = threading.Thread(target=_update_insights_cache_sync, daemon=True)
        thread.start()

    finally:
        db_pool.putconn(conn)


def get_db_connection():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database connection unavailable")
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


# ------------------------------------------------------------
# Models
# ------------------------------------------------------------
class SearchResult(BaseModel):
    id: str
    name: str
    type: str
    context: Optional[str] = None

class Entity(BaseModel):
    id: str
    name: str
    type: str
    details: Optional[Dict[str, Any]] = None

class PropertyItem(BaseModel):
    address: Optional[str]
    unit: Optional[str] = None
    city: Optional[str]
    owner: Optional[str]
    assessed_value: Optional[float]
    details: Dict[str, Any]
    subsidies: Optional[List[Dict[str, Any]]] = []
    complex_name: Optional[str] = None
    management_company: Optional[str] = None

class NetworkStep(BaseModel):
    entity_id: str
    entity_type: str
    depth: int = 1

class IncrementalNetworkResponse(BaseModel):
    new_entities: List[Entity]
    new_properties: List[PropertyItem]
    new_links: Dict[str, List[str]]
    has_more: bool
    next_entities: List[Dict[str, str]]

class ReportItem(BaseModel):
    key: str
    value: str

class Report(BaseModel):
    title: str
    data: List[ReportItem]

class AIReportRequest(BaseModel):
    entity: str
    entity_type: str  # 'owner' | 'business'
    force: bool = False

class CachedReportInfo(BaseModel):
    norm_name: str
    entity_name: str
    created_at: datetime
    size: int

class NetworkLoadRequest(BaseModel):
    entity_id: str
    entity_type: str
    entity_name: Optional[str] = None

class PrincipalInfo(BaseModel):
    name: str
    state: Optional[str] = None

class BusinessInfo(BaseModel):
    name: str
    state: Optional[str] = None

class InsightItem(BaseModel):
    entity_id: str
    entity_name: str
    entity_type: str
    value: int
    total_assessed_value: Optional[float] = None
    total_appraised_value: Optional[float] = None
    subsidized_property_count: Optional[int] = 0
    subsidy_programs: Optional[List[str]] = None
    controlling_business_name: Optional[str] = None
    controlling_business_id: Optional[str] = None
    business_count: Optional[int] = 0
    principals: Optional[List[PrincipalInfo]] = None
    businesses: Optional[List[BusinessInfo]] = None
    network_id: Optional[int] = None
    city: Optional[str] = None
    representative_entities: Optional[List[dict]] = None




# ------------------------------------------------------------
# BATCH GEOCODING
# ------------------------------------------------------------
from api.geocoding_utils import geocode_census, geocode_nominatim

class GeocodeResult(BaseModel):
    id: str
    lat: float
    lon: float

class BatchGeocodeRequest(BaseModel):
    property_ids: List[int]

@app.post("/api/geocoding/batch", response_model=List[GeocodeResult])
def batch_geocode_properties(req: BatchGeocodeRequest, conn=Depends(get_db_connection)):
    """
    Parallel geocoding for on-the-fly requests.
    """
    if not req.property_ids:
        return []
        
    logger.info(f"Batch geocoding request for {len(req.property_ids)} properties.")
    results = []
    
    # 1. Fetch address info for these IDs if they don't have coords
    to_process = []
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            cursor.execute("""
                SELECT id, location, property_city, property_zip, latitude, longitude
                FROM properties
                WHERE id = ANY(%s::bigint[])
            """, (req.property_ids,))
            
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Database error in batch_geocode_properties: {e}")
            raise HTTPException(status_code=500, detail="Database query failed during batch geocoding")
        
        for r in rows:
            # If already has coords, return them
            if r['latitude'] and r['longitude']:
                results.append(GeocodeResult(id=str(r['id']), lat=float(r['latitude']), lon=float(r['longitude'])))
            elif r['location']:
                # Needs geocoding
                to_process.append(r)

    # 2. Process in parallel
    if to_process:
        with ThreadPoolExecutor(max_workers=50) as executor: # Higher workers for IO bound
            future_to_id = {}
            for row in to_process:
                address_full = f"{row['location']}, {row['property_city'] or ''}, CT {row['property_zip'] or ''}".strip()
                future = executor.submit(geocode_census, address_full)
                future_to_id[future] = (row['id'], address_full)
            
            # Collect results
            updates = []
            for future in as_completed(future_to_id):
                pid, addr = future_to_id[future]
                try:
                    lat, lon = future.result()
                    if not lat:
                         # Fallback to Nominatim (sequential inside the thread or just call it)
                         # Note: Nominatim is strictly rate limited. doing it in parallel threads might get banned.
                         # Ideally we skip nominatim in parallel batch or do it very carefully.
                         # For now, let's try it with a lock or just skip it to be safe and fast.
                         # Getting blocked by Nominatim would break the app.
                         # Let's try one attempt.
                         lat, lon = geocode_nominatim(addr)

                    if lat and lon:
                        results.append(GeocodeResult(id=str(pid), lat=lat, lon=lon))
                        updates.append((lat, lon, pid))
                except Exception as e:
                    logger.error(f"Error geocoding {pid}: {e}")

            # 3. Bulk Update DB
            if updates:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, """
                        UPDATE properties SET latitude = %s, longitude = %s WHERE id = %s
                    """, updates)
                conn.commit()

    return results

# ------------------------------------------------------------
# AI ANALYSIS
# ------------------------------------------------------------
@app.get("/api/ai_analysis")
def get_ai_analysis(entity_name: str, entity_type: str):
    """
    1. Search Google via SerpAPI for news/context.
    2. If OpenAI available, summarize finding.
    """
    if not SERPAPI_API_KEY:
        return {"summary": "SerpAPI not configured.", "sources": [], "risk": "Unknown"}

    # Construct query
    query = f"{entity_name} Connecticut real estate"
    if entity_type == 'business':
        query += " business LLC"
    else:
        query += " landlord property owner"

    # Call SerpAPI
    try:
        params = {
            "q": query,
            "api_key": SERPAPI_API_KEY,
            "tbm": "nws", # News search
            "num": 5
        }
        resp = requests.get("https://serpapi.com/search", params=params)
        data = resp.json()
        
        # Fallback to web search if no news
        if "error" in data or not data.get("news_results"):
             params.pop("tbm")
             resp = requests.get("https://serpapi.com/search", params=params)
             data = resp.json()

        results = data.get("news_results", []) or data.get("organic_results", [])
        
        snippets = []
        sources = []
        for r in results[:5]:
            title = r.get("title", "")
            snippet = r.get("snippet", "")
            link = r.get("link", "")
            source = r.get("source", "Web")
            snippets.append(f"- {title}: {snippet}")
            sources.append({"title": title, "link": link, "source": source})

        if not snippets:
            return {"summary": "No public news records found.", "sources": [], "risk": "Low"}

        # Summarize with OpenAI
        summary_text = "Found recent mentions."
        risk_level = "Unknown"
        
        if openai and OPENAI_API_KEY:
            try:
                system_prompt = (
                    "You are a real estate investigator. Analyze these search snippets about a landlord/entity. "
                    "Provide a 1-2 sentence summary of their reputation and mention any legal issues or controversies. "
                    "Classify risk as Low, Moderate, or High."
                )
                user_msg = f"Entity: {entity_name}\nSnippets:\n" + "\n".join(snippets)
                
                chat_completion = openai.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg}
                    ],
                    max_tokens=100
                )
                content = chat_completion.choices[0].message.content
                summary_text = content
                if "High" in content: risk_level = "High"
                elif "Moderate" in content: risk_level = "Moderate"
                else: risk_level = "Low"
            except Exception as e:
                logger.error(f"OpenAI error: {e}")
                summary_text = "AI Summary unavailable. Reference sources below."

        return {
            "summary": summary_text,
            "sources": [s['link'] for s in sources], # Simplified for frontend
            "risk": risk_level
        }

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        return {"summary": "Analysis failed.", "sources": [], "risk": "Unknown"}


# ------------------------------------------------------------
# AUTOCOMPLETE
# ------------------------------------------------------------
@app.get("/api/autocomplete")
def autocomplete(q: str, type: str, conn=Depends(get_db_connection)):
    """
    Fast prefix matching for search suggestions.
    type: 'business' | 'owner' | 'address'
    """
    if not q: return []
    q = q.strip()
    
    if len(q) < 2:
        return []

    limit = 50
    limit_extended = 20 # Fetch more to allow for deduping
    results = []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            t_prefix = q.lower() + "%"
            t_infix = "%" + q + "%"
            
            # Construct a flexible pattern for address matching:
            # "304 Barbour" -> "304%Barbour%" matches "304-306 Barbour St"
            t_flexible = q.replace(" ", "%") + "%"

            if type == "business":
                cursor.execute(
                    "SELECT DISTINCT id, name FROM businesses WHERE lower(name) LIKE %s ORDER BY name ASC LIMIT %s",
                    (t_prefix, limit)
                )
                results = [{"label": r["name"], "value": r["name"], "id": str(r["id"]), "type": "Business", "context": "Business Entity"} for r in cursor.fetchall()]
                
                if len(results) < limit:
                    cursor.execute(
                        "SELECT id, name FROM businesses WHERE name ILIKE %s LIMIT %s",
                        (t_infix, limit - len(results))
                    )
                    existing = {x["value"] for x in results}
                    for r in cursor.fetchall():
                        if r["name"] not in existing:
                            results.append({"label": r["name"], "value": r["name"], "id": str(r["id"]), "type": "Business", "context": "Business Entity"})

            elif type == "owner":
                # UNIFIED SEARCH: "Owner" now means "All" (Principals, Businesses, Properties)
                # Optimized for "Infix" search with gin_trgm_ops indices
                t_infix = f"%{q.strip()}%"

                # Helpers
                def fmt_princ(biz):
                    if not biz: return "Business Principal"
                    bs = [x for x in biz.split('||') if x]
                    if not bs: return "Business Principal"
                    return f"Principal of {bs[0]}" + (f" + {len(bs)-1} more" if len(bs)>1 else "")

                def fmt_owner(loc, city, zip_code):
                    parts = []
                    if loc: parts.append(loc)
                    if city: parts.append(city)
                    parts.append("CT")
                    return "Owner of " + ", ".join(parts) if loc else "Property Owner"

                def fmt_co_owner(loc, city, zip_code):
                    parts = []
                    if loc: parts.append(loc)
                    if city: parts.append(city)
                    parts.append("CT")
                    return "Co-Owner of " + ", ".join(parts) if loc else "Property Co-Owner"

                # 1. Search Principals
                # Uses idx_principals_norm_name_trgm on normalize_person_name(name_c)
                cursor.execute(
                    """
                    SELECT 
                        mode() WITHIN GROUP (ORDER BY p.name_c) as name, 
                        string_agg(DISTINCT b.name, '||') as businesses 
                    FROM principals p 
                    LEFT JOIN businesses b ON p.business_id = b.id
                    WHERE p.name_c IS NOT NULL AND normalize_person_name(p.name_c) ILIKE %s 
                    GROUP BY normalize_person_name(p.name_c)
                    LIMIT %s
                    """,
                    (t_infix, limit)
                )
                principal_results = [{
                    "label": r["name"], "value": r["name"], 
                    "type": "Business Principal", "context": fmt_princ(r["businesses"])
                } for r in cursor.fetchall()]

                # 2. Search Property Owners
                # Uses idx_properties_owner_norm_trgm
                cursor.execute(
                    """
                    SELECT 
                        mode() WITHIN GROUP (ORDER BY owner) as name, 
                        location as loc, property_city as city, property_zip as zip
                    FROM properties 
                    WHERE owner_norm IS NOT NULL AND owner_norm ILIKE %s 
                    GROUP BY owner_norm, location, property_city, property_zip
                    limit %s
                    """,
                    (t_infix, limit)
                )
                owner_results = []
                # Use a set to avoid duplicates within owners
                seen_owners = set()
                for r in cursor.fetchall():
                    # Dedupe within same type by name
                # 2. Search Principals/Owners (Unified)
                # ... (omitted) ...
                    # Dedupe within same type by name
                    if r["name"] not in seen_owners:
                        ctx = fmt_owner(r["loc"], r["city"], r["zip"])
                        owner_results.append({
                            "label": r["name"], "value": r["name"], "id": r["name"],
                            "type": "Property Owner", "context": ctx
                        })
                        seen_owners.add(r["name"])

                # 3. Search Co-Owners
                # ...
                # ...
                    if r["name"] not in seen_co:
                        ctx = fmt_co_owner(r["loc"], r["city"], r["zip"])
                        co_owner_results.append({
                            "label": r["name"], "value": r["name"], "id": r["name"],
                            "type": "Property Co-Owner", "context": ctx
                        })
                        seen_co.add(r["name"])

                # ... (Merge logic unchanged) ...

            elif type == "address":
                cursor.execute(
                    """
                    SELECT MAX(id) as id, location, property_city, property_zip, MAX(similarity(location, %s)) as rank
                    FROM properties 
                    WHERE location IS NOT NULL AND location %% %s 
                    GROUP BY location, property_city, property_zip
                    ORDER BY rank DESC, location ASC 
                    LIMIT %s
                    """,
                    (q, q, limit)
                )
                
                results = []
                for r in cursor.fetchall():
                    parts = [r["location"]]
                    if r.get("property_city"): parts.append(r["property_city"])
                    parts.append("CT")
                    if r.get("property_zip"):
                        # Handle float-like zips from DB (e.g. 6107.0)
                        raw_zip = r["property_zip"]
                        try:
                            z_int = int(float(raw_zip))
                            z = f"{z_int:05d}"
                        except Exception:
                            z = str(raw_zip).strip()
                            if len(z) < 5 and z.isdigit(): z = z.zfill(5)
                        parts.append(z)
                    results.append({
                        "label": ", ".join(parts),
                        "value": r["location"],
                        "id": str(r['id']),
                        "type": "Address",
                        "context": r.get("property_city", "")
                    })

    except Exception as e:
        logger.error(f"Autocomplete Error: {e}")
        return []

    return results[:limit]


# ------------------------------------------------------------
# SEARCH
# ------------------------------------------------------------
@app.get("/api/search", response_model=List[SearchResult])
def search_entities(type: str, term: str, conn=Depends(get_db_connection)):
    """
    type: 'business' | 'owner' | 'address'
    """
    if len(term or "") < 3:
        raise HTTPException(status_code=400, detail="Search term must be at least 3 characters long.")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            t = term.upper()

            if type == "business":
                cursor.execute(
                    "SELECT id, name, business_address AS context FROM businesses WHERE upper(name) LIKE %s LIMIT 50",
                    (f"%{t}%",)
                )
                rows = cursor.fetchall()
                return [SearchResult(id=str(r["id"]), name=r["name"], type="business", context=r.get("context")) for r in rows]

            elif type == "owner":
                # UNIFIED SEARCH: "Owner" now means "All" (Principals, Businesses, Properties)
                results: List[SearchResult] = []
                t_exact = f"%{t}%"

                # 1. Search Principals
                # ------------------------------------------------------------------
                name_vars = get_name_variations(term, "principal")
                norm_set = list({ normalize_person_name_py(v) for v in name_vars if v })
                
                # Direct Principal Match
                cursor.execute(
                    """
                    SELECT DISTINCT name_c AS name
                    FROM principals
                    WHERE name_c IS NOT NULL AND upper(name_c) LIKE %s
                    LIMIT 50
                    """,
                    (t_exact,)
                )
                for r in cursor.fetchall():
                    if r["name"]:
                        results.append(SearchResult(id=r["name"], name=r["name"], type="owner", context="Principal"))

                # Property Owner Match (if we have normalized vars)
                # Property Owner Match (if we have normalized vars)
                # Strategy: 
                # 1. Try strict match on owner_norm (indexed, fast)
                # 2. If valid vars exist, also try a LIKE match on raw owner/co_owner columns for robustness
                #    (This helps when normalization might be slightly off or for partial names)
                
                params = []
                where_clauses = []
                
                if norm_set:
                    where_clauses.append("owner_norm = ANY(%s)")
                    params.append(norm_set)
                    where_clauses.append("co_owner_norm = ANY(%s)")
                    params.append(norm_set)
                
                # Also Add partial match on the input term itself against raw columns
                where_clauses.append("upper(owner) LIKE %s")
                params.append(t_exact)
                where_clauses.append("upper(co_owner) LIKE %s")
                params.append(t_exact)
                
                if where_clauses:
                    sql = f"""
                        SELECT DISTINCT owner AS name
                        FROM properties
                        WHERE {' OR '.join(where_clauses)}
                        LIMIT 50
                    """
                    cursor.execute(sql, params)
                    for r in cursor.fetchall():
                        if r["name"]:
                            # Avoid duplicates if possible, but list append is fast
                            if not any(x.id == r["name"] and x.type == "owner" for x in results):
                                results.append(SearchResult(id=r["name"], name=r["name"], type="owner", context="Property Owner"))


                # 2. Search Businesses
                # ------------------------------------------------------------------
                cursor.execute(
                    "SELECT id, name, business_address AS context FROM businesses WHERE upper(name) LIKE %s LIMIT 20",
                    (t_exact,)
                )
                for r in cursor.fetchall():
                    results.append(SearchResult(id=str(r["id"]), name=r["name"], type="business", context=r.get("context")))

                # 3. Search Properties (Addresses)
                # ------------------------------------------------------------------
                cursor.execute(
                    """
                    SELECT id, location, owner, co_owner, property_city, business_id
                    FROM properties
                    WHERE location %% %s
                    ORDER BY similarity(location, %s) DESC
                    LIMIT 20
                    """,
                    (t, t)
                )

                prop_rows = cursor.fetchall()

                # Resolve context for properties (Business vs Owner)
                if prop_rows:
                    business_ids = {str(r['business_id']) for r in prop_rows if r.get('business_id')}
                    biz_map = {}
                    if business_ids:
                         cursor.execute("SELECT id, name FROM businesses WHERE id::text = ANY(%s)", (list(business_ids),))
                         for b in cursor.fetchall():
                             biz_map[str(b['id'])] = b['name']

                    seen_locs = set()
                    for r in prop_rows:
                        loc = r['location']
                        if loc in seen_locs: continue
                        
                        target_id = None
                        target_type = None
                        # Resolve context for the dropdown
                        if r.get('business_id') and str(r['business_id']) in biz_map:
                             ctx = f"Owned by {biz_map[str(r['business_id'])]}"
                        elif r.get('owner'):
                             ctx = f"Owner: {r['owner']}"
                        
                        if loc:
                            results.append(SearchResult(
                                id=loc,
                                name=loc,
                                type="address",
                                context=ctx
                            ))
                            seen_locs.add(loc)

                # Deduplicate final list by ID+Type just in case
                unique_results = []
                seen_ids = set()
                for res in results:
                    key = f"{res.type}:{res.id}:{res.name}"
                    if key not in seen_ids:
                        unique_results.append(res)
                        seen_ids.add(key)
                
                return unique_results[:50]

            elif type == "address":
                # Use pg_trgm for fuzzy matching, order by similarity, and fetch co_owner.
                cursor.execute(
                    """
                    SELECT location, owner, co_owner, property_city, business_id
                    FROM properties
                    WHERE location %% %s
                    ORDER BY similarity(location, %s) DESC
                    LIMIT 50
                    """,
                    (t, t)
                )
                rows = cursor.fetchall()
                if not rows:
                    return []

                # Batch collect business IDs and potential owner names for efficient lookup
                business_ids = {str(r['business_id']) for r in rows if r.get('business_id')}
                owner_names = {name.upper() for r in rows for name in (r.get('owner'), r.get('co_owner')) if name}

                # Create lookup maps for businesses found by ID or name
                business_info_by_id = {}
                business_info_by_name = {}

                if business_ids:
                    cursor.execute(
                        "SELECT id, name FROM businesses WHERE id::text = ANY(%s)",
                        (list(business_ids),)
                    )
                    for b in cursor.fetchall():
                        business_info_by_id[str(b['id'])] = {'name': b['name'], 'id': str(b['id'])}

                if owner_names:
                    cursor.execute(
                        "SELECT id, name, upper(name) as upper_name FROM businesses WHERE upper(name) = ANY(%s)",
                        (list(owner_names),)
                    )
                    for b in cursor.fetchall():
                        business_info_by_name[b['upper_name']] = {'name': b['name'], 'id': str(b['id'])}

                results: List[SearchResult] = []
                seen_locations = set()
                for r in rows:
                    if r.get('location') in seen_locations:
                        continue

                    entity_id, entity_type, context_owner_name = None, None, None

                    if r.get('business_id') and str(r['business_id']) in business_info_by_id:
                        biz = business_info_by_id[str(r['business_id'])]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    elif r.get('owner') and r['owner'].upper() in business_info_by_name:
                        biz = business_info_by_name[r['owner'].upper()]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    elif r.get('co_owner') and r['co_owner'].upper() in business_info_by_name:
                        biz = business_info_by_name[r['co_owner'].upper()]
                        entity_id = biz['id']
                        entity_type = 'business'
                        context_owner_name = biz['name']
                    else:
                        primary_owner = r.get('owner') or r.get('co_owner')
                        if primary_owner:
                            entity_id = primary_owner
                            entity_type = 'owner'
                            context_owner_name = primary_owner

                    if entity_id:
                        # For address search results we want the UI to receive the
                        # actual entity to load in the `name` field (business or owner)
                        # and the address as the `context` so the frontend can display
                        # the address and pivot to the owner's/business's network.
                        display_name = context_owner_name if context_owner_name else r.get("location")
                        results.append(SearchResult(
                            id=str(entity_id),
                            name=display_name,
                            type=entity_type,
                            context=r.get("location")
                        ))
                        seen_locations.add(r['location'])

                return results

            else:
                raise HTTPException(status_code=400, detail="Invalid search type.")
    except psycopg2.Error:
        logger.exception("Database search error")
        raise HTTPException(status_code=500, detail="Database query failed.")


# ------------------------------------------------------------
# Incremental network expansion (non-streaming JSON)
# ------------------------------------------------------------
@app.post("/api/network/step", response_model=IncrementalNetworkResponse)
def get_network_step(step: NetworkStep, conn=Depends(get_db_connection)):
    """
    Restored behavior but in a single JSON payload (used by some UIs for incremental load).
    Reads from precomputed entity_networks; isolated fallback mirrors stream_load.
    """
    new_entities: Dict[str, Entity] = {}
    new_properties: Dict[int, Dict[str, Any]] = {}
    new_links: Dict[str, Set[str]] = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        network_id = None
        if step.entity_type == "business":
            cursor.execute(
                "SELECT network_id FROM entity_networks WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                (step.entity_id,)
            )
            row = cursor.fetchone()
            if row:
                network_id = row["network_id"]
        else:
            pname_norm = normalize_person_name_py(step.entity_id)
            cursor.execute(
                "SELECT network_id FROM entity_networks WHERE entity_type = 'principal' AND entity_id = %s LIMIT 1",
                (pname_norm,)
            )
            row = cursor.fetchone()
            if row:
                network_id = row["network_id"]

        if not network_id:
            # isolated fallback
            if step.entity_type == "business":
                cursor.execute("SELECT * FROM businesses WHERE id = %s", (step.entity_id,))
                b = cursor.fetchone()
                if b:
                    b_key = f"business_{b['id']}"
                    new_entities[b_key] = Entity(id=b["id"], name=b["name"], type="business", details=b)
                    cursor.execute("SELECT * FROM properties WHERE business_id = %s", (b["id"],))
                    for p in cursor.fetchall():
                        new_properties[p["id"]] = p
            else:
                pname_norm = normalize_person_name_py(step.entity_id)
                p_key = f"principal_{pname_norm}"
                new_entities[p_key] = Entity(id=pname_norm, name=step.entity_id, type="principal", details={})
                cursor.execute(
                    "SELECT * FROM properties WHERE principal_id = %s OR owner_norm = %s OR co_owner_norm = %s",
                    (pname_norm, pname_norm, pname_norm)
                )
                for p in cursor.fetchall():
                    new_properties[p["id"]] = p

            # Fetch subsidies for isolated properties
            isolated_ids = list(new_properties.keys())
            isolated_subsidies_map = defaultdict(list)
            if isolated_ids:
                cursor.execute("""
                    SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                    FROM property_subsidies
                    WHERE property_id = ANY(%s)
                """, (isolated_ids,))
                for s_row in cursor.fetchall():
                    isolated_subsidies_map[s_row['property_id']].append(dict(s_row))

            return IncrementalNetworkResponse(
                new_entities=list(new_entities.values()),
                new_properties=[
                    PropertyItem(
                        address=v.get("location"),
                        city=v.get("property_city"),
                        owner=v.get("owner"),
                        assessed_value=v.get("assessed_value"),
                        subsidies=isolated_subsidies_map.get(v['id'], []),
                        details=v,
                    ) for v in new_properties.values()
                ],
                new_links={k: list(v) for k, v in new_links.items()},
                has_more=False,
                next_entities=[],
            )

        # Full network
        cursor.execute(
            "SELECT b.* "
            "FROM entity_networks en JOIN businesses b ON b.id::text = en.entity_id "
            "WHERE en.network_id = %s AND en.entity_type = 'business'",
            (network_id,)
        )
        businesses = cursor.fetchall()
        biz_ids = [b["id"] for b in businesses]
        for b in businesses:
            new_entities[f"business_{b['id']}"] = Entity(id=b["id"], name=b["name"], type="business", details=b)

        cursor.execute(
            "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
            "FROM entity_networks WHERE network_id = %s AND entity_type = 'principal'",
            (network_id,)
        )
        principals = cursor.fetchall()
        principal_ids = [r["principal_id"] for r in principals]
        for pr in principals:
            pkey = f"principal_{normalize_person_name_py(pr['principal_id'])}"
            new_entities[pkey] = Entity(id=pr["principal_id"], name=pr.get("principal_name") or pr["principal_id"], type="principal", details={"name_c": pr.get("principal_name")})

        if biz_ids:
            cursor.execute(
                "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                "FROM principals WHERE business_id = ANY(%s)",
                (biz_ids,)
            )
            for r in cursor.fetchall():
                if not r.get("pname"):
                    continue
                b_key = f"business_{r['business_id']}"
                p_key = f"principal_{normalize_person_name_py(r['pname'])}"
                new_links.setdefault(b_key, set()).add(p_key)
                new_links.setdefault(p_key, set()).add(b_key)

        cursor.execute(
            "SELECT * FROM properties WHERE (business_id = ANY(%s)) OR (principal_id = ANY(%s))",
            (biz_ids or [None], principal_ids or [None])
        )
        for p in cursor.fetchall():
            new_properties[p["id"]] = p

        # Fetch subsidies for network properties
        network_prop_ids = list(new_properties.keys())
        network_subsidies_map = defaultdict(list)
        if network_prop_ids:
            cursor.execute("""
                SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                FROM property_subsidies
                WHERE property_id = ANY(%s)
            """, (network_prop_ids,))
            for s_row in cursor.fetchall():
                network_subsidies_map[s_row['property_id']].append(dict(s_row))

    return IncrementalNetworkResponse(
        new_entities=list(new_entities.values()),
        new_properties=[
            PropertyItem(
                address=v.get("location"),
                city=v.get("property_city"),
                owner=v.get("owner"),
                assessed_value=v.get("assessed_value"),
                subsidies=network_subsidies_map.get(v['id'], []),
                details=v,
            ) for v in new_properties.values()
        ],
        new_links={k: list(v) for k, v in new_links.items()},
        has_more=False,
        next_entities=[],
    )



# ------------------------------------------------------------
# Streaming NDJSON (back-compat with existing UI reader)
# ------------------------------------------------------------
from fastapi import Request

@app.post("/api/network/stream_load")
async def stream_load_network(req: Request, conn=Depends(get_db_connection)):
    """
    Restored: use precomputed entity_networks when available,
    otherwise fall back to isolated owner/business view.
    Streams NDJSON frames: entities, properties, done.
    """
    payload = await req.json()
    entity_id = (
        payload.get("entity_id")
        or payload.get("entityId")
        or payload.get("id")
        or payload.get("entity_name")
        or payload.get("name")
    )
    entity_type = (
        payload.get("entity_type")
        or payload.get("entityType")
        or payload.get("type")
        or "owner"
    )
    entity_name = payload.get("entity_name") or payload.get("name") or entity_id

    if not entity_id:
        raise HTTPException(status_code=400, detail="Missing entity_id/name")

    def _yield(s: str):
        return s + "\n"

    def _principal_key(name: str) -> str:
        return f"principal_{normalize_person_name_py(name)}"

    def generate_network_data():
        nonlocal entity_type, entity_id, entity_name
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                network_ids = []

                if entity_type == "business":
                    cursor.execute(
                        "SELECT network_id FROM entity_networks "
                        "WHERE entity_type = 'business' AND entity_id = %s LIMIT 1",
                        (entity_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        network_ids = [row["network_id"]]

                elif entity_type == "address":
                    # Lookup property by exact location (assuming entity_id passed is the address string)
                    # Fetch owner details too so we can fall back to isolated view if no network
                    cursor.execute(
                        "SELECT p.business_id, p.principal_id, p.owner_norm, p.owner, p.co_owner, b.name as business_name "
                        "FROM properties p "
                        "LEFT JOIN businesses b ON p.business_id = b.id "
                        "WHERE p.location = %s LIMIT 1",
                        (entity_id,) 
                    )
                    prop = cursor.fetchone()
                    if prop:
                         # Try to pivot to the owner's network
                         if prop["business_id"]:
                             cursor.execute("SELECT network_id FROM entity_networks WHERE entity_type='business' AND entity_id=%s", (str(prop["business_id"]),))
                             row = cursor.fetchone()
                             if row: network_ids = [row["network_id"]]
                         
                         if not network_ids and prop["principal_id"]:
                             # Resolve principal ID to name first (since we link by name_c mostly)
                             cursor.execute("SELECT name_c FROM principals WHERE id=%s", (prop["principal_id"],))
                             p_row = cursor.fetchone()
                             if p_row:
                                 cursor.execute("SELECT network_id FROM entity_networks WHERE entity_type='principal' AND entity_id=%s", (p_row["name_c"],))
                                 row = cursor.fetchone()
                                 if row: network_ids = [row["network_id"]]
                                 
                         if not network_ids and prop["owner_norm"]:
                             cursor.execute("SELECT network_id FROM entity_networks WHERE entity_type='principal' AND entity_id=%s", (prop["owner_norm"],))
                             row = cursor.fetchone()
                             if row: network_ids = [row["network_id"]]
                        
                         # --- FALLBACK: If no network found, redirect to Isolated Owner View ---
                         if not network_ids:
                             if prop["business_id"]:
                                 entity_type = "business"
                                 entity_id = str(prop["business_id"])
                                 entity_name = prop.get("business_name")
                             else:
                                 entity_type = "principal"
                                 # Use explicit owner name for the isolated view title
                                 entity_name = prop.get("owner") or prop.get("co_owner") or "Unknown Owner"
                                 entity_id = entity_name

                else:
                    pname_norm = normalize_person_name_py(entity_name or entity_id)
                    # Fetch ALL networks this principal is part of
                    cursor.execute(
                        "SELECT network_id FROM entity_networks "
                        "WHERE entity_type = 'principal' AND (entity_id = %s OR normalized_name = %s)",
                        (entity_id, pname_norm)
                    )
                    rows = cursor.fetchall()
                    if rows:
                        network_ids = [r["network_id"] for r in rows]
                    
                    # Fallback: If ID didn't match, check if ID exists in principals table and try matching by that name
                    if not network_ids and entity_id.isdigit():
                         cursor.execute("SELECT name_c FROM principals WHERE id = %s", (entity_id,))
                         pr_row = cursor.fetchone()
                         if pr_row and pr_row['name_c']:
                             fallback_name = pr_row['name_c']
                             fallback_norm = normalize_person_name_py(fallback_name)
                             cursor.execute(
                                 "SELECT network_id FROM entity_networks "
                                 "WHERE entity_type = 'principal' AND (normalized_name = %s OR entity_name = %s)",
                                 (fallback_norm, fallback_name)
                             )
                             rows = cursor.fetchall()
                             if rows:
                                 network_ids = [r["network_id"] for r in rows]
                             
                    # Fallback 2: Check by entity_name from payload (e.g. "Menachem Gurevitch")
                    if not network_ids and entity_name and entity_name != entity_id:
                         fallback_norm = normalize_person_name_py(entity_name)
                         cursor.execute(
                             "SELECT network_id FROM entity_networks "
                             "WHERE entity_type = 'principal' AND (normalized_name = %s OR entity_name = %s)",
                             (fallback_norm, entity_name)
                         )
                         rows = cursor.fetchall()
                         if rows:
                             network_ids = [r["network_id"] for r in rows]




                # --- If no network found → isolated view
                if not network_ids:
                    if entity_type == "business":
                        cursor.execute("SELECT * FROM businesses WHERE id = %s", (entity_id,))
                        business = cursor.fetchone()
                        if not business:
                            yield _yield(json.dumps({"type": "done", "data": "Entity not found"}))
                            return
                        ent = {
                            "id": business["id"],
                            "name": business["name"],
                            "type": "business",
                            "status": business.get("status"),
                            "details": business,
                            "connections": [],
                        }
                        yield _yield(json.dumps(
                            {"type": "entities", "data": {"entities": [ent], "links": {}}},
                            default=json_converter
                        ))

                        cursor.execute("SELECT * FROM properties WHERE business_id = %s", (entity_id,))
                        all_properties = cursor.fetchall()
                        grouped_properties = group_properties_into_complexes(all_properties)
                        for prop_or_complex in grouped_properties:
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [prop_or_complex]},
                                default=json_converter
                            ))

                    else:
                        pname_norm = normalize_person_name_py(entity_name or entity_id)
                        ent = {
                            "id": pname_norm,
                            "name": entity_name or entity_id,
                            "type": "principal",
                            "details": {},
                            "connections": [],
                        }
                        yield _yield(json.dumps(
                            {"type": "entities", "data": {"entities": [ent], "links": {}}},
                            default=json_converter
                        ))

                        cursor.execute(
                            "SELECT * FROM properties "
                            "WHERE principal_id = %s OR owner_norm = %s OR co_owner_norm = %s OR owner = %s",
                            (pname_norm, pname_norm, pname_norm, entity_name)
                        )
                        all_properties = cursor.fetchall()
                        grouped_properties = group_properties_into_complexes(all_properties)
                        for prop_or_complex in grouped_properties:
                            yield _yield(json.dumps(
                                {"type": "properties", "data": [prop_or_complex]},
                                default=json_converter
                            ))

                    yield _yield(json.dumps({"type": "done"}))
                    return

                # --- If network found → load entire network (businesses, principals, properties)
                # Lookup "Human" Name from Cached Insights if available
                cursor.execute(
                    "SELECT network_name, primary_entity_name FROM cached_insights "
                    "WHERE title = 'Statewide' AND (primary_entity_id = %s OR network_name = %s OR primary_entity_name = %s) LIMIT 1",
                    (entity_id, entity_name, entity_name)
                )
                insight_row = cursor.fetchone()
                
                # We need the Network ID name from the networks table first to be safe
                # Summing up business count if multiple networks
                cursor.execute("SELECT SUM(business_count) as bc, MIN(primary_name) as bn FROM networks WHERE id = ANY(%s)", (network_ids,))
                net_row = cursor.fetchone()
                
                header_name = net_row.get("bn") if net_row else "Unknown Network"
                
                # Override if Insight has a better name (Human Principal)
                if insight_row and insight_row.get('primary_entity_name') and insight_row['primary_entity_name'] not in ('NULL', 'None', ''):
                     header_name = insight_row['primary_entity_name']

                yield _yield(json.dumps({
                    "type": "network_info", 
                    "data": {
                        "id": network_ids[0], # Just use first ID as canonical ID for now
                        "name": header_name,
                        "business_count": net_row.get("bc") if net_row else 0
                    }
                }))
                
                # Businesses
                cursor.execute(
                    "SELECT b.* FROM entity_networks en "
                    "JOIN businesses b ON b.id::text = en.entity_id "
                    "WHERE en.network_id = ANY(%s) AND en.entity_type = 'business'",
                    (network_ids,)
                )
                businesses = cursor.fetchall()

                # Principals
                cursor.execute(
                    "SELECT entity_id AS principal_id, COALESCE(entity_name, entity_id) AS principal_name "
                    "FROM entity_networks "
                    "WHERE network_id = ANY(%s) AND entity_type = 'principal'",
                    (network_ids,)
                )
                principals_in_network = cursor.fetchall()

                # --- FIX START: Consolidate Principal Details ---
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
                        if not name_c:
                            continue
                        
                        if name_c not in merged_principals:
                            merged_principals[name_c] = record
                        else:
                            # Merge details, prioritizing non-null values
                            for key, value in record.items():
                                if merged_principals[name_c].get(key) is None and value is not None:
                                    merged_principals[name_c][key] = value
                # --- FIX END ---
                
                entities_dict: Dict[str, Dict[str, Any]] = {}
                links = {"business_to_principal": [], "principal_to_business": []}

                for b in businesses:
                    b_key = f"business_{b['id']}"
                    entities_dict[b_key] = {
                        "id": b["id"],
                        "name": b["name"],
                        "type": "business",
                        "status": b.get("status"),
                        "details": b,
                        "connections": [],
                    }

                for pr in principals_in_network:
                    principal_name = pr.get("principal_name") or pr["principal_id"]
                    p_key = _principal_key(principal_name)
                    # Use the merged details if available, otherwise create a shell
                    details = merged_principals.get(principal_name, {"name_c": principal_name})

                    entities_dict[p_key] = {
                        "id": pr["principal_id"],
                        "name": principal_name,
                        "type": "principal",
                        "details": details,
                        "connections": [],
                    }


                # Build links
                if businesses:
                    cursor.execute(
                        "SELECT business_id, COALESCE(name_c, trim(concat_ws(' ', firstname,middlename,lastname,suffix))) AS pname "
                        "FROM principals WHERE business_id = ANY(%s)",
                        ([b["id"] for b in businesses],)
                    )
                    for r in cursor.fetchall():
                        if not r.get("pname"):
                            continue
                        b_key = f"business_{r['business_id']}"
                        p_key = _principal_key(r["pname"])
                        if b_key in entities_dict and p_key in entities_dict:
                            links["business_to_principal"].append({"source": b_key, "target": p_key})
                            links["principal_to_business"].append({"source": p_key, "target": b_key})

                # --- NEW: Shared Address Links for Visualization ---
                # We want to show the user that these businesses are linked because they share an address.
                # We can reuse the same normalization logic or just check exact string match 
                # (since discover_networks.py already grouped them by norm address).
                from .shared_utils import normalize_mailing_address
                
                # Group businesses by normalized address locally for this network
                addr_groups = defaultdict(list)
                for b in businesses:
                    raw_addr = b.get('mail_address') or b.get('business_address')
                    if raw_addr:
                        norm = normalize_mailing_address(raw_addr)
                        if norm and len(norm) > 4:
                            addr_groups[norm].append(f"business_{b['id']}")

                links["shared_address"] = []
                for addr, b_keys in addr_groups.items():
                    if len(b_keys) > 1:
                        # Link them in a chain or all-to-all? Chain is cleaner for graph.
                        for i in range(len(b_keys) - 1):
                            links["shared_address"].append({
                                "source": b_keys[i], 
                                "target": b_keys[i+1],
                                "label": "Shared Address"
                            })
                # ---------------------------------------------------

                yield _yield(json.dumps(
                    {"type": "entities", "data": {"entities": list(entities_dict.values()), "links": links}},
                    default=json_converter,
                ))

                # Stream properties for all businesses/principals in the network
                # Stream properties for all businesses/principals in the network
                biz_ids = [b["id"] for b in businesses]
                biz_names = [b["name"] for b in businesses]
                principal_ids = [pr["principal_id"] for pr in principals_in_network]

                # Match by:
                # 1. business_id (direct link)
                # 2. owner_norm/co_owner_norm = principal_id (person owns it)
                # 3. owner = business_name (business owns it, simple string match)
                # We normalize the business names for better matching if possible, but exact match is a safe start.
                
                # Match by explicit link in entity_networks (Source of Truth)
                # This ensures we get exactly the properties counted in the insights card.
                # Stream flat properties with incremental updates (Frontend handles grouping)
                # DEDUPLICATION: Track seen IDs to handle overlaps between chunks
                seen_props = set()
                
                # Prepare global lists of targets
                all_biz_ids = [b["id"] for b in businesses]
                all_raw_p_ids = [pr["principal_id"] for pr in principals_in_network]
                
                # Chunking configuration
                CHUNK_SIZE = 50
                
                # We can iterate through businesses and principals in parallel chunks or just one after another.
                # Simplest strategy: Interleave them or just process lists.
                # To maximize "parallel" feel, we process chunks of both.
                
                import math
                n_biz = len(all_biz_ids)
                n_princ = len(all_raw_p_ids)
                max_steps = max(math.ceil(n_biz / CHUNK_SIZE), math.ceil(n_princ / CHUNK_SIZE))
                
                for i in range(max_steps):
                    # 1. Slice IDs for this chunk
                    b_chunk = all_biz_ids[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE]
                    p_chunk = all_raw_p_ids[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE]
                    
                    if not b_chunk and not p_chunk:
                        break
                        
                    # 2. Query properties for this chunk
                    # We replicate the UNION logic but filtered to this batch
                    # Note: We must still handle the "Business Principal" link indirectly? 
                    # No, we have ALL businesses in `all_biz_ids` so `business_id IN b_chunk` covers LLCs.
                    # We have ALL principals in `all_raw_p_ids` so `principal_id IN p_chunk OR owner_norm IN p_chunk` covers Humans.
                    
                    # Optimization: Simple OR query is faster than UNIONs for batch ID lookups
                    try:
                         # Build safe IN clauses
                         # We rely on Postgres arrays for ANY(%s)
                         
                         query_parts = []
                         params = []
                         
                         if b_chunk:
                             query_parts.append("business_id = ANY(%s)")
                             params.append(b_chunk)
                         
                         if p_chunk:
                             # principal_id column is TEXT (name_c usually?) - Wait check Schema.
                             # principals.id is SERIAL/INT. properties.principal_id is TEXT?
                             # Let's check Schema or previous query. 
                             # Previous query: JOIN principals pr ON p.principal_id = pr.id::text
                             # So p.principal_id is the string representation of the numeric ID.
                             # But `principals_in_network` (from entity_networks) has `principal_id` as the ENTITY ID (string name?).
                             # Wait, entity_networks for principals: entity_id = name_c.
                             # BUT `p.principal_id` stores the string numeric ID of the principal record?
                             # Let's check `api/build_networks.py` or Schema.
                             
                             # Re-reading previous big query carefully:
                             # JOIN principals pr ON p.principal_id = pr.id::text
                             # JOIN entity_networks en ON pr.name_c = en.entity_id
                             
                             # So `en.entity_id` is the NAME_C.
                             # We have `p_chunk` which is a list of `en.entity_id` (NAME_C).
                             # We cannot directly match `p.principal_id` (numeric string) against `p_chunk` (Name).
                             # We need the numeric IDs associated with these Names.
                             
                             # Resolve Names to IDs for this chunk? 
                             # Or just match on owner_norm / co_owner_norm which match Name?
                             # properties.owner_norm usually matches normalized Name.
                             # entity_networks.entity_id (for principal) is Name.
                             
                             # Query A: owner_norm = ANY(p_chunk) OR co_owner_norm = ANY(p_chunk)
                             # Query B: properties linked via principal_id? 
                             # We need to look up principal raw IDs for these names first?
                             # Faster: We can just match owner_norm.
                             pass
                         
                         # Let's do a sub-lookup for Principal IDs to be safe
                         # OR just rely on owner_norm.
                         # The Big Query used `p.principal_id = pr.id` then `pr.name_c = en.entity_id`.
                         # So properties where the LINKED principal has the target Name.
                         
                         # To replicate this efficiently in batches:
                         # 1. Find Principal INT IDs for the names in p_chunk
                         # 2. Query properties WHERE principal_id IN (ints) OR owner_norm IN (names)
                         
                         current_p_int_ids = []
                         if p_chunk:
                             cursor.execute("SELECT id FROM principals WHERE name_c = ANY(%s)", (p_chunk,))
                             current_p_int_ids = [str(r['id']) for r in cursor.fetchall()]
                         
                         clauses = []
                         args = []
                         
                         if b_chunk:
                             clauses.append("business_id = ANY(%s)")
                             args.append(b_chunk)
                             
                         if p_chunk:
                             clauses.append("owner_norm = ANY(%s)")
                             args.append(p_chunk)
                             clauses.append("co_owner_norm = ANY(%s)")
                             args.append(p_chunk) # Assuming p_chunk names are compatible with owner_norm (normalization?)
                             # en.entity_id is name_c. properties.owner_norm is normalized.
                             # If they differ, direct match fails.
                             # The Big Query joined `p.owner_norm = en.entity_id`. So they MUST match for that link to work.
                             
                         if current_p_int_ids:
                             clauses.append("principal_id = ANY(%s)")
                             args.append(current_p_int_ids)
                         
                         if not clauses:
                             continue

                         # NEIGHBOR FETCH (Crucial for Completeness)
                         # We want All units in the building if one unit matches.
                         # Logic:
                         # 1. Find matched properties (anchors) -> Get their `location` + `property_city`
                         # 2. Query ALL properties with that `location` (base address)
                         
                         # Step 1: Get Anchors
                         sql_anchors = f"SELECT location, property_city FROM properties WHERE {' OR '.join(clauses)}"
                         cursor.execute(sql_anchors, args)
                         anchors = cursor.fetchall()
                         
                         if not anchors:
                             continue
                             
                         # Extract unique addresses to fetch buildings
                         # Simple normalization: trim unit numbers? 
                         # We'll use the regex from the main query but in Python for batching?
                         # Or just simple "Startswith" in SQL?
                         
                         # Group by city for index efficiency
                         city_locs = defaultdict(set)
                         for a in anchors:
                             if a['location']:
                                 # Heuristic: Trim unit suffix like " UNIT 4" or " APT 5"
                                 # simplified "base_loc"
                                 base = re.sub(r'\s+(UNIT|APT|#|FL|STE).*$', '', a['location'], flags=re.IGNORECASE).strip()
                                 if base:
                                     city_locs[a['property_city']].add(base)
                         
                         # Step 2: Fetch Buildings
                         # We do this per city to use the index effectively
                         batch_results = []
                         for city, locs in city_locs.items():
                             if not locs: continue
                             
                             # Optimization: If too many locations, maybe just exact match?
                             # Or LIKE ANY?
                             # "location LIKE '123 MAIN ST%'"
                             
                             # Prepare LIKE patterns
                             patterns = [f"{l}%" for l in locs]
                             
                             # Fetch properties
                             # Note: This might re-fetch properties already seen in other chunks. 
                             cursor.execute("""
                                 SELECT * FROM properties 
                                 WHERE property_city = %s AND location LIKE ANY(%s)
                             """, (city, patterns))
                             
                             batch_results.extend(cursor.fetchall())

                         # Dedupe global
                         new_props = []
                         new_ids = []
                         for p in batch_results:
                             if p['id'] not in seen_props:
                                 seen_props.add(p['id'])
                                 new_props.append(p)
                                 new_ids.append(p['id'])
                                 
                         if not new_props:
                             continue
                             
                         # Fetch subsidies for this batch
                         s_map = defaultdict(list)
                         cursor.execute("""
                            SELECT property_id, program_name, subsidy_type, units_subsidized, expiry_date, source_url
                            FROM property_subsidies
                            WHERE property_id = ANY(%s)
                         """, (new_ids,))
                         for s in cursor.fetchall():
                             s_map[s['property_id']].append(dict(s))
                             
                         # Shape and Yield
                         shaped = [shape_property_row(p, s_map.get(p['id'])) for p in new_props]
                         yield _yield(json.dumps(
                             {"type": "properties", "data": shaped},
                             default=json_converter
                         ))
                         
                    except Exception as e:
                         # Log but don't crash stream
                         logger.error(f"Chunk error: {e}")
                         # Continue to next chunk
                         pass

                yield _yield(json.dumps({"type": "done"}))

        except Exception as e:
            logging.exception("stream_load_network error")
            yield _yield(json.dumps({"type": "done", "error": str(e)}))

    return StreamingResponse(generate_network_data(), media_type="application/x-ndjson")


# ------------------------------------------------------------
# Batch properties (multi-owner)
# ------------------------------------------------------------
@app.get("/api/properties/batch")
def get_properties_batch(owner_names: str, conn=Depends(get_db_connection)):
    names = [n.strip() for n in (owner_names or "").split(",") if n.strip()]
    if not names:
        return []
    norm_set = list({ normalize_person_name_py(n) for n in names })
    props: List[PropertyItem] = []
    seen_ids: Set[int] = set()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM properties
            WHERE owner_norm = ANY(%s) OR co_owner_norm = ANY(%s)
            """,
            (norm_set, norm_set)
        )
        for r in cursor.fetchall():
            if r["id"] in seen_ids:
                continue
            seen_ids.add(r["id"])
            props.append(PropertyItem(
                address=r.get("location"),
                unit=r.get("unit"),
                city=r.get("property_city"),
                owner=r.get("owner"),
                assessed_value=r.get("assessed_value"),
                details=r
            ))
            if len(props) >= 100:
                break
    return props


# ------------------------------------------------------------
# Reports / Insights
# ------------------------------------------------------------
def _column_exists(cursor, table: str, col: str) -> bool:
    cursor.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s
        LIMIT 1
    """, (table, col))
    return cursor.fetchone() is not None

def _calculate_and_cache_insights(cursor, town_col: Optional[str], town_filter: Optional[str], sort_mode: str = 'total'):
    """
    Highly optimized logic for calculating top networks.
    Aggregates first by (network, entity) to avoid redundant scans,
    then picks the best display entity for each network.
    
    sort_mode: 'total' (default) or 'subsidized'
    """
    params = {}
    town_filter_clause = ""
    if town_col and town_filter:
        town_filter_clause = f"AND p.{town_col} = %(town_filter)s"
        params['town_filter'] = town_filter

    # Sorting logic
    order_clause = "ns.total_property_count DESC"
    where_clause = "1=1"
    final_order_clause = "re.total_property_count DESC"
    
    if sort_mode == 'subsidized':
        order_clause = "ns.subsidized_property_count DESC"
        where_clause = "ns.subsidized_property_count > 0"
        final_order_clause = "re.total_property_count DESC" # Keep ordering by size, but filter applies? 
        # Wait, if I sort by total size but filter > 0, I show biggest landlords who have subsidies.
        # If I sort by subsidized count, I show landlords with MOST subsidies.
        # User request: "subsidized properties" toggle. Usually expects rank by subsidy count?
        final_order_clause = "re.subsidized_property_count DESC, re.total_property_count DESC"

    query = f"""
        WITH property_links AS (
            -- All properties linked to a network, tagged with the linking entity
            SELECT p.id as property_id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value
            FROM properties p
            JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
            WHERE p.business_id IS NOT NULL {town_filter_clause}
            
            UNION ALL
            
            -- Direct link to principal via property.principal_id
            SELECT p.id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value
            FROM properties p
            JOIN principals pr ON p.principal_id = pr.id::text
            JOIN entity_networks en ON pr.name_c = en.entity_id AND en.entity_type = 'principal'
            WHERE p.principal_id IS NOT NULL {town_filter_clause}

            UNION ALL

            -- Direct link to principal via property.owner_norm
            SELECT p.id, en.network_id, en.entity_id, en.entity_type, en.entity_name, p.assessed_value, p.appraised_value
            FROM properties p
            JOIN entity_networks en ON p.owner_norm = en.entity_id AND en.entity_type = 'principal'
            WHERE p.owner_norm IS NOT NULL {town_filter_clause}

            UNION ALL

            -- CRITICAL: Link properties to principals VIA their businesses
            -- This ensures human principals get "credit" for all properties owned by their LLCs
            SELECT p.id, en_p.network_id, en_p.entity_id, en_p.entity_type, en_p.entity_name, p.assessed_value, p.appraised_value
            FROM properties p
            JOIN entity_networks en_b ON p.business_id::text = en_b.entity_id AND en_b.entity_type = 'business'
            JOIN principals pr ON en_b.entity_id = pr.business_id
            JOIN entity_networks en_p ON pr.name_c = en_p.entity_id AND en_p.entity_type = 'principal'
            WHERE p.business_id IS NOT NULL {town_filter_clause}
        ),
        network_stats AS (
            -- Total stats for each network
            SELECT 
                pl.network_id,
                COUNT(DISTINCT pl.property_id) as total_property_count,
                SUM(pl.assessed_value) as total_assessed_value,
                SUM(pl.appraised_value) as total_appraised_value,
                COUNT(DISTINCT ps.property_id) as subsidized_property_count,
                coalesce(
                    jsonb_agg(DISTINCT ps.program_name) FILTER (WHERE ps.program_name IS NOT NULL), 
                    '[]'::jsonb
                ) as subsidy_programs
            FROM property_links pl
            LEFT JOIN property_subsidies ps ON pl.property_id = ps.property_id
            GROUP BY pl.network_id
        ),
        entity_stats AS (
            -- Stats for each entity within its network
            SELECT 
                network_id,
                entity_id,
                entity_type,
                entity_name,
                COUNT(DISTINCT property_id) as entity_property_count
            FROM property_links
            GROUP BY network_id, entity_id, entity_type, entity_name
        ),
        ranked_entities AS (
            -- Pick the best entity to represent each network
            SELECT 
                es.*,
                ns.total_property_count,
                ns.total_assessed_value,
                ns.total_appraised_value,
                ns.subsidized_property_count,
                ns.subsidy_programs,
                ROW_NUMBER() OVER (
                    PARTITION BY es.network_id 
                    ORDER BY 
                        CASE WHEN es.entity_type = 'principal' THEN 0 ELSE 1 END,
                        CASE 
                            WHEN es.entity_name ILIKE '%% LLC' THEN 2 
                            WHEN es.entity_name ILIKE '%% INC%%' THEN 2 
                            WHEN es.entity_name ILIKE '%% CORP%%' THEN 2 
                            WHEN es.entity_name ILIKE '%% LTD%%' THEN 2 
                            ELSE 0 
                        END,
                        es.entity_property_count DESC
                ) as rank
            FROM entity_stats es
            JOIN network_stats ns ON es.network_id = ns.network_id
            WHERE {where_clause}
        ),
        controlling_business AS (
             -- Best business to use as a deduplication key
             SELECT DISTINCT ON (network_id)
                network_id,
                entity_name as business_name,
                entity_id as business_id
             FROM entity_stats
             WHERE entity_type = 'business'
             ORDER BY network_id, entity_property_count DESC
        )
        SELECT 
            re.entity_id,
            re.entity_name,
            re.entity_type,
            re.total_property_count as value,
            re.total_assessed_value,
            re.total_appraised_value,
            re.subsidized_property_count,
            re.subsidy_programs,
            re.network_id,
            (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = re.network_id AND en.entity_type = 'business') as business_count,
            cb.business_name as controlling_business_name,
            cb.business_id as controlling_business_id
        FROM ranked_entities re
        LEFT JOIN controlling_business cb ON re.network_id = cb.network_id
        WHERE re.rank = 1
        ORDER BY {final_order_clause}
        LIMIT 50;
    """
    cursor.execute(query, params)
    # Using RealDictCursor, so we get dicts
    raw_networks = cursor.fetchall()
    
    # Graceful Merge / Deduplication
    merged_networks = []
    seen_keys = {} # Map unique_key -> index in merged_networks
    
    for net in raw_networks:
        # Create a unique key based on controlling business or entity ID
        c_id = net.get('controlling_business_id')
        unique_key = c_id if c_id else f"ent_{net['entity_id']}"
        
        # Make a mutable copy
        network = dict(net)
        
        if unique_key in seen_keys:
            # Merge into existing
            existing_idx = seen_keys[unique_key]
            existing_net = merged_networks[existing_idx]
            
            # Merge logic:
            
            # 1. Prioritize Human Principal for the Main Title
            # If existing is a business but incoming is a principal (human), swap to human.
            if existing_net['entity_type'] == 'business' and network['entity_type'] == 'principal':
                existing_net['entity_name'] = network['entity_name']
                existing_net['entity_type'] = 'principal'
                existing_net['entity_id'] = network['entity_id']
            
            # If both are principals, append names if distinct (joint title)
            elif existing_net['entity_type'] == 'principal' and network['entity_type'] == 'principal':
                 if network['entity_name'] not in existing_net['entity_name']:
                     if len(existing_net['entity_name']) < 60: # Avoid overly long titles
                        existing_net['entity_name'] += f" & {network['entity_name']}"
            
            # If incoming has a controlling business name and existing doesn't, take it
            if not existing_net.get('controlling_business_name') and network.get('controlling_business_name'):
                existing_net['controlling_business_name'] = network['controlling_business_name']
                existing_net['controlling_business_id'] = network['controlling_business_id']

            # 2. Update Stats: Take the MAX of duplicate fragments (don't sum updates/overlaps)
            if network['value'] > existing_net['value']:
                existing_net['value'] = network['value']
                existing_net['total_assessed_value'] = network['total_assessed_value']
                existing_net['total_appraised_value'] = network['total_appraised_value']
            
            # Always max out business count
            existing_net['business_count'] = max(existing_net.get('business_count', 0), network.get('business_count', 0))
            
            continue
        
        seen_keys[unique_key] = len(merged_networks)
        merged_networks.append(network)
    
    # 2. Enrich the Final Top 10
    final_networks = merged_networks[:10]
    result = []
    
    for network in final_networks:
        # Top Principals
        cursor.execute("""
            SELECT name, state FROM (
                SELECT DISTINCT ON(UPPER(pr.name_c))
                    pr.name_c as name,
                    pr.state,
                    COUNT(*) as link_count
                FROM entity_networks en
                JOIN principals pr ON en.entity_id = pr.business_id
                WHERE en.network_id = %s AND en.entity_type = 'business' AND pr.name_c IS NOT NULL
                GROUP BY pr.name_c, pr.state
                ORDER BY UPPER(pr.name_c), link_count DESC
            ) as distinct_principals
            ORDER BY link_count DESC
            LIMIT 3;
        """, (network['network_id'],))
        network['principals'] = cursor.fetchall()

        # Top Businesses (Representative Entities)
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

def _update_insights_cache_sync():
    """
    Background worker to refresh the heavy insights query.
    """
    if db_pool:
        conn = db_pool.getconn()
        try:
            logger.info("Starting background refresh of insights cache...")
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Load existing cache to preserve other cities if we crash
                cursor.execute("SELECT value FROM kv_cache WHERE key = 'insights'")
                row = cursor.fetchone()
                insights_by_municipality = row['value'] if row and row['value'] else {}
                
                # 1. Statewide
                logger.info("Calculating STATEWIDE insights...")
                insights_by_municipality['STATEWIDE'] = _calculate_and_cache_insights(cursor, None, None, sort_mode='total')
                insights_by_municipality['STATEWIDE_SUBSIDIZED'] = _calculate_and_cache_insights(cursor, None, None, sort_mode='subsidized')
                
                # Helper to save partial results
                def save_partial(data):
                     cursor.execute("""
                        INSERT INTO kv_cache (key, value) VALUES (%s, %s::jsonb)
                        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now()
                    """, ('insights', json.dumps(data, default=json_converter)))
                     conn.commit()

                save_partial(insights_by_municipality)
                
                # 2. Major Cities
                major_cities = ['Bridgeport', 'New Haven', 'Hartford', 'Stamford', 'Waterbury', 'Norwalk', 'Danbury', 'New Britain']
                for t in major_cities:
                    logger.info("Calculating insights for %s...", t)
                    try:
                        # Standard
                        town_networks = _calculate_and_cache_insights(cursor, 'property_city', t, sort_mode='total')
                        if town_networks:
                            insights_by_municipality[t.upper()] = town_networks
                        
                        # Subsidized
                        sub_networks = _calculate_and_cache_insights(cursor, 'property_city', t, sort_mode='subsidized')
                        if sub_networks:
                            insights_by_municipality[f"{t.upper()}_SUBSIDIZED"] = sub_networks

                        save_partial(insights_by_municipality)
                        logger.info("✅ Saved insights for %s", t)
                    except Exception:
                        logger.exception("Failed to calculate insights for %s", t)
                
            logger.info("✅ Background refresh of insights cache complete.")
        except Exception:
            logger.exception("Background cache refresh failed")
            if conn: conn.rollback()
        finally:
            db_pool.putconn(conn)
    else:
        logger.error("DB pool not available for cache refresh.")

@app.get("/api/insights", response_model=Dict[str, List[InsightItem]])
def get_insights(conn=Depends(get_db_connection)):
    """
    Serves pre-calculated insights from the cache table for fast response times.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT value FROM kv_cache WHERE key = 'insights'")
            row = cursor.fetchone()
            if not row or not row['value']:
                return {}
            return row['value']
    except Exception:
        logger.exception("Could not fetch insights from cache.")
        raise HTTPException(status_code=500, detail="Failed to retrieve insights.")

@app.get("/api/properties/{property_id}/user_data")
def get_property_user_data(property_id: int, conn=Depends(get_db_connection)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT notes, photos FROM property_user_data WHERE property_id = %s", (property_id,))
            row = cursor.fetchone()
            return row if row else {"notes": "", "photos": []}
    except Exception:
        logger.exception("Failed to fetch user data")
        return {"notes": "", "photos": []}

@app.post("/api/properties/{property_id}/user_data")
def save_property_user_data(property_id: int, data: dict, conn=Depends(get_db_connection)):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO property_user_data (property_id, notes, photos, updated_at)
                VALUES (%s, %s, %s::jsonb, now())
                ON CONFLICT (property_id) DO UPDATE SET
                    notes = EXCLUDED.notes,
                    photos = EXCLUDED.photos,
                    updated_at = now()
            """, (property_id, data.get("notes"), json.dumps(data.get("photos", []))))
            conn.commit()
            return {"status": "success"}
    except Exception as e:
        logger.exception("Failed to save user data")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cached-reports", response_model=List[CachedReportInfo])
def get_cached_reports(conn=Depends(get_db_connection)):
    """
    Returns a list of previously generated AI reports.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT
                    entity as norm_name,
                    title as entity_name,
                    created_at,
                    LENGTH(content) as size
                FROM ai_reports
                ORDER BY created_at DESC
                LIMIT 100;
            """)
            return cursor.fetchall()
    except Exception:
        logger.exception("Could not fetch cached reports.")
        raise HTTPException(status_code=500, detail="Failed to retrieve cached reports.")


@app.get("/api/reports", response_model=List[Report])
def get_reports(conn=Depends(get_db_connection)):
    reports: List[Report] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT owner AS key, COUNT(*) AS value
            FROM properties
            WHERE owner IS NOT NULL AND owner != ''
            GROUP BY owner
            ORDER BY value DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        reports.append(Report(
            title="Top Owners by Property Count",
            data=[ReportItem(key=r["key"], value=f"{int(r['value']):,} properties") for r in rows]
        ))

        cursor.execute("""
            SELECT owner AS key, COALESCE(SUM(assessed_value), 0) AS value
            FROM properties
            WHERE owner IS NOT NULL AND owner != '' AND assessed_value > 0
            GROUP BY owner
            ORDER BY value DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        reports.append(Report(
            title="Top Owners by Assessed Value",
            data=[ReportItem(key=r["key"], value=f"${int(r['value'] or 0):,}") for r in rows]
        ))
    
    # 3. Top Networks (Custom Logic) - Prioritize Human Names
    # Note: Logic moved upstream or we query here?
    # For now, let's just ensure we return consistent structure.
    # The user wants "Menachem Gurevitch" (Principal) as header if linked.
    # But this function `_get_top_networks` currently returns OWNERS.
    # We need a separate `top_networks` endpoint or check `get_network_graph`.
    
    return reports

# ------------------------------------------------------------
# AI Report (cached per day)
# ------------------------------------------------------------
def _compute_local_context(conn, entity: str, entity_type: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"entity": entity, "entity_type": entity_type}
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if entity_type in ("owner", "principal"):
            cursor.execute("""
                SELECT COUNT(*) AS cnt, COALESCE(SUM(assessed_value),0) AS total_value
                FROM properties
                WHERE owner_norm = normalize_person_name(%s)
                   OR co_owner_norm = normalize_person_name(%s)
            """, (entity, entity))
            row = cursor.fetchone() or {}
            ctx["property_count"] = int(row.get("cnt") or 0)
            ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)

            cursor.execute("""
                SELECT property_city AS city, COUNT(*) AS cnt
                FROM properties
                WHERE owner_norm = normalize_person_name(%s)
                   OR co_owner_norm = normalize_person_name(%s)
                GROUP BY property_city
                ORDER BY cnt DESC
                LIMIT 10
            """, (entity, entity))
            ctx["top_cities"] = cursor.fetchall()

        elif entity_type == "business":
            cursor.execute("""
                SELECT COUNT(*) AS cnt, COALESCE(SUM(assessed_value),0) AS total_value
                FROM properties
                WHERE owner = %s
                   OR owner_norm = normalize_person_name(%s)
            """, (entity, entity))
            row = cursor.fetchone() or {}
            ctx["property_count"] = int(row.get("cnt") or 0)
            ctx["total_assessed_value"] = float(row.get("total_value") or 0.0)

            cursor.execute("""
                SELECT property_city AS city, COUNT(*) AS cnt
                FROM properties
                WHERE owner = %s
                   OR owner_norm = normalize_person_name(%s)
                GROUP BY property_city
                ORDER BY cnt DESC
                LIMIT 10
            """, (entity, entity))
            ctx["top_cities"] = cursor.fetchall()
    return ctx

def _draft_ai_report_text(context: Dict[str, Any]) -> Tuple[str, str]:
    title = f"AI report — {context.get('entity')}"
    if not (openai and OPENAI_API_KEY):
        cities = ", ".join([f"{r['city']} ({r['cnt']})" for r in context.get("top_cities", []) if r.get("city")])
        body = (
            f"Summary for {context.get('entity_type')}: {context.get('entity')}\n\n"
            f"- Properties found: {context.get('property_count', 0)}\n"
            f"- Total assessed value: ${int(context.get('total_assessed_value', 0)):,}\n"
            f"- Top cities by count: {cities or 'N/A'}\n"
            "\n(Generated without external AI due to missing API key.)"
        )
        return (title, body)
    prompt = (
        "You are generating a concise investigative briefing for a property-ownership network tool. "
        "Write in clear, scannable bullets with short sections. Include actionable insights. "
        "Use only the structured context provided.\n\n"
        f"CONTEXT JSON:\n{json.dumps(context, default=str)}\n\n"
        "Output sections:\n"
        "1) Snapshot (counts, value)\n"
        "2) Geography focus (top cities)\n"
        "3) Investigative Observations (ownership patterns, corporate history, or notable controversies)\n"
    )
    try:
        resp = openai.ChatCompletion.create(  # type: ignore[attr-defined]
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a meticulous analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=700,
        )
        content = resp["choices"][0]["message"]["content"].strip()
        return (title, content)
    except Exception as e:  # pragma: no cover
        logger.warning("OpenAI error: %s", e)
        # deterministic fallback
        cities = ", ".join([f"{r['city']} ({r['cnt']})" for r in context.get("top_cities", []) if r.get("city")])
        body = (
            f"Summary for {context.get('entity_type')}: {context.get('entity')}\n\n"
            f"- Properties found: {context.get('property_count', 0)}\n"
            f"- Total assessed value: ${int(context.get('total_assessed_value', 0)):,}\n"
            f"- Top cities by count: {cities or 'N/A'}\n"
            "\n(OpenAI call failed; generated fallback.)"
        )
        return (title, body)

@app.post("/api/ai-report")
def create_ai_report(req: AIReportRequest, conn=Depends(get_db_connection)):
    today = date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if not req.force:
            cursor.execute("""
                SELECT * FROM ai_reports
                WHERE entity = %s AND entity_type = %s AND report_date = %s
                LIMIT 1
            """, (req.entity, req.entity_type, today))
            existing = cursor.fetchone()
            if existing:
                return existing
        context = _compute_local_context(conn, req.entity, req.entity_type)
        title, content = _draft_ai_report_text(context)
        cursor.execute("""
            INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity, entity_type, report_date)
            DO UPDATE SET title=EXCLUDED.title, content=EXCLUDED.content, sources=EXCLUDED.sources
            RETURNING *
        """, (req.entity, req.entity_type, today, title, content, json.dumps({"internal_stats": context})))
        row = cursor.fetchone()
        conn.commit()
        return row

@app.get("/api/ai-report")
def get_ai_report(entity: str, entity_type: str, report_date: Optional[str] = None, conn=Depends(get_db_connection)):
    the_date = date.fromisoformat(report_date) if report_date else date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT * FROM ai_reports
            WHERE entity = %s AND entity_type = %s AND report_date = %s
            LIMIT 1
        """, (entity, entity_type, the_date))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No report found for the given date")
        return row


# ------------------------------------------------------------
# Health
# ------------------------------------------------------------
@app.get("/api/healthz")
def healthz():
    return {"ok": True}


# ------------------------------------------------------------
# Network Digest (Batch Analysis)
# ------------------------------------------------------------

class DigestItem(BaseModel):
    name: str
    type: str
    property_count: Optional[int] = 0
    total_value: Optional[float] = 0.0

class NetworkDigestRequest(BaseModel):
    entities: List[DigestItem]
    force: bool = False

@app.post("/api/network_digest")
def create_network_digest(req: NetworkDigestRequest, conn=Depends(get_db_connection)):
    """
    Generate AI-powered analysis of a property network.
    
    Privacy Protection: Only analyzes networks meeting minimum thresholds
    to protect privacy of small landlords and mom-and-pop operations.
    """
    # Privacy threshold: Only analyze substantial networks
    total_props = sum(e.property_count for e in req.entities)
    total_val = sum(e.total_value for e in req.entities)
    entity_count = len(req.entities)
    
    MIN_PROPERTIES = 10
    MIN_VALUE = 3_000_000  # $3M
    MIN_ENTITIES = 5
    
    # Allow if ANY threshold is met (OR logic for flexibility)
    meets_threshold = (
        total_props >= MIN_PROPERTIES or 
        total_val >= MIN_VALUE or 
        entity_count >= MIN_ENTITIES
    )
    
    if not meets_threshold:
        return {
            "entity": "PRIVACY_PROTECTED",
            "entity_type": "network_digest",
            "report_date": date.today(),
            "title": "Analysis Not Available",
            "content": (
                f"**Privacy Protection Active**\n\n"
                f"AI Digest is only available for substantial property networks to protect "
                f"the privacy of small landlords and family-owned properties.\n\n"
                f"**Current Network:**\n"
                f"- {entity_count} entities\n"
                f"- {total_props} properties\n"
                f"- ${total_val:,.0f} total assessed value\n\n"
                f"**Minimum Requirements (any one):**\n"
                f"- {MIN_PROPERTIES}+ properties\n"
                f"- ${MIN_VALUE:,.0f}+ total value\n"
                f"- {MIN_ENTITIES}+ related entities"
            ),
            "sources": []
        }
    
    # 1. Generate Stable Hash (Cache Key)
    # Include stats in hash so if data changes (e.g. value updates), we regenerate
    sorted_ents = sorted(req.entities, key=lambda x: (x.type, x.name))
    # v2: Updated to invalidate old cached errors/v0.28 format
    CACHE_VERSION = "v3_20260117" 
    blob = json.dumps([
        {"n": e.name, "t": e.type, "c": e.property_count, "v": e.total_value} for e in sorted_ents
    ] + [{"_v": CACHE_VERSION}], sort_keys=True)
    digest_hash = hashlib.md5(blob.encode("utf-8")).hexdigest()
    digest_id = f"DIGEST_{digest_hash}"
    
    today = date.today()
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if not req.force:
            cursor.execute("""
                SELECT * FROM ai_reports
                WHERE entity = %s AND entity_type = 'network_digest' AND report_date = %s
                LIMIT 1
            """, (digest_id, today))
            existing = cursor.fetchone()
            if existing:
                return existing

        # 2. Perform Analysis (Parallel Web Search)
        combined_context = []

        def fetch_entity_context(ent: DigestItem):
            if not SERPAPI_API_KEY:
                return {"context": f"Entity: {ent.name} ({ent.type}) - SerpAPI not configured.", "sources": []}
            
            # Enhanced query for more comprehensive results
            base_query = f'"{ent.name}"'
            
            if ent.type == 'business':
                query = f'{base_query} Connecticut (complaints OR lawsuit OR eviction OR violation OR "code enforcement" OR tenants)'
            else:
                query = f'{base_query} Connecticut (landlord OR "property owner" OR lawsuit OR eviction OR LLC OR business)'
            
            try:
                url = "https://serpapi.com/search"
                params = {
                   "q": query,
                   "api_key": SERPAPI_API_KEY,
                   "hl": "en",
                   "gl": "us",
                   "num": 5  # Increased from 3 to get more context
                }
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                
                snippets = []
                sources = []
                if "organic_results" in data:
                    for res in data["organic_results"]:
                         title = res.get("title", "")
                         snip = res.get("snippet", "")
                         link = res.get("link", "")
                         if title or snip:
                             snippets.append(f"- {title}: {snip} (Source: {link})")
                         if link:
                             sources.append({"title": title, "url": link})
                
                if snippets:
                    return {
                        "context": f"Entity: {ent.name} ({ent.type})\n" + "\n".join(snippets),
                        "sources": sources
                    }
                else:
                    return {"context": f"Entity: {ent.name} ({ent.type}) - No significant results.", "sources": []}
            except Exception as e:
                logger.error(f"Search failed for {ent.name}: {e}")
                return {"context": f"Entity: {ent.name} ({ent.type}) - Search Error.", "sources": []}

        # Execute searches in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            # We'll rely on Frontend to send a reasonable number (e.g. top 10).
            results = list(executor.map(fetch_entity_context, req.entities))
            combined_context = [r["context"] for r in results]
            all_sources = []
            seen_links = set()
            for r in results:
                for s in r["sources"]:
                    if s["url"] not in seen_links:
                        all_sources.append(s)
                        seen_links.add(s["url"])

        full_text_context = "\n\n".join(combined_context)
        
        # Calculate aggregate stats for the prompt
        total_props = sum(e.property_count for e in req.entities)
        total_val = sum(e.total_value for e in req.entities)
        
        # 3. Summarize with OpenAI
        final_summary = "Analysis Unavailable."
        title = f"AI Digest - Network of {len(req.entities)} Entities"
        
        if openai and OPENAI_API_KEY:
            # Build entity list for context
            entity_list = "\n".join([f"- {e.name} ({e.type}) - {e.property_count} properties, ${e.total_value:,.0f}" for e in req.entities])
            
            prompt = (
                f"You are an investigative journalist analyzing a Connecticut property ownership network. "
                f"This network consists of {len(req.entities)} interconnected entities controlling {total_props} properties "
                f"worth ${total_val:,.0f} in assessed value.\n\n"
                
                "ENTITIES IN NETWORK:\n"
                f"{entity_list}\n\n"
                
                "YOUR TASK:\n"
                "Analyze the web search results below to uncover:\n"
                "1. WHO: Identify all principals, aliases, and related business entities\n"
                "2. WHAT: Document complaints, legal issues, evictions, code violations, and controversies\n"
                "3. PATTERNS: Spot acquisition strategies, management practices, or systemic issues\n"
                "4. CONTEXT: Note any regulatory actions, media coverage, or tenant activism\n\n"
                
                "OUTPUT FORMAT (use this exact structure):\n\n"
                
                "## NETWORK OVERVIEW\n"
                "[2-3 sentences describing the scale, geographic focus, and primary business model of this ownership group]\n\n"
                
                "## KEY PRINCIPALS & ALIASES\n"
                "[List main individuals and their associated business entities. Include known aliases or DBAs. Format: Name (Role) - Related Entities]\n\n"
                
                "## FINDINGS & RED FLAGS\n"
                "[Bullet points of specific issues found: complaints, legal cases, evictions, code violations, controversies. "
                "Each bullet should cite source URL in parentheses. NO SPECULATION - only cite what's documented.]\n\n"
                
                "## BUSINESS ENTITIES & RELATIONSHIPS\n"
                "[List key LLCs, partnerships, or corporations and their relationships to principals. Note any shell company patterns.]\n\n"
                
                "## RISK ASSESSMENT\n"
                "[One paragraph: Based on findings, assess tenant risk, regulatory scrutiny, and reputation. Be specific and evidence-based.]\n\n"
                
                "CRITICAL RULES:\n"
                "- NO marketing language, NO promotional content, NO fluff\n"
                "- ONLY factual information from search results\n"
                "- CITE sources inline as (Source: URL) for all specific claims\n"
                "- If no negative info found, state that clearly - don't invent concerns\n"
                "- Focus on actionable intelligence for tenants, advocates, and researchers\n"
                "- Identify patterns across entities (e.g., 'Multiple LLCs share same registered agent')\n\n"
                
                f"WEB SEARCH DATA:\n{full_text_context}\n"
            )
            try:
                 # Check for v1.0+ vs older SDK
                try:
                    # Try v1.0.0+ Client first
                    from openai import OpenAI
                    client = OpenAI(api_key=OPENAI_API_KEY)
                    resp = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are an investigative journalist specializing in property ownership accountability. Your reports are factual, well-sourced, and focused on protecting tenant rights and community interests."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.2,  # Lower temperature for more factual, less creative output
                        max_tokens=2000,  # Increased for detailed analysis
                    )
                    final_summary = resp.choices[0].message.content.strip()
                except ImportError:
                    # Fallback to older <1.0.0 interface
                    resp = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a meticulous investigative analyst."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3,
                        max_tokens=1500,
                    )
                    final_summary = resp["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.error(f"OpenAI Digest Error: {e}")
                final_summary = f"AI Synthesis Encountered an Error. Displaying raw search hits instead:\n\n{full_text_context}\n\nDEBUG_ERROR_DETAILS: {repr(e)}"
        else:
             final_summary = "OpenAI API Key not configured. Displaying raw web search results:\n\n" + full_text_context

        # 4. Save to Cache
        cursor.execute("""
            INSERT INTO ai_reports (entity, entity_type, report_date, title, content, sources)
            VALUES (%s, 'network_digest', %s, %s, %s, %s)
            ON CONFLICT (entity, entity_type, report_date)
            DO UPDATE SET title=EXCLUDED.title, content=EXCLUDED.content, sources=EXCLUDED.sources
            RETURNING *
        """, (digest_id, today, title, final_summary, json.dumps(all_sources)))
        
        row = cursor.fetchone()
        conn.commit()
        return row

@app.patch("/api/properties/{property_id}/geocode")
def update_property_geocode(property_id: int, lat: float, lon: float, conn=Depends(get_db_connection)):
    """
    Update the latitude and longitude for a specific property.
    This allows the frontend to persist on-the-fly geocoding results.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE properties SET latitude = %s, longitude = %s WHERE id = %s",
                (lat, lon, property_id)
            )
            # If no row updated, we might want to know, but idempotency is fine.
            conn.commit()
            return {"status": "success", "id": property_id, "lat": lat, "lon": lon}
    except Exception as e:
        logger.error(f"Failed to update geocode for property {property_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/api/freshness")
def get_data_freshness(conn=Depends(get_db_connection)):
    """
    Returns the last refresh status and external 'Last Updated' dates 
    for all configured data sources.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT source_name, source_type, external_last_updated, last_refreshed_at, refresh_status, details
                FROM data_source_status
                ORDER BY source_type, source_name
            """)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to fetch data freshness: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------
# Tenant Toolbox V2 API
# ------------------------------------------------------------
class TagRequest(BaseModel):
    tag_text: str
    color: str = "blue"

class NoteRequest(BaseModel):
    note_text: str

class AssignmentRequest(BaseModel):
    user_id: int
    role: str = "tenant"

@app.get("/api/groups/{group_id}/properties/{property_id}/metadata")
def get_property_metadata(group_id: int, property_id: int, conn=Depends(get_db_connection)):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Get Tags
        cursor.execute("SELECT * FROM property_tags WHERE group_id = %s AND property_id = %s", (group_id, property_id))
        tags = cursor.fetchall()
        
        # Get Notes
        cursor.execute("""
            SELECT pn.*, u.full_name, u.picture_url 
            FROM property_notes pn
            LEFT JOIN users u ON pn.user_id = u.id
            WHERE pn.group_id = %s AND pn.property_id = %s
            ORDER BY pn.created_at DESC
        """, (group_id, property_id))
        notes = cursor.fetchall()
        
        # Get Photos
        cursor.execute("""
            SELECT pp.*, u.full_name 
            FROM property_photos pp
            LEFT JOIN users u ON pp.user_id = u.id
            WHERE pp.group_id = %s AND pp.property_id = %s
            ORDER BY pp.created_at DESC
        """, (group_id, property_id))
        photos = cursor.fetchall()
        
        # Get Assignments
        cursor.execute("""
            SELECT pa.*, u.full_name, u.email, u.picture_url
            FROM property_assignments pa
            JOIN users u ON pa.user_id = u.id
            WHERE pa.group_id = %s AND pa.property_id = %s
        """, (group_id, property_id))
        assignments = cursor.fetchall()
        
        return {
            "tags": tags,
            "notes": notes,
            "photos": photos,
            "assignments": assignments
        }

@app.post("/api/groups/{group_id}/properties/{property_id}/tags")
def add_property_tag(group_id: int, property_id: int, tag: TagRequest, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO property_tags (group_id, property_id, tag_text, color) VALUES (%s, %s, %s, %s) RETURNING id",
            (group_id, property_id, tag.tag_text, tag.color)
        )
        new_id = cursor.fetchone()[0]
    return {"id": new_id, "status": "added"}

@app.delete("/api/groups/{group_id}/properties/{property_id}/tags/{tag_id}")
def delete_property_tag(group_id: int, property_id: int, tag_id: int, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM property_tags WHERE id = %s AND group_id = %s", (tag_id, group_id))
    return {"status": "deleted"}

@app.post("/api/groups/{group_id}/properties/{property_id}/notes")
def add_property_note(group_id: int, property_id: int, note: NoteRequest, conn=Depends(get_db_connection)):
    # Verify user from session? For now, assume user_id=1 alias 'Demo User' if no auth
    # In real app, `current_user` dependency
    user_id = 1 
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO property_notes (group_id, property_id, user_id, note_text) VALUES (%s, %s, %s, %s) RETURNING id",
            (group_id, property_id, user_id, note.note_text)
        )
        new_id = cursor.fetchone()[0]
    return {"id": new_id, "status": "added"}

# ------------------------------------------------------------
# Complexes API (Toolbox V2 Refinement)
# ------------------------------------------------------------
class ComplexRequest(BaseModel):
    name: str
    municipality: str = None
    color: str = "blue"

class PropertyMetadataRequest(BaseModel):
    custom_unit: Optional[str] = None
    custom_address: Optional[str] = None

class AssigneeRequest(BaseModel):
    user_id: int

class BatchAssignRequest(BaseModel):
    property_ids: List[int]
    complex_id: int = None # If None, unassign

class BuildingImportRequest(BaseModel):
    source_property_id: int
    target_area: Optional[str] = None

class RenameTargetRequest(BaseModel):
    old_name: str
    new_name: str

@app.get("/api/groups/{group_id}/complexes")
def get_group_complexes(group_id: int, conn=Depends(get_db_connection)):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT * FROM group_complexes WHERE group_id = %s ORDER BY created_at ASC", (group_id,))
        return cursor.fetchall()

@app.post("/api/groups/{group_id}/complexes")
def create_complex(group_id: int, req: ComplexRequest, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO group_complexes (group_id, name, municipality, color) VALUES (%s, %s, %s, %s) RETURNING id",
            (group_id, req.name, req.municipality, req.color)
        )
        new_id = cursor.fetchone()[0]
    return {"id": new_id, "status": "created"}

@app.delete("/api/groups/{group_id}/complexes/{complex_id}")
def delete_complex(group_id: int, complex_id: int, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM group_complexes WHERE id = %s AND group_id = %s", (complex_id, group_id))
    return {"status": "deleted"}

@app.put("/api/groups/{group_id}/properties/assign")
def batch_assign_properties(group_id: int, req: BatchAssignRequest, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        # Verify complex ownership if not null
        if req.complex_id is not None:
             cursor.execute("SELECT id FROM group_complexes WHERE id = %s AND group_id = %s", (req.complex_id, group_id))
             if not cursor.fetchone():
                 raise HTTPException(status_code=404, detail="Complex not found in this group")
        
        real_ids = [p for p in req.property_ids if p > 0]
        custom_ids = [abs(p) for p in req.property_ids if p < 0]

        # Update Real properties
        if real_ids:
            cursor.execute("""
                UPDATE group_properties 
                SET complex_id = %s 
                WHERE group_id = %s AND property_id = ANY(%s)
            """, (req.complex_id, group_id, real_ids))
        
        # Update Custom units
        if custom_ids:
            cursor.execute("""
                UPDATE group_custom_units
                SET complex_id = %s
                WHERE group_id = %s AND id = ANY(%s)
            """, (req.complex_id, group_id, custom_ids))
        
    return {"status": "updated", "count": len(req.property_ids)}

# ------------------------------------------------------------
# V3: Unit Management API
# ------------------------------------------------------------

@app.put("/api/groups/{group_id}/properties/{property_id}/metadata")
def update_property_metadata(group_id: int, property_id: int, req: PropertyMetadataRequest, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE group_properties 
            SET custom_unit = %s, custom_address = %s
            WHERE group_id = %s AND property_id = %s
        """, (req.custom_unit, req.custom_address, group_id, property_id))
    return {"status": "updated"}

@app.post("/api/groups/{group_id}/properties/{property_id}/assignees")
def assign_user_to_property(group_id: int, property_id: int, req: AssigneeRequest, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO group_property_assignees (group_id, property_id, user_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_id, property_id, user_id) DO NOTHING
            RETURNING id
        """, (group_id, property_id, req.user_id))
        res = cursor.fetchone()
        new_id = res[0] if res else None
    return {"status": "assigned", "id": new_id}

@app.delete("/api/groups/{group_id}/properties/{property_id}/assignees/{user_id}")
def remove_assignee(group_id: int, property_id: int, user_id: int, conn=Depends(get_db_connection)):
    with conn.cursor() as cursor:
        cursor.execute("""
            DELETE FROM group_property_assignees 
            WHERE group_id = %s AND property_id = %s AND user_id = %s
        """, (group_id, property_id, user_id))
    return {"status": "removed"}

@app.post("/api/groups/{group_id}/properties/{property_id}/photos")
def upload_property_photo(group_id: int, property_id: int, url: str, caption: str = None, conn=Depends(get_db_connection)):
    # Note: For now accepting URL directly (e.g. from signed S3 upload or external). 
    # Real implementation needs multipart/form-data handler. 
    # Mocking storage by just saving the URL.
    user_id = 1 # TODO: Get from session
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO property_photos (group_id, property_id, url, caption, uploaded_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (group_id, property_id, url, caption, user_id))
        new_id = cursor.fetchone()[0]
    return {"status": "uploaded", "id": new_id}
@app.put("/api/groups/{group_id}/complexes/{complex_id}")
async def update_complex(group_id: int, complex_id: int, payload: Dict[str, Any], request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    name = payload.get('name')
    color = payload.get('color')
    municipality = payload.get('municipality')

    with cursor_context() as cur:
        # Check membership
        cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="Not a member of this group")

        # Update
        updates = []
        params = []
        if name is not None:
            updates.append("name = %s")
            params.append(name)
        if color is not None:
            updates.append("color = %s")
            params.append(color)
        if municipality is not None:
            updates.append("municipality = %s")
            params.append(municipality)
        
        if updates:
            sql = f"UPDATE group_complexes SET {', '.join(updates)} WHERE id = %s AND group_id = %s"
            params.extend([complex_id, group_id])
            cur.execute(sql, tuple(params))
                
    return {"status": "updated"}

@app.delete("/api/groups/{group_id}/complexes/{complex_id}")
async def delete_complex(group_id: int, complex_id: int, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    with cursor_context() as cur:
        cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="Not a member")

        # Reset properties in this complex to unassigned (complex_id = NULL)
        cur.execute("UPDATE group_properties SET complex_id = NULL WHERE group_id = %s AND complex_id = %s", (group_id, complex_id))
        
        # Delete complex
        cur.execute("DELETE FROM group_complexes WHERE id = %s AND group_id = %s", (complex_id, group_id))
    
    return {"status": "deleted"}

@app.post("/api/groups/{group_id}/import_building")
async def import_building(group_id: int, req: BuildingImportRequest, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    with db_pool.getconn() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check membership
                cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
                if not cur.fetchone():
                    raise HTTPException(status_code=403, detail="Not a member")
                
                # 1. Fetch info about the source property
                # NOTE: complex_id is NOT in properties table (schema shared with 6262)
                cur.execute("SELECT property_city, normalized_address, location FROM properties WHERE id = %s", (req.source_property_id,))
                source = cur.fetchone()
                if not source:
                    raise HTTPException(status_code=404, detail="Property not found")
                
                city = source['property_city']
                addr = source['normalized_address'] or source['location'] or "Unknown Address"
                
                target_area = req.target_area or city or "General"
                
                # 2. Get all property IDs in that building (grouped by address)
                if addr and addr != "Unknown Address":
                    base_addr = _extract_street_address(addr)
                    # Find related units by address match
                    cur.execute("""
                        SELECT id FROM properties 
                        WHERE property_city = %s 
                          AND (location ILIKE %s OR normalized_address ILIKE %s)
                    """, (city, f"{base_addr}%", f"{base_addr}%"))
                    pids = [r['id'] for r in cur.fetchall()]
                    if not pids: pids = [req.source_property_id]
                else:
                    pids = [req.source_property_id]
                    
                # 3. Create/Find the group_complex
                cur.execute("""
                    SELECT id FROM group_complexes 
                    WHERE group_id = %s AND name = %s AND municipality = %s
                """, (group_id, addr, target_area))
                existing_c = cur.fetchone()
                
                if existing_c:
                    new_complex_id = existing_c['id']
                else:
                    cur.execute("""
                        INSERT INTO group_complexes (group_id, name, municipality, color)
                        VALUES (%s, %s, %s, 'blue')
                        RETURNING id
                    """, (group_id, addr, target_area))
                    new_complex_id = cur.fetchone()['id']
                    
                # 4. Insert/Update group_properties
                for pid in pids:
                    cur.execute("""
                        INSERT INTO group_properties (group_id, property_id, added_by, complex_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (group_id, property_id) DO UPDATE SET complex_id = EXCLUDED.complex_id
                    """, (group_id, pid, user['id'], new_complex_id))
                    
                conn.commit()
            return {"status": "success", "complex_id": new_complex_id, "added_count": len(pids)}
        finally:
            db_pool.putconn(conn)

@app.put("/api/groups/{group_id}/targets/rename")
async def rename_group_target(group_id: int, req: RenameTargetRequest, request: Request):
    if not TOOLBOX_ENABLED: raise HTTPException(status_code=503, detail="Toolbox features disabled")
    user = request.session.get('user')
    if not user: raise HTTPException(status_code=401, detail="Authentication required")
    
    with cursor_context() as cur:
         # Check role
         cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user['id']))
         role_row = cur.fetchone()
         if not role_row or role_row.get('role') != 'organizer':
             raise HTTPException(status_code=403, detail="Only organizers can rename targets")
             
         cur.execute("""
             UPDATE group_complexes 
             SET municipality = %s 
             WHERE group_id = %s AND municipality = %s
         """, (req.new_name, group_id, req.old_name))
         
    return {"status": "success"}

