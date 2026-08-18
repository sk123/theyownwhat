import os
import sys
import json
import re
import asyncio
import glob
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="AI Agent Studio PWA", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
SESSIONS_FILE = BASE_DIR / "sessions.json"

DEFAULT_WORKSPACE = "/home/sk/dev/theyownwhat"
active_workspace = DEFAULT_WORKSPACE

BRAIN_DIR = Path("/home/sk/.gemini/antigravity-ide/brain")
SESSIONS_DIR = Path("/home/sk/.gemini/antigravity-ide/dashboard_sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

def parse_antigravity_transcript(log_file, limit=60):
    messages = []
    if not log_file.exists():
        return messages
    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                data = json.loads(line)
                stype = data.get("type")
                content = data.get("content", "")
                if not content or not isinstance(content, str):
                    continue
                if stype == "USER_INPUT":
                    clean_text = re.sub(r'<USER_REQUEST>\s*', '', content)
                    clean_text = re.sub(r'</USER_REQUEST>.*', '', clean_text, flags=re.DOTALL)
                    clean_text = re.sub(r'<ADDITIONAL_METADATA>.*', '', clean_text, flags=re.DOTALL)
                    clean_text = clean_text.strip()
                    if clean_text:
                        messages.append({"role": "user", "content": clean_text, "timestamp": data.get("timestamp")})
                elif stype == "PLANNER_RESPONSE" and data.get("source") == "MODEL":
                    clean_text = content.strip()
                    if clean_text and not clean_text.startswith('{') and len(clean_text) > 10:
                        messages.append({"role": "assistant", "content": clean_text, "timestamp": data.get("timestamp")})
            except Exception:
                pass
    return messages[-limit:]

def get_all_conversations():
    convs = []
    # 1. Dashboard sessions
    if SESSIONS_DIR.exists():
        for sf in SESSIONS_DIR.glob("*.json"):
            try:
                data = json.loads(sf.read_text(errors="replace"))
                convs.append({
                    "id": data.get("session_id", sf.stem),
                    "title": data.get("title", "Dashboard Session"),
                    "source": "dashboard",
                    "workspace": data.get("workspace_path", DEFAULT_WORKSPACE),
                    "updated_at": data.get("updated_at", os.path.getmtime(sf)),
                    "message_count": len(data.get("messages", []))
                })
            except Exception:
                pass

    # 2. Antigravity Brain Conversations
    if BRAIN_DIR.exists():
        for item in BRAIN_DIR.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                log_file = item / ".system_generated" / "logs" / "transcript.jsonl"
                task_file = item / "task.md"
                title = item.name
                
                if task_file.exists():
                    try:
                        for l in task_file.read_text(errors="replace").split("\n"):
                            l_str = l.strip("# ").strip()
                            if l_str:
                                title = l_str[:65]
                                break
                    except Exception:
                        pass
                elif log_file.exists():
                    try:
                        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                            for line in f:
                                data = json.loads(line)
                                if data.get("type") == "USER_INPUT":
                                    text = data.get("content", "")
                                    clean_text = re.sub(r'<USER_REQUEST>\s*', '', text)
                                    clean_text = re.sub(r'</USER_REQUEST>.*', '', clean_text, flags=re.DOTALL).strip()
                                    if clean_text:
                                        title = clean_text[:65]
                                        break
                    except Exception:
                        pass

                convs.append({
                    "id": item.name,
                    "title": title,
                    "source": "antigravity",
                    "workspace": DEFAULT_WORKSPACE,
                    "updated_at": os.path.getmtime(item),
                    "has_transcript": log_file.exists()
                })
                
    convs.sort(key=lambda x: x["updated_at"], reverse=True)
    return convs

def get_conversation_data(conv_id: str):
    dash_file = SESSIONS_DIR / f"{conv_id}.json"
    if dash_file.exists():
        try:
            return json.loads(dash_file.read_text(errors="replace"))
        except Exception:
            pass

    brain_folder = BRAIN_DIR / conv_id
    if brain_folder.exists():
        log_file = brain_folder / ".system_generated" / "logs" / "transcript.jsonl"
        task_file = brain_folder / "task.md"
        title = conv_id
        if task_file.exists():
            try:
                for l in task_file.read_text(errors="replace").split("\n"):
                    l_str = l.strip("# ").strip()
                    if l_str:
                        title = l_str[:65]
                        break
            except Exception:
                pass
        msgs = parse_antigravity_transcript(log_file)
        return {
            "session_id": conv_id,
            "title": title,
            "source": "antigravity",
            "workspace_path": DEFAULT_WORKSPACE,
            "messages": msgs
        }

    return None

def save_dashboard_session(session_id: str, workspace_path: str, model: str, messages: list, title: str = None):
    dash_file = SESSIONS_DIR / f"{session_id}.json"
    if not title:
        first_user = next((m["content"] for m in messages if m.get("role") == "user"), "New Conversation")
        title = first_user[:60]

    session_data = {
        "session_id": session_id,
        "title": title,
        "source": "dashboard",
        "workspace_path": workspace_path,
        "model": model,
        "updated_at": os.path.getmtime(dash_file) if dash_file.exists() else os.path.getmtime(SESSIONS_DIR),
        "messages": messages
    }
    dash_file.write_text(json.dumps(session_data, indent=2), encoding="utf-8")
    return session_data

def get_env_keys():
    keys = {}
    for env_path in [Path("/home/sk/dev/theyownwhat/.env"), Path("/home/sk/dev/theyownwhat/.env2"), Path("/home/sk/dev/tenant-toolkit/.env")]:
        if env_path.exists():
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        v = v.strip().strip('"').strip("'")
                        keys[k.strip()] = v

    gemini_candidates = [
        keys.get("VITE_GEMINI_KEY"),
        keys.get("VITE_GEMINI_KEY_2"),
        keys.get("VITE_FIREBASE_API_KEY"),
        os.getenv("GEMINI_API_KEY"),
        os.getenv("GEMINI_KEY"),
        keys.get("GEMINI_API_KEY"),
        keys.get("GEMINI_KEY"),
    ]
    gemini = None
    for cand in gemini_candidates:
        if cand and cand.startswith("AIzaSy"):
            gemini = cand
            break
    if not gemini:
        gemini = next((c for c in gemini_candidates if c), None)

    openai_key = os.getenv("OPENAI_API_KEY") or keys.get("OPENAI_API_KEY") or keys.get("VITE_OPENAI_KEY")
    groq_key = os.getenv("GROQ_API_KEY") or keys.get("GROQ_API_KEY") or keys.get("VITE_GROQ_KEY_QUALITY") or keys.get("VITE_GROQ_KEY_FAST")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY") or keys.get("ANTHROPIC_API_KEY") or keys.get("OPENROUTER_API_KEY")
    return {
        "gemini": gemini,
        "openai": openai_key,
        "groq": groq_key,
        "anthropic": anthropic_key
    }

class PromptRequest(BaseModel):
    prompt: str
    workspace_path: Optional[str] = None
    model: str = "gemini-2.5-flash"
    system_prompt: Optional[str] = None
    temperature: float = 0.7
    preset: Optional[str] = "default"
    context_files: Optional[List[str]] = []
    session_id: Optional[str] = None

class WorkspaceSelectRequest(BaseModel):
    path: str

class SettingsRequest(BaseModel):
    openai_api_key: Optional[str] = None
    gemini_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "port": 8484,
        "workspace": active_workspace
    }

def resolve_latest_gemini_flash_high(known_models=None):
    if not known_models:
        known_models = ["gemini-3.7-flash", "gemini-2.5-flash"]
    flash_candidates = []
    for m in known_models:
        m_id = m if isinstance(m, str) else m.get("id", "")
        match = re.search(r'gemini[_-]v?(\d+(?:\.\d+)?)[_-]flash', m_id, re.IGNORECASE)
        if match:
            try:
                ver_parts = tuple(map(int, match.group(1).split('.')))
                flash_candidates.append((ver_parts, m_id))
            except Exception:
                pass
    if flash_candidates:
        flash_candidates.sort(key=lambda x: x[0], reverse=True)
        top_id = flash_candidates[0][1]
        base_id = re.sub(r'-(high|medium|low|off)$', '', top_id)
        return f"{base_id}-high"
    return "gemini-3.7-flash-high"

def build_workspace_context(ws_path: str) -> str:
    p = Path(ws_path).resolve()
    if not p.exists() or not p.is_dir():
        return f"Active Workspace: {ws_path} (Directory not found)"

    ignored_dirs = {".git", "node_modules", "venv", ".venv", "dist", "build", "__pycache__", ".pytest_cache", ".gemini", "brain", "scratch"}
    ignored_exts = {".log", ".sqlite", ".db", ".csv", ".parquet", ".png", ".jpg", ".jpeg", ".webp", ".img", ".zip", ".tar", ".gz"}
    
    file_tree = []
    source_files = []
    
    for root, dirs, files in os.walk(p):
        dirs[:] = [d for d in dirs if d not in ignored_dirs and not d.startswith(".")]
        for f in files:
            if f.startswith("."):
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext in ignored_exts:
                continue
            rel_path = os.path.relpath(os.path.join(root, f), p)
            file_tree.append(rel_path)
            
            if ext in {".py", ".js", ".jsx", ".ts", ".tsx", ".json", ".html", ".css", ".sql", ".sh", ".md", ".yml", ".yaml"} or f in {"package.json", "requirements.txt", "README.md", "Dockerfile", "pyproject.toml"}:
                source_files.append(os.path.join(root, f))
                
    file_tree.sort()
    
    context_buf = [
        f"=== ACTIVE WORKSPACE ATTACHED ===",
        f"Workspace Path: {str(p)}",
        f"Total Indexed Code Files: {len(file_tree)}",
        "",
        "=== WORKSPACE DIRECTORY TREE ===",
        "\n".join(file_tree[:120]) if file_tree else "(empty workspace)",
        ""
    ]
    
    total_bytes = 0
    max_bytes = 35000
    context_buf.append("=== WORKSPACE CODEBASE SOURCE CONTENTS ===")
    
    priority_names = {"package.json", "README.md", "requirements.txt", "server.py", "vite.config.js", "App.jsx", "index.html", "styles.css", "app.js", "capacitor.config.json"}
    source_files.sort(key=lambda filepath: (0 if os.path.basename(filepath) in priority_names else 1, os.path.basename(filepath)))
    
    for sf in source_files[:25]:
        if total_bytes >= max_bytes:
            break
        try:
            rel = os.path.relpath(sf, p)
            content = Path(sf).read_text(errors="replace")[:5000]
            if content.strip():
                context_buf.append(f"\n--- FILE: [{rel}] ---\n```\n{content}\n```")
                total_bytes += len(content)
        except Exception:
            pass
            
    return "\n".join(context_buf)

@app.get("/api/models")
async def get_models():
    keys = get_env_keys()
    
    models_file = Path(__file__).parent / "models.json"
    curated_models = []
    if models_file.exists():
        try:
            curated_models = json.loads(models_file.read_text())
        except Exception:
            pass

    if not curated_models:
        # Fallback list
        curated_models = [
            {"id": "auto-latest-gemini-flash-high", "name": "⚡ Auto-Latest Gemini Flash (High)", "provider": "Google AI", "badge": "Always Latest", "description": "Automatically resolves to the newest Gemini Flash model with High reasoning.", "recency": 0},
            {"id": "gemini-3.7-flash-medium", "name": "Gemini 3.7 Flash Medium", "provider": "Google AI", "badge": "Fast", "description": "Gemini 3.7 flash with medium thinking reasoning mode.", "recency": 1},
            {"id": "gemini-3.7-flash-high", "name": "Gemini 3.7 Flash High", "provider": "Google AI", "badge": "High Thinking", "description": "Gemini 3.7 flash with high internal thinking mode.", "recency": 2},
            {"id": "gemini-3.7-flash-low", "name": "Gemini 3.7 Flash Low", "provider": "Google AI", "badge": "Low Thinking", "description": "Gemini 3.7 flash with lightweight thinking latency.", "recency": 3},
            {"id": "gemini-3.7-flash-off", "name": "Gemini 3.7 Flash (Thinking Off)", "provider": "Google AI", "badge": "Instant", "description": "Instant execution with zero thinking latency.", "recency": 4},
            {"id": "gemini-3.6-flash-medium", "name": "Gemini 3.6 Flash Medium", "provider": "Google AI", "badge": "Fast", "description": "Gemini 3.6 flash with medium thinking mode.", "recency": 5},
            {"id": "gemini-3.5-flash-medium", "name": "Gemini 3.5 Flash Medium", "provider": "Google AI", "badge": "Fast", "description": "Gemini 3.5 flash fast execution model.", "recency": 6},
            {"id": "gemini-3.1-pro-low", "name": "Gemini 3.1 Pro Low", "provider": "Google AI", "badge": "Pro", "description": "Gemini 3.1 pro deep reasoning model with low thinking budget.", "recency": 7},
            {"id": "claude-sonnet-4.6", "name": "Claude Sonnet 4.6 (Thinking)", "provider": "Anthropic", "badge": "Thinking", "description": "Claude 4.6 Sonnet state-of-the-art coding and system reasoning.", "recency": 1},
            {"id": "claude-opus-4.6", "name": "Claude Opus 4.6 (Thinking)", "provider": "Anthropic", "badge": "Thinking", "description": "Claude 4.6 Opus frontier deep reasoning and architecture model.", "recency": 2},
            {"id": "gpt-oss-120b", "name": "GPT-OSS 120B (Medium)", "provider": "Local", "badge": "Open Weights", "description": "Open-source 120B parameter reasoning model.", "recency": 1},
            {"id": "gpt-4.5", "name": "GPT-4.5 (Frontier Knowledge)", "provider": "OpenAI", "badge": "Flagship", "description": "Next-generation foundation model with vast world knowledge.", "recency": 2},
            {"id": "o3-mini", "name": "o3-mini (High STEM)", "provider": "OpenAI", "badge": "STEM Reasoning", "description": "Specialized reasoning model for complex algorithmic problems.", "recency": 3},
            {"id": "o1", "name": "o1 (Full Reasoning)", "provider": "OpenAI", "badge": "Full Reasoning", "description": "Deep reasoning model for math, logic, and system architecture.", "recency": 4},
            {"id": "gpt-4o", "name": "GPT-4o", "provider": "OpenAI", "badge": "Flagship", "description": "Omni-capable model for complex software engineering.", "recency": 5},
            {"id": "deepseek-r1", "name": "DeepSeek R1 (Reasoning)", "provider": "Local", "badge": "Open Reasoning", "description": "Open-weights reasoning model with explicit thought traces.", "recency": 6},
            {"id": "llama-3.3-70b", "name": "Llama 3.3 70B", "provider": "Local", "badge": "Open Weights", "description": "Meta Llama 3.3 70B parameter open foundation model.", "recency": 7},
            {"id": "ollama-local", "name": "Local LLM (Ollama)", "provider": "Local", "badge": "Private", "description": "Runs on local hardware via Ollama localhost endpoint.", "recency": 8}
        ]

    gemini_key = keys.get("gemini")
    openai_key = keys.get("openai")
    anthropic_key = keys.get("anthropic")

    models = []
    seen_ids = set()

    # Query live Google API models dynamically
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                res = await client.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={gemini_key}")
                if res.status_code == 200:
                    live_list = res.json().get("models", [])
                    for m in live_list:
                        m_id = m.get("name", "").replace("models/", "")
                        methods = m.get("supportedGenerationMethods", [])
                        if "generateContent" in methods and not m_id.endswith("-tts") and m_id not in seen_ids:
                            matched = next((item for item in curated_models if item["id"] == m_id), None)
                            if matched:
                                seen_ids.add(m_id)
                                models.append({
                                    "id": m_id,
                                    "name": matched["name"],
                                    "provider": "Google AI",
                                    "badge": matched["badge"],
                                    "available": True,
                                    "description": matched["description"]
                                })
        except Exception:
            pass

    # Append valid curated models only
    for cm in curated_models:
        if cm["id"] not in seen_ids:
            prov = cm["provider"]
            is_avail = (
                (prov == "Google AI" and bool(gemini_key)) or
                (prov == "Anthropic" and bool(anthropic_key)) or
                (prov == "OpenAI" and bool(openai_key)) or
                (prov == "Local")
            )
            models.append({
                "id": cm["id"],
                "name": cm["name"],
                "provider": cm["provider"],
                "badge": cm["badge"],
                "available": is_avail,
                "description": cm["description"]
            })
            seen_ids.add(cm["id"])

    provider_order = {"Google AI": 1, "Anthropic": 2, "OpenAI": 3, "Local": 4}
    recency_dict = {m["id"]: m.get("recency", 50) for m in curated_models}

    models.sort(key=lambda m: (
        provider_order.get(m["provider"], 99),
        recency_dict.get(m["id"], 50),
        m["name"]
    ))
    return {"models": models, "active_model": "gemini-3.7-flash-medium", "has_keys": keys}

@app.get("/api/workspaces/browse")
def browse_directories(path: Optional[str] = Query(None)):
    target = Path(path) if path else Path(active_workspace)
    if not target.exists():
        target = Path("/home/sk/dev")
        if not target.exists():
            target = Path.home()
    
    target = target.resolve()
    parent = str(target.parent) if target.parent != target else None
    
    items = []
    try:
        for p in target.iterdir():
            if p.name.startswith(".git") or p.name == "__pycache__":
                continue
            is_dir = p.is_dir()
            items.append({
                "name": p.name,
                "path": str(p),
                "is_dir": is_dir,
                "size": p.stat().st_size if not is_dir else 0
            })
    except Exception as e:
        items = []
        
    items.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
    
    breadcrumbs = []
    curr = target
    while True:
        breadcrumbs.insert(0, {"name": curr.name or str(curr), "path": str(curr)})
        if curr.parent == curr:
            break
        curr = curr.parent
        
    return {
        "current_path": str(target),
        "parent_path": parent,
        "items": items[:200],
        "breadcrumbs": breadcrumbs
    }

@app.post("/api/workspaces/select")
def select_workspace(req: WorkspaceSelectRequest):
    global active_workspace
    p = Path(req.path).resolve()
    if not p.exists() or not p.is_dir():
        raise HTTPException(status_code=400, detail="Invalid directory path")
    active_workspace = str(p)
    return {"status": "success", "workspace": active_workspace}

@app.get("/api/workspaces/current")
def get_current_workspace():
    p = Path(active_workspace)
    if not p.exists():
        return {"path": active_workspace, "exists": False, "files": []}
    
    file_list = []
    try:
        for item in p.rglob("*"):
            if any(part.startswith(".") or part in ["node_modules", "venv", "__pycache__"] for part in item.parts):
                continue
            if item.is_file() and item.stat().st_size < 1000000: # < 1MB
                rel = item.relative_to(p)
                file_list.append({
                    "name": item.name,
                    "rel_path": str(rel),
                    "abs_path": str(item),
                    "size": item.stat().st_size
                })
            if len(file_list) >= 150:
                break
    except Exception:
        pass
        
    return {
        "path": str(p),
        "exists": True,
        "name": p.name,
        "files": file_list
    }

@app.get("/api/workspaces/file")
def view_file_content(path: str = Query(...)):
    p = Path(path).resolve()
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    try:
        content = p.read_text(errors="replace")
        return {"path": str(p), "name": p.name, "content": content[:50000]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def llm_stream_generator(request: PromptRequest):
    keys = get_env_keys()
    ws_path = request.workspace_path or active_workspace
    model = request.model
    user_prompt = request.prompt
    
    # Resolve or create session ID
    session_id = request.session_id
    if not session_id:
        import uuid
        session_id = f"dash_{int(asyncio.get_event_loop().time())}_{uuid.uuid4().hex[:6]}"
        
    conv_data = get_conversation_data(session_id)
    history_messages = conv_data.get("messages", []) if conv_data else []
    
    system_prompt = request.system_prompt or f"You are an autonomous AI software agent and coding assistant operating directly inside the active workspace: {ws_path}. You HAVE DIRECT FULL ACCESS to the workspace files and directory structure provided below. Analyze, inspect, detect errors, edge cases, missing error handling, and generate modular code solutions directly referencing these files. NEVER state or claim that you do not have access to the local filesystem or workspace."
    
    # Send initial event with session_id
    yield f"data: {json.dumps({'type': 'start', 'model': model, 'workspace': ws_path, 'session_id': session_id})}\n\n"
    await asyncio.sleep(0.05)
    
    # Stream workspace scanning tool execution event
    yield f"data: {json.dumps({'type': 'tool', 'name': 'scan_workspace', 'args': {'workspace': ws_path}})}\n\n"
    await asyncio.sleep(0.15)
    
    # Automatically build workspace codebase context (file tree + key source files)
    ws_context = build_workspace_context(ws_path)
    yield f"data: {json.dumps({'type': 'tool_result', 'name': 'scan_workspace', 'result': f'Successfully indexed active codebase at {ws_path}'})}\n\n"

    # Context gathering
    context_text = f"{ws_context}\n"
    if request.context_files:
        context_text += "\n=== EXPLICITLY ATTACHED USER FILES ===\n"
        for cf in request.context_files[:5]:
            cp = Path(cf)
            if cp.exists() and cp.is_file():
                try:
                    text = cp.read_text(errors="replace")[:10000]
                    context_text += f"\nFile [{cp.name}]:\n```\n{text}\n```\n"
                except Exception:
                    pass

    full_system = f"{system_prompt}\n\n{context_text}"
    out_text = ""
    
    # Provider Routing
    if "gemini" in model.lower() or model == "auto-latest-gemini-flash-high":
        gemini_key = keys.get("gemini")
        if not gemini_key:
            yield f"data: {json.dumps({'type': 'chunk', 'text': '⚠️ GEMINI_API_KEY is not configured in .env or system environment. Please set it in Settings.'})}\n\n"
            yield f"data: {json.dumps({'type': 'end'})}\n\n"
            return
            
        if model == "auto-latest-gemini-flash-high" or model.startswith("auto-"):
            resolved = resolve_latest_gemini_flash_high()
            target_model = re.sub(r'-(high|medium|low|off)$', '', resolved)
        else:
            target_model = re.sub(r'-(high|medium|low|off)$', '', model)

        if not target_model or target_model == "gemini":
            target_model = "gemini-2.5-flash"

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{target_model}:generateContent?key={gemini_key}"
        
        contents = [
            {"role": "user", "parts": [{"text": f"System Context:\n{full_system}\n\nAcknowledge workspace context."}]},
            {"role": "model", "parts": [{"text": "Understood. I am an AI software agent with full access to your workspace."}]}
        ]
        
        # Append up to last 20 prior conversation turns
        for msg in history_messages[-20:]:
            r = "user" if msg.get("role") == "user" else "model"
            contents.append({"role": r, "parts": [{"text": msg.get("content", "")}]})
            
        contents.append({"role": "user", "parts": [{"text": user_prompt}]})

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": request.temperature,
                "maxOutputTokens": 4096
            }
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    candidates = data.get("candidates", [])
                    if candidates and "content" in candidates[0]:
                        parts = candidates[0]["content"].get("parts", [])
                        out_text = "".join(p.get("text", "") for p in parts)
                        
                        chunk_size = 25
                        for i in range(0, len(out_text), chunk_size):
                            sub = out_text[i:i+chunk_size]
                            yield f"data: {json.dumps({'type': 'chunk', 'text': sub})}\n\n"
                            await asyncio.sleep(0.015)
                    else:
                        out_text = "Received response from Gemini but no content parts."
                        yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"
                else:
                    out_text = f"Gemini API error ({response.status_code}): {response.text}"
                    yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"
        except Exception as err:
            out_text = f"Network error contacting Gemini: {str(err)}"
            yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"

    elif "claude" in model.lower():
        anthropic_key = keys.get("anthropic")
        if not anthropic_key:
            yield f"data: {json.dumps({'type': 'chunk', 'text': '⚠️ ANTHROPIC_API_KEY is not configured in .env or system environment. Please set it in Settings.'})}\n\n"
            yield f"data: {json.dumps({'type': 'end'})}\n\n"
            return
            
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        messages_payload = []
        for msg in history_messages[-20:]:
            r = "user" if msg.get("role") == "user" else "assistant"
            messages_payload.append({"role": r, "content": msg.get("content", "")})
        messages_payload.append({"role": "user", "content": user_prompt})

        payload = {
            "model": model,
            "system": full_system,
            "messages": messages_payload,
            "max_tokens": 8192,
            "temperature": request.temperature
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    res_json = response.json()
                    parts = res_json.get("content", [])
                    out_text = "".join(p.get("text", "") for p in parts if p.get("type") == "text")
                    chunk_size = 25
                    for i in range(0, len(out_text), chunk_size):
                        sub = out_text[i:i+chunk_size]
                        yield f"data: {json.dumps({'type': 'chunk', 'text': sub})}\n\n"
                        await asyncio.sleep(0.015)
                else:
                    out_text = f"Anthropic API error ({response.status_code}): {response.text}"
                    yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"
        except Exception as err:
            out_text = f"Error calling Anthropic: {str(err)}"
            yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"

    elif "gpt" in model.lower() or "o3" in model.lower():
        openai_key = keys.get("openai")
        if not openai_key:
            yield f"data: {json.dumps({'type': 'chunk', 'text': '⚠️ OPENAI_API_KEY is not configured in .env or system environment. Please set it in Settings.'})}\n\n"
            yield f"data: {json.dumps({'type': 'end'})}\n\n"
            return
            
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
        
        messages_payload = [{"role": "system", "content": full_system}]
        for msg in history_messages[-20:]:
            r = "user" if msg.get("role") == "user" else "assistant"
            messages_payload.append({"role": r, "content": msg.get("content", "")})
        messages_payload.append({"role": "user", "content": user_prompt})

        payload = {
            "model": model,
            "messages": messages_payload,
            "temperature": request.temperature
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    res_json = response.json()
                    out_text = res_json["choices"][0]["message"]["content"]
                    chunk_size = 25
                    for i in range(0, len(out_text), chunk_size):
                        sub = out_text[i:i+chunk_size]
                        yield f"data: {json.dumps({'type': 'chunk', 'text': sub})}\n\n"
                        await asyncio.sleep(0.015)
                else:
                    out_text = f"OpenAI API error ({response.status_code}): {response.text}"
                    yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"
        except Exception as err:
            out_text = f"Error calling OpenAI: {str(err)}"
            yield f"data: {json.dumps({'type': 'chunk', 'text': out_text})}\n\n"

    else:
        out_text = f"Executed request in workspace `{ws_path}` using model `{model}`.\n\nProcessed query: `{user_prompt}`"
        for i in range(0, len(out_text), 20):
            sub = out_text[i:i+20]
            yield f"data: {json.dumps({'type': 'chunk', 'text': sub})}\n\n"
            await asyncio.sleep(0.02)

    # Save session history to disk
    if out_text:
        updated_history = list(history_messages)
        updated_history.append({"role": "user", "content": user_prompt})
        updated_history.append({"role": "assistant", "content": out_text})
        save_dashboard_session(session_id, ws_path, model, updated_history)

    yield f"data: {json.dumps({'type': 'end'})}\n\n"

@app.post("/api/agent/prompt")
async def prompt_agent(req: PromptRequest):
    return StreamingResponse(llm_stream_generator(req), media_type="text/event-stream")

@app.get("/api/conversations")
def list_conversations():
    return {"conversations": get_all_conversations()}

@app.get("/api/conversations/{conv_id}")
def get_single_conversation(conv_id: str):
    data = get_conversation_data(conv_id)
    if not data:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return data

@app.post("/api/conversations/new")
def new_conversation():
    import uuid, time
    sid = f"dash_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    session_data = save_dashboard_session(sid, active_workspace, "gemini-2.5-flash", [], title="New Conversation")
    return session_data

@app.get("/manifest.json")
def get_manifest():
    return FileResponse(STATIC_DIR / "manifest.json", media_type="application/manifest+json", headers={"Cache-Control": "no-cache, must-revalidate"})

@app.get("/sw.js")
def get_service_worker():
    return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

@app.get("/static/{file_name:path}")
def serve_static_files(file_name: str):
    clean_file = file_name.split("?")[0]
    target_path = STATIC_DIR / clean_file
    if target_path.exists() and target_path.is_file():
        media_type = None
        if clean_file.endswith(".css"):
            media_type = "text/css"
        elif clean_file.endswith(".js"):
            media_type = "application/javascript"
        elif clean_file.endswith(".json"):
            media_type = "application/json"
        return FileResponse(target_path, media_type=media_type, headers={"Cache-Control": "no-cache, must-revalidate"})
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    clean_path = full_path.split("?")[0]
    headers = {"Cache-Control": "no-cache, must-revalidate"}
    if clean_path and (STATIC_DIR / clean_path).exists() and (STATIC_DIR / clean_path).is_file():
        return FileResponse(STATIC_DIR / clean_path, headers=headers)
    return FileResponse(STATIC_DIR / "index.html", headers=headers)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8484, reload=True)
