import os
import sys
import json
import re
import time
import uuid
import socket
import asyncio
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="AI Agent Studio PWA (Antigravity Bridge)", version="3.0.0")

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

# Antigravity Brains: CLI and IDE
CLI_BRAIN_DIR = Path("/home/sk/.gemini/antigravity-cli/brain")
IDE_BRAIN_DIR = Path("/home/sk/.gemini/antigravity-ide/brain")
SESSIONS_DIR = Path("/home/sk/.gemini/antigravity-ide/dashboard_sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# Antigravity CLI binary path
AGY_PATH = shutil.which("agy") or "/home/sk/.local/bin/agy"

KNOWN_PROJECTS = [
    {"name": "theyownwhat", "path": "/home/sk/dev/theyownwhat", "icon": "🏛️", "desc": "Hidden property ownership & investigative network mapping"},
    {"name": "tenant-toolkit", "path": "/home/sk/dev/tenant-toolkit", "icon": "🛠️", "desc": "Tenant repair advocacy, rent ledger & legal housing tools"},
    {"name": "troubadour", "path": "/home/sk/dev/troubadour", "icon": "🎵", "desc": "Tour routing radar, grassroots concert booking & encrypted private vault"},
    {"name": "scripts", "path": "/home/sk/scripts", "icon": "📜", "desc": "System maintenance & IDE update scripts"},
    {"name": "dev", "path": "/home/sk/dev", "icon": "📁", "desc": "All development projects workspace"}
]

# Track running processes for cancellation
active_subprocesses: Dict[str, asyncio.subprocess.Process] = {}

# Dynamic models cache
cached_models: List[Dict[str, Any]] = []
models_cache_timestamp: float = 0
MODELS_CACHE_TTL = 600  # 10 minutes

def parse_antigravity_transcript(log_file: Path, limit: int = 80) -> List[Dict[str, Any]]:
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
                        messages.append({
                            "role": "user",
                            "content": clean_text,
                            "timestamp": data.get("created_at") or data.get("timestamp")
                        })
                elif stype == "PLANNER_RESPONSE" and data.get("source") == "MODEL":
                    clean_text = content.strip()
                    if clean_text and not clean_text.startswith('{') and len(clean_text) > 1:
                        messages.append({
                            "role": "assistant",
                            "content": clean_text,
                            "timestamp": data.get("created_at") or data.get("timestamp")
                        })
            except Exception:
                pass
    return messages[-limit:]

def find_conversation_folder(conv_id: str) -> Optional[Path]:
    """Search for conversation across CLI brain and IDE brain."""
    if not conv_id:
        return None
    cli_dir = CLI_BRAIN_DIR / conv_id
    if cli_dir.exists() and cli_dir.is_dir():
        return cli_dir
    ide_dir = IDE_BRAIN_DIR / conv_id
    if ide_dir.exists() and ide_dir.is_dir():
        return ide_dir
    return None

def get_all_conversations() -> List[Dict[str, Any]]:
    convs = []
    seen_ids = set()

    # 1. Dashboard sessions
    if SESSIONS_DIR.exists():
        for sf in SESSIONS_DIR.glob("*.json"):
            try:
                data = json.loads(sf.read_text(errors="replace"))
                sid = data.get("session_id", sf.stem)
                if sid not in seen_ids:
                    seen_ids.add(sid)
                    convs.append({
                        "id": sid,
                        "title": data.get("title", "Dashboard Session"),
                        "source": "dashboard",
                        "workspace": data.get("workspace_path", active_workspace),
                        "updated_at": data.get("updated_at", os.path.getmtime(sf)),
                        "message_count": len(data.get("messages", []))
                    })
            except Exception:
                pass

    # 2. CLI Brain Conversations (primary for `agy`)
    for brain_root in [CLI_BRAIN_DIR, IDE_BRAIN_DIR]:
        if not brain_root.exists():
            continue
        for item in brain_root.iterdir():
            if not item.is_dir() or item.name.startswith(".") or item.name in seen_ids:
                continue
            
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
                            d = json.loads(line)
                            if d.get("type") == "USER_INPUT":
                                text = d.get("content", "")
                                clean = re.sub(r'<USER_REQUEST>\s*', '', text)
                                clean = re.sub(r'</USER_REQUEST>.*', '', clean, flags=re.DOTALL).strip()
                                if clean:
                                    title = clean[:65]
                                    break
                except Exception:
                    pass

            seen_ids.add(item.name)
            convs.append({
                "id": item.name,
                "title": title,
                "source": "antigravity",
                "workspace": active_workspace,
                "updated_at": os.path.getmtime(item),
                "has_transcript": log_file.exists()
            })
            
    convs.sort(key=lambda x: x["updated_at"], reverse=True)
    return convs

def get_conversation_data(conv_id: str) -> Optional[Dict[str, Any]]:
    # Check dashboard session file first
    dash_file = SESSIONS_DIR / f"{conv_id}.json"
    if dash_file.exists():
        try:
            return json.loads(dash_file.read_text(errors="replace"))
        except Exception:
            pass

    folder = find_conversation_folder(conv_id)
    if folder:
        log_file = folder / ".system_generated" / "logs" / "transcript.jsonl"
        task_file = folder / "task.md"
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
            "workspace_path": active_workspace,
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
        "updated_at": time.time(),
        "messages": messages
    }
    dash_file.write_text(json.dumps(session_data, indent=2), encoding="utf-8")
    return session_data

class PromptRequest(BaseModel):
    prompt: str
    workspace_path: Optional[str] = None
    model: str = "gemini-3.8-flash-high"
    system_prompt: Optional[str] = None
    temperature: float = 0.7
    preset: Optional[str] = "default"
    context_files: Optional[List[str]] = []
    session_id: Optional[str] = None
    dev_port: Optional[int] = 1597
    is_new_project: Optional[bool] = False
    project_name: Optional[str] = None
    mode: Optional[str] = "accept-edits"

class WorkspaceSelectRequest(BaseModel):
    path: str

class CreateProjectRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    port: Optional[int] = 1597
    init_git: Optional[bool] = True
    initial_prompt: Optional[str] = None
    model: Optional[str] = "gemini-3.8-flash-high"

class TerminalExecRequest(BaseModel):
    command: str
    cwd: Optional[str] = None
    timeout: Optional[int] = 60

class FileSaveRequest(BaseModel):
    path: str
    content: str

class CancelRequest(BaseModel):
    session_id: str

@app.get("/api/health")
def health(request: Request):
    cf_email = request.headers.get("cf-access-authenticated-user-email") or request.headers.get("x-auth-request-email")
    return {
        "status": "ok",
        "port": 8484,
        "engine": "Antigravity CLI Bridge",
        "agy_binary": AGY_PATH,
        "agy_available": Path(AGY_PATH).exists(),
        "workspace": active_workspace,
        "workspace_name": Path(active_workspace).name,
        "cli_brain": str(CLI_BRAIN_DIR),
        "ide_brain": str(IDE_BRAIN_DIR),
        "authenticated_user": cf_email,
        "auth_provider": "Cloudflare Access" if cf_email else "Local Direct"
    }

@app.get("/api/auth/me")
def get_auth_user(request: Request):
    cf_email = request.headers.get("cf-access-authenticated-user-email") or request.headers.get("x-auth-request-email")
    cf_identity = request.headers.get("cf-access-jwt-assertion")
    return {
        "authenticated": bool(cf_email or cf_identity),
        "email": cf_email or "local@localhost",
        "provider": "Cloudflare Access" if cf_email else "Local Direct"
    }

async def fetch_dynamic_agy_models(force: bool = False) -> List[Dict[str, Any]]:
    global cached_models, models_cache_timestamp
    now = time.time()
    if not force and cached_models and (now - models_cache_timestamp < MODELS_CACHE_TTL):
        return cached_models

    models = []
    if Path(AGY_PATH).exists():
        try:
            proc = await asyncio.create_subprocess_exec(
                AGY_PATH, "models",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
            raw_lines = stdout.decode("utf-8", errors="replace").splitlines()
            for l in raw_lines:
                clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", l)
                clean = re.sub(r"^[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏\s\r]+Fetching available models\.\.\.", "", clean).strip()
                if not clean:
                    continue
                parts = clean.split(maxsplit=1)
                if len(parts) >= 1:
                    m_id = parts[0].strip()
                    m_name = parts[1].strip() if len(parts) > 1 else m_id

                    # Assign badges, recency and description dynamically
                    badge = "Ready"
                    desc = f"Antigravity native model: {m_name}"
                    recency = 50

                    if "3.8" in m_id:
                        if "high" in m_id:
                            badge = "Flagship (Thinking)"
                            desc = "Google's newest reasoning flagship with deep thinking budget."
                            recency = 1
                        elif "medium" in m_id:
                            badge = "Default / Balanced"
                            desc = "Fast execution with high-quality reasoning."
                            recency = 2
                        else:
                            badge = "Ultra Fast"
                            desc = "Instant response for rapid tasks and code completions."
                            recency = 3
                    elif "claude-sonnet-4-6" in m_id:
                        badge = "Frontier Reasoning"
                        desc = "Anthropic Claude 4.6 Sonnet state-of-the-art coding engine."
                        recency = 4
                    elif "claude-opus-4-6" in m_id:
                        badge = "Deep Architecture"
                        desc = "Anthropic Claude 4.6 Opus frontier system architecture model."
                        recency = 5
                    elif "3.7" in m_id:
                        badge = "3.7 Generation"
                        desc = "Google's 3.7 reasoning model."
                        recency = 10
                    elif "gpt-oss-120b" in m_id:
                        badge = "Open Weights"
                        desc = "Open-source 120B parameter reasoning model."
                        recency = 15
                    elif "3.1-pro" in m_id:
                        badge = "Pro"
                        desc = "Pro tier multi-file reasoning."
                        recency = 20

                    models.append({
                        "id": m_id,
                        "name": m_name,
                        "provider": "Anthropic" if "claude" in m_id else ("OpenAI / OSS" if "gpt" in m_id or "oss" in m_id else "Google Antigravity"),
                        "badge": badge,
                        "available": True,
                        "description": desc,
                        "recency": recency
                    })
        except Exception as e:
            print(f"Error fetching agy models: {e}")

    # Fallback if command timed out or returned empty
    if not models:
        models = [
            {"id": "gemini-3.8-flash-high", "name": "Gemini 3.8 Flash (High)", "provider": "Google Antigravity", "badge": "Flagship (Thinking)", "available": True, "description": "Google's newest reasoning flagship.", "recency": 1},
            {"id": "gemini-3.8-flash-medium", "name": "Gemini 3.8 Flash (Medium)", "provider": "Google Antigravity", "badge": "Default / Balanced", "available": True, "description": "Fast execution with balanced chain-of-thought.", "recency": 2},
            {"id": "gemini-3.8-flash-low", "name": "Gemini 3.8 Flash (Low)", "provider": "Google Antigravity", "badge": "Ultra Fast", "available": True, "description": "Instant execution for interactive prompts.", "recency": 3},
            {"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6 (Thinking)", "provider": "Anthropic", "badge": "Frontier Reasoning", "available": True, "description": "State-of-the-art coding and reasoning.", "recency": 4},
            {"id": "claude-opus-4-6-thinking", "name": "Claude Opus 4.6 (Thinking)", "provider": "Anthropic", "badge": "Deep Architecture", "available": True, "description": "Frontier system architecture.", "recency": 5},
            {"id": "gemini-3.7-flash-high", "name": "Gemini 3.7 Flash (High)", "provider": "Google Antigravity", "badge": "3.7 Generation", "available": True, "description": "Google 3.7 reasoning model.", "recency": 10},
            {"id": "gpt-oss-120b-medium", "name": "GPT-OSS 120B (Medium)", "provider": "OpenAI / OSS", "badge": "Open Weights", "available": True, "description": "Open weights 120B parameter model.", "recency": 15}
        ]

    models.sort(key=lambda m: (m["recency"], m["name"]))
    cached_models = models
    models_cache_timestamp = now
    return models

@app.get("/api/models")
async def get_models(force: bool = False):
    models = await fetch_dynamic_agy_models(force=force)
    active_default = models[0]["id"] if models else "gemini-3.8-flash-high"
    return {"models": models, "active_model": active_default, "has_keys": {"antigravity": True}}

@app.post("/api/models/refresh")
async def refresh_models():
    models = await fetch_dynamic_agy_models(force=True)
    return {"status": "refreshed", "count": len(models), "models": models}

# ─────────────────────────────────────────────────────────────────────────────
# Workspace & File Explorer APIs
# ─────────────────────────────────────────────────────────────────────────────

def build_file_tree(dir_path: Path, max_depth: int = 3, current_depth: int = 0) -> List[Dict[str, Any]]:
    if current_depth >= max_depth:
        return []
    
    ignore_dirs = {
        ".git", "node_modules", "venv", "__pycache__", "dist", ".next", ".cache", 
        ".idea", ".vscode", "build", "data", "hartford_images", "static", "uploads", 
        "logs", "media", "tempmediaStorage", ".system_generated"
    }
    entries = []
    try:
        items = sorted(list(dir_path.iterdir()), key=lambda p: (not p.is_dir(), p.name.lower()))
        count = 0
        for item in items:
            if count >= 45:
                entries.append({
                    "name": f"... ({len(items) - count} more items)",
                    "path": str(dir_path),
                    "is_dir": False,
                    "size": 0
                })
                break
            if item.name.startswith(".") and item.name != ".env":
                continue
            if item.name in ignore_dirs:
                continue

            is_dir = item.is_dir()
            node = {
                "name": item.name,
                "path": str(item),
                "is_dir": is_dir,
                "size": item.stat().st_size if not is_dir else 0,
            }
            if is_dir:
                node["children"] = build_file_tree(item, max_depth, current_depth + 1)
            entries.append(node)
            count += 1
    except Exception:
        pass
    return entries

@app.get("/api/files/tree")
def get_file_tree(path: Optional[str] = Query(None)):
    target = Path(path) if path else Path(active_workspace)
    if not target.exists() or not target.is_dir():
        target = Path(active_workspace)
    
    tree = build_file_tree(target)
    return {
        "workspace": str(target),
        "name": target.name,
        "tree": tree
    }

@app.get("/api/files/read")
def read_file_content(path: str = Query(...)):
    p = Path(path).resolve()
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    try:
        content = p.read_text(encoding="utf-8", errors="replace")
        ext = p.suffix.lower().lstrip(".")
        return {
            "path": str(p),
            "name": p.name,
            "ext": ext,
            "size": p.stat().st_size,
            "content": content
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/files/save")
def save_file_content(req: FileSaveRequest):
    p = Path(req.path).resolve()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(req.content, encoding="utf-8")
        return {"status": "saved", "path": str(p), "size": p.stat().st_size}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/files/git-status")
def get_git_status():
    ws = Path(active_workspace)
    if not (ws / ".git").exists():
        return {"is_git": False, "branch": "", "files": []}
    try:
        b_res = subprocess.run(["git", "branch", "--show-current"], cwd=str(ws), capture_output=True, text=True)
        branch = b_res.stdout.strip()

        s_res = subprocess.run(["git", "status", "--porcelain"], cwd=str(ws), capture_output=True, text=True)
        changed_files = []
        for line in s_res.stdout.splitlines():
            if len(line) >= 4:
                status_code = line[:2].strip()
                file_name = line[3:].strip()
                changed_files.append({"status": status_code, "file": file_name})

        return {"is_git": True, "branch": branch or "main", "files": changed_files}
    except Exception as e:
        return {"is_git": True, "branch": "unknown", "error": str(e), "files": []}

# ─────────────────────────────────────────────────────────────────────────────
# Terminal Runner API
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/terminal/exec")
async def exec_terminal_command(req: TerminalExecRequest):
    cwd = req.cwd or active_workspace
    cmd = req.command.strip()
    if not cmd:
        raise HTTPException(status_code=400, detail="Empty command")

    try:
        env = os.environ.copy()
        env["PATH"] = f"/home/sk/.local/bin:/home/sk/.gemini/antigravity-ide/bin:/home/sk/.nvm/versions/node/v22.22.0/bin:{env.get('PATH', '')}"

        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=req.timeout or 60)
            return {
                "command": cmd,
                "exit_code": proc.returncode,
                "stdout": stdout.decode("utf-8", errors="replace"),
                "stderr": stderr.decode("utf-8", errors="replace")
            }
        except asyncio.TimeoutError:
            proc.kill()
            return {
                "command": cmd,
                "exit_code": -1,
                "stdout": "",
                "stderr": f"Command timed out after {req.timeout}s"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# Dev Server & Live Preview Port Inspector
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/preview/status")
def check_preview_port(port: int = Query(1597)):
    """Check if dev server is listening on target port."""
    is_open = False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        res = s.connect_ex(("127.0.0.1", port))
        is_open = (res == 0)
    return {
        "port": port,
        "running": is_open,
        "url": f"http://localhost:{port}" if is_open else None
    }

# ─────────────────────────────────────────────────────────────────────────────
# Workspace Directory Navigation
# ─────────────────────────────────────────────────────────────────────────────

def get_directory_listing(target_path: Optional[str] = None):
    target = Path(target_path) if target_path else Path(active_workspace)
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
                "isDir": is_dir,
                "size": p.stat().st_size if not is_dir else 0
            })
    except Exception:
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
        "path": str(target),
        "current_path": str(target),
        "name": target.name or str(target),
        "parent_path": parent,
        "items": items[:250],
        "files": items[:250],
        "breadcrumbs": breadcrumbs,
        "quick_projects": KNOWN_PROJECTS
    }

@app.get("/api/workspaces/browse")
@app.get("/api/workspaces/list")
def list_directories(path: Optional[str] = Query(None)):
    return get_directory_listing(path)

@app.get("/api/workspaces/projects")
def get_known_projects():
    projects = []
    for proj in KNOWN_PROJECTS:
        p = Path(proj["path"])
        exists = p.exists() and p.is_dir()
        projects.append({
            **proj,
            "exists": exists,
            "is_active": str(p.resolve()) == str(Path(active_workspace).resolve())
        })
    return {"projects": projects, "active_workspace": active_workspace}

@app.post("/api/workspaces/create")
def create_new_project(req: CreateProjectRequest):
    global active_workspace
    raw_name = req.name.strip().lower()
    slug = re.sub(r'[^a-z0-9_-]+', '-', raw_name).strip('-')
    if not slug:
        raise HTTPException(status_code=400, detail="Invalid project name")
    
    target_dir = Path("/home/sk/dev") / slug
    is_new = not target_dir.exists()
    target_dir.mkdir(parents=True, exist_ok=True)
    
    if req.init_git and not (target_dir / ".git").exists():
        try:
            subprocess.run(["git", "init"], cwd=str(target_dir), check=False, capture_output=True)
        except Exception:
            pass
            
    gitignore_file = target_dir / ".gitignore"
    if not gitignore_file.exists():
        try:
            gitignore_file.write_text("node_modules/\n.env\n*.log\n__pycache__/\n.DS_Store\ndist/\n", encoding="utf-8")
        except Exception:
            pass
            
    active_workspace = str(target_dir)
    sid = str(uuid.uuid4())
    title = f"🚀 {slug.replace('-', ' ').title()}"
    dev_port = req.port or 1597
    
    save_dashboard_session(
        sid, 
        active_workspace, 
        req.model or "gemini-3.8-flash-high", 
        [], 
        title=title
    )
    
    return {
        "status": "success",
        "workspace": active_workspace,
        "name": slug,
        "session_id": sid,
        "is_new": is_new,
        "dev_port": dev_port,
        "initial_prompt": req.initial_prompt
    }

@app.post("/api/workspaces/select")
def select_workspace(req: WorkspaceSelectRequest):
    global active_workspace
    p = Path(req.path).resolve()
    if not p.exists() or not p.is_dir():
        raise HTTPException(status_code=400, detail="Invalid directory path")
    active_workspace = str(p)
    return {"status": "success", "workspace": active_workspace, "name": p.name}

@app.get("/api/workspaces/current")
def get_current_workspace():
    p = Path(active_workspace)
    if not p.exists():
        return {"path": active_workspace, "name": "unavailable", "exists": False, "files": []}
    
    file_list = []
    try:
        for item in p.rglob("*"):
            if any(part.startswith(".") or part in ["node_modules", "venv", "__pycache__", "dist"] for part in item.parts):
                continue
            if item.is_file() and item.stat().st_size < 1000000:
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
        "name": p.name,
        "exists": True,
        "files": file_list,
        "quick_projects": KNOWN_PROJECTS
    }

# ─────────────────────────────────────────────────────────────────────────────
# Agent Subprocess Cancellation
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/agent/cancel")
def cancel_active_agent(req: CancelRequest):
    sid = req.session_id
    if sid in active_subprocesses:
        proc = active_subprocesses[sid]
        try:
            proc.terminate()
            return {"status": "cancelled", "session_id": sid}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {"status": "not_running", "session_id": sid}

# ─────────────────────────────────────────────────────────────────────────────
# Prompt Streaming Generator (Antigravity CLI Bridge)
# ─────────────────────────────────────────────────────────────────────────────

async def llm_stream_generator(request: PromptRequest):
    ws_path = request.workspace_path or active_workspace
    user_prompt = request.prompt
    session_id = request.session_id
    agy_model = request.model or "gemini-3.8-flash-high"
    
    conv_data = get_conversation_data(session_id) if session_id else None
    history_messages = conv_data.get("messages", []) if conv_data else []
    
    init_session_id = session_id or str(uuid.uuid4())
    yield f"data: {json.dumps({'type': 'start', 'model': agy_model, 'workspace': ws_path, 'session_id': init_session_id})}\n\n"
    await asyncio.sleep(0.01)
    
    if not Path(AGY_PATH).exists():
        yield f"data: {json.dumps({'type': 'error', 'text': f'❌ Antigravity CLI (`agy`) not found at `{AGY_PATH}`.'})}\n\n"
        yield f"data: {json.dumps({'type': 'end'})}\n\n"
        return

    effective_prompt = user_prompt
    if request.dev_port or request.is_new_project:
        port = request.dev_port or 1597
        port_instruction = f"\n\n[Studio Development Directive: Any local development server, preview server, or web application created or served for this project MUST listen on port {port} (0.0.0.0:{port}) so it can be previewed immediately in the Studio Live Preview pane.]"
        if str(port) not in effective_prompt:
            effective_prompt = f"{user_prompt}{port_instruction}"

    cmd = [
        AGY_PATH,
        "-p", effective_prompt,
        "--model", agy_model,
        "--output-format", "stream-json",
        "--dangerously-skip-permissions",
        "--mode", request.mode or "accept-edits",
        "--add-dir", ws_path
    ]
    
    # Resume conversation if a valid Antigravity conversation directory exists
    if session_id:
        folder = find_conversation_folder(session_id)
        if folder:
            cmd.extend(["--conversation", session_id])

    out_text = ""
    thinking_text = ""
    recorded_session_id = init_session_id
    
    env = os.environ.copy()
    env["PATH"] = f"/home/sk/.local/bin:/home/sk/.gemini/antigravity-ide/bin:/home/sk/.nvm/versions/node/v22.22.0/bin:{env.get('PATH', '')}"

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=ws_path,
            env=env
        )
        active_subprocesses[recorded_session_id] = proc
        
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            raw_str = line.decode("utf-8", errors="replace").strip()
            if not raw_str:
                continue
                
            try:
                event_obj = json.loads(raw_str)
                event_type = event_obj.get("event")
                
                if event_type == "init":
                    conv_id = event_obj.get("conversation_id")
                    if conv_id:
                        # Transfer active subprocess key if new ID generated
                        if recorded_session_id in active_subprocesses:
                            del active_subprocesses[recorded_session_id]
                        recorded_session_id = conv_id
                        active_subprocesses[recorded_session_id] = proc
                        yield f"data: {json.dumps({'type': 'start', 'session_id': conv_id, 'model': agy_model, 'workspace': ws_path})}\n\n"
                        
                elif event_type == "step_update":
                    su = event_obj.get("step_update", {})
                    stype = su.get("step_type")
                    state = su.get("state")
                    
                    if stype == "error_message":
                        err_msg = su.get("error") or su.get("message") or su.get("text") or "Operation encountered an issue"
                        is_quota = "quota" in err_msg.lower() or "resource_exhausted" in err_msg.lower() or "429" in err_msg
                        yield f"data: {json.dumps({'type': 'error', 'text': err_msg, 'is_quota': is_quota})}\n\n"
                    
                    elif stype == "tool_call":
                        t_name = su.get("tool_name") or su.get("tool") or "tool"
                        if state == "ACTIVE":
                            yield f"data: {json.dumps({'type': 'tool', 'name': t_name, 'args': su.get('args', {})})}\n\n"
                        elif state == "DONE":
                            summary = su.get("output", "Completed")
                            if isinstance(summary, (dict, list)):
                                summary = json.dumps(summary)
                            yield f"data: {json.dumps({'type': 'tool_result', 'name': t_name, 'result': str(summary)[:250]})}\n\n"
                            
                    elif stype == "agent_response":
                        # Handle thinking / internal chain of thought
                        thinking_delta = su.get("thinking_delta")
                        if thinking_delta:
                            thinking_text += thinking_delta
                            yield f"data: {json.dumps({'type': 'thinking', 'text': thinking_delta})}\n\n"

                        delta = su.get("text_delta")
                        if delta:
                            out_text += delta
                            yield f"data: {json.dumps({'type': 'chunk', 'text': delta})}\n\n"
                            
                elif event_type == "result":
                    res = event_obj.get("result", {})
                    res_status = res.get("status")
                    res_text = res.get("response")
                    res_err = res.get("error")
                    
                    if res_err and res_status == "ERROR":
                        is_quota = "quota" in res_err.lower() or "resource_exhausted" in res_err.lower() or "429" in res_err
                        yield f"data: {json.dumps({'type': 'error', 'text': res_err, 'is_quota': is_quota})}\n\n"
                    elif res_text and not out_text:
                        out_text = res_text
                        yield f"data: {json.dumps({'type': 'chunk', 'text': res_text})}\n\n"
                        
            except json.JSONDecodeError:
                if raw_str:
                    out_text += raw_str + "\n"
                    yield f"data: {json.dumps({'type': 'chunk', 'text': raw_str + '\n'})}\n\n"
                    
        await proc.wait()
        
        if proc.returncode != 0 and not out_text:
            err_output = (await proc.stderr.read()).decode("utf-8", errors="replace")
            if err_output:
                is_quota = "quota" in err_output.lower() or "resource_exhausted" in err_output.lower() or "429" in err_output
                yield f"data: {json.dumps({'type': 'error', 'text': f'Antigravity execution error: {err_output}', 'is_quota': is_quota})}\n\n"
            
    except asyncio.CancelledError:
        yield f"data: {json.dumps({'type': 'chunk', 'text': '\n⚠️ Prompt cancelled by user.\n'})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'text': f'Bridge execution error: {str(e)}', 'is_quota': False})}\n\n"
    finally:
        active_subprocesses.pop(recorded_session_id, None)

    if out_text:
        updated_history = list(history_messages)
        updated_history.append({"role": "user", "content": user_prompt})
        updated_history.append({"role": "assistant", "content": out_text})
        save_dashboard_session(recorded_session_id, ws_path, agy_model, updated_history)

    yield f"data: {json.dumps({'type': 'end', 'session_id': recorded_session_id})}\n\n"

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
    sid = str(uuid.uuid4())
    session_data = save_dashboard_session(sid, active_workspace, "gemini-3.8-flash-high", [], title="New Conversation")
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
