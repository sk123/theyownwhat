import os
import sys
import json
import re
import asyncio
import glob
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="AI Agent Studio PWA (Antigravity Bridge)", version="2.0.0")

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

# Antigravity CLI binary path
AGY_PATH = shutil.which("agy") or "/home/sk/.local/bin/agy"

KNOWN_PROJECTS = [
    {"name": "theyownwhat", "path": "/home/sk/dev/theyownwhat", "icon": "🏛️", "desc": "Hidden property ownership & investigative network mapping"},
    {"name": "tenant-toolkit", "path": "/home/sk/dev/tenant-toolkit", "icon": "🛠️", "desc": "Tenant repair advocacy, rent ledger & legal housing tools"},
    {"name": "troubadour", "path": "/home/sk/dev/troubadour", "icon": "🎵", "desc": "Tour routing radar, grassroots concert booking & encrypted private vault"},
    {"name": "scripts", "path": "/home/sk/scripts", "icon": "📜", "desc": "System maintenance & IDE update scripts"},
    {"name": "dev", "path": "/home/sk/dev", "icon": "📁", "desc": "All development projects workspace"}
]

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
                    "workspace": data.get("workspace_path", active_workspace),
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
                    "workspace": active_workspace,
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
        "updated_at": os.path.getmtime(dash_file) if dash_file.exists() else os.path.getmtime(SESSIONS_DIR),
        "messages": messages
    }
    dash_file.write_text(json.dumps(session_data, indent=2), encoding="utf-8")
    return session_data

class PromptRequest(BaseModel):
    prompt: str
    workspace_path: Optional[str] = None
    model: str = "gemini-3.6-flash-high"
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
        "engine": "Antigravity CLI Bridge",
        "agy_binary": AGY_PATH,
        "agy_available": Path(AGY_PATH).exists(),
        "workspace": active_workspace,
        "workspace_name": Path(active_workspace).name
    }

def resolve_agy_model(model_name: str) -> tuple[str, Optional[str]]:
    """Resolves requested model to a supported agy model and effort."""
    m = (model_name or "").lower().strip()
    
    # Effort extraction
    effort = "high"
    if "low" in m:
        effort = "low"
    elif "medium" in m:
        effort = "medium"
    elif "high" in m:
        effort = "high"
        
    # Gemini 3.7 Flash (Flagship Native Model)
    if not m or "3.7" in m or m in ["auto", "auto-latest-gemini-flash-high", "default", "gemini"]:
        return "gemini-3.7-flash", effort

    # Claude models
    if "claude-opus" in m:
        return "claude-opus-4-6-thinking", "high"
    if "claude" in m or "sonnet" in m:
        return "claude-sonnet-4-6", "high"
        
    # Open-weights / GPT
    if "oss" in m or "120b" in m or "llama" in m or "deepseek" in m:
        return "gpt-oss-120b-medium", "medium"
        
    # Pro models
    if "pro" in m:
        if "low" in m:
            return "gemini-3.1-pro-low", "low"
        return "gemini-3.1-pro-high", "high"

    # Gemini 3.6 / 3.5
    if "3.5" in m:
        return f"gemini-3.5-flash-{effort}", effort
    if "3.6" in m:
        return f"gemini-3.6-flash-{effort}", effort
        
    return "gemini-3.7-flash", effort

@app.get("/api/models")
async def get_models():
    models_file = Path(__file__).parent / "models.json"
    curated_models = []
    if models_file.exists():
        try:
            curated_models = json.loads(models_file.read_text())
        except Exception:
            pass

    if not curated_models:
        curated_models = [
            {"id": "gemini-3.7-flash-high", "name": "⚡ Gemini 3.7 Flash (High Thinking)", "provider": "Antigravity Native", "badge": "Flagship Thinking", "description": "Google's newest reasoning flagship with deep internal thinking budget.", "recency": 0},
            {"id": "gemini-3.7-flash-medium", "name": "Gemini 3.7 Flash (Medium Reasoning)", "provider": "Antigravity Native", "badge": "Fast & Smart", "description": "Optimal balance of high-speed execution and deep chain-of-thought.", "recency": 1},
            {"id": "gemini-3.7-flash-low", "name": "Gemini 3.7 Flash (Low / Instant)", "provider": "Antigravity Native", "badge": "Instant", "description": "Rapid reasoning response for interactive prompts and quick inspections.", "recency": 2},
            {"id": "gemini-3.1-pro-high", "name": "Gemini 3.1 Pro (High Thinking)", "provider": "Antigravity Native", "badge": "Pro", "description": "Deep architecture, multi-file reasoning, and complex software tasks.", "recency": 3},
            {"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6 (Thinking)", "provider": "Antigravity Native", "badge": "Frontier", "description": "Claude 4.6 Sonnet state-of-the-art coding and reasoning via Antigravity.", "recency": 4},
            {"id": "claude-opus-4-6-thinking", "name": "Claude Opus 4.6 (Thinking)", "provider": "Antigravity Native", "badge": "Deep Reasoning", "description": "Claude 4.6 Opus frontier system architecture model.", "recency": 5},
            {"id": "gpt-oss-120b-medium", "name": "GPT-OSS 120B (Medium)", "provider": "Antigravity Native", "badge": "Open Weights", "description": "Open-source 120B parameter reasoning model via Antigravity runtime.", "recency": 6}
        ]

    models = []
    for cm in curated_models:
        models.append({
            "id": cm["id"],
            "name": cm["name"],
            "provider": cm.get("provider", "Antigravity Native"),
            "badge": cm.get("badge", "Ready"),
            "available": True,
            "description": cm.get("description", "")
        })

    return {"models": models, "active_model": "gemini-3.7-flash-high", "has_keys": {"antigravity": True}}

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
def browse_directories(path: Optional[str] = Query(None)):
    return get_directory_listing(path)

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
        "name": p.name,
        "exists": True,
        "files": file_list,
        "quick_projects": KNOWN_PROJECTS
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
    ws_path = request.workspace_path or active_workspace
    user_prompt = request.prompt
    session_id = request.session_id
    
    agy_model, effort = resolve_agy_model(request.model)
    
    conv_data = get_conversation_data(session_id) if session_id else None
    history_messages = conv_data.get("messages", []) if conv_data else []
    
    # Emit initial start event
    init_session_id = session_id or f"dash_{int(asyncio.get_event_loop().time())}"
    yield f"data: {json.dumps({'type': 'start', 'model': agy_model, 'workspace': ws_path, 'session_id': init_session_id})}\n\n"
    await asyncio.sleep(0.02)
    
    # Check if agy binary exists
    if not Path(AGY_PATH).exists():
        yield f"data: {json.dumps({'type': 'chunk', 'text': f'❌ Antigravity CLI (`agy`) not found at `{AGY_PATH}`. Please check installation.'})}\n\n"
        yield f"data: {json.dumps({'type': 'end'})}\n\n"
        return

    # Build agy command line
    cmd = [
        AGY_PATH,
        "-p", user_prompt,
        "--model", agy_model,
        "--output-format", "stream-json",
        "--dangerously-skip-permissions",
        "--add-dir", ws_path
    ]
    if effort:
        cmd.extend(["--effort", effort])
        
    # If session is a valid Antigravity conversation UUID, resume it
    if session_id and not session_id.startswith("dash_"):
        brain_folder = BRAIN_DIR / session_id
        if brain_folder.exists():
            cmd.extend(["--conversation", session_id])

    out_text = ""
    recorded_session_id = init_session_id
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=ws_path
        )
        
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
                        recorded_session_id = conv_id
                        yield f"data: {json.dumps({'type': 'start', 'session_id': conv_id, 'model': agy_model, 'workspace': ws_path})}\n\n"
                        
                elif event_type == "step_update":
                    su = event_obj.get("step_update", {})
                    stype = su.get("step_type")
                    state = su.get("state")
                    
                    if stype == "tool_call":
                        t_name = su.get("tool_name") or su.get("tool") or "tool"
                        if state == "ACTIVE":
                            yield f"data: {json.dumps({'type': 'tool', 'name': t_name, 'args': su.get('args', {})})}\n\n"
                        elif state == "DONE":
                            summary = su.get("output", "Completed")
                            if isinstance(summary, (dict, list)):
                                summary = json.dumps(summary)
                            yield f"data: {json.dumps({'type': 'tool_result', 'name': t_name, 'result': str(summary)[:200]})}\n\n"
                            
                    elif stype == "agent_response":
                        delta = su.get("text_delta")
                        if delta:
                            out_text += delta
                            yield f"data: {json.dumps({'type': 'chunk', 'text': delta})}\n\n"
                            
                elif event_type == "result":
                    res = event_obj.get("result", {})
                    res_text = res.get("response")
                    if res_text and not out_text:
                        out_text = res_text
                        yield f"data: {json.dumps({'type': 'chunk', 'text': res_text})}\n\n"
                        
            except json.JSONDecodeError:
                # Raw text output from CLI or tool
                if raw_str:
                    out_text += raw_str + "\n"
                    yield f"data: {json.dumps({'type': 'chunk', 'text': raw_str + '\n'})}\n\n"
                    
        await proc.wait()
        
        if proc.returncode != 0 and not out_text:
            err_output = (await proc.stderr.read()).decode("utf-8", errors="replace")
            yield f"data: {json.dumps({'type': 'chunk', 'text': f'⚠️ Antigravity execution error: {err_output}'})}\n\n"
            
    except Exception as e:
        yield f"data: {json.dumps({'type': 'chunk', 'text': f'❌ Bridge execution failed: {str(e)}'})}\n\n"

    if out_text:
        updated_history = list(history_messages)
        updated_history.append({"role": "user", "content": user_prompt})
        updated_history.append({"role": "assistant", "content": out_text})
        save_dashboard_session(recorded_session_id, ws_path, agy_model, updated_history)

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
    session_data = save_dashboard_session(sid, active_workspace, "gemini-3.6-flash-high", [], title="New Conversation")
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
