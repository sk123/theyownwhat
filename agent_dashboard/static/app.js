// AI Agent Studio PWA Client Application
let activeWorkspace = "/home/sk/dev/theyownwhat";
let selectedModel = "gemini-3.7-flash-high";
let availableModels = [];
let contextFiles = [];
let deferredPrompt = null;
let activeSessionId = null;
let allConversations = [];

document.addEventListener('DOMContentLoaded', () => {
    initPWAInstall();
    loadHealthAndModels();
    loadWorkspaceInfo();
    setupEventListeners();
    registerServiceWorker();
    loadConversations();
});

function registerServiceWorker() {
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.getRegistrations().then(registrations => {
            for (let registration of registrations) {
                registration.update();
            }
        });
        navigator.serviceWorker.register('/sw.js?v=5.0.0')
            .then(reg => {
                console.log('PWA ServiceWorker registered:', reg.scope);
                reg.update();
            })
            .catch(err => console.log('ServiceWorker registration failed:', err));
    }
}

function initPWAInstall() {
    window.addEventListener('beforeinstallprompt', (e) => {
        e.preventDefault();
        deferredPrompt = e;
        const installBtn = document.getElementById('pwaInstallBtn');
        if (installBtn) {
            installBtn.style.display = 'flex';
        }
    });

    window.addEventListener('appinstalled', () => {
        console.log('PWA installed successfully');
        deferredPrompt = null;
        const installBtn = document.getElementById('pwaInstallBtn');
        if (installBtn) {
            installBtn.style.display = 'none';
        }
    });
}

function installPWA() {
    if (deferredPrompt) {
        deferredPrompt.prompt();
        deferredPrompt.userChoice.then((choiceResult) => {
            if (choiceResult.outcome === 'accepted') {
                console.log('User accepted PWA installation');
            }
            deferredPrompt = null;
            const installBtn = document.getElementById('pwaInstallBtn');
            if (installBtn) {
                installBtn.style.display = 'none';
            }
        });
    } else {
        const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
        if (isIOS) {
            alert('To install on iOS: Tap the Share button in Safari, then select "Add to Home Screen".');
        } else {
            alert('To install: Open your browser menu and select "Install App" or "Add to Home screen".');
        }
    }
}

async function loadHealthAndModels() {
    try {
        const res = await fetch('/api/models');
        const data = await res.json();
        availableModels = data.models || [];
        if (data.active_model) {
            selectedModel = data.active_model;
        }
        renderModelSelector();
    } catch (e) {
        console.error("Failed to load models", e);
    }
}

function renderModelSelector() {
    const container = document.getElementById('modelSelectorPill');
    if (!container) return;
    
    const currentModelObj = availableModels.find(m => m.id === selectedModel);
    const displayName = currentModelObj ? currentModelObj.name : selectedModel;
    const providerName = currentModelObj ? currentModelObj.provider : 'Antigravity Native';
    const isAvail = currentModelObj ? currentModelObj.available : true;
    
    container.innerHTML = `
        <span class="dot" style="width: 8px; height: 8px; border-radius: 50%; background: ${isAvail ? '#10b981' : '#f59e0b'};"></span>
        <div style="display: flex; flex-direction: column; text-align: left; line-height: 1.15;">
            <span style="font-weight: 700; font-size: 13px; color: #fff;">${displayName}</span>
            <span style="font-size: 10px; color: #a5b4fc; text-transform: uppercase; font-weight: 600;">${providerName}</span>
        </div>
        <span style="font-size: 10px; color: #64748b; margin-left: 4px;">▼</span>
    `;
    
    container.onclick = openModelModal;
}

function openModelModal() {
    const modal = document.getElementById('modelModal');
    const list = document.getElementById('modelList');
    if (!modal || !list) return;
    
    let html = `
        <div style="margin-bottom: 14px; padding: 12px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px;">
            <div style="font-size: 11px; font-weight: 800; text-transform: uppercase; color: #a5b4fc; margin-bottom: 6px;">✨ Custom Model Identifier</div>
            <div style="display: flex; gap: 8px;">
                <input id="customModelInput" type="text" placeholder="e.g. gemini-3.7-flash-high, claude-sonnet-4-6, gpt-oss-120b-medium" value="${selectedModel}" style="flex: 1; background: #020617; border: 1px solid rgba(255,255,255,0.2); border-radius: 8px; color: #fff; padding: 8px 12px; font-size: 13px; outline: none;" />
                <button onclick="applyCustomModel()" style="background: #6366f1; color: #fff; border: none; padding: 8px 14px; border-radius: 8px; font-weight: 700; font-size: 12px; cursor: pointer;">Apply</button>
            </div>
        </div>
    `;

    const providerIcons = {
        "Antigravity Native": "⚡ Antigravity Native Execution",
        "Google AI": "🔷 Google AI (Gemini)",
        "Anthropic": "🟣 Anthropic (Claude)",
        "OpenAI": "🟢 OpenAI (GPT & o-series)",
        "Local": "⚙️ Local & Open Source"
    };

    let currentProvider = "";

    availableModels.forEach(m => {
        if (m.provider !== currentProvider) {
            currentProvider = m.provider;
            const headerTitle = providerIcons[currentProvider] || currentProvider;
            html += `
                <div style="font-size: 11px; font-weight: 800; text-transform: uppercase; color: #94a3b8; margin: 14px 0 6px 0; letter-spacing: 0.08em; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 4px;">
                    ${headerTitle}
                </div>
            `;
        }

        html += `
            <div class="folder-item ${m.id === selectedModel ? 'active' : ''}" onclick="selectModel('${m.id}')" style="justify-content: space-between; margin-bottom: 6px; cursor: pointer;">
                <div>
                    <div style="font-weight: 800; color: #fff;">${m.name} <span style="font-size: 10px; font-weight: 700; color: #818cf8; background: rgba(99,102,241,0.2); padding: 2px 6px; border-radius: 4px; margin-left: 6px;">${m.badge}</span></div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 2px;">${m.description}</div>
                </div>
                <span style="font-size: 11px; font-weight: 700; color: #34d399; flex-shrink: 0; margin-left: 10px;">
                    Ready
                </span>
            </div>
        `;
    });
    
    list.innerHTML = html;
    modal.classList.add('open');
}

function applyCustomModel() {
    const input = document.getElementById('customModelInput');
    if (input && input.value.trim()) {
        selectedModel = input.value.trim();
        renderModelSelector();
        closeModal('modelModal');
    }
}

function selectModel(modelId) {
    selectedModel = modelId;
    renderModelSelector();
    closeModal('modelModal');
}

async function loadConversations() {
    try {
        const res = await fetch('/api/conversations');
        const data = await res.json();
        allConversations = data.conversations || [];
        renderHistoryList();
    } catch (e) {
        console.error("Failed to load conversations:", e);
    }
}

function openHistoryModal() {
    loadConversations();
    const modal = document.getElementById('historyModal');
    if (modal) modal.classList.add('open');
}

function renderHistoryList(filter = '') {
    const list = document.getElementById('historyList');
    if (!list) return;

    const searchInput = document.getElementById('historySearchInput');
    const term = (filter || (searchInput ? searchInput.value : '')).toLowerCase().trim();

    const filtered = allConversations.filter(c => 
        !term || c.title.toLowerCase().includes(term) || c.id.toLowerCase().includes(term)
    );

    if (filtered.length === 0) {
        list.innerHTML = `<div style="padding: 20px; text-align: center; color: #94a3b8; font-size: 13px;">No matching conversation histories found.</div>`;
        return;
    }

    let html = '';
    filtered.forEach(c => {
        const isAntigravity = c.source === 'antigravity';
        const badgeColor = isAntigravity ? '#6366f1' : '#10b981';
        const badgeIcon = isAntigravity ? '⚡ Antigravity' : '💬 Dashboard';
        const isCurrent = c.id === activeSessionId;

        html += `
            <div class="folder-item ${isCurrent ? 'active' : ''}" onclick="selectConversation('${c.id}')" style="justify-content: space-between; padding: 12px 14px; margin-bottom: 6px; cursor: pointer;">
                <div style="flex: 1; overflow: hidden; margin-right: 10px;">
                    <div style="font-weight: 700; color: #f8fafc; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;">${escapeHtml(c.title)}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 4px;">
                        <span style="font-weight: 600; color: ${badgeColor}; margin-right: 6px;">${badgeIcon}</span>
                        <span>ID: ${c.id.substring(0, 8)}...</span>
                    </div>
                </div>
                <span style="font-size: 11px; color: #818cf8; white-space: nowrap;">Select ➔</span>
            </div>
        `;
    });

    list.innerHTML = html;
}

async function selectConversation(convId) {
    try {
        const res = await fetch(`/api/conversations/${convId}`);
        if (!res.ok) throw new Error("Conversation not found");
        const data = await res.json();

        activeSessionId = convId;
        const historyBadgeTitle = document.getElementById('historyBadgeTitle');
        if (historyBadgeTitle) {
            historyBadgeTitle.innerText = data.title ? (data.title.substring(0, 18) + '...') : 'History';
        }

        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) chatMessages.innerHTML = '';

        if (data.messages && data.messages.length > 0) {
            data.messages.forEach(msg => {
                const role = msg.role === 'user' ? 'user' : 'agent';
                if (role === 'user') {
                    appendMessage('user', msg.content);
                } else {
                    const card = appendMessage('agent', '', 'msg-' + Date.now());
                    const contentDiv = card.querySelector('.message-content');
                    renderMarkdownResponse(contentDiv, msg.content);
                }
            });
        } else {
            appendMessage('agent', `Loaded conversation **${data.title}**.`);
        }

        closeModal('historyModal');
    } catch (e) {
        alert("Could not load conversation: " + e.message);
    }
}

async function startNewConversation() {
    try {
        const res = await fetch('/api/conversations/new', { method: 'POST' });
        const data = await res.json();
        activeSessionId = data.session_id;

        const historyBadgeTitle = document.getElementById('historyBadgeTitle');
        if (historyBadgeTitle) historyBadgeTitle.innerText = 'New Chat';

        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.innerHTML = `
                <div class="message-card agent">
                    <div class="avatar agent">AI</div>
                    <div class="message-content">
                        <h3>💬 New Antigravity Conversation Started</h3>
                        <p style="margin-top: 6px; color: #94a3b8;">
                            Prompt agent on active workspace. Conversational memory and tool execution are active.
                        </p>
                    </div>
                </div>
            `;
        }

        closeModal('historyModal');
    } catch (e) {
        console.error("Failed to start new conversation:", e);
    }
}

function loadWorkspaceInfo() {
    try {
        const badge = document.getElementById('workspaceBadge');
        if (badge) {
            const folderName = activeWorkspace.split('/').filter(Boolean).pop() || 'workspace';
            badge.innerHTML = `📁 <span>${folderName}</span>`;
            badge.title = `Active Workspace: ${activeWorkspace}`;
        }
    } catch (e) {
        console.error("Failed to load workspace info", e);
    }
}

async function openWorkspaceModal(path = null) {
    await loadWorkspaceList(path || activeWorkspace);
    const modal = document.getElementById('workspaceModal');
    if (modal) modal.classList.add('open');
}

async function loadWorkspaceList(dirPath) {
    try {
        const res = await fetch(`/api/workspaces/list?path=${encodeURIComponent(dirPath || '')}`);
        if (!res.ok) throw new Error("Could not browse directory");
        const data = await res.json();
        
        const currentPath = data.path || data.current_path || dirPath || '/home/sk/dev';
        const folderName = data.name || currentPath.split('/').filter(Boolean).pop() || 'dev';
        const items = data.items || data.files || [];
        const quickProjects = data.quick_projects || [
            { name: "theyownwhat", path: "/home/sk/dev/theyownwhat", icon: "🏛️" },
            { name: "tenant-toolkit", path: "/home/sk/dev/tenant-toolkit", icon: "🛠️" },
            { name: "troubadour", path: "/home/sk/dev/troubadour", icon: "🎵" },
            { name: "scripts", path: "/home/sk/scripts", icon: "📜" },
            { name: "dev", path: "/home/sk/dev", icon: "📁" }
        ];

        const breadcrumbs = document.getElementById('workspaceBreadcrumbs');
        if (breadcrumbs) {
            const parts = currentPath.split('/').filter(Boolean);
            let accum = '';
            const crumbHtml = parts.map(p => {
                accum += '/' + p;
                const pathTarget = accum;
                return `<span style="cursor: pointer; color: #818cf8; font-weight: 600;" onclick="loadWorkspaceList('${pathTarget}')">/ ${p}</span>`;
            }).join(' ');

            const quickHtml = `
                <div style="margin-bottom: 12px; display: flex; flex-wrap: wrap; gap: 6px;">
                    ${quickProjects.map(q => `
                        <button onclick="selectWorkspaceDirect('${q.path}')" class="pill-btn" style="font-size: 11px; padding: 4px 10px;">
                            ${q.icon} ${q.name}
                        </button>
                    `).join('')}
                </div>
            `;

            breadcrumbs.innerHTML = quickHtml + `<div style="font-family: monospace; font-size: 12px;">${crumbHtml || '/'}</div>`;
        }

        const list = document.getElementById('workspaceList');
        if (list) {
            let html = `
                <div class="folder-item" onclick="selectWorkspaceDirect('${currentPath}')" style="background: rgba(99, 102, 241, 0.15); border: 1px solid rgba(99, 102, 241, 0.4); margin-bottom: 10px;">
                    <div>
                        <strong style="color: #a5b4fc;">✅ Select Current Folder:</strong>
                        <div style="font-size: 12px; color: #fff; font-family: monospace;">${currentPath}</div>
                    </div>
                    <span style="font-size: 12px; font-weight: 700; color: #a5b4fc;">Use Workspace ➔</span>
                </div>
            `;

            if (data.parent_path) {
                html += `
                    <div class="folder-item" onclick="loadWorkspaceList('${data.parent_path}')">
                        <span>📁 <strong>.. (Parent Directory)</strong></span>
                    </div>
                `;
            }

            items.forEach(item => {
                const isDir = item.is_dir || item.isDir;
                if (isDir) {
                    html += `
                        <div class="folder-item" onclick="loadWorkspaceList('${item.path}')">
                            <span>📁 ${item.name}</span>
                            <span style="color: #64748b; font-size: 11px;">Folder ➔</span>
                        </div>
                    `;
                }
            });

            list.innerHTML = html;
        }
    } catch (e) {
        console.error("Failed to list directory:", e);
    }
}

async function selectWorkspaceDirect(path) {
    try {
        const res = await fetch('/api/workspaces/select', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
        });
        if (!res.ok) throw new Error("Could not select workspace");
        const data = await res.json();
        activeWorkspace = data.workspace;
        loadWorkspaceInfo();
        closeModal('workspaceModal');
    } catch (e) {
        alert("Failed to select workspace: " + e.message);
    }
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.classList.remove('open');
}

function setupEventListeners() {
    const promptInput = document.getElementById('promptInput');
    const sendBtn = document.getElementById('sendBtn');
    const historySearchInput = document.getElementById('historySearchInput');
    
    if (promptInput) {
        promptInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                submitPrompt();
            }
        });
    }
    
    if (sendBtn) {
        sendBtn.onclick = submitPrompt;
    }

    if (historySearchInput) {
        historySearchInput.addEventListener('input', (e) => {
            renderHistoryList(e.target.value);
        });
    }
    
    document.querySelectorAll('.preset-chip').forEach(chip => {
        chip.onclick = () => {
            const presetText = chip.getAttribute('data-preset');
            if (promptInput && presetText) {
                promptInput.value = presetText;
                promptInput.focus();
            }
        };
    });
}

async function submitPrompt() {
    const promptInput = document.getElementById('promptInput');
    if (!promptInput) return;
    
    const text = promptInput.value.trim();
    if (!text) return;
    
    promptInput.value = '';
    
    // Append User Message
    appendMessage('user', text);
    
    // Create Agent Message Skeleton
    const msgId = 'msg-' + Date.now();
    const messageContainer = appendMessage('agent', '', msgId);
    const contentDiv = messageContainer.querySelector('.message-content');
    
    // Initial thinking indicator
    contentDiv.innerHTML = `
        <div id="${msgId}-thinking" style="display: flex; align-items: center; gap: 8px; font-style: italic; color: #a5b4fc; padding: 4px 0;">
            <span style="display: inline-block; animation: spin 1s linear infinite;">⚡</span>
            <span>Thinking with <strong>${selectedModel}</strong>...</span>
        </div>
        <div id="${msgId}-tools" style="margin-bottom: 8px;"></div>
        <div id="${msgId}-text"></div>
    `;
    
    const toolsContainer = document.getElementById(`${msgId}-tools`);
    const textContainer = document.getElementById(`${msgId}-text`);
    const thinkingContainer = document.getElementById(`${msgId}-thinking`);
    
    try {
        const response = await fetch('/api/agent/prompt', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                prompt: text,
                workspace_path: activeWorkspace,
                model: selectedModel,
                session_id: activeSessionId,
                context_files: contextFiles
            })
        });
        
        if (!response.ok) {
            throw new Error(`Server returned HTTP ${response.status}`);
        }
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let fullResponse = '';
        let sseBuffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            sseBuffer += decoder.decode(value, { stream: true });
            const events = sseBuffer.split('\n\n');
            sseBuffer = events.pop(); // Keep partial chunk in buffer
            
            for (const eventStr of events) {
                const lines = eventStr.split('\n');
                for (const line of lines) {
                    const trimmed = line.trim();
                    if (trimmed.startsWith('data: ')) {
                        try {
                            const jsonStr = trimmed.substring(6).trim();
                            const eventData = JSON.parse(jsonStr);
                            
                            if (eventData.type === 'start' && eventData.session_id) {
                                activeSessionId = eventData.session_id;
                            } else if (eventData.type === 'tool') {
                                if (thinkingContainer) thinkingContainer.style.display = 'none';
                                const badge = document.createElement('div');
                                badge.className = 'tool-badge';
                                badge.innerHTML = `⚙️ Tool: <strong>${escapeHtml(eventData.name || 'executing')}</strong>`;
                                if (toolsContainer) toolsContainer.appendChild(badge);
                            } else if (eventData.type === 'tool_result') {
                                const resultDiv = document.createElement('div');
                                resultDiv.style.fontSize = '11px';
                                resultDiv.style.color = '#94a3b8';
                                resultDiv.style.marginBottom = '6px';
                                resultDiv.innerText = `↳ ${eventData.result || 'Done'}`;
                                if (toolsContainer) toolsContainer.appendChild(resultDiv);
                            } else if (eventData.type === 'chunk') {
                                if (thinkingContainer) thinkingContainer.style.display = 'none';
                                fullResponse += eventData.text;
                                renderMarkdownResponse(textContainer, fullResponse);
                            } else if (eventData.type === 'end') {
                                if (thinkingContainer) thinkingContainer.style.display = 'none';
                            }
                        } catch (err) {
                            console.warn("SSE JSON Parse skipped:", err, trimmed);
                        }
                    }
                }
            }
        }
        
        if (thinkingContainer) thinkingContainer.style.display = 'none';
        
        if (!fullResponse && (!toolsContainer || toolsContainer.children.length === 0)) {
            textContainer.innerHTML = `<div style="color: #94a3b8; font-style: italic;">(Completed turn)</div>`;
        }
        
    } catch (e) {
        if (thinkingContainer) thinkingContainer.style.display = 'none';
        contentDiv.innerHTML += `<div style="color: #ef4444; margin-top: 8px;">❌ Error executing agent prompt: ${escapeHtml(e.message)}</div>`;
    }
}

function appendMessage(role, text, id = null) {
    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) return null;
    
    const card = document.createElement('div');
    card.className = `message-card ${role}`;
    if (id) card.id = id;
    
    card.innerHTML = `
        <div class="avatar ${role}">${role === 'user' ? 'U' : '⚡'}</div>
        <div class="message-content">${escapeHtml(text)}</div>
    `;
    
    chatMessages.appendChild(card);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return card;
}

function renderMarkdownResponse(container, text) {
    if (!container) return;
    
    // Safe markdown parsing
    let formatted = escapeHtml(text);
    
    // Code blocks with syntax container
    formatted = formatted.replace(/```([a-zA-Z0-9_-]*)\n([\s\S]*?)```/g, (match, lang, code) => {
        return `<pre><code class="lang-${lang}">${code.trim()}</code></pre>`;
    });
    formatted = formatted.replace(/```([\s\S]*?)```/g, (match, code) => {
        return `<pre><code>${code.trim()}</code></pre>`;
    });
    
    // Inline code
    formatted = formatted.replace(/`([^`]+)`/g, '<code style="background: rgba(255,255,255,0.1); padding: 2px 6px; border-radius: 4px; font-family: monospace;">$1</code>');
    
    // Bold & Headers
    formatted = formatted.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    formatted = formatted.replace(/^### (.*$)/gim, '<h4 style="margin: 10px 0 4px 0; color: #a5b4fc;">$1</h4>');
    formatted = formatted.replace(/^## (.*$)/gim, '<h3 style="margin: 12px 0 6px 0; color: #fff;">$1</h3>');
    formatted = formatted.replace(/^# (.*$)/gim, '<h2 style="margin: 14px 0 8px 0; color: #fff;">$1</h2>');
    
    // Bullet lists
    formatted = formatted.replace(/^\s*[-*]\s+(.*$)/gim, '<li style="margin-left: 18px; margin-bottom: 4px;">$1</li>');
    
    // Convert newlines outside pre blocks to <br>
    const parts = formatted.split(/(<pre>[\s\S]*?<\/pre>)/);
    for (let i = 0; i < parts.length; i += 2) {
        parts[i] = parts[i].replace(/\n/g, '<br>');
    }
    formatted = parts.join('');
    
    container.innerHTML = formatted;
    
    const chatMessages = document.getElementById('chatMessages');
    if (chatMessages) chatMessages.scrollTop = chatMessages.scrollHeight;
}

function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return String(unsafe)
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}
