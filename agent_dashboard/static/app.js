// AI Agent Studio PWA Client Application
let activeWorkspace = "/home/sk/dev/theyownwhat";
let selectedModel = "gemini-3.7-flash-medium";
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
        navigator.serviceWorker.register('/sw.js?v=4.0.0')
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
        if (data.active_model && (!selectedModel || selectedModel === 'gemini-3.6-flash')) {
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
    const providerName = currentModelObj ? currentModelObj.provider : 'Custom Model';
    const isAvail = currentModelObj ? currentModelObj.available : true;
    
    container.innerHTML = `
        <span class="dot" style="width: 8px; height: 8px; border-radius: 50%; background: ${isAvail ? '#10b981' : '#f59e0b'};"></span>
        <div style="display: flex; flex-direction: column; text-align: left; line-height: 1.15;">
            <span style="font-weight: 700; font-size: 13px; color: #fff;">${displayName}</span>
            <span style="font-size: 10px; color: #94a3b8; text-transform: uppercase; font-weight: 600;">${providerName}</span>
        </div>
        <span style="font-size: 10px; color: #64748b;">▼</span>
    `;
    
    container.onclick = openModelModal;
}

function openModelModal() {
    const modal = document.getElementById('modelModal');
    const list = document.getElementById('modelList');
    if (!modal || !list) return;
    
    let html = `
        <div style="margin-bottom: 14px; padding: 12px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px;">
            <div style="font-size: 11px; font-weight: 800; text-transform: uppercase; color: #a5b4fc; margin-bottom: 6px;">✨ Custom / Frontier Model Identifier</div>
            <div style="display: flex; gap: 8px;">
                <input id="customModelInput" type="text" placeholder="e.g. gemini-3.7-flash-medium, claude-sonnet-4.6, claude-opus-4.6, gpt-oss-120b" value="${selectedModel}" style="flex: 1; background: #020617; border: 1px solid rgba(255,255,255,0.2); border-radius: 8px; color: #fff; padding: 8px 12px; font-size: 13px; outline: none;" />
                <button onclick="applyCustomModel()" style="background: #6366f1; color: #fff; border: none; padding: 8px 14px; border-radius: 8px; font-weight: 700; font-size: 12px; cursor: pointer;">Apply</button>
            </div>
        </div>
    `;

    const providerIcons = {
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
            <div class="folder-item ${m.id === selectedModel ? 'active' : ''}" onclick="selectModel('${m.id}')" style="justify-content: space-between; margin-bottom: 6px;">
                <div>
                    <div style="font-weight: 800; color: #fff;">${m.name} <span style="font-size: 10px; font-weight: 700; color: #818cf8; background: rgba(99,102,241,0.2); padding: 2px 6px; border-radius: 4px; margin-left: 6px;">${m.badge}</span></div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 2px;">${m.description}</div>
                </div>
                <span style="font-size: 11px; font-weight: 700; color: ${m.available ? '#34d399' : '#f59e0b'}; shrink: 0; margin-left: 10px;">
                    ${m.available ? 'Ready' : 'API Key Required'}
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
            <div class="folder-item ${isCurrent ? 'active' : ''}" onclick="selectConversation('${c.id}')" style="justify-content: space-between; padding: 12px 14px; margin-bottom: 6px;">
                <div style="flex: 1; overflow: hidden; margin-right: 10px;">
                    <div style="font-weight: 700; color: #f8fafc; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;">${escapeHtml(c.title)}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 4px;">
                        <span style="font-weight: 600; color: ${badgeColor}; margin-right: 6px;">${badgeIcon}</span>
                        <span>ID: ${c.id.substring(0, 8)}...</span>
                    </div>
                </div>
                <span style="font-size: 11px; color: #64748b; white-space: nowrap;">Select ➔</span>
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
            appendMessage('agent', `Started conversation **${data.title}**.`);
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
                        <h3>💬 New Conversation Started</h3>
                        <p style="margin-top: 6px; color: #94a3b8;">
                            Prompt agent on active workspace. Conversational memory is active.
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
            badge.innerHTML = `📁 <span>${activeWorkspace.split('/').pop()}</span>`;
        }
    } catch (e) {
        console.error("Failed to load workspace info", e);
    }
}

async function openWorkspaceModal(path = null) {
    loadWorkspaceList(path || activeWorkspace);
    const modal = document.getElementById('workspaceModal');
    if (modal) modal.classList.add('open');
}

async function loadWorkspaceList(dirPath) {
    try {
        const res = await fetch(`/api/workspaces/list?path=${encodeURIComponent(dirPath)}`);
        const data = await res.json();
        
        const breadcrumbs = document.getElementById('workspaceBreadcrumbs');
        if (breadcrumbs) {
            const parts = data.path.split('/').filter(Boolean);
            let accum = '';
            breadcrumbs.innerHTML = parts.map(p => {
                accum += '/' + p;
                const pathTarget = accum;
                return `<span style="cursor: pointer; color: #818cf8;" onclick="loadWorkspaceList('${pathTarget}')">/ ${p}</span>`;
            }).join(' ');
        }
        
        const list = document.getElementById('workspaceList');
        if (!list) return;
        
        let html = `
            <div class="folder-item" onclick="selectWorkspace('${data.path}')" style="background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3); margin-bottom: 10px;">
                <span style="font-weight: 700; color: #818cf8;">✅ Select Current Folder: ${data.name}</span>
            </div>
        `;
        
        if (data.files && data.files.length > 0) {
            data.files.forEach(item => {
                if (item.isDir) {
                    html += `
                        <div class="folder-item" onclick="loadWorkspaceList('${item.path}')">
                            <span>📁 ${item.name}</span>
                        </div>
                    `;
                }
            });
        }
        
        list.innerHTML = html;
    } catch (e) {
        console.error("Failed to load workspace files:", e);
    }
}

async function selectWorkspace(dirPath) {
    try {
        await fetch('/api/workspaces/select', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({path: dirPath})
        });
        activeWorkspace = dirPath;
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
    
    // Preset buttons
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
    
    contentDiv.innerHTML = `<div style="font-style: italic; color: #94a3b8;">Thinking with ${selectedModel}...</div>`;
    
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
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let fullResponse = '';
        
        contentDiv.innerHTML = ''; // Clear thinking placeholder
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            const chunk = decoder.decode(value);
            const lines = chunk.split('\n\n');
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const eventData = JSON.parse(line.replace('data: ', ''));
                        
                        if (eventData.type === 'start' && eventData.session_id) {
                            activeSessionId = eventData.session_id;
                        } else if (eventData.type === 'tool') {
                            const badge = document.createElement('div');
                            badge.className = 'tool-badge';
                            badge.innerHTML = `⚙️ Executing Tool: <strong>${eventData.name}</strong>`;
                            contentDiv.appendChild(badge);
                        } else if (eventData.type === 'tool_result') {
                            const resultDiv = document.createElement('div');
                            resultDiv.style.fontSize = '11px';
                            resultDiv.style.color = '#94a3b8';
                            resultDiv.style.marginBottom = '8px';
                            resultDiv.innerText = `Result: ${eventData.result}`;
                            contentDiv.appendChild(resultDiv);
                        } else if (eventData.type === 'chunk') {
                            fullResponse += eventData.text;
                            renderMarkdownResponse(contentDiv, fullResponse);
                        }
                    } catch (err) {
                        // ignore JSON parse chunk splits
                    }
                }
            }
        }
    } catch (e) {
        contentDiv.innerHTML = `<div style="color: #ef4444;">Error executing agent prompt: ${e.message}</div>`;
    }
}

function appendMessage(role, text, id = null) {
    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) return;
    
    const card = document.createElement('div');
    card.className = `message-card ${role}`;
    if (id) card.id = id;
    
    card.innerHTML = `
        <div class="avatar ${role}">${role === 'user' ? 'U' : 'AI'}</div>
        <div class="message-content">${escapeHtml(text)}</div>
    `;
    
    chatMessages.appendChild(card);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return card;
}

function renderMarkdownResponse(container, text) {
    // Simple markdown code block parser
    let formatted = escapeHtml(text);
    
    // Replace ```code``` with pre blocks
    formatted = formatted.replace(/```([\s\S]*?)```/g, (match, code) => {
        return `<pre><code>${code.trim()}</code></pre>`;
    });
    
    // Replace inline `code`
    formatted = formatted.replace(/`([^`]+)`/g, '<code style="background: rgba(255,255,255,0.1); padding: 2px 6px; border-radius: 4px;">$1</code>');
    
    // Bold
    formatted = formatted.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    
    container.innerHTML = formatted;
    
    const chatMessages = document.getElementById('chatMessages');
    if (chatMessages) chatMessages.scrollTop = chatMessages.scrollHeight;
}

function escapeHtml(unsafe) {
    return unsafe
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}
