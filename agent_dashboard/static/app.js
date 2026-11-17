// AI Agent Studio PWA Client Application (Antigravity CLI Bridge)
let activeWorkspace = "/home/sk/dev/theyownwhat";
let selectedModel = "gemini-3.8-flash-high";
let availableModels = [];
let activeSessionId = null;
let allConversations = [];
let activeTab = "chat";
let openFilePath = null;
let fileContentOriginal = "";
let isAgentRunning = false;
let previewPort = 1597;
let currentPreviewUrl = "";

document.addEventListener('DOMContentLoaded', () => {
    initStudio();
    setupEventListeners();
    registerServiceWorker();
});

function registerServiceWorker() {
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js?v=6.0.0')
            .catch(err => console.log('ServiceWorker registration error:', err));
    }
}

async function initStudio() {
    await loadHealthAndConfig();
    await loadModels();
    await loadGitStatus();
    await loadFileTree();
    await loadKnownProjects();
    loadConversations();
    checkPreviewStatus();
}

async function loadHealthAndConfig() {
    try {
        const res = await fetch('/api/health');
        if (res.ok) {
            const data = await res.json();
            if (data.workspace) {
                activeWorkspace = data.workspace;
                updateWorkspaceLabels();
            }
        }
    } catch (e) {
        console.error("Failed to load health", e);
    }
}

function updateWorkspaceLabels() {
    const folderName = activeWorkspace.split('/').filter(Boolean).pop() || 'theyownwhat';
    const badgeText = document.getElementById('workspaceNameText');
    if (badgeText) badgeText.innerText = folderName;
    
    const treeTitle = document.getElementById('fileTreeTitle');
    if (treeTitle) treeTitle.innerText = folderName;

    const termCwd = document.getElementById('terminalCwdText');
    if (termCwd) termCwd.innerText = activeWorkspace.replace('/home/sk', '~');
}

// ─────────────────────────────────────────────────────────────────────────────
// Dynamic Model Registry
// ─────────────────────────────────────────────────────────────────────────────

async function loadModels(force = false) {
    try {
        const res = await fetch(`/api/models?force=${force}`);
        const data = await res.json();
        availableModels = data.models || [];
        if (data.active_model && !selectedModel) {
            selectedModel = data.active_model;
        }
        renderModelSelector();
    } catch (e) {
        console.error("Failed to load models", e);
    }
}

async function refreshModelsList(force = true) {
    const btn = document.querySelector('#modelModal button.btn-secondary');
    if (btn) btn.innerText = "⟳ Querying...";
    await loadModels(force);
    openModelModal();
    if (btn) btn.innerText = "⟳ Refresh Live";
}

function renderModelSelector() {
    const pill = document.getElementById('modelSelectorPill');
    const pillName = document.getElementById('modelPillName');
    if (!pill || !pillName) return;

    const currentModelObj = availableModels.find(m => m.id === selectedModel);
    const displayName = currentModelObj ? currentModelObj.name : selectedModel;
    pillName.innerText = displayName;
    pill.title = `Active Model: ${displayName}`;
}

function openModelModal() {
    const modal = document.getElementById('modelModal');
    const list = document.getElementById('modelList');
    if (!modal || !list) return;

    let html = `
        <div style="margin-bottom: 12px; padding: 10px; background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 8px;">
            <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; color: #a5b4fc; margin-bottom: 6px;">Custom Model Identifier</div>
            <div style="display: flex; gap: 8px;">
                <input id="customModelInput" type="text" placeholder="e.g. gemini-3.8-flash-high, claude-sonnet-4-6" value="${selectedModel}" class="form-input" style="flex: 1; padding: 6px 10px; font-size: 12px;" />
                <button onclick="applyCustomModel()" class="btn-primary" style="padding: 6px 12px; font-size: 12px;">Apply</button>
            </div>
        </div>
    `;

    availableModels.forEach(m => {
        const isCurrent = m.id === selectedModel;
        html += `
            <div class="folder-item ${isCurrent ? 'active' : ''}" onclick="selectModel('${m.id}')" style="justify-content: space-between; padding: 10px 14px;">
                <div>
                    <div style="font-weight: 700; color: #fff; display: flex; align-items: center; gap: 8px;">
                        <span>${escapeHtml(m.name)}</span>
                        <span style="font-size: 10px; font-weight: 700; color: #38bdf8; background: rgba(56,189,248,0.15); padding: 2px 6px; border-radius: 4px;">${escapeHtml(m.badge || 'Ready')}</span>
                    </div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 3px;">${escapeHtml(m.description || m.provider)}</div>
                </div>
                <span style="font-size: 11px; font-weight: 700; color: ${isCurrent ? '#38bdf8' : '#34d399'};">
                    ${isCurrent ? '✓ Active' : 'Select'}
                </span>
            </div>
        `;
    });

    list.innerHTML = html;
    modal.classList.add('open');
}

function selectModel(modelId) {
    selectedModel = modelId;
    renderModelSelector();
    closeModal('modelModal');
}

function applyCustomModel() {
    const input = document.getElementById('customModelInput');
    if (input && input.value.trim()) {
        selectedModel = input.value.trim();
        renderModelSelector();
        closeModal('modelModal');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Studio Tab Switching
// ─────────────────────────────────────────────────────────────────────────────

function switchStudioTab(tabId) {
    activeTab = tabId;

    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-tab') === tabId);
    });

    document.querySelectorAll('.studio-panel').forEach(panel => {
        panel.classList.remove('active');
    });

    const targetPanel = document.getElementById(`panel${tabId.charAt(0).toUpperCase() + tabId.slice(1)}`);
    if (targetPanel) {
        targetPanel.classList.add('active');
    }

    if (tabId === 'preview') {
        checkPreviewStatus();
    } else if (tabId === 'editor' && !openFilePath) {
        // suggest opening a file
    }
}

function toggleSidebar() {
    const sidebar = document.getElementById('studioSidebar');
    if (sidebar) {
        sidebar.classList.toggle('collapsed');
    }
}

function switchSidebarTab(tabName) {
    document.querySelectorAll('.sidebar-tab-btn').forEach(b => {
        b.classList.toggle('active', b.innerText.toLowerCase().includes(tabName));
    });

    document.querySelectorAll('.sidebar-pane').forEach(p => p.classList.remove('active'));
    if (tabName === 'files') document.getElementById('sidebarFilesView').classList.add('active');
    else if (tabName === 'projects') document.getElementById('sidebarProjectsView').classList.add('active');
    else if (tabName === 'git') {
        document.getElementById('sidebarGitView').classList.add('active');
        loadGitStatus();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// File Explorer & Code Editor
// ─────────────────────────────────────────────────────────────────────────────

async function loadFileTree() {
    const container = document.getElementById('fileTreeContainer');
    if (!container) return;
    container.innerHTML = '<div class="tree-loading">Loading workspace files...</div>';

    try {
        const res = await fetch(`/api/files/tree?path=${encodeURIComponent(activeWorkspace)}`);
        const data = await res.json();
        const tree = data.tree || [];
        if (tree.length === 0) {
            container.innerHTML = '<div class="tree-loading">No files in active workspace</div>';
            return;
        }

        container.innerHTML = renderTreeNodes(tree);
    } catch (e) {
        container.innerHTML = `<div class="tree-loading" style="color: #ef4444;">Failed to load files: ${escapeHtml(e.message)}</div>`;
    }
}

function getFileIcon(fileName) {
    const ext = fileName.split('.').pop().toLowerCase();
    if (['py'].includes(ext)) return '🐍';
    if (['js', 'mjs', 'cjs'].includes(ext)) return '📜';
    if (['ts', 'tsx'].includes(ext)) return '🔷';
    if (['html', 'htm'].includes(ext)) return '🌐';
    if (['css', 'scss', 'sass'].includes(ext)) return '🎨';
    if (['json'].includes(ext)) return '📦';
    if (['md', 'txt'].includes(ext)) return '📝';
    if (['sh', 'bash'].includes(ext)) return '⚡';
    if (['yml', 'yaml'].includes(ext)) return '⚙️';
    if (['sql'].includes(ext)) return '🗄️';
    if (['png', 'jpg', 'jpeg', 'svg', 'gif'].includes(ext)) return '🖼️';
    return '📄';
}

function renderTreeNodes(nodes) {
    let html = '';
    nodes.forEach(node => {
        if (node.is_dir) {
            html += `
                <div class="tree-node">
                    <div class="tree-node-content" onclick="toggleTreeNode(this)">
                        <span>📁</span>
                        <strong>${escapeHtml(node.name)}</strong>
                    </div>
                    <div class="tree-children" style="display: none;">
                        ${node.children && node.children.length > 0 ? renderTreeNodes(node.children) : '<div style="color:#64748b; padding: 2px 8px;">(empty)</div>'}
                    </div>
                </div>
            `;
        } else {
            html += `
                <div class="tree-node">
                    <div class="tree-node-content file" onclick="openFileInEditor('${escapeHtml(node.path)}')">
                        <span>${getFileIcon(node.name)}</span>
                        <span>${escapeHtml(node.name)}</span>
                    </div>
                </div>
            `;
        }
    });
    return html;
}

function toggleTreeNode(element) {
    const children = element.nextElementSibling;
    if (children) {
        const isHidden = children.style.display === 'none';
        children.style.display = isHidden ? 'block' : 'none';
        const folderIcon = element.querySelector('span');
        if (folderIcon) folderIcon.innerText = isHidden ? '📂' : '📁';
    }
}

async function openFileInEditor(filePath) {
    try {
        const res = await fetch(`/api/files/read?path=${encodeURIComponent(filePath)}`);
        if (!res.ok) throw new Error("Could not read file");
        const data = await res.json();

        openFilePath = data.path;
        fileContentOriginal = data.content;

        const pathElem = document.getElementById('editorCurrentFilePath');
        if (pathElem) pathElem.innerText = data.path.replace('/home/sk', '~');

        const textarea = document.getElementById('editorTextarea');
        if (textarea) {
            textarea.value = data.content;
            updateEditorLineNumbers();
        }

        const dirtyIndicator = document.getElementById('editorDirtyIndicator');
        if (dirtyIndicator) dirtyIndicator.style.display = 'none';

        const saveBtn = document.getElementById('editorSaveBtn');
        if (saveBtn) saveBtn.disabled = true;

        const badge = document.getElementById('editorFileBadge');
        if (badge) {
            badge.innerText = data.name;
            badge.style.display = 'inline-block';
        }

        switchStudioTab('editor');
    } catch (e) {
        alert("Failed to open file: " + e.message);
    }
}

function updateEditorLineNumbers() {
    const textarea = document.getElementById('editorTextarea');
    const lineContainer = document.getElementById('editorLineNumbers');
    if (!textarea || !lineContainer) return;

    const lineCount = textarea.value.split('\n').length;
    let linesHtml = '';
    for (let i = 1; i <= Math.max(lineCount, 1); i++) {
        linesHtml += i + '<br>';
    }
    lineContainer.innerHTML = linesHtml;
}

async function saveCurrentFile() {
    if (!openFilePath) return;
    const textarea = document.getElementById('editorTextarea');
    if (!textarea) return;

    const saveBtn = document.getElementById('editorSaveBtn');
    if (saveBtn) saveBtn.innerText = "💾 Saving...";

    try {
        const res = await fetch('/api/files/save', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                path: openFilePath,
                content: textarea.value
            })
        });
        if (!res.ok) throw new Error("Save failed");
        
        fileContentOriginal = textarea.value;
        const dirtyIndicator = document.getElementById('editorDirtyIndicator');
        if (dirtyIndicator) dirtyIndicator.style.display = 'none';
        if (saveBtn) saveBtn.disabled = true;
        loadGitStatus();
    } catch (e) {
        alert("Failed to save file: " + e.message);
    } finally {
        if (saveBtn) saveBtn.innerText = "💾 Save (Ctrl+S)";
    }
}

function reloadCurrentFile() {
    if (openFilePath) openFileInEditor(openFilePath);
}

function promptAgentAboutFile() {
    if (!openFilePath) return;
    const promptInput = document.getElementById('promptInput');
    const fileName = openFilePath.split('/').pop();
    switchStudioTab('chat');
    if (promptInput) {
        promptInput.value = `Regarding file \`${openFilePath}\`: `;
        promptInput.focus();
    }
}

function createNewFilePrompt() {
    const name = prompt("Enter file name (relative to active workspace):");
    if (!name) return;
    const fullPath = activeWorkspace.replace(/\/$/, '') + '/' + name.replace(/^\//, '');
    openFilePath = fullPath;
    fileContentOriginal = "";
    const textarea = document.getElementById('editorTextarea');
    if (textarea) textarea.value = "";
    const pathElem = document.getElementById('editorCurrentFilePath');
    if (pathElem) pathElem.innerText = fullPath.replace('/home/sk', '~');
    switchStudioTab('editor');
    const saveBtn = document.getElementById('editorSaveBtn');
    if (saveBtn) saveBtn.disabled = false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Workspace Git Status
// ─────────────────────────────────────────────────────────────────────────────

async function loadGitStatus() {
    try {
        const res = await fetch('/api/files/git-status');
        const data = await res.json();
        
        const branchElem = document.getElementById('gitBranchName');
        if (branchElem) branchElem.innerText = data.branch || 'main';

        const gitList = document.getElementById('sidebarGitList');
        if (gitList) {
            if (!data.files || data.files.length === 0) {
                gitList.innerHTML = '<div style="color: #64748b; font-size: 12px; padding: 10px;">Working tree clean. No changes.</div>';
                return;
            }

            let html = '';
            data.files.forEach(f => {
                const isModified = f.status.includes('M');
                const badgeColor = isModified ? '#f59e0b' : '#34d399';
                html += `
                    <div class="quick-proj-card" onclick="openFileInEditor('${activeWorkspace}/${f.file}')">
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                            <span class="quick-proj-name" style="font-family: monospace; font-size: 12px;">${escapeHtml(f.file)}</span>
                            <span style="font-size: 10px; font-weight: 700; color: ${badgeColor};">${escapeHtml(f.status)}</span>
                        </div>
                    </div>
                `;
            });
            gitList.innerHTML = html;
        }
    } catch (e) {
        console.error("Failed to load git status", e);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Workspace Terminal
// ─────────────────────────────────────────────────────────────────────────────

async function runTerminalCommand() {
    const input = document.getElementById('terminalInput');
    if (!input) return;
    const cmd = input.value.trim();
    if (!cmd) return;
    input.value = '';
    await executeCommandInConsole(cmd);
}

function runQuickCommand(cmd) {
    executeCommandInConsole(cmd);
}

async function executeCommandInConsole(cmd) {
    const consoleElem = document.getElementById('terminalConsole');
    if (!consoleElem) return;

    appendTerminalLine(`$ ${cmd}`, 'cmd');

    try {
        const res = await fetch('/api/terminal/exec', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                command: cmd,
                cwd: activeWorkspace,
                timeout: 60
            })
        });
        const data = await res.json();
        
        if (data.stdout) {
            appendTerminalLine(data.stdout, 'stdout');
        }
        if (data.stderr) {
            appendTerminalLine(data.stderr, 'stderr');
        }
        if (data.exit_code !== 0) {
            appendTerminalLine(`[Process exited with code ${data.exit_code}]`, 'stderr');
        } else {
            appendTerminalLine(`✓ Done (exit 0)`, 'info');
        }
        
        // Refresh git and file tree if command was git or file modifying
        if (cmd.includes('git') || cmd.includes('touch') || cmd.includes('rm') || cmd.includes('npm') || cmd.includes('pip')) {
            loadGitStatus();
            loadFileTree();
        }
    } catch (e) {
        appendTerminalLine(`Error executing command: ${e.message}`, 'stderr');
    }
}

function appendTerminalLine(text, type = 'stdout') {
    const consoleElem = document.getElementById('terminalConsole');
    if (!consoleElem) return;
    const div = document.createElement('div');
    div.className = `term-line ${type}`;
    div.innerText = text;
    consoleElem.appendChild(div);
    consoleElem.scrollTop = consoleElem.scrollHeight;
}

function clearTerminal() {
    const consoleElem = document.getElementById('terminalConsole');
    if (consoleElem) {
        consoleElem.innerHTML = '<div class="term-line info">⚡ Terminal cleared.</div>';
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Live Web Preview Frame
// ─────────────────────────────────────────────────────────────────────────────

async function checkPreviewStatus() {
    const portInput = document.getElementById('previewPortInput');
    const port = portInput ? parseInt(portInput.value, 10) || 1597 : 1597;
    previewPort = port;

    const dot = document.getElementById('previewStatusDot');
    const text = document.getElementById('previewStatusText');
    const pill = document.getElementById('previewPortPill');
    if (pill) pill.innerText = `:${port}`;

    try {
        const res = await fetch(`/api/preview/status?port=${port}`);
        const data = await res.json();
        
        if (data.running) {
            if (dot) dot.className = 'preview-status-dot active';
            if (text) text.innerText = `🟢 Listening on Port ${port}`;
            loadPreviewFrame(port);
        } else {
            if (dot) dot.className = 'preview-status-dot';
            if (text) text.innerText = `⚪ No server listening on port ${port}`;
        }
    } catch (e) {
        if (text) text.innerText = `Error checking port ${port}`;
    }
}

function updatePreviewPort() {
    checkPreviewStatus();
}

function loadPreviewFrame(port) {
    const iframe = document.getElementById('previewIframe');
    if (!iframe) return;
    const host = window.location.hostname || 'localhost';
    const targetUrl = `http://${host}:${port}`;
    if (currentPreviewUrl !== targetUrl || iframe.src === 'about:blank') {
        currentPreviewUrl = targetUrl;
        iframe.src = targetUrl;
    }
}

function refreshPreviewFrame() {
    const iframe = document.getElementById('previewIframe');
    if (iframe && currentPreviewUrl) {
        iframe.src = currentPreviewUrl;
    }
    checkPreviewStatus();
}

function openPreviewExternal() {
    if (currentPreviewUrl) {
        window.open(currentPreviewUrl, '_blank');
    } else {
        const host = window.location.hostname || 'localhost';
        window.open(`http://${host}:${previewPort}`, '_blank');
    }
}

function setPreviewViewport(vp) {
    document.querySelectorAll('.vp-btn').forEach(b => {
        b.classList.toggle('active', b.getAttribute('data-vp') === vp);
    });
    const wrapper = document.getElementById('previewFrameWrapper');
    if (wrapper) {
        wrapper.className = `preview-frame-wrapper ${vp}`;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Agent Prompt & Streaming (Antigravity CLI)
// ─────────────────────────────────────────────────────────────────────────────

async function submitPrompt(customPrompt = null, options = {}) {
    if (isAgentRunning) return;

    const promptInput = document.getElementById('promptInput');
    const text = (customPrompt !== null ? customPrompt : (promptInput ? promptInput.value : '')).trim();
    if (!text) return;

    if (customPrompt === null && promptInput) {
        promptInput.value = '';
    }

    appendMessage('user', text);

    const msgId = 'msg-' + Date.now();
    const messageContainer = appendMessage('agent', '', msgId);
    const contentDiv = messageContainer.querySelector('.message-content');

    contentDiv.innerHTML = `
        <div id="${msgId}-thinking-container" class="thinking-accordion" style="display: none;">
            <div class="thinking-header" onclick="toggleThinking('${msgId}')">
                <span>⚡ Agent Internal Reasoning / Thinking</span>
                <span id="${msgId}-thinking-toggle">▼</span>
            </div>
            <div id="${msgId}-thinking-body" class="thinking-body"></div>
        </div>
        <div id="${msgId}-tools"></div>
        <div id="${msgId}-text" style="margin-top: 6px;">
            <span style="color: #a5b4fc; font-style: italic;">Antigravity agent starting on active workspace...</span>
        </div>
    `;

    const toolsContainer = document.getElementById(`${msgId}-tools`);
    const textContainer = document.getElementById(`${msgId}-text`);
    const thinkingContainer = document.getElementById(`${msgId}-thinking-container`);
    const thinkingBody = document.getElementById(`${msgId}-thinking-body`);

    setAgentRunning(true);

    try {
        const response = await fetch('/api/agent/prompt', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                prompt: text,
                workspace_path: activeWorkspace,
                model: selectedModel,
                session_id: activeSessionId,
                dev_port: options.dev_port || previewPort,
                is_new_project: options.is_new_project || false,
                mode: "accept-edits"
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
            sseBuffer = events.pop();

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
                            } else if (eventData.type === 'thinking') {
                                if (thinkingContainer) thinkingContainer.style.display = 'block';
                                if (thinkingBody) {
                                    thinkingBody.innerText += eventData.text;
                                    thinkingBody.scrollTop = thinkingBody.scrollHeight;
                                }
                            } else if (eventData.type === 'tool') {
                                const badge = document.createElement('div');
                                badge.className = 'tool-badge';
                                badge.innerHTML = `⚙️ Tool: <strong>${escapeHtml(eventData.name || 'executing')}</strong>`;
                                if (toolsContainer) toolsContainer.appendChild(badge);
                            } else if (eventData.type === 'tool_result') {
                                const resBox = document.createElement('div');
                                resBox.className = 'tool-result-box';
                                resBox.innerText = `↳ ${eventData.result || 'Done'}`;
                                if (toolsContainer) toolsContainer.appendChild(resBox);
                            } else if (eventData.type === 'chunk') {
                                fullResponse += eventData.text;
                                renderMarkdownResponse(textContainer, fullResponse);
                            } else if (eventData.type === 'error') {
                                renderErrorAlert(contentDiv, eventData.text, eventData.is_quota);
                            } else if (eventData.type === 'end') {
                                // turn finished
                            }
                        } catch (err) {
                            console.warn("SSE Parse Warning:", err);
                        }
                    }
                }
            }
        }

        if (!fullResponse && (!toolsContainer || toolsContainer.children.length === 0)) {
            textContainer.innerHTML = `<span style="color: #94a3b8; font-style: italic;">(Completed turn)</span>`;
        }

        // Refresh workspace state after prompt completes
        loadGitStatus();
        loadFileTree();
        checkPreviewStatus();

    } catch (e) {
        contentDiv.innerHTML += `<div class="quota-alert-card"><div class="quota-alert-title">❌ Bridge Execution Error</div><div class="quota-alert-body">${escapeHtml(e.message)}</div></div>`;
    } finally {
        setAgentRunning(false);
    }
}

function setAgentRunning(running) {
    isAgentRunning = running;
    const sendBtn = document.getElementById('sendBtn');
    const cancelBtn = document.getElementById('cancelBtn');

    if (sendBtn) sendBtn.style.display = running ? 'none' : 'flex';
    if (cancelBtn) cancelBtn.style.display = running ? 'block' : 'none';
}

async function cancelActivePrompt() {
    if (!activeSessionId) return;
    try {
        await fetch('/api/agent/cancel', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ session_id: activeSessionId })
        });
        setAgentRunning(false);
    } catch (e) {
        console.error("Error cancelling prompt", e);
    }
}

function toggleThinking(msgId) {
    const body = document.getElementById(`${msgId}-thinking-body`);
    const toggle = document.getElementById(`${msgId}-thinking-toggle`);
    if (!body || !toggle) return;
    const isHidden = body.style.display === 'none';
    body.style.display = isHidden ? 'block' : 'none';
    toggle.innerText = isHidden ? '▼' : '▲';
}

function renderErrorAlert(container, errorText, isQuota) {
    const alertDiv = document.createElement('div');
    alertDiv.className = 'quota-alert-card';
    
    if (isQuota) {
        alertDiv.innerHTML = `
            <div class="quota-alert-title">⚠️ Individual Antigravity Quota Limit Reached</div>
            <div class="quota-alert-body">
                ${escapeHtml(errorText)}<br>
                Your cloud quota for this model is currently exhausted. You can instantly switch models and continue developing.
            </div>
            <div style="display: flex; gap: 8px; margin-top: 8px;">
                <button class="quota-switch-btn" onclick="openModelModal()">🧠 Switch Model &amp; Retry</button>
            </div>
        `;
    } else {
        alertDiv.innerHTML = `
            <div class="quota-alert-title">⚠️ Execution Alert</div>
            <div class="quota-alert-body">${escapeHtml(errorText)}</div>
        `;
    }
    container.appendChild(alertDiv);
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

    let formatted = escapeHtml(text);

    // Code blocks
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

// ─────────────────────────────────────────────────────────────────────────────
// Project Creation & History
// ─────────────────────────────────────────────────────────────────────────────

async function loadKnownProjects() {
    try {
        const res = await fetch('/api/workspaces/projects');
        const data = await res.json();
        const list = document.getElementById('sidebarProjectsList');
        if (!list) return;

        let html = '';
        (data.projects || []).forEach(p => {
            html += `
                <div class="quick-proj-card ${p.is_active ? 'active' : ''}" onclick="selectWorkspaceDirect('${p.path}')">
                    <div class="quick-proj-name">${p.icon} ${escapeHtml(p.name)}</div>
                    <div class="quick-proj-desc">${escapeHtml(p.desc || p.path)}</div>
                </div>
            `;
        });
        list.innerHTML = html;
    } catch (e) {
        console.error("Failed to load projects", e);
    }
}

function openNewProjectModal() {
    const modal = document.getElementById('newProjectModal');
    if (modal) {
        modal.classList.add('open');
        const nameInput = document.getElementById('newProjectName');
        if (nameInput) {
            nameInput.value = '';
            setTimeout(() => nameInput.focus(), 150);
        }
        const promptInput = document.getElementById('newProjectPrompt');
        if (promptInput) promptInput.value = '';
    }
}

async function handleCreateProject(e) {
    if (e) e.preventDefault();
    const nameInput = document.getElementById('newProjectName');
    const promptInput = document.getElementById('newProjectPrompt');
    const portInput = document.getElementById('newProjectPort');
    const modelSelect = document.getElementById('newProjectModelSelect');

    const name = nameInput ? nameInput.value.trim() : '';
    const initialPrompt = promptInput ? promptInput.value.trim() : '';
    const port = portInput ? parseInt(portInput.value, 10) || 1597 : 1597;
    const model = modelSelect ? modelSelect.value : selectedModel;

    if (!name) {
        alert("Please enter a project name");
        return;
    }

    try {
        const res = await fetch('/api/workspaces/create', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: name,
                initial_prompt: initialPrompt,
                port: port,
                init_git: true,
                model: model
            })
        });

        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || "Could not create project");
        }

        const data = await res.json();
        activeWorkspace = data.workspace;
        activeSessionId = data.session_id;
        previewPort = data.dev_port || 1597;

        closeModal('newProjectModal');
        updateWorkspaceLabels();
        loadFileTree();
        loadGitStatus();
        loadKnownProjects();

        // Reset chat
        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.innerHTML = `
                <div class="message-card agent">
                    <div class="avatar agent">🚀</div>
                    <div class="message-content">
                        <h3>✨ Project Initialized: ${escapeHtml(data.name)}</h3>
                        <p style="margin-top: 6px; color: #94a3b8;">
                            Created directory <code>${escapeHtml(data.workspace)}</code> with Git initialized.
                        </p>
                        <div style="margin-top: 10px;">
                            <span class="port-mini-pill" style="font-size: 12px; padding: 4px 8px;">Dev Port Assigned: ${previewPort}</span>
                        </div>
                    </div>
                </div>
            `;
        }

        if (initialPrompt) {
            setTimeout(() => {
                submitPrompt(initialPrompt, { dev_port: previewPort, is_new_project: true });
            }, 300);
        }
    } catch (err) {
        alert("Failed to create project: " + err.message);
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
        updateWorkspaceLabels();
        loadFileTree();
        loadGitStatus();
        loadKnownProjects();
        closeModal('workspaceModal');
    } catch (e) {
        alert("Failed to select workspace: " + e.message);
    }
}

async function openWorkspaceModal(path = null) {
    await loadWorkspaceBrowserList(path || activeWorkspace);
    const modal = document.getElementById('workspaceModal');
    if (modal) modal.classList.add('open');
}

async function loadWorkspaceBrowserList(dirPath) {
    try {
        const res = await fetch(`/api/workspaces/list?path=${encodeURIComponent(dirPath || '')}`);
        if (!res.ok) throw new Error("Could not browse directory");
        const data = await res.json();

        const currentPath = data.path || dirPath || '/home/sk/dev';
        const items = data.items || [];

        const breadcrumbs = document.getElementById('workspaceBreadcrumbs');
        if (breadcrumbs) {
            const parts = currentPath.split('/').filter(Boolean);
            let accum = '';
            const crumbHtml = parts.map(p => {
                accum += '/' + p;
                const target = accum;
                return `<span style="cursor: pointer; color: #38bdf8; font-weight: 600;" onclick="loadWorkspaceBrowserList('${target}')">/ ${p}</span>`;
            }).join(' ');
            breadcrumbs.innerHTML = crumbHtml || '/';
        }

        const list = document.getElementById('workspaceList');
        if (list) {
            let html = `
                <div class="folder-item" onclick="selectWorkspaceDirect('${currentPath}')" style="background: rgba(99, 102, 241, 0.2); border: 1px solid rgba(99, 102, 241, 0.4); margin-bottom: 8px;">
                    <div>
                        <strong style="color: #a5b4fc;">✅ Select This Workspace:</strong>
                        <div style="font-size: 12px; color: #fff; font-family: monospace;">${currentPath}</div>
                    </div>
                    <span style="font-size: 12px; font-weight: 700; color: #a5b4fc;">Select ➔</span>
                </div>
            `;

            if (data.parent_path) {
                html += `
                    <div class="folder-item" onclick="loadWorkspaceBrowserList('${data.parent_path}')">
                        <span>📁 <strong>.. (Parent Directory)</strong></span>
                    </div>
                `;
            }

            items.forEach(item => {
                if (item.is_dir) {
                    html += `
                        <div class="folder-item" onclick="loadWorkspaceBrowserList('${item.path}')">
                            <span>📁 ${escapeHtml(item.name)}</span>
                            <span style="color: #64748b; font-size: 11px;">Open ➔</span>
                        </div>
                    `;
                }
            });

            list.innerHTML = html;
        }
    } catch (e) {
        console.error("Failed to browse directory", e);
    }
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

    const term = (filter || '').toLowerCase().trim();
    const filtered = allConversations.filter(c => 
        !term || c.title.toLowerCase().includes(term) || c.id.toLowerCase().includes(term)
    );

    if (filtered.length === 0) {
        list.innerHTML = `<div style="padding: 20px; text-align: center; color: #94a3b8; font-size: 13px;">No conversations found.</div>`;
        return;
    }

    let html = '';
    filtered.slice(0, 100).forEach(c => {
        const isAntigravity = c.source === 'antigravity';
        const badgeColor = isAntigravity ? '#6366f1' : '#10b981';
        const badgeIcon = isAntigravity ? '⚡ Antigravity CLI' : '💬 Dashboard';
        const isCurrent = c.id === activeSessionId;

        html += `
            <div class="folder-item ${isCurrent ? 'active' : ''}" onclick="selectConversation('${c.id}')" style="justify-content: space-between; padding: 10px 14px;">
                <div style="flex: 1; overflow: hidden; margin-right: 10px;">
                    <div style="font-weight: 700; color: #f8fafc; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;">${escapeHtml(c.title)}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 3px;">
                        <span style="font-weight: 700; color: ${badgeColor}; margin-right: 6px;">${badgeIcon}</span>
                        <span>${c.id.substring(0, 8)}...</span>
                    </div>
                </div>
                <span style="font-size: 11px; color: #38bdf8; font-weight: 700;">Open ➔</span>
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
            historyBadgeTitle.innerText = data.title ? (data.title.substring(0, 15) + '...') : 'Chats';
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
            appendMessage('agent', `Loaded session **${data.title}**.`);
        }

        closeModal('historyModal');
        switchStudioTab('chat');
    } catch (e) {
        alert("Could not load conversation: " + e.message);
    }
}

async function startNewConversation() {
    try {
        const res = await fetch('/api/conversations/new', { method: 'POST' });
        const data = await res.json();
        activeSessionId = data.session_id;

        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.innerHTML = `
                <div class="message-card agent">
                    <div class="avatar agent">⚡</div>
                    <div class="message-content">
                        <h3>💬 New Conversation Session</h3>
                        <p style="margin-top: 6px; color: #94a3b8;">
                            Prompt Antigravity on active workspace. Multi-turn session memory and filesystem modifications are enabled.
                        </p>
                    </div>
                </div>
            `;
        }

        closeModal('historyModal');
        switchStudioTab('chat');
    } catch (e) {
        console.error("Failed to start conversation", e);
    }
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.classList.remove('open');
}

// ─────────────────────────────────────────────────────────────────────────────
// Event Listeners & Hotkeys
// ─────────────────────────────────────────────────────────────────────────────

function setupEventListeners() {
    const promptInput = document.getElementById('promptInput');
    const sendBtn = document.getElementById('sendBtn');
    const terminalInput = document.getElementById('terminalInput');
    const editorTextarea = document.getElementById('editorTextarea');

    if (promptInput) {
        promptInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                submitPrompt();
            }
        });
    }

    if (sendBtn) sendBtn.onclick = () => submitPrompt();

    if (terminalInput) {
        terminalInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                runTerminalCommand();
            }
        });
    }

    if (editorTextarea) {
        editorTextarea.addEventListener('input', () => {
            updateEditorLineNumbers();
            const isDirty = editorTextarea.value !== fileContentOriginal;
            const dirtyDot = document.getElementById('editorDirtyIndicator');
            const saveBtn = document.getElementById('editorSaveBtn');
            if (dirtyDot) dirtyDot.style.display = isDirty ? 'inline' : 'none';
            if (saveBtn) saveBtn.disabled = !isDirty;
        });

        // Tab indentation support
        editorTextarea.addEventListener('keydown', (e) => {
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = editorTextarea.selectionStart;
                const end = editorTextarea.selectionEnd;
                editorTextarea.value = editorTextarea.value.substring(0, start) + '  ' + editorTextarea.value.substring(end);
                editorTextarea.selectionStart = editorTextarea.selectionEnd = start + 2;
                updateEditorLineNumbers();
            }
        });
    }

    // Global Hotkeys: Ctrl+S to save editor file
    window.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
            if (activeTab === 'editor' && openFilePath) {
                e.preventDefault();
                saveCurrentFile();
            }
        }
    });

    const historySearch = document.getElementById('historySearchInput');
    if (historySearch) {
        historySearch.addEventListener('input', (e) => renderHistoryList(e.target.value));
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

function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return String(unsafe)
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}
