'use strict';

(function () {
  const CSRF_KEY = 'ps5_agent_csrf';
  const USER_KEY = 'ps5_agent_user';
  const SIDEBAR_COLLAPSED_KEY = 'ps5_agent_sidebar_collapsed';
  const RAW_OUTPUT_KEY = 'ps5_agent_chat_raw_output';
  const BUILTIN_TOOL_IDS = new Set([
    'http_get',
    'http_post',
    'fetch_website',
    'extract_html_text',
    'json_api_request',
    'list_files',
    'read_file',
    'write_file',
    'create_directory',
    'delete_file',
    'calculator',
    'datetime',
    'json_process',
    'text_process',
    'send_agent_message',
    'assign_agent_task',
    'query_agent_status',
    'read_agent_memory',
    'request_temporary_specialist',
    'load_skill',
    'schedule_create',
    'schedule_list',
    'schedule_update',
    'schedule_delete',
    'web_search',
    'web_fetch',
    'browser',
    'edit_file',
    'apply_patch',
    'grep_search',
    'glob_files',
    'sandbox_exec',
    'process',
    'memory_read',
    'memory_write',
    'memory_search',
    'memory_get',
    'system_info',
    'ps5_info',
    'doctor',
    'write_artifact',
    'sessions_list',
    'sessions_spawn',
    'sessions_send',
    'session_status'
  ]);

  const SLASH_COMMANDS = [
    { cmd: '/btw', args: '<question>', desc: 'Ask an ephemeral side question (nothing saved to chat or memory)' },
    { cmd: '/status', args: '', desc: 'Inspect active agent, model, PS5LM URL, sandbox policy & tasks' },
    { cmd: '/context', args: '', desc: 'Show prompt token/char footprint, active brain files & memories' },
    { cmd: '/tools', args: '[agent]', desc: 'List effective enabled tools, denied tools & group expansions' },
    { cmd: '/compact', args: '', desc: 'Summarize older conversation messages cleanly' },
    { cmd: '/think', args: '[off|minimal|low|medium|high|xhigh|max|auto|<mode>]', desc: 'Set thinking level or custom mode (einstein, spoon, 007)' },
    { cmd: '/model', args: '[name]', desc: 'Report active model or validated override' },
    { cmd: '/sandbox', args: '[mode|policy]', desc: 'Inspect or update sandbox mode & approval policy' },
    { cmd: '/doctor', args: '[fix]', desc: 'Run 12-point system health diagnostics (or auto-fix)' },
    { cmd: '/new', args: '', desc: 'Create a fresh conversation and switch to it' },
    { cmd: '/help', args: '', desc: 'List built-in slash commands and custom workflows' }
  ];

  const SECTION_META = {
    overview: {
      title: 'Overview',
      subtitle: 'PS5 telemetry, inference health, and pending approvals'
    },
    chat: {
      title: 'Main Chat',
      subtitle: 'Direct and multi-agent group conversations with live streaming'
    },
    agents: {
      title: 'Agents',
      subtitle: 'Manage built-in and custom specialists, permissions, and tools'
    },
    a2a: {
      title: 'Agent Communication',
      subtitle: 'Agent-to-agent message envelopes and loop-prevention delivery trace'
    },
    tasks: {
      title: 'Tasks',
      subtitle: 'Active queue, delegation tree, progress, and execution history'
    },
    schedules: {
      title: 'Schedules & Autonomous Tasks',
      subtitle: 'Background & timed autonomous agent tasks'
    },
    memory: {
      title: 'Memory',
      subtitle: 'Scoped memory browser across conversation, agent, project, and preferences'
    },
    tools: {
      title: 'Tools',
      subtitle: 'Registered tool policies, live tester, and execution audit history'
    },
    files: {
      title: 'Workspace Files',
      subtitle: 'Browse, upload, download, and preview workspace artifacts'
    },
    extensions: {
      title: 'Extensions',
      subtitle: 'Manage Skills, MCP Servers, and Plugins'
    },
    sandbox: {
      title: 'Sandbox & Doctor',
      subtitle: 'Isolation policy, 12-point diagnostics, browser console, brain files, and webhooks'
    },
    logs: {
      title: 'Logs',
      subtitle: 'Runtime ring buffer logs and security audit trail'
    },
    settings: {
      title: 'Settings',
      subtitle: 'Inference endpoint, runtime limits, password, and daemon lifecycle'
    }
  };

  const state = {
    authenticated: false,
    csrfToken: sessionStorage.getItem(CSRF_KEY) || '',
    username: sessionStorage.getItem(USER_KEY) || 'admin',
    currentSection: 'overview',
    sse: null,
    sseConnected: false,
    sseWasConnectedOnce: false,
    sseNeedsResync: false,
    sseReconnectTimer: null,
    sseRetryMs: 1500,
    fallbackPollTimer: null,
    metricsPollTimer: null,

    metrics: null,
    approvals: [],
    agents: [],
    tools: [],
    toolExecutions: [],
    conversations: [],
    activeConversationId: null,
    convLoadSeq: 0,
    taskRehydrateSeq: 0,
    chatMessages: [],
    chatStreams: new Map(), // taskId -> { taskId, conversationId, agentId, text, reasoning, toolCalls, bubbleEl, dirty }
    chatStreamRafId: null,
    chatStreamTimerId: null,
    chatStreamLastFlushMs: 0,
    pendingAttachments: [],
    slashMenuMatches: [],
    slashMenuIndex: 0,
    slashPaletteOpen: false,
    slashPaletteMatches: [],
    slashPaletteIndex: 0,
    slashWorkflowCommands: [],
    sidebarCollapsed: (() => {
      try { return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === 'true'; } catch (_) { return false; }
    })(),
    showRawOutput: (() => {
      try { return localStorage.getItem(RAW_OUTPUT_KEY) === 'true'; } catch (_) { return false; }
    })(),
    dragConvId: null,
    pendingFilePreview: null,
    modalBlobUrl: null,
    filePreviewBlobUrl: null,
    fileLoadSeq: 0,
    filePreviewSeq: 0,
    filesCurrentPath: '',
    filesEntries: [],
    selectedFilePath: '',
    selectedFileContent: '',
    filesHtmlPreviewActive: false,
    fileEditMode: false,
    fileViewMode: 'preview',
    extensionsTab: 'skills',
    extensionsData: { skills: [], plugins: [], mcp_servers: [] },
    inferenceCapabilities: null,
    inferenceModels: [],
    sandboxState: null,
    doctorReport: null,
    browserSessions: [],
    activeBrowserSessionId: '',
    agenticSearchEnabled: false,
    liveBrowserTabs: new Map(),
    liveBrowserOrder: [],
    activeLiveBrowserSessionId: '',
    liveBrowserFeed: [],
    liveBrowserRev: 0,
    liveBrowserPreviewTimers: { chat: null, sandbox: null },
    liveBrowserLastSrc: { chat: '', sandbox: '' },
    liveBrowserLastSnapshotFetchMs: new Map(),
    liveBrowserCursorTimers: new Set(),
    liveOverlayResizeTimer: null,
    chatLiveBrowserDismissed: false,
    sandboxAgenticConversationId: '',
    sandboxAgenticTaskIds: new Set(),
    chatTimingsByTaskId: new Map(),
    chatTimingsByMsgId: new Map(),
    brainFiles: [],
    activeBrainFile: '',
    brainDirty: false,
    webhooks: [],
    activeChatTasks: new Map(),
    completedChatTaskIds: new Set(),
    chatErrors: new Map(),
    tasks: [],
    taskFilter: 'all',
    schedules: [],
    scheduleFilter: 'all',
    aiScheduleDraft: null,
    a2aMessages: [],
    memories: [],
    memoryFilters: { scope: '', namespace: '', ownerAgentId: '' },
    logsTab: 'runtime',
    runtimeLogs: [],
    auditLogs: [],
    settingsRaw: null
  };

  /* -------------------- DOM Helpers (XSS-safe: DOM APIs only) -------------------- */
  function $(id) {
    return document.getElementById(id);
  }

  function getEl(id) {
    return document.getElementById(id);
  }

  function clearEl(node) {
    if (node) {
      node.replaceChildren();
    }
  }

  const SVG_NS = 'http://www.w3.org/2000/svg';

  function svgEl(tag, attrs, ...children) {
    const node = document.createElementNS(SVG_NS, tag);
    if (attrs) {
      for (const [key, val] of Object.entries(attrs)) {
        if (val === undefined || val === null || val === false) continue;
        const lower = String(key).toLowerCase();
        if (lower === 'style' || lower.startsWith('on')) continue;
        if (key === 'className' || key === 'class') {
          node.setAttribute('class', String(val));
        } else {
          node.setAttribute(key, String(val));
        }
      }
    }
    for (const child of children) {
      if (child === undefined || child === null || child === false) continue;
      if (typeof child === 'string' || typeof child === 'number') {
        node.appendChild(document.createTextNode(String(child)));
      } else {
        node.appendChild(child);
      }
    }
    return node;
  }

  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    if (attrs) {
      for (const [key, val] of Object.entries(attrs)) {
        if (val === undefined || val === null || val === false) continue;
        if (key === 'className') {
          node.className = val;
        } else if (key === 'dataset' && typeof val === 'object') {
          for (const [dk, dv] of Object.entries(val)) {
            if (dv !== undefined && dv !== null) {
              node.dataset[dk] = String(dv);
            }
          }
        } else if (key.startsWith('on')) {
          if (typeof val === 'function') {
            node.addEventListener(key.slice(2).toLowerCase(), val);
          }
        } else if (key === 'checked' || key === 'disabled' || key === 'selected' || key === 'required' || key === 'open') {
          node[key] = Boolean(val);
        } else {
          node.setAttribute(key, String(val));
        }
      }
    }
    for (const child of children) {
      if (child === undefined || child === null || child === false) continue;
      if (Array.isArray(child)) {
        for (const nested of child) {
          if (nested !== undefined && nested !== null && nested !== false) {
            node.appendChild(typeof nested === 'string' || typeof nested === 'number'
              ? document.createTextNode(String(nested))
              : nested);
          }
        }
      } else if (typeof child === 'string' || typeof child === 'number') {
        node.appendChild(document.createTextNode(String(child)));
      } else {
        node.appendChild(child);
      }
    }
    return node;
  }

  function setText(id, text) {
    const node = $(id);
    if (node) {
      node.textContent = text === undefined || text === null || text === '' ? '\u2014' : String(text);
    }
  }

  function showToast(message, variant) {
    const container = $('toast-container');
    if (!container) return;
    const tone = variant || 'info';
    const toast = el('div', { className: `toast toast-${tone}` },
      el('span', null, String(message)),
      el('button', {
        type: 'button',
        className: 'btn btn-ghost btn-icon',
        'aria-label': 'Dismiss notification',
        onClick: () => toast.remove()
      }, '\u00d7')
    );
    container.appendChild(toast);
    window.setTimeout(() => {
      if (toast.isConnected) toast.remove();
    }, 5200);
  }

  /* -------------------- Formatting Helpers -------------------- */
  function fmtBytes(bytes) {
    if (bytes === null || bytes === undefined || Number.isNaN(Number(bytes))) return '\u2014';
    const n = Number(bytes);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
  }

  function fmtDuration(secs) {
    if (secs === null || secs === undefined) return '\u2014';
    const s = Math.max(0, Math.floor(Number(secs)));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const rem = s % 60;
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${rem}s`;
    return `${rem}s`;
  }

  function fmtTime(iso) {
    if (!iso) return '\u2014';
    try {
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch (_) {
      return String(iso);
    }
  }

  function fmtDateTime(iso) {
    if (!iso) return '\u2014';
    try {
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso);
      return d.toLocaleString();
    } catch (_) {
      return String(iso);
    }
  }

  function shortId(id) {
    if (!id) return '\u2014';
    const str = String(id);
    return str.length > 12 ? `${str.slice(0, 8)}\u2026` : str;
  }

  function prettyJson(raw) {
    if (raw === null || raw === undefined) return '';
    if (typeof raw !== 'string') {
      try { return JSON.stringify(raw, null, 2); } catch (_) { return String(raw); }
    }
    try {
      return JSON.stringify(JSON.parse(raw), null, 2);
    } catch (_) {
      return raw;
    }
  }

  function unwrapList(data, keys) {
    if (Array.isArray(data)) return data;
    if (data && typeof data === 'object') {
      for (const k of keys) {
        if (Array.isArray(data[k])) return data[k];
      }
      if (Array.isArray(data.items)) return data.items;
      if (Array.isArray(data.data)) return data.data;
    }
    return [];
  }

  function statusPillClass(status) {
    const s = String(status || '').toUpperCase();
    if (['COMPLETED', 'DELIVERED', 'PROCESSED', 'APPROVED', 'OK', 'ONLINE', 'INFO'].includes(s)) return 'pill-success';
    if (['PENDING', 'QUEUED'].includes(s)) return 'pill-pending';
    if (['WAITING', 'WARN', 'WARNING'].includes(s)) return 'pill-warning';
    if (['FAILED', 'CANCELLED', 'REJECTED', 'BLOCKED_LOOP', 'DENIED', 'ERROR', 'OFFLINE'].includes(s)) return 'pill-danger';
    if (['RUNNING', 'ASSIGNED', 'DEBUG'].includes(s)) return 'pill-info';
    return 'pill-muted';
  }

  function agentDisplayName(agentId) {
    if (!agentId) return 'System';
    const found = state.agents.find((a) => a.id === agentId);
    return found ? found.name : agentId;
  }

  /* -------------------- API Client & Auth Handling -------------------- */
  function revokeTrackedBlobUrl(key) {
    if (state[key]) {
      try {
        URL.revokeObjectURL(state[key]);
      } catch (_) {}
      state[key] = null;
    }
  }

  const MAX_LIVE_BROWSER_TABS = 12;
  const MAX_LIVE_BROWSER_FEED = 100;
  const MAX_CACHED_TIMINGS = 256;
  const MAX_CHAT_STREAM_BUCKETS = 32;
  const MAX_SVG_HOTSPOTS = 40;
  const MAX_SVG_RIPPLES = 4;

  function clampPct(val) {
    if (typeof val !== 'number' || !Number.isFinite(val)) return null;
    return Math.max(0, Math.min(100, val));
  }

  function clearAllStreamBuckets() {
    if (state.chatStreamRafId) {
      window.cancelAnimationFrame(state.chatStreamRafId);
      state.chatStreamRafId = 0;
    }
    if (state.chatStreamTimerId) {
      window.clearTimeout(state.chatStreamTimerId);
      state.chatStreamTimerId = 0;
    }
    if (state.chatStreams) {
      for (const bucket of state.chatStreams.values()) {
        if (bucket && bucket.bubbleEl && bucket.bubbleEl.isConnected) {
          bucket.bubbleEl.remove();
        }
      }
      state.chatStreams.clear();
    }
    updateActiveStreamsBadge();
  }

  function pruneChatStreamBuckets() {
    if (!state.chatStreams) return;
    const now = Date.now();
    for (const [tid, bucket] of [...state.chatStreams.entries()]) {
      const ageMs = now - (bucket && bucket.updatedAtMs ? bucket.updatedAtMs : now);
      if (bucket && bucket.completed && ageMs > 5000) {
        if (bucket.bubbleEl && bucket.bubbleEl.isConnected) bucket.bubbleEl.remove();
        state.chatStreams.delete(tid);
      }
    }
    if (state.chatStreams.size <= MAX_CHAT_STREAM_BUCKETS) return;
    const entries = [...state.chatStreams.entries()].sort((a, b) => {
      const aDone = a[1] && a[1].completed ? 0 : 1;
      const bDone = b[1] && b[1].completed ? 0 : 1;
      if (aDone !== bDone) return aDone - bDone;
      return (a[1]?.updatedAtMs || 0) - (b[1]?.updatedAtMs || 0);
    });
    const toRemove = entries.length - MAX_CHAT_STREAM_BUCKETS;
    for (let i = 0; i < toRemove; i++) {
      const [tid, bucket] = entries[i];
      if (bucket && bucket.bubbleEl && bucket.bubbleEl.isConnected) bucket.bubbleEl.remove();
      state.chatStreams.delete(tid);
    }
    updateActiveStreamsBadge();
  }

  function clearTimingsCaches() {
    if (state.chatTimingsByTaskId) state.chatTimingsByTaskId.clear();
    if (state.chatTimingsByMsgId) state.chatTimingsByMsgId.clear();
  }

  function setBoundedMapEntry(map, key, val, limit) {
    if (!map || !key) return;
    if (map.has(key)) {
      map.delete(key);
    } else if (map.size >= limit) {
      const oldest = map.keys().next().value;
      if (oldest !== undefined) map.delete(oldest);
    }
    map.set(key, val);
  }

  function cacheMessageTimings(msgId, taskId, convId, timings) {
    if (!timings || typeof timings !== 'object') return;
    const rec = { timings, convId: convId || '' };
    if (msgId) setBoundedMapEntry(state.chatTimingsByMsgId, String(msgId), rec, MAX_CACHED_TIMINGS);
    if (taskId) setBoundedMapEntry(state.chatTimingsByTaskId, String(taskId), rec, MAX_CACHED_TIMINGS);
  }

  function resolveMessageTimings(msg) {
    if (!msg) return null;
    if (msg.timings && typeof msg.timings === 'object') return msg.timings;
    if (msg.id && state.chatTimingsByMsgId && state.chatTimingsByMsgId.has(String(msg.id))) {
      const hit = state.chatTimingsByMsgId.get(String(msg.id));
      return hit ? (hit.timings || hit) : null;
    }
    if (msg.task_id && state.chatTimingsByTaskId && state.chatTimingsByTaskId.has(String(msg.task_id))) {
      const hit = state.chatTimingsByTaskId.get(String(msg.task_id));
      return hit ? (hit.timings || hit) : null;
    }
    return null;
  }

  function buildTimingsPillEl(timings) {
    if (!timings || typeof timings !== 'object') return null;
    const pred = Number(timings.predicted_per_second);
    const promptRate = Number(timings.prompt_per_second);
    const cacheN = Number(timings.cache_n);
    const hasPred = Number.isFinite(pred) && pred > 0;
    const hasPrompt = Number.isFinite(promptRate) && promptRate > 0;
    const hasCache = Number.isFinite(cacheN) && cacheN > 0;
    if (!hasPred && !hasPrompt && !hasCache) return null;

    const parts = [];
    if (hasPred) {
      parts.push(`${pred.toFixed(1)} tok/s`);
    } else if (hasPrompt) {
      parts.push(`${promptRate.toFixed(1)} pp tok/s`);
    }
    if (hasCache) {
      parts.push(`KV ${Math.round(cacheN)}`);
    }

    const titleParts = [];
    if (hasPred) titleParts.push(`Decode: ${pred.toFixed(1)} tok/s`);
    if (hasPrompt) titleParts.push(`Prompt eval: ${promptRate.toFixed(1)} tok/s`);
    const predN = Number(timings.predicted_n);
    if (Number.isFinite(predN) && predN > 0) titleParts.push(`Decoded: ${Math.round(predN)} tokens`);
    if (hasCache) titleParts.push(`KV cache hit: ${Math.round(cacheN)} tokens`);

    return el('span', {
      className: hasCache ? 'msg-timings-pill is-kv-hit' : 'msg-timings-pill',
      title: titleParts.join(' | ')
    }, parts.join(' \u00b7 '));
  }

  function removeLiveBrowserTab(sid) {
    if (!sid) return;
    if (state.liveBrowserTabs) state.liveBrowserTabs.delete(sid);
    if (state.liveBrowserLastSnapshotFetchMs) state.liveBrowserLastSnapshotFetchMs.delete(sid);
    state.liveBrowserOrder = state.liveBrowserOrder.filter((x) => x !== sid);
    if (state.activeLiveBrowserSessionId === sid) {
      state.activeLiveBrowserSessionId = state.liveBrowserOrder[0] || '';
    }
  }

  function trimLiveBrowserTabs() {
    while (state.liveBrowserOrder.length > MAX_LIVE_BROWSER_TABS) {
      let victimIdx = -1;
      for (let i = state.liveBrowserOrder.length - 1; i >= 0; i--) {
        if (state.liveBrowserOrder[i] !== state.activeLiveBrowserSessionId) {
          victimIdx = i;
          break;
        }
      }
      if (victimIdx < 0) victimIdx = state.liveBrowserOrder.length - 1;
      const [evictedSid] = state.liveBrowserOrder.splice(victimIdx, 1);
      if (evictedSid) {
        if (state.liveBrowserTabs) state.liveBrowserTabs.delete(evictedSid);
        if (state.liveBrowserLastSnapshotFetchMs) state.liveBrowserLastSnapshotFetchMs.delete(evictedSid);
        if (state.activeLiveBrowserSessionId === evictedSid) {
          state.activeLiveBrowserSessionId = state.liveBrowserOrder[0] || '';
        }
      }
    }
  }

  function cleanupConversationClientState(convId) {
    if (!convId) return;
    if (state.chatTimingsByMsgId) {
      for (const [k, rec] of [...state.chatTimingsByMsgId.entries()]) {
        if (rec && rec.convId === convId) state.chatTimingsByMsgId.delete(k);
      }
    }
    if (state.chatTimingsByTaskId) {
      for (const [k, rec] of [...state.chatTimingsByTaskId.entries()]) {
        if (rec && rec.convId === convId) state.chatTimingsByTaskId.delete(k);
      }
    }
    if (state.sandboxAgenticConversationId === convId) {
      state.sandboxAgenticConversationId = '';
    }
    if (state.liveBrowserTabs) {
      for (const [sid, tab] of [...state.liveBrowserTabs.entries()]) {
        if (tab && tab.conversation_id === convId && sid.startsWith('tool-')) {
          removeLiveBrowserTab(sid);
        }
      }
      renderAllLiveBrowserViewports({ refreshPreview: false });
    }
  }

  function setAgenticSearchEnabled(enabled) {
    state.agenticSearchEnabled = Boolean(enabled);
    const btn = getEl('btn-agentic-search-toggle');
    if (btn) {
      btn.setAttribute('aria-pressed', state.agenticSearchEnabled ? 'true' : 'false');
    }
    if (state.agenticSearchEnabled) {
      state.chatLiveBrowserDismissed = false;
    }
  }

  function resetSvgOverlay(svgId) {
    const svg = getEl(svgId);
    if (!svg) return;
    clearEl(svg);
    svg._ps5Init = false;
    svg._ps5Hotspots = null;
    svg._ps5Ripples = null;
    svg._ps5Cursor = null;
    svg._ps5LabelWrap = null;
    svg._ps5LabelBg = null;
    svg._ps5LabelText = null;
  }

  function clearLiveBrowserTimers() {
    if (state.liveBrowserPreviewTimers) {
      for (const dockKey of ['chat', 'sandbox']) {
        if (state.liveBrowserPreviewTimers[dockKey]) {
          window.clearTimeout(state.liveBrowserPreviewTimers[dockKey]);
          state.liveBrowserPreviewTimers[dockKey] = null;
        }
      }
    }
    if (state.liveBrowserCursorTimers) {
      for (const tid of state.liveBrowserCursorTimers) {
        window.clearTimeout(tid);
      }
      state.liveBrowserCursorTimers.clear();
    }
    if (state.liveOverlayResizeTimer) {
      window.clearTimeout(state.liveOverlayResizeTimer);
      state.liveOverlayResizeTimer = null;
    }
  }

  function clearLiveIframeSrc(frameId, dockKey) {
    const frame = getEl(frameId);
    if (frame) frame.setAttribute('src', 'about:blank');
    if (state.liveBrowserLastSrc && dockKey) state.liveBrowserLastSrc[dockKey] = '';
  }

  function resetLiveBrowserState() {
    clearLiveBrowserTimers();
    if (state.liveBrowserTabs) state.liveBrowserTabs.clear();
    state.liveBrowserOrder = [];
    state.activeLiveBrowserSessionId = '';
    state.liveBrowserFeed = [];
    state.liveBrowserRev = 0;
    state.chatLiveBrowserDismissed = false;
    if (state.liveBrowserLastSnapshotFetchMs) state.liveBrowserLastSnapshotFetchMs.clear();
    if (state.sandboxAgenticTaskIds) state.sandboxAgenticTaskIds.clear();
    state.sandboxAgenticConversationId = '';

    clearLiveIframeSrc('chat-live-browser-frame', 'chat');
    clearLiveIframeSrc('sandbox-live-browser-frame', 'sandbox');
    resetSvgOverlay('chat-live-browser-overlay');
    resetSvgOverlay('sandbox-live-browser-overlay');

    for (const id of ['chat-live-browser-tabs', 'sandbox-live-browser-tabs', 'sandbox-live-browser-feed']) {
      const node = getEl(id);
      if (node) clearEl(node);
    }
    setText('chat-live-browser-url', 'about:blank');
    setText('sandbox-live-browser-url', 'about:blank');
    const chatSt = getEl('chat-live-browser-status');
    if (chatSt) {
      chatSt.className = 'pill pill-info';
      chatSt.textContent = 'Idle';
    }
    const sbSt = getEl('sandbox-live-browser-status');
    if (sbSt) {
      sbSt.className = 'pill pill-muted';
      sbSt.textContent = 'Idle';
    }
    const chatDock = getEl('chat-live-browser-dock');
    if (chatDock) chatDock.hidden = true;
    setAgenticSearchEnabled(false);
  }

  function openChatLiveBrowserDock(opts) {
    if (state.chatLiveBrowserDismissed && !(opts && opts.force)) return;
    const dock = getEl('chat-live-browser-dock');
    if (dock) dock.hidden = false;
    if (!state.activeLiveBrowserSessionId && state.liveBrowserOrder.length > 0) {
      state.activeLiveBrowserSessionId = state.liveBrowserOrder[0];
    }
    renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: true });
    if (opts && opts.statusText) {
      const pill = getEl('chat-live-browser-status');
      if (pill) {
        pill.className = `pill ${opts.pillClass || 'pill-info'}`;
        pill.textContent = opts.statusText;
      }
    }
    syncChatLiveBrowserToggleBtn(true);
  }

  function hideChatLiveBrowserDock() {
    state.chatLiveBrowserDismissed = true;
    const dock = getEl('chat-live-browser-dock');
    if (dock) dock.hidden = true;
    syncChatLiveBrowserToggleBtn(false);
  }

  function toggleChatLiveBrowserDock(forceState) {
    const dock = getEl('chat-live-browser-dock');
    if (!dock) return;
    const shouldOpen = (typeof forceState === 'boolean') ? forceState : dock.hidden;
    if (shouldOpen) {
      state.chatLiveBrowserDismissed = false;
      openChatLiveBrowserDock({ force: true });
    } else {
      hideChatLiveBrowserDock();
    }
  }

  function syncChatLiveBrowserToggleBtn(isOpen) {
    const btn = getEl('btn-chat-live-browser-toggle');
    if (!btn) return;
    const dock = getEl('chat-live-browser-dock');
    const open = (typeof isOpen === 'boolean') ? isOpen : Boolean(dock && !dock.hidden);
    btn.setAttribute('aria-pressed', open ? 'true' : 'false');
    if (open) {
      btn.classList.add('is-active');
      btn.title = 'Hide Live Agent Browser Dock';
    } else {
      btn.classList.remove('is-active');
      btn.title = 'Open Live Agent Browser Dock';
    }
  }

  function syncChatLiveBrowserDockForConversation(convId) {
    const dock = getEl('chat-live-browser-dock');
    if (!dock) return;
    const tabs = state.liveBrowserTabs ? [...state.liveBrowserTabs.values()] : [];
    const hasMatch = Boolean(convId && tabs.some((t) => t && t.conversation_id === convId));
    if (hasMatch && !state.chatLiveBrowserDismissed) {
      dock.hidden = false;
      const matchingTab = tabs.find((t) => t && t.conversation_id === convId);
      if (matchingTab && (!state.activeLiveBrowserSessionId || !state.liveBrowserTabs.has(state.activeLiveBrowserSessionId) || (state.liveBrowserTabs.get(state.activeLiveBrowserSessionId) && state.liveBrowserTabs.get(state.activeLiveBrowserSessionId).conversation_id !== convId))) {
        state.activeLiveBrowserSessionId = matchingTab.session_id;
      }
      renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: true });
      syncChatLiveBrowserToggleBtn(true);
    } else if (!state.agenticSearchEnabled) {
      dock.hidden = true;
      syncChatLiveBrowserToggleBtn(false);
    } else {
      syncChatLiveBrowserToggleBtn(!dock.hidden);
    }
  }

  function restoreChatLiveStatusFromSearching(convId, taskId) {
    const isSandboxTask = Boolean(taskId && state.sandboxAgenticTaskIds && state.sandboxAgenticTaskIds.has(taskId));
    if (isSandboxTask) return;
    if (convId && convId !== state.activeConversationId) return;
    const chatSt = getEl('chat-live-browser-status');
    if (!chatSt || chatSt.textContent !== 'Searching...') return;
    const activeTab = state.activeLiveBrowserSessionId ? state.liveBrowserTabs.get(state.activeLiveBrowserSessionId) : null;
    const meta = activeTab ? formatBrowserPillMeta(activeTab.action, activeTab.status) : { text: 'Idle', cls: 'pill-info' };
    chatSt.className = `pill ${meta.cls}`;
    chatSt.textContent = meta.text;
  }

  function formatBrowserPillMeta(action, status) {
    const st = String(status || '').trim().toLowerCase();
    const act = String(action || '').trim().toLowerCase();
    const labelRaw = String(status || action || 'Idle');
    const label = labelRaw.charAt(0).toUpperCase() + labelRaw.slice(1);
    if (st === 'error' || st === 'failed') return { text: label, cls: 'pill-danger' };
    if (st === 'navigating' || st === 'running' || act === 'search' || act === 'web_search') {
      return { text: label, cls: 'pill-info' };
    }
    if (st === 'closed' || st === 'idle') return { text: label, cls: 'pill-muted' };
    return { text: label, cls: 'pill-success' };
  }

  function ensureLiveSvgOverlay(svgId) {
    const svg = getEl(svgId);
    if (!svg) return null;
    if (svg._ps5Init && svg._ps5Cursor && svg._ps5Cursor.isConnected) {
      return svg;
    }
    clearEl(svg);
    const hotspotsG = svgEl('g', { className: 'vmouse-hotspots' });
    const ripplesG = svgEl('g', { className: 'vmouse-ripples' });
    const pointerPath = svgEl('path', {
      className: 'vmouse-pointer',
      d: 'M 0 0 L 0 17 L 4.8 12.5 L 8.6 19.8 L 11.4 18.3 L 7.6 11 L 13.8 10.6 Z',
      fill: '#22d3ee',
      stroke: '#0b0f17',
      'stroke-width': '1.2',
      'stroke-linejoin': 'round'
    });
    const labelBg = svgEl('rect', {
      className: 'vmouse-label-bg',
      x: '0',
      y: '0',
      width: '64',
      height: '20',
      rx: '5'
    });
    const labelText = svgEl('text', {
      className: 'vmouse-label',
      x: '7',
      y: '13.8'
    }, 'Browser');
    const labelWrap = svgEl('g', {
      className: 'vmouse-label-wrap',
      transform: 'translate(14 14)'
    }, labelBg, labelText);
    const cursorG = svgEl('g', {
      className: 'vmouse-cursor',
      transform: 'translate(320 50)'
    }, pointerPath, labelWrap);

    svg.appendChild(hotspotsG);
    svg.appendChild(ripplesG);
    svg.appendChild(cursorG);

    svg._ps5Init = true;
    svg._ps5Hotspots = hotspotsG;
    svg._ps5Ripples = ripplesG;
    svg._ps5Cursor = cursorG;
    svg._ps5LabelWrap = labelWrap;
    svg._ps5LabelBg = labelBg;
    svg._ps5LabelText = labelText;
    return svg;
  }

  function renderLiveSvgOverlay(svgId, tab, opts) {
    const options = opts || {};
    if (!tab) {
      resetSvgOverlay(svgId);
      return;
    }
    const svg = ensureLiveSvgOverlay(svgId);
    if (!svg) return;

    const rect = svg.getBoundingClientRect();
    const stageW = rect.width >= 40 ? Math.round(rect.width) : 640;
    const stageH = rect.height >= 40 ? Math.round(rect.height) : 280;
    svg.setAttribute('viewBox', `0 0 ${stageW} ${stageH}`);
    svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');

    const x = clampPct(tab.x_pct) ?? 50;
    const y = clampPct(tab.y_pct) ?? 18;
    const px = (x / 100) * stageW;
    const py = (y / 100) * stageH;
    svg._ps5Cursor.setAttribute('transform', `translate(${px.toFixed(1)} ${py.toFixed(1)})`);
    svg._ps5Cursor.setAttribute('data-x-pct', x.toFixed(2));
    svg._ps5Cursor.setAttribute('data-y-pct', y.toFixed(2));

    const rawAgent = tab.agent_id ? agentDisplayName(tab.agent_id) : 'Browser';
    const badgeRaw = tab.status ? `${rawAgent} · ${tab.status}` : rawAgent;
    const badgeText = badgeRaw.length > 26 ? `${badgeRaw.slice(0, 25)}\u2026` : badgeRaw;
    const labelW = Math.max(54, Math.min(stageW - 24, Math.round(badgeText.length * 6.8 + 16)));
    const labelX = (px + 16 + labelW > stageW - 6) ? Math.max(-px + 4, -(labelW + 6)) : 14;
    const labelY = (py + 14 + 22 > stageH - 4) ? -22 : 14;
    svg._ps5LabelText.textContent = badgeText;
    svg._ps5LabelBg.setAttribute('width', String(labelW));
    svg._ps5LabelWrap.setAttribute('transform', `translate(${labelX.toFixed(1)} ${labelY.toFixed(1)})`);

    // Hotspots for interactive [ref=eN] elements
    const hotspotsG = svg._ps5Hotspots;
    clearEl(hotspotsG);
    const activeSelector = tab.selector ? String(tab.selector) : '';
    let renderedActive = false;
    if (Array.isArray(tab.elements) && tab.elements.length > 0) {
      const prioritized = activeSelector
        ? [
            ...tab.elements.filter((e) => e && e.ref === activeSelector),
            ...tab.elements.filter((e) => e && e.ref !== activeSelector)
          ]
        : tab.elements;
      const limit = Math.min(MAX_SVG_HOTSPOTS, prioritized.length);
      for (let i = 0; i < limit; i++) {
        const elItem = prioritized[i];
        if (!elItem) continue;
        const ex = clampPct(elItem.x_pct);
        const ey = clampPct(elItem.y_pct);
        const ew = clampPct(elItem.w_pct);
        const eh = clampPct(elItem.h_pct);
        if (ex === null || ey === null || ew === null || eh === null) continue;
        const isMatch = Boolean(activeSelector && elItem.ref === activeSelector);
        if (isMatch) renderedActive = true;
        const hx = (ex / 100) * stageW;
        const hy = (ey / 100) * stageH;
        const hw = Math.max(8, (ew / 100) * stageW);
        const hh = Math.max(8, (eh / 100) * stageH);
        hotspotsG.appendChild(svgEl('rect', {
          className: isMatch ? 'vmouse-hotspot is-active' : 'vmouse-hotspot',
          x: hx.toFixed(1),
          y: hy.toFixed(1),
          width: hw.toFixed(1),
          height: hh.toFixed(1),
          rx: '4',
          'data-ref': elItem.ref || ''
        }));
      }
    }
    if (!renderedActive && activeSelector && typeof tab.w_pct === 'number' && typeof tab.h_pct === 'number' && tab.w_pct > 0 && tab.h_pct > 0) {
      const fxPct = Math.max(0, x - tab.w_pct / 2);
      const fyPct = Math.max(0, y - tab.h_pct / 2);
      const hx = (fxPct / 100) * stageW;
      const hy = (fyPct / 100) * stageH;
      const hw = Math.max(8, (tab.w_pct / 100) * stageW);
      const hh = Math.max(8, (tab.h_pct / 100) * stageH);
      hotspotsG.appendChild(svgEl('rect', {
        className: 'vmouse-hotspot is-active',
        x: hx.toFixed(1),
        y: hy.toFixed(1),
        width: hw.toFixed(1),
        height: hh.toFixed(1),
        rx: '4',
        'data-ref': activeSelector
      }));
    }

    if (options.ripple) {
      svg._ps5Cursor.classList.add('is-clicking');
      const clickTimerId = window.setTimeout(() => {
        state.liveBrowserCursorTimers.delete(clickTimerId);
        if (svg._ps5Cursor && svg._ps5Cursor.isConnected) {
          svg._ps5Cursor.classList.remove('is-clicking');
        }
      }, 280);
      state.liveBrowserCursorTimers.add(clickTimerId);

      const ripplesG = svg._ps5Ripples;
      while (ripplesG.childNodes.length >= MAX_SVG_RIPPLES) {
        ripplesG.firstChild.remove();
      }
      const circle = svgEl('circle', {
        className: 'vmouse-ripple',
        cx: px.toFixed(1),
        cy: py.toFixed(1),
        r: '14'
      });
      const cleanupRipple = () => {
        if (circle.isConnected) circle.remove();
      };
      circle.addEventListener('animationend', cleanupRipple, { once: true });
      const rippleTimerId = window.setTimeout(() => {
        state.liveBrowserCursorTimers.delete(rippleTimerId);
        cleanupRipple();
      }, 780);
      state.liveBrowserCursorTimers.add(rippleTimerId);
      ripplesG.appendChild(circle);
    }
  }

  function scheduleLiveBrowserPreview(dockKey, frameId, sessionId, forceImmediate) {
    const frame = getEl(frameId);
    if (!frame) return;
    if (!sessionId || String(sessionId).startsWith('tool-')) {
      if (state.liveBrowserLastSrc[dockKey] !== 'about:blank') {
        clearLiveIframeSrc(frameId, dockKey);
        state.liveBrowserLastSrc[dockKey] = 'about:blank';
      }
      return;
    }
    const tab = state.liveBrowserTabs.get(sessionId);
    const applySrc = () => {
      if (state.liveBrowserPreviewTimers[dockKey]) {
        state.liveBrowserPreviewTimers[dockKey] = null;
      }
      const latestTab = state.liveBrowserTabs.get(sessionId);
      if (!latestTab && sessionId !== state.activeLiveBrowserSessionId) return;
      state.liveBrowserRev += 1;
      const previewUrl = `/api/v1/sandbox/browser/preview?session_id=${encodeURIComponent(sessionId)}&_t=${state.liveBrowserRev}`;
      if (frame.isConnected) {
        frame.setAttribute('src', previewUrl);
        state.liveBrowserLastSrc[dockKey] = previewUrl;
        if (latestTab) latestTab.hasLoadedPreview = true;
      }
    };

    if (state.liveBrowserPreviewTimers[dockKey]) {
      window.clearTimeout(state.liveBrowserPreviewTimers[dockKey]);
      state.liveBrowserPreviewTimers[dockKey] = null;
    }
    if (forceImmediate || (tab && !tab.hasLoadedPreview)) {
      applySrc();
    } else {
      state.liveBrowserPreviewTimers[dockKey] = window.setTimeout(applySrc, 150);
    }
  }

  function renderLiveTabsStrip(containerId, activeSessionId) {
    const strip = getEl(containerId);
    if (!strip) return;
    clearEl(strip);
    const orderedIds = state.liveBrowserOrder.filter((id) => state.liveBrowserTabs.has(id));
    if (orderedIds.length === 0) {
      strip.appendChild(el('span', { className: 'text-xs text-muted' }, 'No active browser tabs'));
      return;
    }
    for (const sid of orderedIds) {
      const tab = state.liveBrowserTabs.get(sid);
      if (!tab) continue;
      const isActive = sid === activeSessionId;
      let hostLabel = '';
      if (tab.title) {
        hostLabel = tab.title;
      } else if (tab.url) {
        hostLabel = tab.url.replace(/^[a-z]+:\/\//i, '').slice(0, 28);
      } else {
        hostLabel = sid;
      }
      const prefix = tab.agent_id ? `${agentDisplayName(tab.agent_id)}: ` : '';
      const tabLabel = `${prefix}${hostLabel}`;
      strip.appendChild(el('button', {
        type: 'button',
        className: isActive ? 'live-tab-btn active' : 'live-tab-btn',
        role: 'tab',
        'aria-selected': isActive ? 'true' : 'false',
        title: `${tab.title ? `${tab.title} — ` : ''}${tab.url || sid}`,
        onClick: () => selectLiveBrowserTab(sid)
      }, tabLabel));
    }
  }

  function renderLiveBrowserFeed() {
    const feedEl = getEl('sandbox-live-browser-feed');
    if (!feedEl) return;
    clearEl(feedEl);
    if (!state.liveBrowserFeed || state.liveBrowserFeed.length === 0) {
      feedEl.appendChild(el('div', { className: 'text-xs text-muted' }, 'Live agent browser telemetry will stream here.'));
      return;
    }
    for (const item of state.liveBrowserFeed) {
      const who = item.agent_id ? agentDisplayName(item.agent_id) : 'Browser';
      const coords = (typeof item.x_pct === 'number' && typeof item.y_pct === 'number')
        ? ` (${Math.round(item.x_pct)}%, ${Math.round(item.y_pct)}%)`
        : '';
      const selPart = item.selector ? ` [${item.selector}]` : '';
      const detail = `${item.action || item.status || 'event'}${selPart}${coords}${item.url ? ` · ${item.url}` : ''}`;
      const pill = formatBrowserPillMeta(item.action, item.status);
      feedEl.appendChild(el('div', { className: 'live-browser-feed-item' },
        el('div', { className: 'min-w-0 truncate' },
          el('strong', null, who),
          ` — ${detail}`
        ),
        el('div', { className: 'toolbar-actions' },
          el('span', { className: `pill ${pill.cls}` }, pill.text),
          item.timestamp ? el('span', { className: 'text-xs text-muted mono' }, fmtTime(item.timestamp)) : null
        )
      ));
    }
  }

  function renderAllLiveBrowserViewports(opts) {
    const options = opts || {};
    const activeId = state.activeLiveBrowserSessionId;
    const activeTab = activeId ? (state.liveBrowserTabs.get(activeId) || null) : null;

    renderLiveTabsStrip('chat-live-browser-tabs', activeId);
    renderLiveTabsStrip('sandbox-live-browser-tabs', activeId);
    renderLiveBrowserFeed();

    const meta = activeTab
      ? formatBrowserPillMeta(activeTab.action, activeTab.status)
      : { text: 'Idle', cls: 'pill-muted' };
    const displayUrl = activeTab && activeTab.url ? activeTab.url : 'about:blank';

    const chatUrl = getEl('chat-live-browser-url');
    if (chatUrl) chatUrl.textContent = displayUrl;
    const chatSt = getEl('chat-live-browser-status');
    if (chatSt) {
      chatSt.className = `pill ${activeTab ? meta.cls : 'pill-info'}`;
      chatSt.textContent = activeTab ? meta.text : 'Idle';
    }
    const sbUrl = getEl('sandbox-live-browser-url');
    if (sbUrl) sbUrl.textContent = displayUrl;
    const sbSt = getEl('sandbox-live-browser-status');
    if (sbSt) {
      sbSt.className = `pill ${meta.cls}`;
      sbSt.textContent = meta.text;
    }

    renderLiveSvgOverlay('chat-live-browser-overlay', activeTab, { ripple: Boolean(options.ripple) });
    renderLiveSvgOverlay('sandbox-live-browser-overlay', activeTab, { ripple: Boolean(options.ripple) });

    if (options.refreshPreview) {
      if (!activeTab) {
        clearLiveIframeSrc('chat-live-browser-frame', 'chat');
        clearLiveIframeSrc('sandbox-live-browser-frame', 'sandbox');
      } else {
        const chatDock = getEl('chat-live-browser-dock');
        if (chatDock && !chatDock.hidden) {
          scheduleLiveBrowserPreview('chat', 'chat-live-browser-frame', activeTab.session_id, Boolean(options.immediatePreview));
        }
        scheduleLiveBrowserPreview('sandbox', 'sandbox-live-browser-frame', activeTab.session_id, Boolean(options.immediatePreview));
      }
    }
  }

  function selectLiveBrowserTab(sessionId) {
    if (!sessionId || !state.liveBrowserTabs.has(sessionId)) return;
    state.activeLiveBrowserSessionId = sessionId;
    if (state.browserSessions.some((s) => s.session_id === sessionId)) {
      state.activeBrowserSessionId = sessionId;
      const sel = getEl('browser-session-select');
      if (sel) sel.value = sessionId;
    }
    const tab = state.liveBrowserTabs.get(sessionId);
    if (tab && (!tab.elements || tab.elements.length === 0)) {
      maybeRefreshLiveBrowserElements(sessionId, true);
    }
    renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: true });
  }

  async function maybeRefreshLiveBrowserElements(sessionId, force) {
    if (!sessionId || String(sessionId).startsWith('tool-')) return;
    if (!state.liveBrowserTabs.has(sessionId)) return;
    const now = Date.now();
    const lastFetch = state.liveBrowserLastSnapshotFetchMs.get(sessionId) || 0;
    if (!force && (now - lastFetch) < 1200) return;
    state.liveBrowserLastSnapshotFetchMs.set(sessionId, now);
    try {
      const snap = await apiFetch('/api/v1/sandbox/browser/action', {
        method: 'POST',
        body: { action: 'snapshot', session_id: sessionId }
      });
      const tab = state.liveBrowserTabs.get(sessionId);
      if (tab && snap && Array.isArray(snap.elements)) {
        tab.elements = snap.elements;
        if (snap.cursor && typeof snap.cursor.x_pct === 'number') {
          tab.x_pct = clampPct(snap.cursor.x_pct);
          tab.y_pct = clampPct(snap.cursor.y_pct);
        }
        if (state.activeLiveBrowserSessionId === sessionId) {
          renderLiveSvgOverlay('chat-live-browser-overlay', tab, { ripple: false });
          renderLiveSvgOverlay('sandbox-live-browser-overlay', tab, { ripple: false });
        }
        if (state.currentSection === 'sandbox' && state.activeBrowserSessionId === sessionId) {
          renderBrowserSnapshot(snap, 'snapshot');
        }
      }
    } catch (_) {
      // Session may have closed concurrently; ignore gracefully.
    }
  }

  function syncLiveTabsFromSessionsList(sessions) {
    const serverIds = new Set();
    for (const s of (sessions || [])) {
      if (!s || !s.session_id) continue;
      const sid = String(s.session_id);
      serverIds.add(sid);
      const existing = state.liveBrowserTabs.get(sid) || {
        session_id: sid,
        url: '',
        title: '',
        agent_id: '',
        action: '',
        status: 'opened',
        x_pct: 50,
        y_pct: 18,
        w_pct: null,
        h_pct: null,
        scroll_y: 0,
        selector: '',
        elements: [],
        conversation_id: '',
        task_id: '',
        hasLoadedPreview: false,
        updatedAtMs: Date.now()
      };
      if (s.url) existing.url = s.url;
      if (s.title) existing.title = s.title;
      if (s.cursor && typeof s.cursor === 'object') {
        const cx = clampPct(s.cursor.x_pct);
        const cy = clampPct(s.cursor.y_pct);
        if (cx !== null) existing.x_pct = cx;
        if (cy !== null) existing.y_pct = cy;
        if (typeof s.cursor.scroll_y === 'number') existing.scroll_y = s.cursor.scroll_y;
        if (s.cursor.last_action) existing.action = s.cursor.last_action;
        if (s.cursor.last_selector) existing.selector = s.cursor.last_selector;
        if (s.cursor.agent_id) existing.agent_id = s.cursor.agent_id;
      }
      state.liveBrowserTabs.set(sid, existing);
      if (!state.liveBrowserOrder.includes(sid)) {
        state.liveBrowserOrder.push(sid);
      }
    }
    for (const sid of [...state.liveBrowserTabs.keys()]) {
      if (!String(sid).startsWith('tool-') && !serverIds.has(sid)) {
        removeLiveBrowserTab(sid);
      }
    }
    if (!state.activeLiveBrowserSessionId || !state.liveBrowserTabs.has(state.activeLiveBrowserSessionId)) {
      state.activeLiveBrowserSessionId = state.liveBrowserOrder[0] || '';
    }
    trimLiveBrowserTabs();
    renderAllLiveBrowserViewports({
      refreshPreview: Boolean(state.activeLiveBrowserSessionId),
      immediatePreview: false
    });
  }

  function handleBrowserActivityEvent(ev) {
    if (!ev || !ev.session_id) return;
    const sid = String(ev.session_id).trim();
    if (!sid) return;

    const act = String(ev.action || '').trim().toLowerCase();
    const st = String(ev.status || '').trim().toLowerCase();
    const nowMs = Date.now();

    state.liveBrowserFeed.unshift({
      id: `${nowMs}-${Math.random().toString(36).slice(2, 6)}`,
      timestamp: ev.timestamp || new Date(nowMs).toISOString(),
      session_id: sid,
      agent_id: ev.agent_id || '',
      action: ev.action || st || 'activity',
      status: ev.status || ev.action || '',
      url: ev.url || '',
      title: ev.title || '',
      selector: ev.selector || '',
      x_pct: clampPct(ev.x_pct),
      y_pct: clampPct(ev.y_pct)
    });
    if (state.liveBrowserFeed.length > MAX_LIVE_BROWSER_FEED) {
      state.liveBrowserFeed.length = MAX_LIVE_BROWSER_FEED;
    }

    if (act === 'close' || act === 'closed' || st === 'closed') {
      removeLiveBrowserTab(sid);
      renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: true });
      return;
    }

    let tab = state.liveBrowserTabs.get(sid);
    const isInitialTab = !tab;
    if (!tab) {
      tab = {
        session_id: sid,
        url: '',
        title: '',
        agent_id: '',
        action: '',
        status: '',
        x_pct: 50,
        y_pct: 18,
        w_pct: null,
        h_pct: null,
        scroll_y: 0,
        selector: '',
        elements: [],
        conversation_id: '',
        task_id: '',
        hasLoadedPreview: false,
        updatedAtMs: nowMs
      };
      state.liveBrowserTabs.set(sid, tab);
    }

    if (ev.url) tab.url = String(ev.url);
    if (ev.title) tab.title = String(ev.title);
    if (ev.agent_id) tab.agent_id = String(ev.agent_id);
    if (ev.action) tab.action = String(ev.action);
    if (ev.status) tab.status = String(ev.status);
    if (ev.selector !== undefined && ev.selector !== null) tab.selector = String(ev.selector);
    if (ev.conversation_id) tab.conversation_id = String(ev.conversation_id);
    if (ev.task_id) tab.task_id = String(ev.task_id);
    const nx = clampPct(ev.x_pct);
    const ny = clampPct(ev.y_pct);
    if (nx !== null) tab.x_pct = nx;
    if (ny !== null) tab.y_pct = ny;
    if (typeof ev.w_pct === 'number' && Number.isFinite(ev.w_pct)) tab.w_pct = ev.w_pct;
    if (typeof ev.h_pct === 'number' && Number.isFinite(ev.h_pct)) tab.h_pct = ev.h_pct;
    if (typeof ev.scroll_y === 'number' && Number.isFinite(ev.scroll_y)) tab.scroll_y = ev.scroll_y;
    tab.updatedAtMs = nowMs;

    state.liveBrowserOrder = [sid, ...state.liveBrowserOrder.filter((x) => x !== sid)];
    trimLiveBrowserTabs();

    // Auto-open Main Chat Live Browser Viewport when browser activity belongs to active conversation/task
    const trackedTask = tab.task_id ? state.activeChatTasks.get(tab.task_id) : null;
    const trackedStream = tab.task_id ? state.chatStreams.get(tab.task_id) : null;
    const effectiveConvId = tab.conversation_id || (trackedTask && trackedTask.conversation_id) || (trackedStream && trackedStream.conversationId) || '';
    const isActiveConv = Boolean(effectiveConvId && state.activeConversationId && effectiveConvId === state.activeConversationId);
    const isSandboxTask = Boolean(tab.task_id && state.sandboxAgenticTaskIds && state.sandboxAgenticTaskIds.has(tab.task_id));

    if (isActiveConv && !isSandboxTask) {
      openChatLiveBrowserDock();
    }

    if (
      !state.activeLiveBrowserSessionId
      || !state.liveBrowserTabs.has(state.activeLiveBrowserSessionId)
      || act === 'open'
      || act === 'opened'
      || st === 'opened'
      || isInitialTab
    ) {
      state.activeLiveBrowserSessionId = sid;
    }

    const previewTriggers = new Set(['open', 'opened', 'navigate', 'navigated', 'click', 'clicked', 'fill', 'filled', 'submit', 'submitted', 'back', 'snapshot']);
    const shouldRefreshPreview = !sid.startsWith('tool-')
      && st !== 'navigating'
      && st !== 'error'
      && (!tab.hasLoadedPreview || previewTriggers.has(act) || previewTriggers.has(st));

    const isRipple = act === 'click' || act === 'clicked' || st === 'clicked' || act === 'submit' || act === 'submitted' || st === 'submitted';
    const isActiveShown = sid === state.activeLiveBrowserSessionId;

    if (
      isActiveShown
      && !sid.startsWith('tool-')
      && st !== 'navigating'
      && (!tab.elements || tab.elements.length === 0 || act === 'open' || act === 'opened' || st === 'opened' || act === 'click' || act === 'clicked' || act === 'submit' || act === 'submitted' || act === 'back')
    ) {
      maybeRefreshLiveBrowserElements(sid, isInitialTab);
    }

    renderAllLiveBrowserViewports({
      refreshPreview: isActiveShown && shouldRefreshPreview,
      immediatePreview: false,
      ripple: isActiveShown && isRipple
    });
  }

  function populateOptionalAgentSelect(selectId, defaultLabel) {
    const sel = getEl(selectId);
    if (!sel) return;
    const prev = sel.value;
    clearEl(sel);
    sel.appendChild(el('option', { value: '' }, defaultLabel || 'Auto (Active Agent)'));
    const enabledAgents = state.agents.filter((a) => a.enabled);
    for (const ag of enabledAgents) {
      sel.appendChild(el('option', { value: ag.id }, `${ag.name} (${ag.role})`));
    }
    if (prev && [...sel.options].some((o) => o.value === prev)) {
      sel.value = prev;
    }
  }

  async function ensureSandboxAgenticConversationId(preferredAgentId) {
    if (
      state.sandboxAgenticConversationId
      && state.conversations.some((c) => c && c.id === state.sandboxAgenticConversationId)
    ) {
      return state.sandboxAgenticConversationId;
    }
    const existing = state.conversations.find((c) => c && c.title === 'Sandbox Browser Agentic Search');
    if (existing && existing.id) {
      state.sandboxAgenticConversationId = existing.id;
      return existing.id;
    }
    const fallbackAgent = preferredAgentId
      || (state.agents.find((a) => a.id === 'commander' && a.enabled) || {}).id
      || (state.agents.find((a) => a.enabled) || {}).id
      || 'commander';
    const created = await apiFetch('/api/v1/conversations', {
      method: 'POST',
      body: { title: 'Sandbox Browser Agentic Search', mode: { kind: 'direct', agent_id: fallbackAgent } }
    });
    await loadConversations();
    if (created && created.id) {
      state.sandboxAgenticConversationId = created.id;
      return created.id;
    }
    return null;
  }

  async function runSandboxAgenticSearch() {
    const inputEl = getEl('sandbox-browser-agentic-input');
    const btnEl = getEl('btn-sandbox-agentic-run');
    const agentSel = getEl('sandbox-browser-agent-select');
    const taskPrompt = inputEl ? String(inputEl.value || '').trim() : '';
    if (!taskPrompt) {
      showToast('Enter a task for Sandbox Agentic Search.', 'warning');
      if (inputEl) inputEl.focus();
      return;
    }
    const chosenAgent = agentSel ? String(agentSel.value || '').trim() : '';
    if (btnEl) {
      btnEl.disabled = true;
      btnEl.textContent = 'Running...';
    }
    const sbDock = getEl('sandbox-live-browser-dock');
    if (sbDock) {
      sbDock.hidden = false;
      if (typeof sbDock.scrollIntoView === 'function') {
        sbDock.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    }
    const sbSt = getEl('sandbox-live-browser-status');
    if (sbSt) {
      sbSt.className = 'pill pill-info';
      sbSt.textContent = 'Starting Search...';
    }
    try {
      const convId = await ensureSandboxAgenticConversationId(chosenAgent);
      if (!convId) {
        throw new Error('Could not create Sandbox Agentic Search conversation.');
      }
      const thinkingEl = getEl('thinking-level-select');
      const thinkingLevel = thinkingEl ? thinkingEl.value : 'medium';
      const promptWithHint = `[Agentic Search Mode — use the stateful browser tool (open/snapshot/click/fill) when inspecting web pages so live cursor telemetry and previews appear in the viewport]\n${taskPrompt}`;
      const res = await apiFetch(`/api/v1/conversations/${encodeURIComponent(convId)}/messages`, {
        method: 'POST',
        body: {
          content: promptWithHint,
          agent_id: chosenAgent || null,
          thinking_level: thinkingLevel,
          attachments: [],
          async_dispatch: true,
          immediate: true
        }
      });
      const dispatchedTaskId = (res && res.task && res.task.id)
        ? String(res.task.id)
        : ((res && res.task_id) ? String(res.task_id) : '');
      if (dispatchedTaskId) {
        if (state.sandboxAgenticTaskIds.size >= 32) {
          const oldest = state.sandboxAgenticTaskIds.values().next().value;
          if (oldest !== undefined) state.sandboxAgenticTaskIds.delete(oldest);
        }
        state.sandboxAgenticTaskIds.add(dispatchedTaskId);
      }
      state.liveBrowserFeed.unshift({
        id: `sb-${Date.now()}`,
        timestamp: new Date().toISOString(),
        session_id: state.activeLiveBrowserSessionId || 'sandbox',
        agent_id: chosenAgent || 'commander',
        action: 'agentic_search',
        status: 'running',
        url: taskPrompt.slice(0, 80),
        title: '',
        selector: '',
        x_pct: null,
        y_pct: null
      });
      if (state.liveBrowserFeed.length > MAX_LIVE_BROWSER_FEED) {
        state.liveBrowserFeed.length = MAX_LIVE_BROWSER_FEED;
      }
      renderLiveBrowserFeed();
      if (inputEl) inputEl.value = '';
      showToast('Sandbox Agentic Search dispatched.', 'success');
    } catch (err) {
      if (sbSt) {
        sbSt.className = 'pill pill-danger';
        sbSt.textContent = 'Error';
      }
      showToast(`Sandbox Agentic Search failed: ${err.message}`, 'danger');
    } finally {
      if (btnEl) {
        btnEl.disabled = false;
        btnEl.textContent = 'Run Agentic Search';
      }
    }
  }

  function applyImportedKvFields(res) {
    const imported = (res && typeof res.imported === 'object' && res.imported) ? res.imported : {};
    const applied = (res && typeof res.applied === 'object' && res.applied) ? res.applied : {};
    const merged = Object.assign({}, imported, applied);

    const setSelectIfValid = (id, raw) => {
      if (raw === undefined || raw === null) return '';
      const norm = String(raw).trim().toLowerCase();
      const sel = getEl(id);
      if (!sel) return norm;
      const valid = [...sel.options].some((o) => o.value === norm);
      sel.value = valid ? norm : '';
      return sel.value;
    };

    const kVal = setSelectIfValid('set-inf-cache-type-k', merged.cache_type_k);
    const vVal = setSelectIfValid('set-inf-cache-type-v', merged.cache_type_v);
    const faEl = getEl('set-inf-flash-attn');
    if (faEl && merged.flash_attn !== undefined && merged.flash_attn !== null) {
      faEl.checked = Boolean(merged.flash_attn);
    }
    const smartEl = getEl('set-inf-smart-tool-routing');
    if (smartEl && merged.smart_tool_routing !== undefined && merged.smart_tool_routing !== null) {
      smartEl.checked = Boolean(merged.smart_tool_routing);
    }

    const nCtx = merged.n_ctx ?? merged.ctx_size ?? merged.context_limit;
    if (nCtx !== undefined && nCtx !== null) setInputValue('cfg-context-limit', nCtx);
    if (merged.max_tokens !== undefined && merged.max_tokens !== null) setInputValue('cfg-max-output-tokens', merged.max_tokens);
    if (merged.n_keep !== undefined && merged.n_keep !== null) setInputValue('cfg-n-keep', merged.n_keep);
    if (merged.temperature !== undefined && merged.temperature !== null) setInputValue('cfg-temperature', merged.temperature);
    if (merged.top_k !== undefined && merged.top_k !== null) setInputValue('cfg-top-k', merged.top_k);
    if (merged.top_p !== undefined && merged.top_p !== null) setInputValue('cfg-top-p', merged.top_p);
    if (merged.min_p !== undefined && merged.min_p !== null) setInputValue('cfg-min-p', merged.min_p);
    if (merged.repeat_penalty !== undefined && merged.repeat_penalty !== null) setInputValue('cfg-repeat-penalty', merged.repeat_penalty);
    if (merged.repeat_last_n !== undefined && merged.repeat_last_n !== null) setInputValue('cfg-repeat-last-n', merged.repeat_last_n);
    if (merged.presence_penalty !== undefined && merged.presence_penalty !== null) setInputValue('cfg-presence-penalty', merged.presence_penalty);
    if (merged.frequency_penalty !== undefined && merged.frequency_penalty !== null) setInputValue('cfg-frequency-penalty', merged.frequency_penalty);
    if (merged.dry_multiplier !== undefined && merged.dry_multiplier !== null) setInputValue('cfg-dry-multiplier', merged.dry_multiplier);
    if (merged.dry_base !== undefined && merged.dry_base !== null) setInputValue('cfg-dry-base', merged.dry_base);
    if (merged.dry_allowed_length !== undefined && merged.dry_allowed_length !== null) setInputValue('cfg-dry-allowed-length', merged.dry_allowed_length);
    if (merged.dry_penalty_last_n !== undefined && merged.dry_penalty_last_n !== null) setInputValue('cfg-dry-penalty-last-n', merged.dry_penalty_last_n);
    if (merged.xtc_probability !== undefined && merged.xtc_probability !== null) setInputValue('cfg-xtc-probability', merged.xtc_probability);
    if (merged.xtc_threshold !== undefined && merged.xtc_threshold !== null) setInputValue('cfg-xtc-threshold', merged.xtc_threshold);
    if (Array.isArray(merged.samplers)) {
      setInputValue('cfg-samplers', merged.samplers.join(', '));
    } else if (typeof merged.samplers === 'string') {
      setInputValue('cfg-samplers', merged.samplers);
    }

    return {
      k: kVal || (merged.cache_type_k ? String(merged.cache_type_k) : ''),
      v: vVal || (merged.cache_type_v ? String(merged.cache_type_v) : ''),
      fa: faEl ? faEl.checked : Boolean(merged.flash_attn),
      nCtx
    };
  }

  async function onImportLlamaCppKv() {
    const btn = getEl('btn-import-llamacpp-kv');
    const statusEl = getEl('import-kv-status');
    if (btn) {
      btn.disabled = true;
      btn.classList.add('is-loading');
    }
    if (statusEl) {
      statusEl.className = 'pill text-sm is-loading';
      statusEl.textContent = 'Importing from llama.cpp / PS5LM...';
    }
    try {
      const res = await apiFetch('/api/v1/inference/import-kv', { method: 'POST', body: {} });
      const summary = applyImportedKvFields(res);
      const parts = [];
      parts.push(`K:${summary.k || 'auto'}`);
      parts.push(`V:${summary.v || 'auto'}`);
      parts.push(`FA:${summary.fa ? 'on' : 'off'}`);
      if (summary.nCtx) parts.push(`ctx ${summary.nCtx}`);
      if (res && res.total_slots) parts.push(`${res.total_slots} slots`);
      const text = parts.join(' \u00b7 ');
      if (statusEl) {
        statusEl.className = 'pill text-sm is-success';
        statusEl.textContent = text;
      }
      showToast(`Imported KV & sampling settings (${text}).`, 'success');
    } catch (err) {
      if (statusEl) {
        statusEl.className = 'pill text-sm is-error';
        statusEl.textContent = err && err.message ? err.message : 'Import failed';
      }
      showToast(`KV import failed: ${err.message}`, 'danger');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove('is-loading');
      }
    }
  }

  function resetSessionUiState() {
    state.chatMessages = [];
    clearAllStreamBuckets();
    clearTimingsCaches();
    resetLiveBrowserState();
    if (state.activeChatTasks) state.activeChatTasks.clear();
    if (state.completedChatTaskIds) state.completedChatTaskIds.clear();
    if (state.chatErrors) state.chatErrors.clear();
    state.pendingFilePreview = null;
    state.selectedFilePath = '';
    state.selectedFileContent = '';
    state.pendingAttachments = [];
    state.sseWasConnectedOnce = false;
    state.sseNeedsResync = false;
    revokeTrackedBlobUrl('filePreviewBlobUrl');
    revokeTrackedBlobUrl('modalBlobUrl');
    const epCard = getEl('chat-ephemeral-card');
    if (epCard) epCard.hidden = true;
    const epBody = getEl('chat-ephemeral-body');
    if (epBody) clearEl(epBody);
    const slashPal = getEl('chat-slash-palette');
    if (slashPal) {
      slashPal.hidden = true;
      clearEl(slashPal);
    }
  }

  function handleUnauthorized(msg) {
    state.authenticated = false;
    state.csrfToken = '';
    sessionStorage.removeItem(CSRF_KEY);
    closeSse();
    stopPolling();
    resetSessionUiState();
    showLoginOverlay(msg || 'Please sign in to access the PS5 Agent dashboard.');
  }

  async function apiFetch(path, options) {
    const opts = Object.assign({ method: 'GET' }, options || {});
    const method = opts.method.toUpperCase();
    const headers = Object.assign({ Accept: 'application/json' }, opts.headers || {});

    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method) && state.csrfToken) {
      headers['X-CSRF-Token'] = state.csrfToken;
    }

    let body = opts.body;
    if (body !== undefined && body !== null && typeof body === 'object' && !(body instanceof FormData)) {
      headers['Content-Type'] = 'application/json';
      body = JSON.stringify(body);
    }

    const res = await fetch(path, {
      method,
      headers,
      body,
      credentials: 'same-origin'
    });

    const text = await res.text();
    let payload = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (_) {
        payload = text;
      }
    }

    let errMsg = '';
    if (payload && typeof payload === 'object') {
      if (typeof payload.error === 'string' && payload.error) {
        errMsg = payload.error;
      } else if (payload.error && typeof payload.error === 'object' && payload.error.message) {
        errMsg = String(payload.error.message);
      } else if (typeof payload.message === 'string' && payload.message) {
        errMsg = payload.message;
      }
    } else if (typeof payload === 'string' && payload) {
      errMsg = payload;
    }

    if (res.status === 401) {
      if (!opts.skipAuthRedirect) {
        handleUnauthorized(errMsg || 'Session expired or authentication required.');
      }
      const err = new Error(errMsg || 'Invalid username or password (401)');
      err.status = 401;
      err.payload = payload;
      throw err;
    }

    if (!res.ok) {
      const err = new Error(errMsg || `HTTP ${res.status} ${res.statusText}`);
      err.status = res.status;
      err.payload = payload;
      throw err;
    }

    return payload;
  }

  function showLoginOverlay(errorMsg) {
    ['modal-overlay', 'agent-modal', 'extension-modal', 'image-lightbox-modal', 'modal-schedule'].forEach((id) => {
      const el = $(id);
      if (el) el.hidden = true;
    });
    if (state.activeChatTasks) state.activeChatTasks.clear();
    $('app-shell').hidden = true;
    $('login-overlay').hidden = false;
    const errBox = $('login-error');
    if (errorMsg) {
      errBox.textContent = errorMsg;
      errBox.hidden = false;
    } else {
      errBox.textContent = '';
      errBox.hidden = true;
    }
    window.setTimeout(() => {
      const userField = $('login-username');
      if (userField) userField.focus();
    }, 30);
  }

  function showAppShell() {
    $('login-overlay').hidden = true;
    $('app-shell').hidden = false;
    setText('session-username', state.username || 'admin');
  }

  async function onLoginSubmit(ev) {
    ev.preventDefault();
    const username = $('login-username').value.trim();
    const password = $('login-password').value;
    const errBox = $('login-error');
    const submitBtn = $('login-submit');

    if (!username || !password) {
      errBox.textContent = 'Username and password are required.';
      errBox.hidden = false;
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Signing in\u2026';
    errBox.hidden = true;

    try {
      const data = await apiFetch('/api/v1/auth/login', {
        method: 'POST',
        body: { username, password },
        skipAuthRedirect: true
      });
      const csrf = (data && (data.csrf_token || data.csrfToken)) || '';
      state.csrfToken = csrf;
      state.username = (data && data.username) || username;
      state.authenticated = true;
      if (csrf) {
        sessionStorage.setItem(CSRF_KEY, csrf);
      }
      sessionStorage.setItem(USER_KEY, state.username);
      $('login-password').value = '';
      showAppShell();
      await bootstrapAuthenticatedSession();
    } catch (err) {
      errBox.textContent = err.message || 'Invalid credentials.';
      errBox.hidden = false;
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = 'Sign In';
    }
  }

  async function onLogout() {
    try {
      await apiFetch('/api/v1/auth/logout', { method: 'POST', body: {}, skipAuthRedirect: true });
    } catch (_) {
      // Ignore errors on logout cleanup
    }
    handleUnauthorized('');
  }

  /* -------------------- Modal Dialog Manager -------------------- */
  function openModal(title, bodyNodes, footerNodes) {
    setText('modal-title', title);
    const bodyEl = $('modal-body');
    const footerEl = $('modal-footer');
    clearEl(bodyEl);
    clearEl(footerEl);
    for (const n of bodyNodes) {
      if (n) bodyEl.appendChild(n);
    }
    for (const n of footerNodes) {
      if (n) footerEl.appendChild(n);
    }
    $('modal-overlay').hidden = false;
    window.setTimeout(() => $('modal-close-btn').focus(), 20);
  }

  function closeModal() {
    $('modal-overlay').hidden = true;
    revokeTrackedBlobUrl('modalBlobUrl');
    clearEl($('modal-body'));
    clearEl($('modal-footer'));
  }

  function openAgentDialog(title, bodyNodes, footerNodes) {
    setText('agent-modal-title', title);
    const bodyEl = getEl('agent-modal-body');
    const footerEl = getEl('agent-modal-footer');
    if (bodyEl) {
      clearEl(bodyEl);
      for (const n of bodyNodes) {
        if (n) bodyEl.appendChild(n);
      }
    }
    if (footerEl) {
      clearEl(footerEl);
      for (const n of footerNodes) {
        if (n) footerEl.appendChild(n);
      }
    }
    const overlay = getEl('agent-modal');
    if (overlay) overlay.hidden = false;
    const closeBtn = getEl('agent-modal-close');
    if (closeBtn) window.setTimeout(() => closeBtn.focus(), 20);
  }

  function closeAgentDialog() {
    const overlay = getEl('agent-modal');
    if (overlay) overlay.hidden = true;
  }

  function openExtensionModal(title, bodyNodes, footerNodes) {
    setText('extension-modal-title', title);
    const bodyEl = getEl('extension-modal-body');
    const footerEl = getEl('extension-modal-footer');
    if (bodyEl) {
      clearEl(bodyEl);
      for (const n of bodyNodes) {
        if (n) bodyEl.appendChild(n);
      }
    }
    if (footerEl) {
      clearEl(footerEl);
      for (const n of footerNodes) {
        if (n) footerEl.appendChild(n);
      }
    }
    const overlay = getEl('extension-modal');
    if (overlay) overlay.hidden = false;
    const closeBtn = getEl('extension-modal-close');
    if (closeBtn) window.setTimeout(() => closeBtn.focus(), 20);
  }

  function closeExtensionModal() {
    const overlay = getEl('extension-modal');
    if (overlay) overlay.hidden = true;
    clearEl(getEl('extension-modal-body'));
    clearEl(getEl('extension-modal-footer'));
  }

  /* -------------------- Navigation & Responsive Drawer -------------------- */
  function setSidebarCollapsed(collapsed) {
    state.sidebarCollapsed = Boolean(collapsed);
    try {
      localStorage.setItem(SIDEBAR_COLLAPSED_KEY, state.sidebarCollapsed ? 'true' : 'false');
    } catch (_) { /* ignore */ }
    const shell = getEl('app-shell');
    if (shell) shell.classList.toggle('sidebar-collapsed', state.sidebarCollapsed);
    const collapseBtn = getEl('sidebar-collapse-btn');
    if (collapseBtn) {
      collapseBtn.hidden = state.sidebarCollapsed;
      collapseBtn.setAttribute('aria-expanded', state.sidebarCollapsed ? 'false' : 'true');
    }
    const expandBtn = getEl('sidebar-expand-btn');
    if (expandBtn) {
      expandBtn.hidden = !state.sidebarCollapsed;
      expandBtn.setAttribute('aria-expanded', state.sidebarCollapsed ? 'false' : 'true');
    }
  }

  function setRawOutputMode(enabled) {
    state.showRawOutput = Boolean(enabled);
    try {
      localStorage.setItem(RAW_OUTPUT_KEY, state.showRawOutput ? 'true' : 'false');
    } catch (_) { /* ignore */ }
    const toggle = getEl('chat-raw-output-toggle');
    if (toggle) toggle.checked = state.showRawOutput;
    renderChatMessages();
  }

  function setMobileDrawer(open) {
    const sidebar = $('sidebar');
    const backdrop = $('sidebar-backdrop');
    const btn = $('mobile-menu-btn');
    if (open) {
      sidebar.classList.add('is-open');
      backdrop.hidden = false;
      if (btn) btn.setAttribute('aria-expanded', 'true');
    } else {
      sidebar.classList.remove('is-open');
      backdrop.hidden = true;
      if (btn) btn.setAttribute('aria-expanded', 'false');
    }
  }

  function switchSection(sectionId) {
    if (!SECTION_META[sectionId]) return;
    state.currentSection = sectionId;
    setMobileDrawer(false);

    document.querySelectorAll('.nav-item').forEach((btn) => {
      const active = btn.dataset.section === sectionId;
      btn.classList.toggle('is-active', active);
      btn.setAttribute('aria-current', active ? 'page' : 'false');
    });

    document.querySelectorAll('.view-section').forEach((sec) => {
      sec.hidden = sec.id !== `section-${sectionId}`;
    });

    const meta = SECTION_META[sectionId];
    setText('page-title', meta.title);
    setText('page-subtitle', meta.subtitle);

    loadSection(sectionId);
  }

  async function loadSection(sectionId) {
    if (!state.authenticated) return;
    try {
      switch (sectionId) {
        case 'overview':
          await Promise.all([loadMetrics(), loadApprovals(), refreshSchedulesCount()]);
          break;
        case 'chat':
          await Promise.all([
            loadAgents(),
            loadConversations(),
            loadTasks()
          ]);
          if (state.activeConversationId) {
            await Promise.all([
              loadChatMessages(state.activeConversationId),
              rehydrateActiveChatTasks(state.activeConversationId)
            ]);
          }
          if (state.activeLiveBrowserSessionId) {
            renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: false });
          }
          break;
        case 'agents':
          await Promise.all([loadTools(), loadAgents()]);
          break;
        case 'a2a':
          await Promise.all([loadAgents(), loadA2aMessages()]);
          break;
        case 'tasks':
          await Promise.all([loadAgents(), loadTasks()]);
          break;
        case 'schedules':
          await Promise.all([loadAgents(), loadSchedules()]);
          break;
        case 'memory':
          await Promise.all([loadAgents(), loadMemories()]);
          break;
        case 'tools':
          await Promise.all([loadTools(), loadToolExecutions()]);
          break;
        case 'files':
          await loadFilesSection();
          break;
        case 'extensions':
          await loadExtensionsSection();
          break;
        case 'sandbox':
          await loadSandboxTab();
          if (state.activeLiveBrowserSessionId) {
            renderAllLiveBrowserViewports({ refreshPreview: true, immediatePreview: false });
          }
          break;
        case 'logs':
          if (state.logsTab === 'audit') {
            await loadAuditLogs();
          } else {
            await loadRuntimeLogs();
          }
          break;
        case 'settings':
          await loadSettings();
          break;
      }
    } catch (err) {
      if ((!err || err.status !== 401) && !String(err && err.message).includes('401')) {
        showToast(`Failed to load ${sectionId}: ${err.message}`, 'danger');
      }
    }
  }

  /* -------------------- 1. Overview & Approvals -------------------- */
  async function loadMetrics() {
    const m = await apiFetch('/api/v1/metrics');
    state.metrics = m;
    renderOverview(m);
  }

  function renderOverview(m) {
    if (!m) return;
    setText('ov-ps5-model', m.ps5_model || 'Host / Dev');
    setText('ov-ps5-fw', m.ps5_fw_version || 'N/A');
    const cpu = m.ps5_cpu_temp_c !== null && m.ps5_cpu_temp_c !== undefined ? `${m.ps5_cpu_temp_c}\u00b0C` : '\u2014';
    const soc = m.ps5_soc_temp_c !== null && m.ps5_soc_temp_c !== undefined ? `${m.ps5_soc_temp_c}\u00b0C` : '\u2014';
    setText('ov-ps5-temp', `${cpu} / ${soc}`);
    setText('ov-rss-uptime', `${fmtBytes(m.memory_rss_bytes)} / ${fmtDuration(m.uptime_secs)}`);

    const netPill = $('ov-net-pill');
    if (netPill) {
      netPill.textContent = m.network_degraded ? 'Network Degraded' : 'LAN OK';
      netPill.className = `pill ${m.network_degraded ? 'pill-warning' : 'pill-success'}`;
    }

    const ps5lmPill = $('ov-ps5lm-pill');
    if (ps5lmPill) {
      ps5lmPill.textContent = m.ps5lm_reachable ? 'Reachable' : 'Unreachable';
      ps5lmPill.className = `pill ${m.ps5lm_reachable ? 'pill-success' : 'pill-danger'}`;
    }
    setText('ov-loaded-model', m.ps5lm_loaded_model || 'None');
    setText('ov-latency', `${m.inference_p50_ms || 0} / ${m.inference_p95_ms || 0} / ${m.last_inference_ms || 0} ms`);
    setText('ov-tokens', `${m.tokens_prompt_total || 0} / ${m.tokens_completion_total || 0}`);
    setText('ov-inf-counts', `${m.inference_requests_total || 0} / ${m.inference_errors_total || 0}`);

    const storagePill = $('ov-storage-pill');
    if (storagePill) {
      storagePill.textContent = (m.storage_driver || 'unknown').toUpperCase();
      storagePill.className = 'pill pill-info';
    }
    setText('ov-tasks-active', `${m.active_tasks || 0} / ${m.queued_tasks || 0}`);
    setText('ov-tasks-done', `${m.completed_tasks || 0} / ${m.failed_tasks || 0}`);
    setText('ov-db-stats', `${m.db_ops_total || 0} / ${m.db_avg_us || 0} \u00b5s`);
    setText('ov-http-stats', `${m.http_requests_total || 0} / ${m.http_avg_ms || 0} ms`);

    const tasksBadge = $('nav-badge-tasks');
    const activeCount = Number(m.active_tasks || 0) + Number(m.queued_tasks || 0);
    if (tasksBadge) {
      tasksBadge.textContent = String(activeCount);
      tasksBadge.hidden = activeCount === 0;
    }
  }

  async function loadApprovals() {
    const data = await apiFetch('/api/v1/approvals?pending_only=true');
    const list = unwrapList(data, ['approvals']).filter((a) => String(a.status || 'PENDING').toUpperCase() === 'PENDING');
    state.approvals = list;
    renderApprovals();
  }

  function renderApprovals() {
    const count = state.approvals.length;
    const navBadge = $('nav-badge-approvals');
    if (navBadge) {
      navBadge.textContent = String(count);
      navBadge.hidden = count === 0;
    }
    const topBtn = $('topbar-approvals-btn');
    if (topBtn) {
      topBtn.hidden = count === 0;
      setText('topbar-approvals-count', String(count));
    }

    const banner = $('pending-approvals-banner');
    if (!banner) return;
    clearEl(banner);
    if (count === 0) {
      banner.hidden = true;
      return;
    }

    banner.hidden = false;
    banner.appendChild(el('div', { className: 'card-header' },
      el('h2', { className: 'card-title' }, `Pending Security Approvals (${count})`),
      el('span', { className: 'pill pill-warning' }, 'Human Confirmation Required')
    ));

    for (const ap of state.approvals) {
      const approveBtn = el('button', {
        type: 'button',
        className: 'btn btn-success btn-sm',
        onClick: () => resolveApproval(ap.id, true)
      }, 'Approve');
      const denyBtn = el('button', {
        type: 'button',
        className: 'btn btn-danger btn-sm',
        onClick: () => resolveApproval(ap.id, false)
      }, 'Deny');

      banner.appendChild(el('div', { className: 'approval-item' },
        el('div', { className: 'approval-meta' },
          el('div', null,
            el('strong', null, `${agentDisplayName(ap.agent_id)} requested `),
            el('span', { className: 'mono' }, ap.operation || ap.tool_id || 'operation')
          ),
          el('div', { className: 'text-sm text-secondary' }, ap.reason || 'Mutating operation requires approval'),
          ap.arguments_json ? el('pre', { className: 'code-box' }, prettyJson(ap.arguments_json)) : null
        ),
        el('div', { className: 'approval-actions' }, approveBtn, denyBtn)
      ));
    }
  }

  async function resolveApproval(id, approved) {
    const action = approved ? 'approve' : 'deny';
    try {
      await apiFetch(`/api/v1/approvals/${encodeURIComponent(id)}/${action}`, {
        method: 'POST',
        body: {}
      });
      showToast(`Approval ${approved ? 'granted' : 'denied'}.`, approved ? 'success' : 'warning');
      await loadApprovals();
    } catch (err) {
      showToast(`Approval action failed: ${err.message}`, 'danger');
    }
  }

  /* -------------------- Markdown, Files & Lightbox (XSS-safe) -------------------- */
  function isSafeMarkdownLinkUrl(raw) {
    if (!raw || typeof raw !== 'string') return false;
    const trimmed = raw.trim();
    if (!trimmed) return false;
    if (trimmed.startsWith('/api/v1/files/') && !trimmed.startsWith('//')) {
      return true;
    }
    try {
      const parsed = new URL(trimmed);
      return parsed.protocol === 'http:' || parsed.protocol === 'https:';
    } catch (_) {
      return false;
    }
  }

  function isSafeImageSrc(url) {
    if (!url || typeof url !== 'string') return false;
    const trimmed = url.trim();
    const lower = trimmed.toLowerCase();
    if (lower.startsWith('data:image/') && !lower.startsWith('data:image/svg+xml')) {
      return true;
    }
    if (trimmed.startsWith('/api/v1/files/') && !trimmed.startsWith('//')) {
      return true;
    }
    return false;
  }

  function sanitizeWorkspacePath(rawPath) {
    if (!rawPath || typeof rawPath !== 'string') return '';
    let p = rawPath.trim().replace(/\\/g, '/');
    p = p.replace(/^\/+/, '').replace(/^\.\//, '');
    if (!p || p.includes('\0') || p.includes('..') || /^[a-z]+:/i.test(p)) {
      return '';
    }
    return p;
  }

  function isHtmlFilePath(path) {
    return /\.(html|htm)$/i.test(String(path || '').trim());
  }

  function isImageFilePath(path) {
    return /\.(png|jpe?g|webp|gif)$/i.test(String(path || '').trim());
  }

  function pathBasename(path) {
    const parts = String(path || '').split('/');
    return parts[parts.length - 1] || 'file';
  }

  async function copyTextWithFeedback(btn, text, defaultLabel) {
    const label = defaultLabel || 'Copy';
    let ok = false;
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(String(text || ''));
        ok = true;
      }
    } catch (_) {
      ok = false;
    }
    if (!ok) {
      try {
        const ta = document.createElement('textarea');
        ta.value = String(text || '');
        ta.setAttribute('readonly', '');
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        ok = document.execCommand('copy');
        ta.remove();
      } catch (_) {
        ok = false;
      }
    }
    if (btn) {
      btn.textContent = ok ? 'Copied!' : 'Copy failed';
      window.setTimeout(() => {
        if (btn.isConnected) btn.textContent = label;
      }, 1500);
    }
    if (!ok) {
      showToast('Clipboard copy failed.', 'warning');
    }
  }

  async function openHtmlSnippetPreview(code) {
    revokeTrackedBlobUrl('modalBlobUrl');
    const blob = new Blob([String(code || '')], { type: 'text/html;charset=utf-8' });
    const blobUrl = URL.createObjectURL(blob);
    state.modalBlobUrl = blobUrl;
    const iframe = el('iframe', {
      className: 'preview-sandbox-frame',
      sandbox: 'allow-scripts',
      title: 'Sandboxed HTML preview',
      src: blobUrl
    });
    openModal('HTML Preview (Sandboxed)', [iframe], [
      el('button', { type: 'button', className: 'btn btn-primary', onClick: closeModal }, 'Close')
    ]);
  }

  function openWorkspaceFileFromChat(path, opts) {
    const clean = sanitizeWorkspacePath(path);
    if (!clean) return;
    const o = opts || {};
    state.pendingFilePreview = {
      path: clean,
      autoRunHtml: o.autoRunHtml !== undefined ? Boolean(o.autoRunHtml) : (!o.editMode && isHtmlFilePath(clean)),
      editMode: Boolean(o.editMode)
    };
    switchSection('files');
  }

  function openWorkspaceFileInFilesTab(path, autoRunHtml) {
    openWorkspaceFileFromChat(path, { autoRunHtml, editMode: false });
  }

  function isMarkdownFilePath(path) {
    return /\.(md|markdown)$/i.test(String(path || '').trim());
  }

  function readFileAsBase64(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const res = String(reader.result || '');
        const commaIdx = res.indexOf(',');
        resolve(commaIdx >= 0 ? res.slice(commaIdx + 1) : res);
      };
      reader.onerror = () => reject(new Error(`Failed to read ${file.name}`));
      reader.readAsDataURL(file);
    });
  }

  function syncFilesToolbarButtons() {
    const clean = state.selectedFilePath;
    const hasFile = Boolean(clean);
    const isImg = hasFile && isImageFilePath(clean);
    const isHtml = hasFile && isHtmlFilePath(clean);
    const isTextEditable = hasFile && !isImg;

    const dlBtn = getEl('btn-file-download');
    const runBtn = getEl('btn-file-run-html');
    const viewScriptBtn = getEl('btn-file-view-script');
    const editBtn = getEl('btn-file-edit');
    const saveBtn = getEl('btn-file-save');
    const cancelEditBtn = getEl('btn-file-cancel-edit');
    const copyBtn = getEl('btn-file-copy-content');
    const delBtn = getEl('btn-file-delete');

    if (dlBtn) dlBtn.disabled = !hasFile;
    if (delBtn) delBtn.disabled = !hasFile;
    if (copyBtn) {
      copyBtn.disabled = !isTextEditable;
    }

    if (state.fileEditMode && isTextEditable) {
      if (runBtn) runBtn.hidden = true;
      if (viewScriptBtn) viewScriptBtn.hidden = true;
      if (editBtn) editBtn.hidden = true;
      if (saveBtn) saveBtn.hidden = false;
      if (cancelEditBtn) cancelEditBtn.hidden = false;
    } else {
      if (saveBtn) saveBtn.hidden = true;
      if (cancelEditBtn) cancelEditBtn.hidden = true;
      if (runBtn) {
        runBtn.hidden = !isHtml;
        runBtn.textContent = state.filesHtmlPreviewActive ? 'View Source' : 'Run / Preview HTML';
      }
      if (viewScriptBtn) {
        viewScriptBtn.hidden = !isTextEditable;
      }
      if (editBtn) {
        editBtn.hidden = !isTextEditable;
      }
    }
  }

  function clearFilePreview() {
    state.filePreviewSeq = (state.filePreviewSeq || 0) + 1;
    state.selectedFilePath = '';
    state.selectedFileContent = '';
    state.filesHtmlPreviewActive = false;
    state.fileEditMode = false;
    state.fileViewMode = 'preview';
    revokeTrackedBlobUrl('filePreviewBlobUrl');

    const header = getEl('files-preview-header');
    const body = getEl('files-preview-body');
    const iframe = getEl('files-preview-iframe');
    const dlBtn = getEl('btn-file-download');
    const runBtn = getEl('btn-file-run-html');
    const copyBtn = getEl('btn-file-copy-content');
    const delBtn = getEl('btn-file-delete');

    if (header) header.textContent = 'Select a file to preview';
    if (dlBtn) dlBtn.onclick = null;
    if (runBtn) runBtn.onclick = null;
    if (copyBtn) copyBtn.onclick = null;
    if (delBtn) delBtn.onclick = null;
    syncFilesToolbarButtons();

    if (iframe) {
      iframe.hidden = true;
      iframe.setAttribute('sandbox', 'allow-scripts');
      iframe.removeAttribute('src');
    }
    if (body) {
      body.hidden = false;
      clearEl(body);
      body.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No file selected'),
        el('span', { className: 'text-sm' }, 'Choose a file on the left to inspect code, images, or sandboxed HTML apps.')
      ));
    }
  }

  function setFileHtmlPreviewActive(active, filePath, htmlContent) {
    const body = getEl('files-preview-body');
    let iframe = getEl('files-preview-iframe');
    state.filesHtmlPreviewActive = Boolean(active);
    if (state.filesHtmlPreviewActive) {
      state.fileEditMode = false;
      state.fileViewMode = 'html';
    } else if (state.fileViewMode === 'html') {
      state.fileViewMode = 'script';
    }

    if (!iframe) {
      const panel = body ? body.parentElement : null;
      if (panel) {
        iframe = el('iframe', {
          id: 'files-preview-iframe',
          className: 'preview-sandbox-frame',
          sandbox: 'allow-scripts',
          title: 'Sandboxed HTML preview',
          hidden: true
        });
        panel.appendChild(iframe);
      }
    }

    if (iframe) {
      iframe.setAttribute('sandbox', 'allow-scripts');
      revokeTrackedBlobUrl('filePreviewBlobUrl');
      if (active && filePath) {
        const contentStr = typeof htmlContent === 'string'
          ? htmlContent
          : (state.selectedFilePath === filePath ? state.selectedFileContent : null);
        iframe.hidden = false;
        if (typeof contentStr === 'string') {
          const blob = new Blob([contentStr], { type: 'text/html;charset=utf-8' });
          const blobUrl = URL.createObjectURL(blob);
          state.filePreviewBlobUrl = blobUrl;
          iframe.src = blobUrl;
        } else {
          iframe.src = `/api/v1/files/preview?path=${encodeURIComponent(filePath)}&t=${Date.now()}`;
        }
      } else {
        iframe.hidden = true;
        iframe.removeAttribute('src');
      }
    }
    if (body) {
      body.hidden = Boolean(active);
    }
    syncFilesToolbarButtons();
  }

  function renderSelectedFileContentBody() {
    const clean = state.selectedFilePath;
    const body = getEl('files-preview-body');
    if (!clean || !body || isImageFilePath(clean)) return;

    const text = state.selectedFileContent || '';
    if (state.fileEditMode) {
      setFileHtmlPreviewActive(false, clean);
      clearEl(body);
      const editorTa = el('textarea', {
        id: 'file-editor-textarea',
        className: 'textarea file-editor-textarea',
        spellcheck: false
      });
      editorTa.value = text;
      body.appendChild(editorTa);
      syncFilesToolbarButtons();
      editorTa.focus();
      return;
    }

    if (state.fileViewMode === 'html' && isHtmlFilePath(clean)) {
      setFileHtmlPreviewActive(true, clean, text);
      clearEl(body);
      body.appendChild(el('pre', { className: 'preview-code' }, el('code', null, document.createTextNode(text))));
      syncFilesToolbarButtons();
      return;
    }

    setFileHtmlPreviewActive(false, clean);
    clearEl(body);
    if (isMarkdownFilePath(clean) && state.fileViewMode !== 'script') {
      body.appendChild(el('div', { className: 'msg-content' }, renderMarkdownNodes(text)));
    } else {
      body.appendChild(el('pre', { className: 'preview-code' }, el('code', null, document.createTextNode(text))));
    }
    syncFilesToolbarButtons();
  }

  function viewSelectedFileAsScript() {
    if (!state.selectedFilePath || isImageFilePath(state.selectedFilePath)) return;
    state.fileEditMode = false;
    state.fileViewMode = 'script';
    state.filesHtmlPreviewActive = false;
    renderSelectedFileContentBody();
  }

  function enterFileEditMode() {
    if (!state.selectedFilePath || isImageFilePath(state.selectedFilePath)) return;
    state.fileEditMode = true;
    state.fileViewMode = 'edit';
    state.filesHtmlPreviewActive = false;
    renderSelectedFileContentBody();
  }

  function cancelFileEditMode() {
    if (!state.selectedFilePath) return;
    state.fileEditMode = false;
    state.fileViewMode = 'script';
    state.filesHtmlPreviewActive = false;
    renderSelectedFileContentBody();
  }

  async function saveEditedWorkspaceFile() {
    const clean = state.selectedFilePath;
    if (!clean) return;
    const ta = getEl('file-editor-textarea');
    if (!ta) return;
    const nextContent = ta.value;
    const saveBtn = getEl('btn-file-save');
    if (saveBtn) saveBtn.disabled = true;
    try {
      await apiFetch('/api/v1/files/upload', {
        method: 'POST',
        body: { path: clean, content: nextContent }
      });
      state.selectedFileContent = nextContent;
      state.fileEditMode = false;
      state.fileViewMode = 'script';
      state.filesHtmlPreviewActive = false;
      renderSelectedFileContentBody();
      showToast('Script saved.', 'success');
      await loadFilesSection(state.filesCurrentPath);
    } catch (err) {
      showToast(`Save failed: ${err.message}`, 'danger');
    } finally {
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function renderFilesBreadcrumbs(currentPath) {
    const bar = getEl('files-breadcrumbs');
    if (!bar) return;
    clearEl(bar);

    const rootBtn = el('button', {
      type: 'button',
      className: 'btn btn-ghost btn-sm',
      dataset: { filePath: '' },
      onClick: () => loadFilesSection('')
    }, 'workspace');
    bar.appendChild(rootBtn);

    const segments = String(currentPath || '').split('/').filter(Boolean);
    let acc = '';
    for (const seg of segments) {
      acc = acc ? `${acc}/${seg}` : seg;
      const target = acc;
      bar.appendChild(el('span', { className: 'breadcrumb-sep', 'aria-hidden': 'true' }, '/'));
      bar.appendChild(el('button', {
        type: 'button',
        className: 'breadcrumb-btn',
        dataset: { filePath: target },
        onClick: () => loadFilesSection(target)
      }, seg));
    }
  }

  function renderFilesTable(entries, currentPath) {
    const tbody = getEl('files-table-body');
    if (!tbody) return;
    clearEl(tbody);

    if (currentPath) {
      const parts = currentPath.split('/').filter(Boolean);
      parts.pop();
      const parentPath = parts.join('/');
      tbody.appendChild(el('tr', null,
        el('td', null,
          el('button', {
            type: 'button',
            className: 'btn btn-ghost btn-sm mono',
            onClick: () => loadFilesSection(parentPath)
          }, '.. (parent folder)')
        ),
        el('td', null, el('span', { className: 'pill pill-muted' }, 'folder')),
        el('td', { className: 'num' }, '\u2014'),
        el('td', null, '\u2014'),
        el('td', null, '')
      ));
    }

    const sorted = [...entries].sort((a, b) => {
      if (Boolean(a.is_dir) !== Boolean(b.is_dir)) {
        return a.is_dir ? -1 : 1;
      }
      return String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' });
    });

    if (sorted.length === 0) {
      tbody.appendChild(el('tr', null,
        el('td', { colspan: '5' },
          el('div', { className: 'empty-state' },
            el('strong', null, 'Directory is empty'),
            el('span', { className: 'text-sm' }, 'Upload a file or create a subfolder to get started.')
          )
        )
      ));
      return;
    }

    for (const entry of sorted) {
      const isDir = Boolean(entry.is_dir);
      const isHtml = !isDir && isHtmlFilePath(entry.path);
      const isSelected = !isDir && state.selectedFilePath === entry.path;

      const nameBtn = el('button', {
        type: 'button',
        className: `btn btn-ghost btn-sm mono${isSelected ? ' is-active' : ''}`,
        onClick: () => {
          if (isDir) {
            loadFilesSection(entry.path);
          } else {
            previewWorkspaceFile(entry.path, { autoRunHtml: false });
          }
        }
      }, isDir ? `[DIR] ${entry.name}` : entry.name);

      const actionBtns = [];
      if (isDir) {
        actionBtns.push(el('button', {
          type: 'button',
          className: 'btn btn-secondary btn-sm',
          onClick: () => loadFilesSection(entry.path)
        }, 'Open'));
      } else {
        actionBtns.push(el('button', {
          type: 'button',
          className: 'btn btn-secondary btn-sm',
          onClick: () => previewWorkspaceFile(entry.path, { autoRunHtml: isHtml })
        }, isHtml ? 'Run HTML' : 'Preview'));
        actionBtns.push(el('a', {
          className: 'btn btn-secondary btn-sm',
          href: `/api/v1/files/download?path=${encodeURIComponent(entry.path)}`,
          download: pathBasename(entry.path)
        }, 'Download'));
      }
      actionBtns.push(el('button', {
        type: 'button',
        className: 'btn btn-danger btn-sm',
        onClick: () => confirmDeleteWorkspaceEntry(entry)
      }, 'Delete'));

      tbody.appendChild(el('tr', null,
        el('td', null, nameBtn),
        el('td', null, el('span', {
          className: `pill ${isDir ? 'pill-info' : isHtml ? 'pill-success' : 'pill-muted'}`
        }, isDir ? 'folder' : (entry.kind || 'file'))),
        el('td', { className: 'num' }, isDir ? '\u2014' : fmtBytes(entry.size_bytes)),
        el('td', { className: 'mono text-sm' }, entry.modified_at ? fmtDateTime(entry.modified_at) : '\u2014'),
        el('td', null, el('div', { className: 'card-actions' }, actionBtns))
      ));
    }
  }

  async function previewWorkspaceFile(filePath, opts) {
    const clean = sanitizeWorkspacePath(filePath);
    if (!clean) return;
    const options = opts || {};
    const seq = ++state.filePreviewSeq;

    state.selectedFilePath = clean;
    state.selectedFileContent = '';
    state.fileEditMode = false;
    state.fileViewMode = 'preview';

    const header = getEl('files-preview-header');
    const body = getEl('files-preview-body');
    const dlBtn = getEl('btn-file-download');
    const runBtn = getEl('btn-file-run-html');
    const copyBtn = getEl('btn-file-copy-content');
    const delBtn = getEl('btn-file-delete');

    if (header) {
      clearEl(header);
      header.appendChild(el('span', { className: 'mono truncate' }, clean));
    }
    if (dlBtn) {
      dlBtn.disabled = false;
      dlBtn.onclick = () => {
        window.location.assign(`/api/v1/files/download?path=${encodeURIComponent(clean)}`);
      };
    }
    if (delBtn) {
      delBtn.disabled = false;
      delBtn.onclick = () => confirmDeleteWorkspaceEntry({
        path: clean,
        name: pathBasename(clean),
        is_dir: false
      });
    }
    if (copyBtn) {
      copyBtn.disabled = true;
      copyBtn.onclick = null;
    }

    if (isImageFilePath(clean)) {
      if (runBtn) runBtn.hidden = true;
      setFileHtmlPreviewActive(false);
      if (body) {
        clearEl(body);
        const imgSrc = `/api/v1/files/preview?path=${encodeURIComponent(clean)}&t=${Date.now()}`;
        const imgNode = el('img', {
          className: 'preview-image',
          src: imgSrc,
          alt: clean,
          onClick: () => openImageLightbox(imgSrc, clean)
        });
        body.appendChild(imgNode);
      }
      syncFilesToolbarButtons();
      renderFilesTable(state.filesEntries, state.filesCurrentPath);
      return;
    }

    if (isHtmlFilePath(clean)) {
      if (runBtn) {
        runBtn.hidden = false;
        runBtn.onclick = () => {
          if (state.filesHtmlPreviewActive) {
            viewSelectedFileAsScript();
          } else {
            state.fileEditMode = false;
            state.fileViewMode = 'html';
            setFileHtmlPreviewActive(true, clean, state.selectedFileContent);
          }
        };
      }
      const startHtmlPreview = !options.editMode && Boolean(options.autoRunHtml);
      state.fileEditMode = Boolean(options.editMode);
      state.fileViewMode = state.fileEditMode ? 'edit' : (startHtmlPreview ? 'html' : 'script');
      setFileHtmlPreviewActive(false, clean);
      if (body) {
        clearEl(body);
        body.appendChild(el('div', { className: 'empty-state' }, el('span', { className: 'text-sm' }, 'Loading HTML source\u2026')));
      }
      try {
        const res = await apiFetch(`/api/v1/files/content?path=${encodeURIComponent(clean)}`);
        if (seq !== state.filePreviewSeq || state.selectedFilePath !== clean) return;
        const text = (res && typeof res.content === 'string') ? res.content : '';
        state.selectedFileContent = text;
        renderSelectedFileContentBody();
        if (copyBtn) {
          copyBtn.disabled = false;
          copyBtn.onclick = () => copyTextWithFeedback(copyBtn, state.selectedFileContent, 'Copy');
        }
      } catch (err) {
        if (seq !== state.filePreviewSeq || state.selectedFilePath !== clean) return;
        state.fileEditMode = false;
        syncFilesToolbarButtons();
        if (startHtmlPreview) {
          setFileHtmlPreviewActive(true, clean);
        } else if (body) {
          clearEl(body);
          body.appendChild(el('div', { className: 'empty-state' },
            el('strong', null, 'Source preview unavailable'),
            el('span', { className: 'text-sm' }, err.message || 'Use Run / Preview HTML or Download.')
          ));
        }
      }
      renderFilesTable(state.filesEntries, state.filesCurrentPath);
      return;
    }

    if (runBtn) runBtn.hidden = true;
    state.fileEditMode = Boolean(options.editMode);
    state.fileViewMode = state.fileEditMode ? 'edit' : 'preview';
    setFileHtmlPreviewActive(false);

    if (body) {
      clearEl(body);
      body.appendChild(el('div', { className: 'empty-state' }, el('span', { className: 'text-sm' }, 'Loading file content\u2026')));
    }
    try {
      const res = await apiFetch(`/api/v1/files/content?path=${encodeURIComponent(clean)}`);
      if (seq !== state.filePreviewSeq || state.selectedFilePath !== clean) return;
      const text = (res && typeof res.content === 'string') ? res.content : '';
      state.selectedFileContent = text;
      renderSelectedFileContentBody();
      if (copyBtn) {
        copyBtn.disabled = false;
        copyBtn.onclick = () => copyTextWithFeedback(copyBtn, state.selectedFileContent, 'Copy');
      }
    } catch (err) {
      if (seq !== state.filePreviewSeq || state.selectedFilePath !== clean) return;
      state.fileEditMode = false;
      syncFilesToolbarButtons();
      if (body) {
        clearEl(body);
        body.appendChild(el('div', { className: 'empty-state' },
          el('strong', null, 'Preview unavailable'),
          el('span', { className: 'text-sm' }, err.message || 'Use Download to retrieve this file.')
        ));
      }
    }
    renderFilesTable(state.filesEntries, state.filesCurrentPath);
  }

  async function loadFilesSection(pathArg) {
    const seq = ++state.fileLoadSeq;
    let pending = null;
    let targetPath;
    if (pathArg !== undefined) {
      targetPath = sanitizeWorkspacePath(pathArg);
    } else if (state.pendingFilePreview && state.pendingFilePreview.path) {
      pending = state.pendingFilePreview;
      state.pendingFilePreview = null;
      const parts = pending.path.split('/').filter(Boolean);
      parts.pop();
      targetPath = parts.join('/');
    } else {
      targetPath = state.filesCurrentPath || '';
    }

    const qs = targetPath ? `?path=${encodeURIComponent(targetPath)}` : '';
    let data;
    try {
      data = await apiFetch(`/api/v1/files${qs}`);
    } catch (err) {
      if (seq !== state.fileLoadSeq) return;
      showToast(`Failed to load files: ${err.message}`, 'danger');
      return;
    }
    if (seq !== state.fileLoadSeq) return;

    const resolvedPath = sanitizeWorkspacePath((data && data.current_path) || targetPath);
    state.filesCurrentPath = resolvedPath;
    state.filesEntries = Array.isArray(data && data.entries) ? data.entries : [];

    renderFilesBreadcrumbs(resolvedPath);
    renderFilesTable(state.filesEntries, resolvedPath);

    if (pending && pending.path) {
      await previewWorkspaceFile(pending.path, {
        autoRunHtml: pending.autoRunHtml,
        editMode: pending.editMode
      });
    } else if (!state.selectedFilePath) {
      clearFilePreview();
    }
  }

  function openMkdirModal() {
    const folderIn = el('input', {
      id: 'modal-mkdir-path',
      type: 'text',
      className: 'input mono',
      placeholder: state.filesCurrentPath ? 'subfolder-name' : 'docs/notes',
      required: true
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const raw = folderIn.value.trim().replace(/^\/+/, '');
        if (!raw) {
          showToast('Folder name is required.', 'warning');
          return;
        }
        const combined = state.filesCurrentPath && !raw.includes('/')
          ? `${state.filesCurrentPath}/${raw}`
          : raw;
        const clean = sanitizeWorkspacePath(combined);
        if (!clean) {
          showToast('Invalid folder path.', 'danger');
          return;
        }
        try {
          await apiFetch('/api/v1/files/mkdir', {
            method: 'POST',
            body: { path: clean }
          });
          closeModal();
          showToast(`Created folder "${clean}".`, 'success');
          await loadFilesSection(state.filesCurrentPath);
        } catch (err) {
          showToast(`Create folder failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'modal-mkdir-path' }, 'Folder Name or Relative Path'),
        folderIn,
        state.filesCurrentPath
          ? el('p', { className: 'field-hint' }, `Creates inside: ${state.filesCurrentPath}/`)
          : el('p', { className: 'field-hint' }, 'Creates inside workspace root.')
      )
    );

    openModal('Create Workspace Folder', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Create Folder')
    ]);
    window.setTimeout(() => folderIn.focus(), 30);
  }

  async function handleWorkspaceUploadFiles(fileList) {
    if (!fileList || fileList.length === 0) return;
    const files = Array.from(fileList);
    let uploaded = 0;

    for (const file of files) {
      const baseName = pathBasename(file.name || 'upload.bin').replace(/^\.+/, '') || 'upload.bin';
      const targetPath = sanitizeWorkspacePath(
        state.filesCurrentPath ? `${state.filesCurrentPath}/${baseName}` : baseName
      );
      if (!targetPath) {
        showToast(`Skipped invalid filename "${file.name}".`, 'warning');
        continue;
      }
      try {
        const contentBase64 = await readFileAsBase64(file);
        await apiFetch('/api/v1/files/upload', {
          method: 'POST',
          body: {
            path: targetPath,
            content_base64: contentBase64
          }
        });
        uploaded++;
      } catch (err) {
        showToast(`Upload failed for "${file.name}": ${err.message}`, 'danger');
      }
    }

    if (uploaded > 0) {
      showToast(`Uploaded ${uploaded} file${uploaded === 1 ? '' : 's'}.`, 'success');
      await loadFilesSection(state.filesCurrentPath);
    }
  }

  function confirmDeleteWorkspaceEntry(entry) {
    const clean = sanitizeWorkspacePath(entry && entry.path);
    if (!clean) return;
    const label = entry.is_dir ? 'folder' : 'file';
    openModal(`Delete Workspace ${entry.is_dir ? 'Folder' : 'File'}`, [
      el('p', null, `Delete ${label} "${clean}"? This removes it from the workspace.`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/files?path=${encodeURIComponent(clean)}`, { method: 'DELETE' });
            closeModal();
            showToast(`Deleted "${clean}".`, 'success');
            if (state.selectedFilePath === clean || (entry.is_dir && state.selectedFilePath.startsWith(`${clean}/`))) {
              clearFilePreview();
            }
            await loadFilesSection(state.filesCurrentPath);
          } catch (err) {
            showToast(`Delete failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete')
    ]);
  }

  /* -------------------- Extensions (Skills, MCP Servers, Plugins) -------------------- */
  function setExtensionsTab(tab) {
    const valid = ['skills', 'mcp', 'plugins'].includes(tab) ? tab : 'skills';
    state.extensionsTab = valid;

    const tabMap = [
      ['skills', getEl('ext-tab-skills'), getEl('extensions-skills-pane')],
      ['mcp', getEl('ext-tab-mcp'), getEl('extensions-mcp-pane')],
      ['plugins', getEl('ext-tab-plugins'), getEl('extensions-plugins-pane')]
    ];

    for (const [key, btn, pane] of tabMap) {
      const active = key === valid;
      if (btn) {
        btn.classList.toggle('is-active', active);
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-selected', String(active));
      }
      if (pane) {
        pane.hidden = !active;
      }
    }
  }

  async function loadExtensionsSection() {
    setExtensionsTab(state.extensionsTab || 'skills');
    const data = await apiFetch('/api/v1/extensions');
    state.extensionsData = {
      skills: Array.isArray(data && data.skills) ? data.skills : [],
      plugins: Array.isArray(data && data.plugins) ? data.plugins : [],
      mcp_servers: Array.isArray(data && data.mcp_servers) ? data.mcp_servers : []
    };
    renderExtensionsPanes();
  }

  function renderExtensionsPanes() {
    renderSkillsPane();
    renderMcpPane();
    renderPluginsPane();
    setExtensionsTab(state.extensionsTab || 'skills');
  }

  function renderSkillsPane() {
    const pane = getEl('extensions-skills-pane');
    if (!pane) return;
    clearEl(pane);

    const skills = state.extensionsData.skills || [];
    if (skills.length === 0) {
      pane.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, 'No skills installed'),
        el('span', { className: 'text-sm' }, 'Import a SKILL.md bundle or place skills in the workspace skills directory.')
      ));
      return;
    }

    for (const sk of skills) {
      const badges = [
        el('span', { className: `pill ${sk.enabled ? 'pill-success' : 'pill-muted'}` }, sk.enabled ? 'Enabled' : 'Disabled')
      ];
      if (sk.source) badges.push(el('span', { className: 'ext-badge' }, sk.source));
      if (sk.has_scripts) badges.push(el('span', { className: 'ext-badge' }, 'Scripts'));
      if (sk.has_references) badges.push(el('span', { className: 'ext-badge' }, 'References'));
      if (Array.isArray(sk.resources) && sk.resources.length > 0) {
        badges.push(el('span', { className: 'pill pill-muted' }, `${sk.resources.length} resource${sk.resources.length === 1 ? '' : 's'}`));
      }

      pane.appendChild(el('article', { className: 'ext-card' },
        el('div', { className: 'ext-card-header' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title truncate' }, sk.name || sk.id),
            el('div', { className: 'text-sm text-muted mono truncate' }, sk.id)
          ),
          el('div', { className: 'agent-badges' }, badges)
        ),
        el('p', { className: 'text-sm text-secondary' }, sk.description || 'No description provided.'),
        sk.dir_path ? el('div', { className: 'text-sm text-muted mono truncate' }, sk.dir_path) : null,
        el('div', { className: 'card-actions' },
          el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            onClick: () => openSkillDetailModal(sk.id)
          }, 'View'),
          el('button', {
            type: 'button',
            className: `btn btn-sm ${sk.enabled ? 'btn-secondary' : 'btn-success'}`,
            onClick: () => toggleExtensionItem('skill', sk.id, !sk.enabled)
          }, sk.enabled ? 'Disable' : 'Enable'),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteExtensionItem('skill', sk.id, sk.name || sk.id)
          }, 'Delete')
        )
      ));
    }
  }

  function renderMcpPane() {
    const pane = getEl('extensions-mcp-pane');
    if (!pane) return;
    clearEl(pane);

    const servers = state.extensionsData.mcp_servers || [];
    if (servers.length === 0) {
      pane.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, 'No MCP servers configured'),
        el('span', { className: 'text-sm' }, 'Add an MCP server to discover and expose external tools to agents.')
      ));
      return;
    }

    for (const srv of servers) {
      const badges = [
        el('span', { className: `pill ${srv.enabled ? 'pill-success' : 'pill-muted'}` }, srv.enabled ? 'Enabled' : 'Disabled'),
        el('span', { className: `pill ${statusPillClass(srv.status)}` }, srv.status || 'unknown')
      ];
      if (srv.has_auth) badges.push(el('span', { className: 'pill pill-info' }, 'Auth'));
      if (srv.trust_local) badges.push(el('span', { className: 'pill pill-warning' }, 'Trust Local'));

      const headerEntries = srv.masked_headers && typeof srv.masked_headers === 'object'
        ? Object.entries(srv.masked_headers)
        : [];
      const tools = Array.isArray(srv.tools) ? srv.tools : [];

      pane.appendChild(el('article', { className: 'ext-card' },
        el('div', { className: 'ext-card-header' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title mono truncate' }, srv.id),
            el('div', { className: 'text-sm text-muted mono truncate' }, srv.url)
          ),
          el('div', { className: 'agent-badges' }, badges)
        ),
        srv.last_synced_at
          ? el('div', { className: 'text-sm text-muted' }, `Last synced: ${fmtDateTime(srv.last_synced_at)}`)
          : null,
        headerEntries.length > 0
          ? el('div', { className: 'ext-tools-list' },
              headerEntries.map(([hk, hv]) => el('span', { className: 'pill pill-muted' }, `${hk}: ${hv}`))
            )
          : null,
        srv.last_error
          ? el('div', { className: 'alert alert-warning text-sm' }, srv.last_error)
          : null,
        tools.length > 0
          ? el('div', { className: 'field' },
              el('span', { className: 'text-sm text-muted' }, `Discovered Tools (${tools.length})`),
              el('div', { className: 'ext-tools-list' },
                tools.map((t) => el('span', {
                  className: 'ext-badge',
                  title: t.description || t.namespaced_id
                }, t.tool_name || t.namespaced_id))
              )
            )
          : el('div', { className: 'text-sm text-muted' }, 'No tools discovered yet. Click Sync / Test to probe server.'),
        el('div', { className: 'card-actions' },
          el('button', {
            type: 'button',
            className: 'btn btn-primary btn-sm',
            onClick: () => syncMcpServerItem(srv.id)
          }, 'Sync / Test'),
          el('button', {
            type: 'button',
            className: `btn btn-sm ${srv.enabled ? 'btn-secondary' : 'btn-success'}`,
            onClick: () => toggleExtensionItem('mcp', srv.id, !srv.enabled)
          }, srv.enabled ? 'Disable' : 'Enable'),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteExtensionItem('mcp', srv.id, srv.id)
          }, 'Delete')
        )
      ));
    }
  }

  function renderPluginsPane() {
    const pane = getEl('extensions-plugins-pane');
    if (!pane) return;
    clearEl(pane);

    const plugins = state.extensionsData.plugins || [];
    if (plugins.length === 0) {
      pane.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, 'No plugins installed'),
        el('span', { className: 'text-sm' }, 'Import a plugin manifest JSON to register rules, skills, and tools.')
      ));
      return;
    }

    for (const pl of plugins) {
      const badges = [
        el('span', { className: `pill ${pl.enabled ? 'pill-success' : 'pill-muted'}` }, pl.enabled ? 'Enabled' : 'Disabled'),
        el('span', { className: 'ext-badge' }, `v${pl.version || '0.0.0'}`),
        el('span', { className: 'pill pill-muted' }, `${pl.rules_count ?? 0} rules`),
        el('span', { className: 'pill pill-muted' }, `${pl.skills_count ?? 0} skills`)
      ];
      if (pl.has_mcp) badges.push(el('span', { className: 'pill pill-info' }, 'MCP'));

      const tools = Array.isArray(pl.tools) ? pl.tools : [];
      pane.appendChild(el('article', { className: 'ext-card' },
        el('div', { className: 'ext-card-header' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title truncate' }, pl.display_name || pl.name || pl.id),
            el('div', { className: 'text-sm text-muted mono truncate' }, pl.id)
          ),
          el('div', { className: 'agent-badges' }, badges)
        ),
        el('p', { className: 'text-sm text-secondary' }, pl.description || 'No description provided.'),
        tools.length > 0
          ? el('div', { className: 'ext-tools-list' },
              tools.map((toolId) => el('span', { className: 'ext-badge' }, toolId))
            )
          : null,
        el('div', { className: 'card-actions' },
          el('button', {
            type: 'button',
            className: `btn btn-sm ${pl.enabled ? 'btn-secondary' : 'btn-success'}`,
            onClick: () => toggleExtensionItem('plugin', pl.id, !pl.enabled)
          }, pl.enabled ? 'Disable' : 'Enable'),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteExtensionItem('plugin', pl.id, pl.display_name || pl.name || pl.id)
          }, 'Delete')
        )
      ));
    }
  }

  async function openSkillDetailModal(skillId) {
    try {
      const detail = await apiFetch(`/api/v1/extensions/skills/${encodeURIComponent(skillId)}`);
      const resources = Array.isArray(detail.resources) ? detail.resources : [];
      const bodyNodes = [
        detail.description
          ? el('p', { className: 'text-sm text-secondary' }, detail.description)
          : null,
        resources.length > 0
          ? el('div', { className: 'ext-tools-list' },
              resources.map((r) => el('span', { className: 'ext-badge' }, r))
            )
          : null,
        el('div', { className: 'msg-content' },
          renderMarkdownNodes(detail.instructions || detail.raw_markdown || 'No skill instructions.')
        )
      ];
      openExtensionModal(detail.name || detail.id || skillId, bodyNodes, [
        el('button', { type: 'button', className: 'btn btn-primary', onClick: closeExtensionModal }, 'Close')
      ]);
    } catch (err) {
      showToast(`Failed to load skill details: ${err.message}`, 'danger');
    }
  }

  function openImportSkillModal() {
    const slugIn = el('input', {
      id: 'ext-skill-slug',
      type: 'text',
      className: 'input mono',
      placeholder: 'optional-skill-slug'
    });
    const fileIn = el('input', {
      id: 'ext-skill-file',
      type: 'file',
      accept: '.md,.markdown,.txt',
      className: 'input'
    });
    const mdIn = el('textarea', {
      id: 'ext-skill-markdown',
      className: 'input mono',
      rows: 10,
      required: true,
      placeholder: '---\nname: my-skill\ndescription: Brief summary\n---\n\n# Instructions\n...'
    });

    fileIn.addEventListener('change', async () => {
      const file = fileIn.files && fileIn.files[0];
      if (!file) return;
      try {
        const text = await file.text();
        mdIn.value = text;
        if (!slugIn.value.trim()) {
          const suggested = file.name.replace(/\.(md|markdown|txt)$/i, '').toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '');
          if (suggested && suggested !== 'skill') slugIn.value = suggested;
        }
      } catch (err) {
        showToast(`Failed to read file: ${err.message}`, 'danger');
      }
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const markdown = mdIn.value.trim();
        if (!markdown) {
          showToast('Skill markdown is required.', 'warning');
          return;
        }
        const slugVal = slugIn.value.trim();
        try {
          await apiFetch('/api/v1/extensions/skills/import', {
            method: 'POST',
            body: {
              slug: slugVal || null,
              markdown: mdIn.value
            }
          });
          closeExtensionModal();
          state.extensionsTab = 'skills';
          showToast('Skill imported.', 'success');
          await loadExtensionsSection();
        } catch (err) {
          showToast(`Import skill failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'ext-skill-slug' }, 'Skill Slug (optional)'),
        slugIn
      ),
      el('div', { className: 'field' },
        el('label', { for: 'ext-skill-file' }, 'Load from .md file (optional)'),
        fileIn
      ),
      el('div', { className: 'field' },
        el('label', { for: 'ext-skill-markdown' }, 'SKILL.md Content'),
        mdIn
      )
    );

    openExtensionModal('Import Skill', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeExtensionModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Import Skill')
    ]);
  }

  function openAddMcpModal() {
    const idIn = el('input', {
      id: 'ext-mcp-id',
      type: 'text',
      className: 'input mono',
      placeholder: 'my-mcp-server'
    });
    const urlIn = el('input', {
      id: 'ext-mcp-url',
      type: 'url',
      className: 'input mono',
      placeholder: 'http://127.0.0.1:9000/mcp'
    });
    const apiKeyIn = el('input', {
      id: 'ext-mcp-apikey',
      type: 'password',
      className: 'input mono',
      placeholder: 'Optional bearer token / API key'
    });
    const trustCb = el('input', {
      id: 'ext-mcp-trust-local',
      type: 'checkbox',
      checked: false
    });
    const rawJsonIn = el('textarea', {
      id: 'ext-mcp-raw-json',
      className: 'input mono',
      rows: 7,
      placeholder: '{\n  "mcpServers": {\n    "server-id": {\n      "url": "http://127.0.0.1:9000/mcp",\n      "trustLocal": true\n    }\n  }\n}'
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        let rawJson = rawJsonIn.value.trim();
        if (!rawJson) {
          const srvId = idIn.value.trim();
          const srvUrl = urlIn.value.trim();
          if (!srvId || !srvUrl) {
            showToast('Provide either Server ID + URL or full mcpServers JSON.', 'warning');
            return;
          }
          const entry = {
            url: srvUrl,
            trustLocal: trustCb.checked
          };
          const key = apiKeyIn.value.trim();
          if (key) entry.apiKey = key;
          rawJson = JSON.stringify({ mcpServers: { [srvId]: entry } }, null, 2);
        }

        try {
          await apiFetch('/api/v1/extensions/mcp', {
            method: 'POST',
            body: { raw_json: rawJson }
          });
          closeExtensionModal();
          state.extensionsTab = 'mcp';
          showToast('MCP server configuration saved.', 'success');
          await loadExtensionsSection();
        } catch (err) {
          showToast(`Add MCP server failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'form-grid' },
        el('div', { className: 'field' }, el('label', { for: 'ext-mcp-id' }, 'Server ID'), idIn),
        el('div', { className: 'field' }, el('label', { for: 'ext-mcp-url' }, 'Server URL'), urlIn),
        el('div', { className: 'field' }, el('label', { for: 'ext-mcp-apikey' }, 'API Key / Bearer Token (optional)'), apiKeyIn)
      ),
      el('label', { className: 'checkbox-row' },
        trustCb,
        el('span', null, 'Trust Local / LAN Endpoint')
      ),
      el('div', { className: 'field' },
        el('label', { for: 'ext-mcp-raw-json' }, 'Or Paste Full {"mcpServers": {...}} JSON'),
        rawJsonIn
      )
    );

    openExtensionModal('Add MCP Server', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeExtensionModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Save MCP Server')
    ]);
  }

  function openImportPluginModal() {
    const fileIn = el('input', {
      id: 'ext-plugin-file',
      type: 'file',
      accept: '.json,application/json',
      className: 'input'
    });
    const manifestIn = el('textarea', {
      id: 'ext-plugin-manifest',
      className: 'input mono',
      rows: 10,
      required: true,
      placeholder: '{\n  "name": "my-plugin",\n  "display_name": "My Plugin",\n  "version": "1.0.0",\n  "description": "Custom rules and tools"\n}'
    });

    fileIn.addEventListener('change', async () => {
      const file = fileIn.files && fileIn.files[0];
      if (!file) return;
      try {
        manifestIn.value = await file.text();
      } catch (err) {
        showToast(`Failed to read manifest file: ${err.message}`, 'danger');
      }
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const manifestJson = manifestIn.value.trim();
        if (!manifestJson) {
          showToast('Plugin manifest JSON is required.', 'warning');
          return;
        }
        try {
          await apiFetch('/api/v1/extensions/plugins/import', {
            method: 'POST',
            body: { manifest_json: manifestJson }
          });
          closeExtensionModal();
          state.extensionsTab = 'plugins';
          showToast('Plugin imported.', 'success');
          await loadExtensionsSection();
        } catch (err) {
          showToast(`Import plugin failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'ext-plugin-file' }, 'Load manifest from .json file (optional)'),
        fileIn
      ),
      el('div', { className: 'field' },
        el('label', { for: 'ext-plugin-manifest' }, 'Plugin Manifest JSON'),
        manifestIn
      )
    );

    openExtensionModal('Import Plugin', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeExtensionModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Import Plugin')
    ]);
  }

  async function toggleExtensionItem(kind, id, enabled) {
    try {
      await apiFetch(`/api/v1/extensions/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`, {
        method: 'PATCH',
        body: { enabled: Boolean(enabled) }
      });
      showToast(`${kind} "${id}" ${enabled ? 'enabled' : 'disabled'}.`, 'success');
      await loadExtensionsSection();
    } catch (err) {
      showToast(`Toggle ${kind} failed: ${err.message}`, 'danger');
    }
  }

  async function syncMcpServerItem(id) {
    try {
      const updated = await apiFetch(`/api/v1/extensions/mcp/${encodeURIComponent(id)}/sync`, {
        method: 'POST',
        body: {}
      });
      const count = Array.isArray(updated && updated.tools) ? updated.tools.length : 0;
      showToast(`MCP "${id}" synced (${count} tools).`, updated && updated.last_error ? 'warning' : 'success');
      await loadExtensionsSection();
    } catch (err) {
      showToast(`MCP sync failed: ${err.message}`, 'danger');
    }
  }

  function confirmDeleteExtensionItem(kind, id, label) {
    openExtensionModal(`Delete ${kind}`, [
      el('p', null, `Delete ${kind} "${label || id}"?`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeExtensionModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/extensions/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`, {
              method: 'DELETE'
            });
            closeExtensionModal();
            showToast(`Deleted ${kind} "${id}".`, 'success');
            await loadExtensionsSection();
          } catch (err) {
            showToast(`Delete ${kind} failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete')
    ]);
  }

  function openImageLightbox(src, caption) {
    if (!isSafeImageSrc(src)) return;
    const modal = getEl('image-lightbox-modal');
    const img = getEl('lightbox-img');
    const cap = getEl('lightbox-caption');
    const title = getEl('lightbox-title');
    const dl = getEl('lightbox-download');
    if (!modal || !img) return;
    img.src = src;
    img.alt = caption || 'Attached image preview';
    if (title) title.textContent = caption || 'Image Preview';
    if (cap) cap.textContent = caption || '';
    if (dl) {
      dl.href = src;
      dl.setAttribute('download', pathBasename(caption || 'image.png'));
    }
    modal.hidden = false;
    const closeBtn = getEl('lightbox-close');
    if (closeBtn) window.setTimeout(() => closeBtn.focus(), 20);
  }

  function closeImageLightbox() {
    const modal = getEl('image-lightbox-modal');
    const img = getEl('lightbox-img');
    if (modal) modal.hidden = true;
    if (img) img.removeAttribute('src');
  }

  function findClosingInlineDelimiter(str, start, delim) {
    const len = delim.length;
    for (let i = start; i <= str.length - len; i++) {
      if (str[i] === '\\') {
        i++;
        continue;
      }
      if (str.slice(i, i + len) === delim) {
        if (delim === '*' && (str[i + 1] === '*' || (i > start && str[i - 1] === '*'))) {
          continue;
        }
        return i;
      }
    }
    return -1;
  }

  function parseMarkdownLink(str, start) {
    // Expects str[start] === '['
    let labelEnd = -1;
    for (let i = start + 1; i < str.length; i++) {
      if (str[i] === '\\') {
        i++;
        continue;
      }
      if (str[i] === '[' || str[i] === '\n') return null;
      if (str[i] === ']') {
        labelEnd = i;
        break;
      }
    }
    if (labelEnd === -1 || str[labelEnd + 1] !== '(') return null;
    let depth = 1;
    let urlEnd = -1;
    for (let i = labelEnd + 2; i < str.length; i++) {
      const ch = str[i];
      if (ch === '\\' && i + 1 < str.length) {
        i++;
        continue;
      }
      if (ch === '\n') return null;
      if (ch === '(') depth++;
      else if (ch === ')') {
        depth--;
        if (depth === 0) {
          urlEnd = i;
          break;
        }
      }
    }
    if (urlEnd === -1) return null;
    let rawUrl = str.slice(labelEnd + 2, urlEnd).trim();
    const titleMatch = rawUrl.match(/^(\S+)\s+(?:"[^"]*"|'[^']*')$/);
    if (titleMatch) {
      rawUrl = titleMatch[1];
    }
    if (rawUrl.startsWith('<') && rawUrl.endsWith('>') && rawUrl.length > 2) {
      rawUrl = rawUrl.slice(1, -1).trim();
    }
    return {
      label: str.slice(start + 1, labelEnd),
      url: rawUrl,
      nextIndex: urlEnd + 1
    };
  }

  function renderInlineNodes(text, depth) {
    const frag = document.createDocumentFragment();
    if (!text) return frag;
    const str = String(text);
    const level = depth || 0;
    let buf = '';
    const flushBuf = () => {
      if (buf) {
        frag.appendChild(document.createTextNode(buf));
        buf = '';
      }
    };

    let i = 0;
    while (i < str.length) {
      const ch = str[i];

      // Backslash escape
      if (ch === '\\' && i + 1 < str.length && /[\\`*_{}[\]()#+\-.!~|]/.test(str[i + 1])) {
        buf += str[i + 1];
        i += 2;
        continue;
      }

      // Inline code: `...` or ``...``
      if (ch === '`') {
        let tickLen = 1;
        while (i + tickLen < str.length && str[i + tickLen] === '`') tickLen++;
        const closer = '`'.repeat(tickLen);
        let searchFrom = i + tickLen;
        let end = -1;
        while (searchFrom < str.length) {
          const cand = str.indexOf(closer, searchFrom);
          if (cand === -1) break;
          if (str[cand + tickLen] !== '`') {
            end = cand;
            break;
          }
          searchFrom = cand + 1;
        }
        if (end > i + tickLen && !str.slice(i + tickLen, end).includes('\n')) {
          flushBuf();
          const rawCode = str.slice(i + tickLen, end);
          const trimmedCode = tickLen > 1 && rawCode.startsWith(' ') && rawCode.endsWith(' ') && rawCode.trim()
            ? rawCode.slice(1, -1)
            : rawCode;
          frag.appendChild(el('code', { className: 'md-inline-code' }, document.createTextNode(trimmedCode)));
          i = end + tickLen;
          continue;
        }
      }

      if (level < 4) {
        // Inline image: ![alt](src)
        if (ch === '!' && str[i + 1] === '[') {
          const parsed = parseMarkdownLink(str, i + 1);
          if (parsed) {
            flushBuf();
            if (isSafeImageSrc(parsed.url)) {
              frag.appendChild(el('img', {
                className: 'md-inline-img',
                src: parsed.url,
                alt: parsed.label || 'Image'
              }));
            } else {
              frag.appendChild(document.createTextNode(str.slice(i, parsed.nextIndex)));
            }
            i = parsed.nextIndex;
            continue;
          }
        }

        // Link: [label](url)
        if (ch === '[') {
          const parsed = parseMarkdownLink(str, i);
          if (parsed) {
            flushBuf();
            if (isSafeMarkdownLinkUrl(parsed.url)) {
              frag.appendChild(el('a', {
                className: 'md-link',
                href: parsed.url,
                target: '_blank',
                rel: 'noopener noreferrer'
              }, renderInlineNodes(parsed.label, level + 1)));
            } else {
              frag.appendChild(document.createTextNode(str.slice(i, parsed.nextIndex)));
            }
            i = parsed.nextIndex;
            continue;
          }
        }

        // Bold: **...** or __...__
        if ((ch === '*' && str[i + 1] === '*') || (ch === '_' && str[i + 1] === '_')) {
          const delim = ch + ch;
          const end = findClosingInlineDelimiter(str, i + 2, delim);
          if (end > i + 2) {
            flushBuf();
            frag.appendChild(el('strong', { className: 'md-strong' }, renderInlineNodes(str.slice(i + 2, end), level + 1)));
            i = end + 2;
            continue;
          }
        }

        // Strikethrough: ~~...~~
        if (ch === '~' && str[i + 1] === '~') {
          const end = findClosingInlineDelimiter(str, i + 2, '~~');
          if (end > i + 2) {
            flushBuf();
            frag.appendChild(el('del', { className: 'md-del' }, renderInlineNodes(str.slice(i + 2, end), level + 1)));
            i = end + 2;
            continue;
          }
        }

        // Italic: *...* or _..._
        if (ch === '*' || ch === '_') {
          const prevAlnum = i > 0 && /[a-zA-Z0-9]/.test(str[i - 1]);
          const nextSpace = i + 1 >= str.length || /\s/.test(str[i + 1]);
          if (!(ch === '_' && prevAlnum) && !nextSpace) {
            const end = findClosingInlineDelimiter(str, i + 1, ch);
            if (end > i + 1 && !/\s/.test(str[end - 1])) {
              flushBuf();
              frag.appendChild(el('em', { className: 'md-em' }, renderInlineNodes(str.slice(i + 1, end), level + 1)));
              i = end + 1;
              continue;
            }
          }
        }
      }

      buf += ch;
      i++;
    }

    flushBuf();
    return frag;
  }

  function isTableSeparatorLine(line) {
    return /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$/.test(String(line || ''));
  }

  function splitTableRow(line) {
    let s = String(line || '').trim();
    if (s.startsWith('|')) s = s.slice(1);
    if (s.endsWith('|')) s = s.slice(0, -1);
    const cells = [];
    let cur = '';
    let inCode = false;
    for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      if (ch === '\\' && s[i + 1] === '|') {
        cur += '|';
        i++;
        continue;
      }
      if (ch === '`') {
        inCode = !inCode;
        cur += ch;
        continue;
      }
      if (ch === '|' && !inCode) {
        cells.push(cur.trim());
        cur = '';
        continue;
      }
      cur += ch;
    }
    cells.push(cur.trim());
    return cells;
  }

  function renderMarkdownNodes(text, depth) {
    const frag = document.createDocumentFragment();
    if (text === null || text === undefined) return frag;
    const curDepth = typeof depth === 'number' ? depth : 0;
    const normalized = String(text).replace(/\r\n?/g, '\n');
    if (!normalized) return frag;
    if (normalized.length > 262144) {
      frag.appendChild(el('pre', null, document.createTextNode(`${normalized.slice(0, 262144)}\n...[truncated]`)));
      return frag;
    }

    const lines = normalized.split('\n');
    let i = 0;

    while (i < lines.length) {
      const startI = i;
      const line = lines[i];

      // Skip blank lines
      if (!line.trim()) {
        i++;
        continue;
      }

      // Fenced code block (``` or ~~~)
      const fenceMatch = line.match(/^\s*(```+|~~~+)\s*([^\s`~]*)/);
      if (fenceMatch) {
        const fenceChar = fenceMatch[1][0];
        const fenceLen = fenceMatch[1].length;
        const lang = (fenceMatch[2] || '').toLowerCase();
        const codeLines = [];
        i++;
        while (i < lines.length) {
          const closeMatch = lines[i].match(/^\s*(```+|~~~+)\s*$/);
          if (closeMatch && closeMatch[1][0] === fenceChar && closeMatch[1].length >= fenceLen) {
            i++;
            break;
          }
          codeLines.push(lines[i]);
          i++;
        }
        const codeText = codeLines.join('\n');
        const copyBtn = el('button', {
          type: 'button',
          className: 'md-code-btn',
          onClick: () => copyTextWithFeedback(copyBtn, codeText, 'Copy')
        }, 'Copy');
        const actionBtns = [copyBtn];
        if (lang === 'html' || lang === 'htm') {
          actionBtns.push(el('button', {
            type: 'button',
            className: 'md-code-btn',
            onClick: () => openHtmlSnippetPreview(codeText)
          }, 'Preview HTML'));
        }
        frag.appendChild(el('div', { className: 'md-code-block' },
          el('div', { className: 'md-code-header' },
            el('span', { className: 'md-code-lang' }, lang || 'text'),
            el('div', { className: 'md-code-actions' }, actionBtns)
          ),
          el('pre', null, el('code', null, document.createTextNode(codeText)))
        ));
        continue;
      }

      // Horizontal rule
      if (/^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(line)) {
        frag.appendChild(el('hr', { className: 'md-hr' }));
        i++;
        continue;
      }

      // ATX Heading (# .. ######, capped visually at h1..h4)
      const headingMatch = line.match(/^\s{0,3}(#{1,6})\s+(.+?)(?:\s+#+)?\s*$/);
      if (headingMatch) {
        const level = Math.min(4, headingMatch[1].length);
        frag.appendChild(el(`h${level}`, { className: `md-h${level}` }, renderInlineNodes(headingMatch[2], 0)));
        i++;
        continue;
      }

      // Blockquote
      if (/^\s{0,3}>\s?/.test(line)) {
        const quoteLines = [];
        while (i < lines.length && /^\s{0,3}>\s?/.test(lines[i])) {
          quoteLines.push(lines[i].replace(/^\s{0,3}>\s?/, ''));
          i++;
        }
        const quoteText = quoteLines.join('\n');
        frag.appendChild(el('blockquote', { className: 'md-blockquote' },
          curDepth < 6
            ? renderMarkdownNodes(quoteText, curDepth + 1)
            : el('p', { className: 'md-p' }, renderInlineNodes(quoteText, 0))
        ));
        continue;
      }

      // Markdown pipe table
      if (line.includes('|') && i + 1 < lines.length && isTableSeparatorLine(lines[i + 1])) {
        const headers = splitTableRow(line);
        i += 2; // skip header + separator
        const bodyRows = [];
        while (i < lines.length && lines[i].trim() && lines[i].includes('|') && !/^\s*(```|~~~|#|>)/.test(lines[i])) {
          bodyRows.push(splitTableRow(lines[i]));
          i++;
        }
        const theadRow = el('tr', null, headers.map((h) => el('th', null, renderInlineNodes(h, 0))));
        const tbody = el('tbody', null, bodyRows.map((row) =>
          el('tr', null, headers.map((_, colIdx) => el('td', null, renderInlineNodes(row[colIdx] || '', 0))))
        ));
        frag.appendChild(el('div', { className: 'md-table-wrap' },
          el('table', { className: 'data-table md-table' },
            el('thead', null, theadRow),
            tbody
          )
        ));
        continue;
      }

      // Unordered list
      if (/^\s*[-*+]\s+\S/.test(line)) {
        const items = [];
        while (i < lines.length && /^\s*[-*+]\s+(.+)$/.test(lines[i])) {
          const m = lines[i].match(/^\s*[-*+]\s+(.+)$/);
          items.push(el('li', null, renderInlineNodes(m[1], 0)));
          i++;
        }
        if (items.length > 0) {
          frag.appendChild(el('ul', { className: 'md-ul' }, items));
          continue;
        }
      }

      // Ordered list
      if (/^\s*\d+[.)]\s+\S/.test(line)) {
        const items = [];
        while (i < lines.length && /^\s*\d+[.)]\s+(.+)$/.test(lines[i])) {
          const m = lines[i].match(/^\s*\d+[.)]\s+(.+)$/);
          items.push(el('li', null, renderInlineNodes(m[1], 0)));
          i++;
        }
        if (items.length > 0) {
          frag.appendChild(el('ol', { className: 'md-ol' }, items));
          continue;
        }
      }

      // Paragraph (accumulate lines until blank line or block starter)
      const paraLines = [];
      while (
        i < lines.length &&
        lines[i].trim() &&
        !/^\s*(```+|~~~+)/.test(lines[i]) &&
        !/^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(lines[i]) &&
        !/^\s{0,3}#{1,6}\s+\S/.test(lines[i]) &&
        !/^\s{0,3}>\s?/.test(lines[i]) &&
        !/^\s*[-*+]\s+\S/.test(lines[i]) &&
        !/^\s*\d+[.)]\s+\S/.test(lines[i]) &&
        !(lines[i].includes('|') && i + 1 < lines.length && isTableSeparatorLine(lines[i + 1]))
      ) {
        paraLines.push(lines[i]);
        i++;
      }
      if (paraLines.length > 0) {
        frag.appendChild(el('p', { className: 'md-p' }, renderInlineNodes(paraLines.join('\n'), 0)));
      }
      if (i === startI) {
        frag.appendChild(el('p', { className: 'md-p' }, renderInlineNodes(line, 0)));
        i++;
      }
    }

    return frag;
  }

  function renderMarkdownIncremental(contentEl, bucket, text) {
    const normalized = String(text || '').replace(/\r\n?/g, '\n');
    if (!normalized) {
      clearEl(contentEl);
      bucket._streamRenderState = null;
      return;
    }

    let state = bucket._streamRenderState;
    if (!state || !normalized.startsWith(state.committedText)) {
      clearEl(contentEl);
      state = {
        committedText: '',
        committedLineIndex: 0,
        activeBlock: null
      };
      bucket._streamRenderState = state;
    }

    const lines = normalized.split('\n');
    let i = state.committedLineIndex;

    function createCodeBlockEl(lang, codeText) {
      const codeNode = document.createTextNode(codeText);
      const copyBtn = el('button', {
        type: 'button',
        className: 'md-code-btn',
        onClick: () => copyTextWithFeedback(copyBtn, codeNode.data, 'Copy')
      }, 'Copy');
      const actionBtns = [copyBtn];
      if (lang === 'html' || lang === 'htm') {
        actionBtns.push(el('button', {
          type: 'button',
          className: 'md-code-btn',
          onClick: () => openHtmlSnippetPreview(codeNode.data)
        }, 'Preview HTML'));
      }
      const langSpan = el('span', { className: 'md-code-lang' }, lang || 'text');
      const actionsWrap = el('div', { className: 'md-code-actions' }, actionBtns);
      const codeEl = el('code', null, codeNode);
      const blockEl = el('div', { className: 'md-code-block' },
        el('div', { className: 'md-code-header' },
          langSpan,
          actionsWrap
        ),
        el('pre', null, codeEl)
      );
      return { blockEl, codeNode, copyBtn, langSpan, actionsWrap, lang };
    }

    function syncCodeBlockHeader(active, currentLang) {
      if (!active) return;
      if (currentLang && currentLang !== active.lang) {
        active.lang = currentLang;
        if (active.langSpan) active.langSpan.textContent = currentLang;
        if ((currentLang === 'html' || currentLang === 'htm') && active.actionsWrap && active.actionsWrap.childNodes.length === 1) {
          active.actionsWrap.appendChild(el('button', {
            type: 'button',
            className: 'md-code-btn',
            onClick: () => openHtmlSnippetPreview(active.codeNode.data)
          }, 'Preview HTML'));
        }
      }
    }

    function updateInlineContainer(container, inlineText) {
      clearEl(container);
      container.appendChild(renderInlineNodes(inlineText, 0));
    }

    function getCommittedPrefix(linesArr, committedIdx) {
      if (committedIdx <= 0) return '';
      if (committedIdx < linesArr.length) {
        return linesArr.slice(0, committedIdx).join('\n') + '\n';
      }
      return linesArr.slice(0, committedIdx).join('\n');
    }

    while (i < lines.length) {
      const startI = i;
      const line = lines[i];

      if (!line.trim()) {
        i++;
        continue;
      }

      // Fenced code block (``` or ~~~)
      const fenceMatch = line.match(/^\s*(```+|~~~+)\s*([^\s`~]*)/);
      if (fenceMatch) {
        const fenceChar = fenceMatch[1][0];
        const fenceLen = fenceMatch[1].length;
        const lang = (fenceMatch[2] || '').toLowerCase();
        const codeLines = [];
        let closed = false;
        i++;
        while (i < lines.length) {
          const closeMatch = lines[i].match(/^\s*(```+|~~~+)\s*$/);
          if (closeMatch && closeMatch[1][0] === fenceChar && closeMatch[1].length >= fenceLen) {
            closed = true;
            i++;
            break;
          }
          codeLines.push(lines[i]);
          i++;
        }

        const codeText = codeLines.join('\n');

        if (closed) {
          if (state.activeBlock && state.activeBlock.type === 'code' && state.activeBlock.startLine === startI) {
            syncCodeBlockHeader(state.activeBlock, lang);
            state.activeBlock.codeNode.data = codeText;
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const { blockEl } = createCodeBlockEl(lang, codeText);
            contentEl.appendChild(blockEl);
          }
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'code' && state.activeBlock.startLine === startI) {
            syncCodeBlockHeader(state.activeBlock, lang);
            state.activeBlock.codeNode.data = codeText;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const { blockEl, codeNode, copyBtn, langSpan, actionsWrap } = createCodeBlockEl(lang, codeText);
            contentEl.appendChild(blockEl);
            state.activeBlock = {
              type: 'code',
              startLine: startI,
              el: blockEl,
              codeNode,
              copyBtn,
              langSpan,
              actionsWrap,
              lang
            };
          }
          break;
        }
      }

      // Horizontal rule
      if (/^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(line)) {
        if (i + 1 < lines.length) {
          if (state.activeBlock && state.activeBlock.type === 'hr' && state.activeBlock.startLine === startI) {
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            contentEl.appendChild(el('hr', { className: 'md-hr' }));
          }
          i++;
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'hr' && state.activeBlock.startLine === startI) {
            // Already active hr
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const hrEl = el('hr', { className: 'md-hr' });
            contentEl.appendChild(hrEl);
            state.activeBlock = { type: 'hr', startLine: startI, el: hrEl };
          }
          break;
        }
      }

      // ATX Heading
      const headingMatch = line.match(/^\s{0,3}(#{1,6})\s+(.+?)(?:\s+#+)?\s*$/);
      if (headingMatch) {
        const level = Math.min(4, headingMatch[1].length);
        const headingText = headingMatch[2];
        if (i + 1 < lines.length) {
          if (state.activeBlock && state.activeBlock.type === 'heading' && state.activeBlock.startLine === startI) {
            updateInlineContainer(state.activeBlock.el, headingText);
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const hEl = el(`h${level}`, { className: `md-h${level}` });
            updateInlineContainer(hEl, headingText);
            contentEl.appendChild(hEl);
          }
          i++;
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'heading' && state.activeBlock.startLine === startI) {
            updateInlineContainer(state.activeBlock.el, headingText);
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const hEl = el(`h${level}`, { className: `md-h${level}` });
            updateInlineContainer(hEl, headingText);
            contentEl.appendChild(hEl);
            state.activeBlock = { type: 'heading', startLine: startI, el: hEl };
          }
          break;
        }
      }

      // Blockquote
      if (/^\s{0,3}>\s?/.test(line)) {
        const quoteLines = [];
        let hasTerminator = false;
        while (i < lines.length && /^\s{0,3}>\s?/.test(lines[i])) {
          quoteLines.push(lines[i].replace(/^\s{0,3}>\s?/, ''));
          i++;
        }
        if (i < lines.length - 1 && !lines[i].trim()) {
          hasTerminator = true;
          i++;
        }
        const quoteText = quoteLines.join('\n');
        const isCompleteBlock = hasTerminator || (i < lines.length && Boolean(lines[i].trim()));
        if (isCompleteBlock) {
          if (state.activeBlock && state.activeBlock.type === 'quote' && state.activeBlock.startLine === startI) {
            clearEl(state.activeBlock.el);
            state.activeBlock.el.appendChild(renderMarkdownNodes(quoteText, 1));
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const qEl = el('blockquote', { className: 'md-blockquote' }, renderMarkdownNodes(quoteText, 1));
            contentEl.appendChild(qEl);
          }
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'quote' && state.activeBlock.startLine === startI) {
            clearEl(state.activeBlock.el);
            state.activeBlock.el.appendChild(renderMarkdownNodes(quoteText, 1));
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const qEl = el('blockquote', { className: 'md-blockquote' }, renderMarkdownNodes(quoteText, 1));
            contentEl.appendChild(qEl);
            state.activeBlock = { type: 'quote', startLine: startI, el: qEl };
          }
          break;
        }
      }

      // Pipe table
      if (line.includes('|') && i + 1 < lines.length && isTableSeparatorLine(lines[i + 1])) {
        const headers = splitTableRow(line);
        i += 2;
        const bodyRows = [];
        let hasTerminator = false;
        while (i < lines.length && lines[i].trim() && lines[i].includes('|') && !/^\s*(```|~~~|#|>)/.test(lines[i])) {
          bodyRows.push(splitTableRow(lines[i]));
          i++;
        }
        if (i < lines.length - 1 && !lines[i].trim()) {
          hasTerminator = true;
          i++;
        }
        const populateTable = (wrap) => {
          clearEl(wrap);
          const theadRow = el('tr', null, headers.map((h) => el('th', null, renderInlineNodes(h, 0))));
          const tbody = el('tbody', null, bodyRows.map((row) =>
            el('tr', null, headers.map((_, colIdx) => el('td', null, renderInlineNodes(row[colIdx] || '', 0))))
          ));
          wrap.appendChild(el('table', { className: 'data-table md-table' },
            el('thead', null, theadRow),
            tbody
          ));
        };
        const isCompleteBlock = hasTerminator || (i < lines.length && Boolean(lines[i].trim()));
        if (isCompleteBlock) {
          if (state.activeBlock && state.activeBlock.type === 'table' && state.activeBlock.startLine === startI) {
            populateTable(state.activeBlock.el);
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const tblWrap = el('div', { className: 'md-table-wrap' });
            populateTable(tblWrap);
            contentEl.appendChild(tblWrap);
          }
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'table' && state.activeBlock.startLine === startI) {
            populateTable(state.activeBlock.el);
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const tblWrap = el('div', { className: 'md-table-wrap' });
            populateTable(tblWrap);
            contentEl.appendChild(tblWrap);
            state.activeBlock = { type: 'table', startLine: startI, el: tblWrap };
          }
          break;
        }
      }

      // Unordered list
      if (/^\s*[-*+]\s+\S/.test(line)) {
        const items = [];
        let hasTerminator = false;
        while (i < lines.length && /^\s*[-*+](\s+.*)?$/.test(lines[i])) {
          items.push(lines[i].replace(/^\s*[-*+]\s*/, ''));
          i++;
        }
        if (i < lines.length - 1 && !lines[i].trim()) {
          hasTerminator = true;
          i++;
        }
        const populateUl = (ulEl) => {
          clearEl(ulEl);
          for (const it of items) {
            ulEl.appendChild(el('li', null, renderInlineNodes(it, 0)));
          }
        };
        const isCompleteBlock = hasTerminator || (i < lines.length && Boolean(lines[i].trim()));
        if (isCompleteBlock) {
          if (state.activeBlock && state.activeBlock.type === 'ul' && state.activeBlock.startLine === startI) {
            populateUl(state.activeBlock.el);
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const ulEl = el('ul', { className: 'md-ul' });
            populateUl(ulEl);
            contentEl.appendChild(ulEl);
          }
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'ul' && state.activeBlock.startLine === startI) {
            populateUl(state.activeBlock.el);
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const ulEl = el('ul', { className: 'md-ul' });
            populateUl(ulEl);
            contentEl.appendChild(ulEl);
            state.activeBlock = { type: 'ul', startLine: startI, el: ulEl };
          }
          break;
        }
      }

      // Ordered list
      if (/^\s*\d+[.)]\s+\S/.test(line)) {
        const items = [];
        let hasTerminator = false;
        while (i < lines.length && /^\s*\d+[.)]?(\s.*)?$/.test(lines[i])) {
          items.push(lines[i].replace(/^\s*\d+[.)]?\s*/, ''));
          i++;
        }
        if (i < lines.length - 1 && !lines[i].trim()) {
          hasTerminator = true;
          i++;
        }
        const populateOl = (olEl) => {
          clearEl(olEl);
          for (const it of items) {
            olEl.appendChild(el('li', null, renderInlineNodes(it, 0)));
          }
        };
        const isCompleteBlock = hasTerminator || (i < lines.length && Boolean(lines[i].trim()));
        if (isCompleteBlock) {
          if (state.activeBlock && state.activeBlock.type === 'ol' && state.activeBlock.startLine === startI) {
            populateOl(state.activeBlock.el);
            state.activeBlock = null;
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
              state.activeBlock = null;
            }
            const olEl = el('ol', { className: 'md-ol' });
            populateOl(olEl);
            contentEl.appendChild(olEl);
          }
          state.committedLineIndex = i;
          state.committedText = getCommittedPrefix(lines, i);
          continue;
        } else {
          if (state.activeBlock && state.activeBlock.type === 'ol' && state.activeBlock.startLine === startI) {
            populateOl(state.activeBlock.el);
          } else {
            if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
              state.activeBlock.el.remove();
            }
            const olEl = el('ol', { className: 'md-ol' });
            populateOl(olEl);
            contentEl.appendChild(olEl);
            state.activeBlock = { type: 'ol', startLine: startI, el: olEl };
          }
          break;
        }
      }

      // Paragraph
      const paraLines = [];
      let hasTerminator = false;
      while (
        i < lines.length &&
        lines[i].trim() &&
        !/^\s*(```+|~~~+)/.test(lines[i]) &&
        !/^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(lines[i]) &&
        !/^\s{0,3}#{1,6}\s+\S/.test(lines[i]) &&
        !/^\s{0,3}>\s?/.test(lines[i]) &&
        !/^\s*[-*+]\s+\S/.test(lines[i]) &&
        !/^\s*\d+[.)]\s+\S/.test(lines[i]) &&
        !(lines[i].includes('|') && i + 1 < lines.length && isTableSeparatorLine(lines[i + 1]))
      ) {
        paraLines.push(lines[i]);
        i++;
      }
      if (i < lines.length - 1 && !lines[i].trim()) {
        hasTerminator = true;
        i++;
      }

      const paraText = paraLines.join('\n');
      const isCompleteBlock = hasTerminator || (i < lines.length && Boolean(lines[i].trim()));

      if (isCompleteBlock) {
        if (state.activeBlock && state.activeBlock.type === 'para' && state.activeBlock.startLine === startI) {
          updateInlineContainer(state.activeBlock.el, paraText);
          state.activeBlock = null;
        } else {
          if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
            state.activeBlock.el.remove();
            state.activeBlock = null;
          }
          const pEl = el('p', { className: 'md-p' });
          updateInlineContainer(pEl, paraText);
          contentEl.appendChild(pEl);
        }
        state.committedLineIndex = i;
        state.committedText = getCommittedPrefix(lines, i);
        continue;
      } else {
        if (state.activeBlock && state.activeBlock.type === 'para' && state.activeBlock.startLine === startI) {
          updateInlineContainer(state.activeBlock.el, paraText);
        } else {
          if (state.activeBlock && state.activeBlock.el && state.activeBlock.el.parentNode === contentEl) {
            state.activeBlock.el.remove();
          }
          const pEl = el('p', { className: 'md-p' });
          updateInlineContainer(pEl, paraText);
          contentEl.appendChild(pEl);
          state.activeBlock = { type: 'para', startLine: startI, el: pEl };
        }
        break;
      }
    }
  }

  function extractPathFromToolArgs(rawArgs) {
    let obj = rawArgs;
    if (typeof obj === 'string') {
      try {
        obj = JSON.parse(obj);
      } catch (_) {
        return '';
      }
    }
    if (!obj || typeof obj !== 'object') return '';
    return sanitizeWorkspacePath(obj.path || obj.file_path || obj.filename || '');
  }

  function extractCreatedFilePaths(msg) {
    const found = new Set();
    const addPath = (raw) => {
      const clean = sanitizeWorkspacePath(raw);
      if (clean) found.add(clean);
    };

    const inspectToolEntry = (entry) => {
      if (!entry || typeof entry !== 'object') return;
      const name = String(entry.tool_id || entry.name || (entry.function && entry.function.name) || '').toLowerCase();
      if (name === 'write_file' || name === 'file_write' || name === 'create_file' || name === 'write_artifact') {
        const rawArgs = entry.arguments_json || entry.arguments || entry.input || (entry.function && entry.function.arguments);
        addPath(extractPathFromToolArgs(rawArgs));
      }
    };

    if (msg.tool_calls_json) {
      let parsed = msg.tool_calls_json;
      if (typeof parsed === 'string') {
        try {
          parsed = JSON.parse(parsed);
        } catch (_) {
          parsed = null;
        }
      }
      if (Array.isArray(parsed)) {
        for (const item of parsed) inspectToolEntry(item);
      } else if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.tool_calls)) {
          for (const item of parsed.tool_calls) inspectToolEntry(item);
        } else {
          inspectToolEntry(parsed);
        }
      }
    }

    if (Array.isArray(msg.liveTools)) {
      for (const tc of msg.liveTools) {
        inspectToolEntry(tc);
      }
    }

    if (
      typeof msg.content === 'string'
      && (msg.content.includes('[file:') || msg.content.includes('[FILE_CREATED:') || msg.content.includes('[ARTIFACT_CREATED:'))
    ) {
      const re = /\[(?:file|FILE_CREATED|ARTIFACT_CREATED):\s*([^\]\r\n]+?)\s*\]/gi;
      let match;
      while ((match = re.exec(msg.content)) !== null) {
        addPath(match[1]);
      }
    }

    return [...found];
  }

  function buildFileArtifactCard(filePath) {
    const isHtml = isHtmlFilePath(filePath);
    const isImg = isImageFilePath(filePath);
    const actions = [
      el('button', {
        type: 'button',
        className: 'btn btn-primary btn-sm',
        onClick: () => openWorkspaceFileInFilesTab(filePath, isHtml)
      }, 'Open Artifact')
    ];
    if (!isImg) {
      actions.push(
        el('button', {
          type: 'button',
          className: 'btn btn-secondary btn-sm',
          onClick: () => openWorkspaceFileFromChat(filePath, { editMode: true, autoRunHtml: false })
        }, 'Edit Script')
      );
    }
    if (isHtml) {
      actions.push(el('button', {
        type: 'button',
        className: 'btn btn-secondary btn-sm',
        onClick: () => openWorkspaceFileFromChat(filePath, { editMode: false, autoRunHtml: true })
      }, 'Preview HTML'));
    }
    actions.push(el('a', {
      className: 'btn btn-secondary btn-sm',
      href: `/api/v1/files/download?path=${encodeURIComponent(filePath)}`,
      download: pathBasename(filePath)
    }, 'Download'));

    return el('div', { className: 'file-artifact-card chat-artifact-card' },
      el('div', { className: 'file-artifact-info' },
        el('span', { className: `pill ${isHtml ? 'pill-info' : 'pill-muted'}` }, isHtml ? 'HTML App' : isImg ? 'Image' : 'Artifact'),
        el('span', { className: 'file-artifact-name mono' }, filePath)
      ),
      el('div', { className: 'file-artifact-actions' }, actions)
    );
  }

  /* -------------------- 2. Main Chat -------------------- */
  function sortConversationsList(list) {
    return (Array.isArray(list) ? list : []).slice().sort((a, b) => {
      const ao = Number.isFinite(Number(a.sort_order)) ? Number(a.sort_order) : 0;
      const bo = Number.isFinite(Number(b.sort_order)) ? Number(b.sort_order) : 0;
      if (ao !== bo) return ao - bo;
      return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
    });
  }

  async function loadConversations(searchQuery) {
    const q = (searchQuery !== undefined ? searchQuery : $('chat-search-input').value || '').trim();
    const path = q
      ? `/api/v1/conversations/search?q=${encodeURIComponent(q)}`
      : '/api/v1/conversations';
    const data = await apiFetch(path);
    state.conversations = sortConversationsList(unwrapList(data, ['conversations']));
    if (!state.activeConversationId && state.conversations.length > 0) {
      state.activeConversationId = state.conversations[0].id;
    }
    renderConversationList();
    updateChatHeader();
    populateWebhookConvSelect();
  }

  async function reorderConversation(convId, direction) {
    if (!convId) return;
    try {
      const res = await apiFetch('/api/v1/conversations/reorder', {
        method: 'POST',
        body: { conversation_id: convId, direction }
      });
      const updated = unwrapList(res, ['conversations']);
      if (updated.length > 0) {
        state.conversations = sortConversationsList(updated);
        renderConversationList();
        updateChatHeader();
      } else {
        await loadConversations();
      }
    } catch (err) {
      showToast(`Reorder failed: ${err.message}`, 'danger');
    }
  }

  async function reorderConversationsByIds(orderedIds) {
    if (!Array.isArray(orderedIds) || orderedIds.length === 0) return;
    try {
      const res = await apiFetch('/api/v1/conversations/reorder', {
        method: 'POST',
        body: { ordered_ids: orderedIds }
      });
      const updated = unwrapList(res, ['conversations']);
      if (updated.length > 0) {
        state.conversations = sortConversationsList(updated);
        renderConversationList();
        updateChatHeader();
      } else {
        await loadConversations();
      }
    } catch (err) {
      showToast(`Reorder failed: ${err.message}`, 'danger');
    }
  }

  function renderConversationList() {
    const listEl = $('conversation-list');
    clearEl(listEl);
    if (state.conversations.length === 0) {
      listEl.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No conversations yet'),
        el('span', { className: 'text-sm' }, 'Start a direct chat with Commander or create a group chat.')
      ));
      return;
    }

    const total = state.conversations.length;
    state.conversations.forEach((conv, idx) => {
      const isActive = conv.id === state.activeConversationId;
      const modeKind = conv.mode && conv.mode.kind === 'group' ? 'Group' : 'Direct';
      const item = el('div', {
        className: `conv-item${isActive ? ' is-active' : ''}`,
        role: 'listitem',
        draggable: true,
        dataset: { convId: conv.id }
      },
        el('button', {
          type: 'button',
          className: 'conv-select-btn',
          onClick: () => selectConversation(conv.id)
        },
          el('span', { className: 'conv-title truncate' }, conv.title || 'Untitled Conversation'),
          el('span', { className: 'conv-meta' },
            el('span', { className: `pill ${modeKind === 'Group' ? 'pill-info' : 'pill-muted'}` }, modeKind),
            el('span', null, fmtTime(conv.updated_at))
          )
        ),
        el('div', { className: 'conv-actions' },
          el('button', {
            type: 'button',
            className: 'btn btn-ghost btn-icon conv-move-btn',
            title: 'Move up',
            'aria-label': `Move ${conv.title || conv.id} up`,
            disabled: idx === 0,
            onClick: (e) => {
              e.stopPropagation();
              reorderConversation(conv.id, 'up');
            }
          }, '\u2191'),
          el('button', {
            type: 'button',
            className: 'btn btn-ghost btn-icon conv-move-btn',
            title: 'Move down',
            'aria-label': `Move ${conv.title || conv.id} down`,
            disabled: idx === total - 1,
            onClick: (e) => {
              e.stopPropagation();
              reorderConversation(conv.id, 'down');
            }
          }, '\u2193'),
          el('button', {
            type: 'button',
            className: 'btn btn-ghost btn-icon',
            'aria-label': `Delete conversation ${conv.title || conv.id}`,
            onClick: (e) => {
              e.stopPropagation();
              confirmDeleteConversation(conv);
            }
          }, '\u00d7')
        )
      );

      item.addEventListener('dragstart', (e) => {
        if (e.target && e.target.closest && e.target.closest('.conv-actions')) {
          e.preventDefault();
          return;
        }
        state.dragConvId = conv.id;
        item.classList.add('is-dragging');
        if (e.dataTransfer) {
          e.dataTransfer.effectAllowed = 'move';
          e.dataTransfer.setData('text/plain', conv.id);
        }
      });
      item.addEventListener('dragend', () => {
        state.dragConvId = null;
        listEl.querySelectorAll('.conv-item').forEach((n) => n.classList.remove('is-dragging', 'drag-over'));
      });
      item.addEventListener('dragover', (e) => {
        if (!state.dragConvId || state.dragConvId === conv.id) return;
        e.preventDefault();
        if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
        item.classList.add('drag-over');
      });
      item.addEventListener('dragleave', (e) => {
        if (!item.contains(e.relatedTarget)) {
          item.classList.remove('drag-over');
        }
      });
      item.addEventListener('drop', (e) => {
        e.preventDefault();
        item.classList.remove('drag-over');
        const fromId = state.dragConvId || (e.dataTransfer && e.dataTransfer.getData('text/plain'));
        state.dragConvId = null;
        if (!fromId || fromId === conv.id) return;
        const ids = state.conversations.map((c) => c.id);
        const fromIdx = ids.indexOf(fromId);
        const toIdx = ids.indexOf(conv.id);
        if (fromIdx === -1 || toIdx === -1 || fromIdx === toIdx) return;
        ids.splice(fromIdx, 1);
        ids.splice(toIdx, 0, fromId);
        reorderConversationsByIds(ids);
      });

      listEl.appendChild(item);
    });
  }

  function updateComposerEnabled(enabled) {
    const dis = !enabled;
    const input = getEl('chat-input');
    const sendBtn = getEl('chat-send-btn');
    const sendImmediateBtn = getEl('chat-send-immediate-btn');
    const slashBtn = getEl('chat-slash-btn');
    const attachBtn = getEl('btn-chat-attach');
    const agentSelect = getEl('chat-agent-select');
    const thinkingSelect = getEl('thinking-level-select');
    const agenticToggleBtn = getEl('btn-agentic-search-toggle');
    const agenticAgentSelect = getEl('chat-agentic-agent-select');
    if (input) input.disabled = dis;
    if (sendBtn) sendBtn.disabled = dis;
    if (sendImmediateBtn) sendImmediateBtn.disabled = dis;
    if (slashBtn) slashBtn.disabled = dis;
    if (attachBtn) attachBtn.disabled = dis;
    if (agentSelect) agentSelect.disabled = dis;
    if (thinkingSelect) thinkingSelect.disabled = dis;
    if (agenticToggleBtn) agenticToggleBtn.disabled = dis;
    if (agenticAgentSelect) agenticAgentSelect.disabled = dis;
  }

  async function selectConversation(convId) {
    const seq = ++state.convLoadSeq;
    state.activeConversationId = convId;
    state.chatLiveBrowserDismissed = false;
    for (const bucket of state.chatStreams.values()) {
      bucket.bubbleEl = null;
      bucket._refs = null;
    }
    hideSlashAgentMenu();
    hideSlashCommandPalette();
    $('chat-sidebar').classList.remove('is-open');
    renderConversationList();
    updateChatHeader();
    renderChatTaskBar();
    syncChatLiveBrowserDockForConversation(convId);
    try {
      await Promise.all([
        loadChatMessages(convId, { expectedSeq: seq }),
        rehydrateActiveChatTasks(convId)
      ]);
    } catch (err) {
      if (seq === state.convLoadSeq && state.activeConversationId === convId) {
        showToast(`Failed to load conversation: ${err.message}`, 'danger');
      }
    }
  }

  function updateChatHeader() {
    const conv = state.conversations.find((c) => c.id === state.activeConversationId);
    const agentSelect = getEl('chat-agent-select');
    const rawToggle = getEl('chat-raw-output-toggle');
    if (rawToggle) rawToggle.checked = state.showRawOutput;
    const forkBtn = getEl('chat-fork-conv-btn');
    if (forkBtn) forkBtn.disabled = !conv;

    if (!conv) {
      setText('chat-conv-title', 'Select or start a conversation');
      setText('chat-conv-meta', 'Choose Commander or any enabled agent');
      updateComposerEnabled(false);
      renderChatTaskBar();
      clearEl($('chat-messages'));
      $('chat-messages').appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No conversation selected'),
        el('span', { className: 'text-sm' }, 'Create a new Direct or Group chat from the sidebar.')
      ));
      return;
    }

    setText('chat-conv-title', conv.title || 'Conversation');
    let metaText = 'Direct Chat';
    if (conv.mode && conv.mode.kind === 'direct') {
      metaText = `Direct \u00b7 ${agentDisplayName(conv.mode.agent_id)}`;
      if (agentSelect && conv.mode.agent_id) {
        const hasOption = [...agentSelect.options].some((o) => o.value === conv.mode.agent_id);
        if (hasOption) agentSelect.value = conv.mode.agent_id;
      }
    } else if (conv.mode && conv.mode.kind === 'group') {
      const names = (conv.mode.participant_agent_ids || []).map(agentDisplayName).join(', ');
      metaText = `Group \u00b7 ${names || 'Multi-agent'}`;
    }
    setText('chat-conv-meta', metaText);
    updateComposerEnabled(true);
    renderChatTaskBar();
  }

  async function loadChatMessages(convId, reconcileOpts) {
    const expectedSeq = reconcileOpts && reconcileOpts.expectedSeq;
    const data = await apiFetch(`/api/v1/conversations/${encodeURIComponent(convId)}/messages`);
    if (state.activeConversationId !== convId || (expectedSeq && expectedSeq !== state.convLoadSeq)) {
      return;
    }
    state.chatMessages = unwrapList(data, ['messages']);
    let clearedActive = false;
    for (const m of state.chatMessages) {
      if (!m || String(m.role || '').toLowerCase() === 'user' || !m.task_id) continue;
      const tid = String(m.task_id).trim();
      if (!tid) continue;
      if (state.activeChatTasks.has(tid) || state.chatStreams.has(tid)) {
        clearedActive = true;
      }
      markChatTaskTerminal(tid);
    }
    if (clearedActive || state.activeChatTasks.size > 0) {
      renderChatTaskBar();
    }
    if (reconcileOpts && reconcileOpts.fallbackReasoning) {
      let target = null;
      if (reconcileOpts.messageId) {
        target = state.chatMessages.find((m) => m.id === reconcileOpts.messageId);
      }
      if (!target) {
        target = [...state.chatMessages].reverse().find((m) => String(m.role || '').toLowerCase() === 'assistant');
      }
      if (target && !target.reasoning) {
        target.reasoning = reconcileOpts.fallbackReasoning;
      }
    }
    renderChatMessages();
  }

  function renderAttachmentGallery(attachments) {
    if (!Array.isArray(attachments) || attachments.length === 0) return [];
    const imageNodes = [];
    const fileNodes = [];
    for (const att of attachments) {
      if (!att || typeof att !== 'object') continue;
      const mime = String(att.mime_type || '').toLowerCase();
      const wsPath = sanitizeWorkspacePath(att.workspace_path || '');
      const candidateImg = att.data_url || (wsPath && isImageFilePath(wsPath) ? `/api/v1/files/preview?path=${encodeURIComponent(wsPath)}` : '');
      const isImg = (att.kind === 'image' || mime.startsWith('image/')) && isSafeImageSrc(candidateImg);
      if (isImg) {
        imageNodes.push(el('button', {
          type: 'button',
          className: 'chat-thumb',
          title: att.name || 'Image attachment',
          onClick: () => openImageLightbox(candidateImg, att.name || 'Image attachment')
        }, el('img', { src: candidateImg, alt: att.name || 'Image attachment' })));
      } else {
        fileNodes.push(el('div', { className: 'file-artifact-card' },
          el('div', { className: 'file-artifact-info' },
            el('span', { className: 'pill pill-muted' }, 'Attachment'),
            el('span', { className: 'file-artifact-name' }, att.name || 'file'),
            att.size_bytes ? el('span', { className: 'file-artifact-size' }, fmtBytes(att.size_bytes)) : null
          ),
          wsPath ? el('div', { className: 'file-artifact-actions' },
            el('a', {
              className: 'btn btn-secondary btn-sm',
              href: `/api/v1/files/download?path=${encodeURIComponent(wsPath)}`,
              download: pathBasename(wsPath)
            }, 'Download'),
            el('button', {
              type: 'button',
              className: 'btn btn-primary btn-sm',
              onClick: () => openWorkspaceFileInFilesTab(wsPath, isHtmlFilePath(wsPath))
            }, 'Open / Preview')
          ) : null
        ));
      }
    }
    const out = [];
    if (imageNodes.length > 0) {
      out.push(el('div', { className: 'chat-attachments' }, imageNodes));
    }
    for (const fn of fileNodes) out.push(fn);
    return out;
  }

  function buildToolChips(liveTools) {
    if (!Array.isArray(liveTools) || liveTools.length === 0) return [];
    return liveTools.map((tc) => el('details', { className: 'msg-chip', open: tc.running },
      el('summary', null,
        el('span', { className: `pill ${tc.running ? 'pill-info' : tc.success ? 'pill-success' : 'pill-danger'}` },
          tc.running ? 'TOOL RUNNING' : tc.success ? 'TOOL OK' : 'TOOL FAILED'
        ),
        el('span', { className: 'mono' }, tc.tool_id),
        tc.duration_ms !== undefined ? el('span', { className: 'text-muted' }, `${tc.duration_ms} ms`) : null
      ),
      tc.arguments_json ? el('pre', null, prettyJson(tc.arguments_json)) : null
    ));
  }

  function formatRawMessageText(reasoning, content) {
    const r = typeof reasoning === 'string' ? reasoning.trim() : '';
    const c = typeof content === 'string' ? content : '';
    if (!r) return c;
    if (c.includes('<think>')) return c;
    return `<think>\n${r}\n</think>\n\n${c}`;
  }

  function attachReturnedTask(res, targetConvId, fallbackAgentId, seedMsg) {
    const taskObj = res && res.task ? res.task : null;
    const rawTaskId = (taskObj && taskObj.id) || (res && res.task_id);
    const taskId = rawTaskId ? String(rawTaskId).trim() : '';
    const taskStatus = taskObj && taskObj.status ? String(taskObj.status).toUpperCase() : 'PENDING';
    const alreadyPersisted = Boolean(
      taskId &&
      state.chatMessages.some(
        (m) =>
          m &&
          String(m.role || '').toLowerCase() !== 'user' &&
          String(m.task_id || '').trim() === taskId &&
          (!seedMsg || m.id !== seedMsg.id)
      )
    );

    if (!taskId || ['COMPLETED', 'FAILED', 'CANCELLED'].includes(taskStatus) || isChatTaskTerminal(taskId) || alreadyPersisted) {
      if (taskId) {
        markChatTaskTerminal(taskId);
      }
      renderChatTaskBar();
      return false;
    }

    const agentId = (taskObj && taskObj.assigned_agent_id) || fallbackAgentId || (res && res.agent_id) || '';
    const b = ensureStreamBucket({
      task_id: taskId,
      conversation_id: targetConvId,
      agent_id: agentId
    });
    if (seedMsg) {
      if (!b.text && seedMsg.content) b.text = seedMsg.content;
      if (!b.reasoning && seedMsg.reasoning) b.reasoning = seedMsg.reasoning;
    }
    b.dirty = true;
    scheduleStreamFlush();

    const existingTask = state.activeChatTasks.get(taskId);
    const keepExistingRunning = Boolean(
      existingTask &&
      ['RUNNING', 'ASSIGNED', 'WAITING'].includes(String(existingTask.status || '').toUpperCase()) &&
      taskStatus === 'PENDING'
    );
    updateChatTaskBar({
      task_id: taskId,
      conversation_id: targetConvId,
      agent_id: agentId || (existingTask && existingTask.agent_id) || '',
      status: keepExistingRunning ? existingTask.status : taskStatus,
      progress_pct: keepExistingRunning
        ? existingTask.progress_pct
        : ((taskObj && taskObj.progress_pct !== undefined && taskObj.progress_pct !== null)
            ? taskObj.progress_pct
            : (taskStatus === 'PENDING' ? 0 : 5)),
      progress_note: keepExistingRunning
        ? existingTask.progress_note
        : ((taskObj && taskObj.progress_note) || (taskStatus === 'PENDING' ? 'Pending (Queued)' : 'Task queued'))
    });

    window.setTimeout(() => {
      if (state.activeChatTasks.has(taskId) || state.chatStreams.has(taskId)) {
        rehydrateActiveChatTasks(targetConvId);
      }
    }, 1500);
    return true;
  }

  async function retryMessage(msg) {
    const convId = (msg && msg.conversation_id) || state.activeConversationId;
    if (!convId || !msg || !msg.id) return;
    const thinkingLevel = (getEl('thinking-level-select') && getEl('thinking-level-select').value) || 'medium';
    const selectedAgent = (getEl('chat-agent-select') && getEl('chat-agent-select').value) || '';
    const body = { thinking_level: thinkingLevel, async_dispatch: true };
    if (selectedAgent) body.agent_id = selectedAgent;
    try {
      const res = await apiFetch(
        `/api/v1/conversations/${encodeURIComponent(convId)}/messages/${encodeURIComponent(msg.id)}/retry`,
        { method: 'POST', body }
      );
      if (state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
      const started = attachReturnedTask(res, convId, selectedAgent, null);
      if (!started && state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
    } catch (err) {
      showToast(`Retry failed: ${err.message}`, 'danger');
    }
  }

  async function regenerateMessage(msg) {
    const convId = (msg && msg.conversation_id) || state.activeConversationId;
    if (!convId || !msg || !msg.id) return;
    const thinkingLevel = (getEl('thinking-level-select') && getEl('thinking-level-select').value) || 'medium';
    try {
      const res = await apiFetch(
        `/api/v1/conversations/${encodeURIComponent(convId)}/messages/${encodeURIComponent(msg.id)}/regenerate`,
        { method: 'POST', body: { thinking_level: thinkingLevel, async_dispatch: true } }
      );
      if (state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
      const started = attachReturnedTask(res, convId, msg.agent_id || '', null);
      if (!started && state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
    } catch (err) {
      showToast(`Regenerate failed: ${err.message}`, 'danger');
    }
  }

  async function continueMessage(msg) {
    const convId = (msg && msg.conversation_id) || state.activeConversationId;
    if (!convId || !msg || !msg.id) return;
    const thinkingLevel = (getEl('thinking-level-select') && getEl('thinking-level-select').value) || 'medium';
    try {
      const res = await apiFetch(
        `/api/v1/conversations/${encodeURIComponent(convId)}/messages/${encodeURIComponent(msg.id)}/continue`,
        { method: 'POST', body: { thinking_level: thinkingLevel } }
      );
      const started = attachReturnedTask(res, convId, msg.agent_id || '', msg);
      if (!started && state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
    } catch (err) {
      showToast(`Continue failed: ${err.message}`, 'danger');
    }
  }

  async function forkConversation(upToMessageId) {
    const convId = state.activeConversationId;
    if (!convId) {
      showToast('Select a conversation to fork.', 'warning');
      return;
    }
    const body = upToMessageId ? { up_to_message_id: upToMessageId } : {};
    try {
      const forked = await apiFetch(`/api/v1/conversations/${encodeURIComponent(convId)}/fork`, {
        method: 'POST',
        body
      });
      showToast('Conversation forked.', 'success');
      await loadConversations();
      if (forked && forked.id) {
        await selectConversation(forked.id);
      }
    } catch (err) {
      showToast(`Fork failed: ${err.message}`, 'danger');
    }
  }

  async function deleteMessage(msg) {
    const convId = (msg && msg.conversation_id) || state.activeConversationId;
    if (!convId || !msg || !msg.id) return;
    try {
      await apiFetch(
        `/api/v1/conversations/${encodeURIComponent(convId)}/messages/${encodeURIComponent(msg.id)}`,
        { method: 'DELETE' }
      );
      state.chatMessages = state.chatMessages.filter((m) => m.id !== msg.id);
      if (state.activeConversationId === convId) {
        renderChatMessages();
      }
      showToast('Message deleted.', 'success');
    } catch (err) {
      showToast(`Delete message failed: ${err.message}`, 'danger');
    }
  }

  function buildMessageBubble(msg, isLive) {
    const role = String(msg.role || 'assistant').toLowerCase();
    const roleClass = role === 'user' ? 'msg-user' : role === 'system' ? 'msg-system' : role === 'tool' ? 'msg-tool' : 'msg-assistant';
    const author = role === 'user' ? state.username : agentDisplayName(msg.agent_id);

    const headerMeta = [];
    if (msg.created_at) headerMeta.push(fmtTime(msg.created_at));
    if (msg.token_count) headerMeta.push(`${msg.token_count} tok`);
    if (msg.latency_ms) headerMeta.push(`${msg.latency_ms} ms`);
    const timingsPill = role !== 'user' ? buildTimingsPillEl(resolveMessageTimings(msg)) : null;

    const nodes = [
      el('div', { className: 'msg-header' },
        el('span', { className: 'msg-author' },
          author,
          isLive ? ' ' : null,
          isLive ? el('span', { className: 'streaming-dot', 'aria-label': 'Streaming response' }) : null
        ),
        el('span', { className: 'toolbar-actions' },
          timingsPill,
          headerMeta.length > 0 ? el('span', { className: 'mono' }, headerMeta.join(' \u00b7 ')) : null
        )
      )
    ];

    const reasoningText = typeof msg.reasoning === 'string' ? msg.reasoning : '';
    const rawMode = Boolean(state.showRawOutput);
    if (reasoningText.trim() && !rawMode) {
      nodes.push(el('details', { className: 'thinking-drawer', open: Boolean(isLive && !msg.content) },
        el('summary', { className: 'thinking-summary' },
          isLive ? `Reasoning (live \u00b7 ${reasoningText.length} chars)` : `Reasoning (${reasoningText.length} chars)`
        ),
        el('div', { className: 'thinking-body' }, document.createTextNode(reasoningText))
      ));
    }

    if (msg.tool_calls_json) {
      nodes.push(el('details', { className: 'msg-chip' },
        el('summary', null, 'Tool Calls'),
        el('pre', null, prettyJson(msg.tool_calls_json))
      ));
    }

    for (const chip of buildToolChips(msg.liveTools)) {
      nodes.push(chip);
    }

    const contentEl = el('div', { className: 'msg-content' });
    if (rawMode) {
      const rawText = formatRawMessageText(reasoningText, msg.content || '');
      if (rawText) {
        contentEl.appendChild(el('pre', { className: 'msg-raw-output' }, document.createTextNode(rawText)));
      } else if (isLive) {
        contentEl.appendChild(document.createTextNode('Streaming\u2026'));
      }
    } else if (msg.content) {
      contentEl.appendChild(renderMarkdownNodes(msg.content));
    } else if (isLive) {
      contentEl.appendChild(document.createTextNode(reasoningText.trim() ? 'Thinking\u2026' : 'Streaming\u2026'));
    }
    nodes.push(contentEl);

    for (const attNode of renderAttachmentGallery(msg.attachments)) {
      nodes.push(attNode);
    }

    const filePaths = extractCreatedFilePaths(msg);
    for (const filePath of filePaths) {
      nodes.push(buildFileArtifactCard(filePath));
    }

    const msgId = msg.id ? String(msg.id) : '';
    const isPersisted = !isLive && msgId && !msgId.startsWith('tmp-') && !msgId.startsWith('local-');
    if (isPersisted) {
      const copySource = rawMode
        ? formatRawMessageText(reasoningText, msg.content || '')
        : (msg.content || formatRawMessageText(reasoningText, msg.content || ''));
      const actionBtns = [
        el('button', {
          type: 'button',
          className: 'msg-action-btn',
          onClick: (e) => copyTextWithFeedback(e.currentTarget, copySource, 'Copy')
        }, 'Copy')
      ];
      if (role === 'user') {
        actionBtns.push(el('button', {
          type: 'button',
          className: 'msg-action-btn',
          onClick: () => retryMessage(msg)
        }, 'Retry'));
      } else if (role === 'assistant') {
        actionBtns.push(
          el('button', {
            type: 'button',
            className: 'msg-action-btn',
            onClick: () => regenerateMessage(msg)
          }, 'Regenerate'),
          el('button', {
            type: 'button',
            className: 'msg-action-btn',
            onClick: () => continueMessage(msg)
          }, 'Continue')
        );
      }
      actionBtns.push(
        el('button', {
          type: 'button',
          className: 'msg-action-btn',
          onClick: () => forkConversation(msg.id)
        }, 'Fork'),
        el('button', {
          type: 'button',
          className: 'msg-action-btn msg-action-danger',
          onClick: () => deleteMessage(msg)
        }, 'Delete')
      );
      nodes.push(el('div', { className: 'msg-actions' }, actionBtns));
    }

    return el('article', { className: `msg-bubble ${roleClass}` }, nodes);
  }

  function createLiveStreamBubble(bucket) {
    const reasoningBody = el('div', { className: 'thinking-body' });
    const reasoningSummary = el('summary', { className: 'thinking-summary' }, 'Reasoning');
    const reasoningDetails = el('details', { className: 'thinking-drawer', hidden: true },
      reasoningSummary,
      reasoningBody
    );
    reasoningDetails.addEventListener('toggle', () => {
      bucket._userToggledReasoning = true;
    });

    const toolsWrap = el('div', { className: 'live-tools-wrap', hidden: true });
    const contentEl = el('div', { className: 'msg-content' });
    const artifactsWrap = el('div', { className: 'live-artifacts-wrap', hidden: true });

    const agentName = agentDisplayName(bucket.agentId);
    const stopStreamBtn = bucket.taskId ? el('button', {
      type: 'button',
      className: 'stream-stop-btn',
      title: `Stop ${agentName}`,
      onClick: (e) => {
        e.currentTarget.disabled = true;
        stopChatTaskById(bucket.taskId, agentName);
      }
    }, 'Stop') : null;

    const headerEl = el('div', { className: 'msg-header' },
      el('span', { className: 'msg-author' },
        agentName,
        ' ',
        el('span', { className: 'streaming-dot', 'aria-label': 'Streaming response' })
      ),
      el('span', { className: 'chat-task-controls' },
        el('span', { className: 'mono' }, fmtTime(bucket.createdAt)),
        stopStreamBtn
      )
    );

    const bubbleEl = el('article', {
      className: 'msg-bubble msg-assistant',
      dataset: { taskId: bucket.taskId }
    }, headerEl, reasoningDetails, toolsWrap, contentEl, artifactsWrap);

    bucket.bubbleEl = bubbleEl;
    bucket._refs = {
      reasoningDetails,
      reasoningSummary,
      reasoningBody,
      toolsWrap,
      contentEl,
      artifactsWrap
    };
    bucket._renderedRawMode = Boolean(state.showRawOutput);
    bucket._renderedText = null;
    bucket._renderedReasoning = null;
    bucket._renderedToolsSig = '';
    bucket._renderedArtifactsSig = '';
    bucket._streamRenderState = null;

    updateLiveStreamBubbleContent(bucket);
    return bubbleEl;
  }

  function updateLiveStreamBubbleContent(bucket) {
    const refs = bucket._refs;
    if (!refs) return;

    const rawMode = Boolean(state.showRawOutput);
    if (rawMode !== bucket._renderedRawMode) {
      bucket._renderedRawMode = rawMode;
      bucket._renderedText = null;
      bucket._renderedReasoning = null;
      bucket._streamRenderState = null;
    }

    // 1. Reasoning drawer (hidden when inline raw <think> output is shown)
    const rText = bucket.reasoning || '';
    if (rawMode) {
      refs.reasoningDetails.hidden = true;
      bucket._renderedReasoning = null;
    } else if (rText !== bucket._renderedReasoning) {
      bucket._renderedReasoning = rText;
      if (rText.trim()) {
        refs.reasoningDetails.hidden = false;
        refs.reasoningSummary.textContent = `Reasoning (live \u00b7 ${rText.length} chars)`;
        refs.reasoningBody.textContent = rText;
        if (!bucket._userToggledReasoning && !bucket.text) {
          refs.reasoningDetails.open = true;
        }
      } else {
        refs.reasoningDetails.hidden = true;
      }
    }

    // 2. Live tool calls
    const toolsSig = bucket.toolCalls.length > 0
      ? JSON.stringify(bucket.toolCalls.map((t) => [t.tool_id, t.running, t.success, t.duration_ms]))
      : '';
    if (toolsSig !== bucket._renderedToolsSig) {
      bucket._renderedToolsSig = toolsSig;
      clearEl(refs.toolsWrap);
      if (bucket.toolCalls.length > 0) {
        refs.toolsWrap.hidden = false;
        for (const chip of buildToolChips(bucket.toolCalls)) {
          refs.toolsWrap.appendChild(chip);
        }
      } else {
        refs.toolsWrap.hidden = true;
      }
    }

    // 3. Markdown content or Raw Output
    const nextText = bucket.text || '';
    const displayValue = rawMode ? formatRawMessageText(rText, nextText) : nextText;
    if (displayValue !== bucket._renderedText) {
      bucket._renderedText = displayValue;
      if (rawMode) {
        if (!refs._rawPreEl || !refs._rawTextNode) {
          clearEl(refs.contentEl);
          refs._rawPreEl = el('pre', { className: 'msg-raw-output' });
          refs._rawTextNode = document.createTextNode(displayValue);
          refs._rawPreEl.appendChild(refs._rawTextNode);
          refs.contentEl.appendChild(refs._rawPreEl);
          bucket._streamRenderState = null;
        } else {
          refs._rawTextNode.data = displayValue;
        }
      } else {
        if (refs._rawPreEl) {
          clearEl(refs.contentEl);
          delete refs._rawPreEl;
          delete refs._rawTextNode;
          bucket._streamRenderState = null;
        }
        if (nextText) {
          if (refs._placeholderNode) {
            refs._placeholderNode.remove();
            refs._placeholderNode = null;
          }
          renderMarkdownIncremental(refs.contentEl, bucket, nextText);
        } else {
          const placeholderText = !rawMode && rText.trim() ? 'Thinking\u2026' : 'Streaming\u2026';
          if (!refs._placeholderNode) {
            clearEl(refs.contentEl);
            refs._placeholderNode = document.createTextNode(placeholderText);
            refs.contentEl.appendChild(refs._placeholderNode);
            bucket._streamRenderState = null;
          } else {
            refs._placeholderNode.data = placeholderText;
          }
        }
      }
    }

    // 4. Artifact cards detected so far
    if (bucket.toolCalls.length > 0 || nextText.includes('[')) {
      const files = extractCreatedFilePaths({ content: nextText, liveTools: bucket.toolCalls });
      const artSig = files.join('\0');
      if (artSig !== bucket._renderedArtifactsSig) {
        bucket._renderedArtifactsSig = artSig;
        clearEl(refs.artifactsWrap);
        if (files.length > 0) {
          refs.artifactsWrap.hidden = false;
          for (const fp of files) {
            refs.artifactsWrap.appendChild(buildFileArtifactCard(fp));
          }
        } else {
          refs.artifactsWrap.hidden = true;
        }
      }
    }
  }

  function updateLiveStreamBubble(bucket) {
    const container = $('chat-messages');
    if (!container) return;
    const nearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 120;
    const empty = container.querySelector('.empty-state');
    if (empty) empty.remove();

    if (!bucket.bubbleEl || !bucket.bubbleEl.isConnected) {
      container.appendChild(createLiveStreamBubble(bucket));
    } else {
      updateLiveStreamBubbleContent(bucket);
    }

    if (nearBottom) {
      container.scrollTop = container.scrollHeight;
    }
  }

  function updateActiveStreamsBadge() {
    const badge = getEl('chat-active-streams');
    if (!badge) return;
    const count = state.chatStreams.size;
    if (count === 0) {
      badge.textContent = '0 active';
      badge.className = 'pill pill-muted subtle';
      return;
    }
    const names = [...new Set([...state.chatStreams.values()].map((s) => agentDisplayName(s.agentId)))];
    badge.textContent = `${count} active (${names.join(', ')})`;
    badge.className = 'pill pill-info is-active';
  }

  function scheduleStreamFlush() {
    if (state.chatStreamRafId !== null || state.chatStreamTimerId !== null) return;
    const now = performance.now();
    const elapsed = now - state.chatStreamLastFlushMs;
    if (elapsed >= 30) {
      state.chatStreamRafId = window.requestAnimationFrame(flushDirtyStreams);
    } else {
      const wait = Math.max(0, 30 - elapsed);
      state.chatStreamTimerId = window.setTimeout(() => {
        state.chatStreamTimerId = null;
        state.chatStreamRafId = window.requestAnimationFrame(flushDirtyStreams);
      }, wait);
    }
  }

  function flushDirtyStreams() {
    state.chatStreamRafId = null;
    state.chatStreamLastFlushMs = performance.now();
    for (const bucket of state.chatStreams.values()) {
      if (!bucket.dirty) continue;
      bucket.dirty = false;
      if (state.currentSection !== 'chat' || bucket.conversationId !== state.activeConversationId) {
        continue;
      }
      updateLiveStreamBubble(bucket);
    }
  }

  function buildChatErrorBannerEl(errEntry, convId) {
    const dismissBtn = el('button', {
      type: 'button',
      className: 'btn btn-ghost btn-icon',
      'aria-label': 'Dismiss error',
      onClick: (e) => {
        if (convId) {
          const list = state.chatErrors.get(convId) || [];
          const next = list.filter((x) => x.id !== errEntry.id);
          if (next.length > 0) state.chatErrors.set(convId, next);
          else state.chatErrors.delete(convId);
        }
        const banner = e.currentTarget.closest('.msg-error-banner');
        if (banner) banner.remove();
      }
    }, '\u00d7');

    return el('div', { className: 'msg-error-banner', role: 'alert' },
      el('div', { className: 'min-w-0' },
        el('strong', null, errEntry.title ? `${errEntry.title}: ` : 'Task / Inference Error: '),
        el('span', null, errEntry.detail || 'Unknown error'),
        errEntry.timestamp ? el('span', { className: 'text-sm text-muted' }, ` (${fmtTime(errEntry.timestamp)})`) : null
      ),
      dismissBtn
    );
  }

  function clearChatErrorsForConversation(convId) {
    if (!convId) return;
    state.chatErrors.delete(convId);
    if (convId === state.activeConversationId || convId === '__global__') {
      const container = getEl('chat-messages');
      if (container) {
        container.querySelectorAll('.msg-error-banner').forEach((n) => n.remove());
      }
      if (convId !== '__global__') {
        state.chatErrors.delete('__global__');
      }
    }
  }

  function clearChatPendingForConversation(convId, taskId) {
    const tid = taskId ? String(taskId) : '';
    if (tid) {
      removeStreamBucket(tid);
      state.activeChatTasks.delete(tid);
    }
    if (convId) {
      for (const [k, b] of [...state.chatStreams.entries()]) {
        if (b && b.conversationId === convId && (!tid || k === tid)) {
          removeStreamBucket(k);
        }
      }
      for (const [k, t] of [...state.activeChatTasks.entries()]) {
        if (t && t.conversation_id === convId && (!tid || k === tid)) {
          state.activeChatTasks.delete(k);
        }
      }
    }
    renderChatTaskBar();
  }

  let lastChatErrorToastMsg = '';
  let lastChatErrorToastAtMs = 0;

  function showChatErrorBanner(convId, title, detail) {
    const cleanDetail = String(detail || 'Execution failed.').trim();
    const targetConvId = convId || state.activeConversationId || '__global__';
    const now = Date.now();
    const existing = state.chatErrors.get(targetConvId) || [];
    const last = existing[existing.length - 1];
    if (last && last.detail === cleanDetail && (now - last.createdAtMs) < 2500) {
      return false;
    }

    const entry = {
      id: `cerr-${now}-${Math.random().toString(36).slice(2, 6)}`,
      title: String(title || 'Task / Inference Error'),
      detail: cleanDetail,
      timestamp: new Date(now).toISOString(),
      createdAtMs: now
    };
    state.chatErrors.set(targetConvId, existing.concat(entry).slice(-3));

    if (
      state.currentSection === 'chat'
      && (targetConvId === state.activeConversationId || targetConvId === '__global__')
    ) {
      const container = getEl('chat-messages');
      if (container) {
        const empty = container.querySelector('.empty-state');
        if (empty) empty.remove();
        const nearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 140;
        container.appendChild(buildChatErrorBannerEl(entry, targetConvId));
        if (nearBottom || state.chatMessages.length === 0) {
          container.scrollTop = container.scrollHeight;
        }
      }
    }
    return true;
  }

  function showDeduplicatedErrorToast(message) {
    const clean = String(message || 'System error').trim();
    const now = Date.now();
    if (clean === lastChatErrorToastMsg && (now - lastChatErrorToastAtMs) < 2500) {
      return;
    }
    lastChatErrorToastMsg = clean;
    lastChatErrorToastAtMs = now;
    showToast(clean, 'danger');
  }

  function renderChatMessages() {
    const container = $('chat-messages');
    if (!container) return;
    const nearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 120;
    clearEl(container);

    const activeStreams = [...state.chatStreams.values()].filter(
      (b) => b.conversationId === state.activeConversationId
    );
    const globalErrors = state.chatErrors.get('__global__') || [];
    const activeConvErrors = state.activeConversationId
      ? (state.chatErrors.get(state.activeConversationId) || [])
      : [];
    const convErrors = [
      ...globalErrors.map((entry) => ({ entry, convKey: '__global__' })),
      ...activeConvErrors.map((entry) => ({ entry, convKey: state.activeConversationId }))
    ];

    if (state.chatMessages.length === 0 && activeStreams.length === 0 && convErrors.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'Conversation ready'),
        el('span', { className: 'text-sm' }, 'Send a message below to dispatch a task to this agent.')
      ));
      return;
    }

    for (const msg of state.chatMessages) {
      container.appendChild(buildMessageBubble(msg, false));
    }

    for (const bucket of activeStreams) {
      bucket.dirty = false;
      container.appendChild(createLiveStreamBubble(bucket));
    }

    for (const { entry: errEntry, convKey } of convErrors) {
      container.appendChild(buildChatErrorBannerEl(errEntry, convKey));
    }

    if (nearBottom || activeStreams.length > 0 || convErrors.length > 0) {
      container.scrollTop = container.scrollHeight;
    }
  }

  function upsertActiveChatTask(taskInfo) {
    if (!taskInfo) return;
    const rawId = taskInfo.task_id || taskInfo.id;
    if (!rawId) return;
    const taskId = String(rawId);
    const st = String(taskInfo.status || 'PENDING').toUpperCase();
    if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(st)) {
      state.activeChatTasks.delete(taskId);
      return;
    }
    const existing = state.activeChatTasks.get(taskId);
    const bucket = state.chatStreams.get(taskId);
    const convId = taskInfo.conversation_id
      || (existing && existing.conversation_id)
      || (bucket && bucket.conversationId)
      || '';
    if (!convId) return;

    const agentId = taskInfo.agent_id
      || taskInfo.assigned_agent_id
      || (existing && existing.agent_id)
      || (bucket && bucket.agentId)
      || '';
    const pct = Number.isFinite(Number(taskInfo.progress_pct))
      ? Number(taskInfo.progress_pct)
      : (existing ? existing.progress_pct : 0);
    const note = taskInfo.progress_note !== undefined && taskInfo.progress_note !== null
      ? String(taskInfo.progress_note)
      : (existing ? existing.progress_note : '');

    state.activeChatTasks.set(taskId, {
      task_id: taskId,
      conversation_id: convId,
      agent_id: agentId,
      status: st,
      progress_pct: pct,
      progress_note: note
    });
  }

  function renderChatTaskBar() {
    const bar = getEl('chat-active-task-bar');
    if (!bar) return;
    const curConvId = state.activeConversationId;
    const activeForConv = curConvId
      ? [...state.activeChatTasks.values()].filter((t) =>
          t.conversation_id === curConvId
          && ['PENDING', 'ASSIGNED', 'RUNNING', 'WAITING'].includes(String(t.status || '').toUpperCase())
        )
      : [];
    const agentsContainer = getEl('chat-task-agents');

    if (activeForConv.length === 0) {
      bar.hidden = true;
      if (agentsContainer) clearEl(agentsContainer);
      return;
    }

    bar.hidden = false;
    const primary = activeForConv.find((t) => ['RUNNING', 'ASSIGNED', 'WAITING'].includes(t.status)) || activeForConv[0];
    const anyRunning = activeForConv.some((t) => ['RUNNING', 'ASSIGNED', 'WAITING'].includes(t.status));
    const headerStatus = anyRunning ? primary.status : 'PENDING';

    const statusPill = getEl('chat-task-status-pill');
    if (statusPill) {
      statusPill.textContent = headerStatus;
      statusPill.className = `pill ${statusPillClass(headerStatus)}`;
    }

    const noteText = activeForConv.length > 1
      ? `${activeForConv.length} active tasks \u00b7 ${primary.progress_note || `${agentDisplayName(primary.agent_id)} working\u2026`}`
      : (primary.progress_note || (primary.status === 'PENDING'
          ? `${agentDisplayName(primary.agent_id)} \u00b7 Pending (Queued)`
          : `${agentDisplayName(primary.agent_id)} working\u2026`));
    setText('chat-task-note', noteText);

    const avgPct = Math.round(
      activeForConv.reduce((acc, t) => acc + Math.min(100, Math.max(0, Number(t.progress_pct || 0))), 0) / activeForConv.length
    );
    const fillEl = getEl('chat-task-progress-fill');
    if (fillEl) fillEl.value = avgPct;
    setText('chat-task-pct', `${avgPct}%`);

    const cancelAllBtn = getEl('chat-cancel-task-btn');
    if (cancelAllBtn) {
      cancelAllBtn.textContent = activeForConv.length > 1 ? 'Cancel All' : 'Cancel Task';
    }

    if (agentsContainer) {
      clearEl(agentsContainer);
      for (const task of activeForConv) {
        const st = String(task.status || 'PENDING').toUpperCase();
        const agentLabel = agentDisplayName(task.agent_id);
        const detail = st === 'PENDING'
          ? (task.progress_note || 'Pending (Queued)')
          : (task.progress_note || st);
        const chipNodes = [
          el('span', { className: `pill ${statusPillClass(st)}` }, st),
          el('span', { className: 'truncate' }, `${agentLabel} \u00b7 ${detail}`)
        ];
        if (st === 'PENDING') {
          chipNodes.push(el('button', {
            type: 'button',
            className: 'btn-run-now',
            onClick: (e) => {
              e.currentTarget.disabled = true;
              expediteTaskById(task.task_id);
            }
          }, 'Run Immediately'));
        }
        chipNodes.push(el('button', {
          type: 'button',
          className: 'btn-stop-agent',
          onClick: (e) => {
            e.currentTarget.disabled = true;
            stopChatTaskById(task.task_id, agentLabel);
          }
        }, `Stop ${agentLabel} \u00d7`));
        agentsContainer.appendChild(el('div', { className: 'task-agent-chip' }, chipNodes));
      }
    }
  }

  function updateChatTaskBar(taskInfo) {
    if (taskInfo && (taskInfo.task_id || taskInfo.id)) {
      upsertActiveChatTask(taskInfo);
    }
    renderChatTaskBar();
  }

  async function expediteTaskById(taskId) {
    if (!taskId) return;
    try {
      const res = await apiFetch(`/api/v1/tasks/${encodeURIComponent(taskId)}/expedite`, {
        method: 'POST',
        body: {}
      });
      if (res && res.task) {
        upsertActiveChatTask(res.task);
        renderChatTaskBar();
      }
      showToast('Task expedited to run immediately.', 'info');
    } catch (err) {
      renderChatTaskBar();
      showToast(`Expedite failed: ${err.message}`, 'danger');
    }
  }

  async function stopChatTaskById(taskId, agentLabel) {
    if (!taskId) return;
    try {
      await apiFetch(`/api/v1/tasks/${encodeURIComponent(taskId)}/cancel`, {
        method: 'POST',
        body: {}
      });
      state.activeChatTasks.delete(String(taskId));
      removeStreamBucket(String(taskId));
      renderChatTaskBar();
      showToast(`Stopped ${agentLabel || 'task'}.`, 'warning');
    } catch (err) {
      const msgContainer = getEl('chat-messages');
      if (msgContainer) {
        msgContainer.querySelectorAll('.stream-stop-btn').forEach((btn) => {
          btn.disabled = false;
        });
      }
      renderChatTaskBar();
      showToast(`Stop task failed: ${err.message}`, 'danger');
    }
  }

  /* -------------------- Slash /agent <name> Autocomplete -------------------- */
  function matchSlashAgent(token) {
    const q = String(token || '').trim().toLowerCase();
    if (!q) return null;
    const enabled = state.agents.filter((a) => a.enabled);
    return enabled.find((a) => a.id.toLowerCase() === q)
      || enabled.find((a) => a.name.toLowerCase() === q)
      || enabled.find((a) => a.id.toLowerCase().startsWith(q) || a.name.toLowerCase().startsWith(q))
      || null;
  }

  function parseSlashAgentCommand(rawText) {
    const str = String(rawText || '');
    const fullMatch = str.match(/^\s*\/agent\s+(\S+)(?:\s+([\s\S]*))?$/i);
    if (!fullMatch) return null;
    const agent = matchSlashAgent(fullMatch[1]);
    if (!agent) return null;
    return {
      agent,
      prompt: (fullMatch[2] || '').trim()
    };
  }

  function hideSlashAgentMenu() {
    const menu = getEl('slash-agent-menu');
    if (menu) {
      menu.hidden = true;
      clearEl(menu);
    }
    state.slashMenuMatches = [];
    state.slashMenuIndex = 0;
  }

  function applySlashAgentSelection(agent) {
    const input = getEl('chat-input');
    const agentSelect = getEl('chat-agent-select');
    if (!input || !agent) return;
    const cur = input.value;
    const trailingMatch = cur.match(/^\s*\/(?:agent(?:\s+\S*)?|\S*)(\s+[\s\S]*)?$/i);
    const trailing = trailingMatch && trailingMatch[1] ? trailingMatch[1].trimStart() : '';
    input.value = `/agent ${agent.id} ${trailing}`;
    if (agentSelect) {
      agentSelect.value = agent.id;
    }
    hideSlashAgentMenu();
    input.focus();
  }

  function renderSlashAgentMenu() {
    const menu = getEl('slash-agent-menu');
    if (!menu) return;
    clearEl(menu);
    if (state.slashMenuMatches.length === 0) {
      menu.hidden = true;
      return;
    }
    state.slashMenuMatches.forEach((ag, idx) => {
      const active = idx === state.slashMenuIndex;
      const item = el('button', {
        type: 'button',
        role: 'option',
        className: `slash-item${active ? ' is-active' : ''}`,
        'aria-selected': String(active),
        onMouseDown: (e) => {
          e.preventDefault();
          applySlashAgentSelection(ag);
        }
      },
        el('span', null,
          el('strong', { className: 'mono' }, `/agent ${ag.id}`),
          ' \u00b7 ',
          ag.name
        ),
        el('span', { className: 'pill pill-info' }, ag.role)
      );
      menu.appendChild(item);
    });
    menu.hidden = false;
  }

  function updateSlashAgentMenu() {
    const input = getEl('chat-input');
    if (!input || input.disabled) {
      hideSlashAgentMenu();
      return;
    }
    const val = input.value;
    // Sync dropdown automatically if user finished typing `/agent <id> ...`
    const parsedCmd = parseSlashAgentCommand(val);
    if (parsedCmd && /^\s*\/agent\s+\S+\s+/i.test(val)) {
      const sel = getEl('chat-agent-select');
      if (sel) sel.value = parsedCmd.agent.id;
      hideSlashAgentMenu();
      return;
    }

    const agentCmdMatch = val.match(/^\s*\/agent(?:\s+([^\s]*))?$/i);
    if (!agentCmdMatch) {
      hideSlashAgentMenu();
      return;
    }

    const enabled = state.agents.filter((a) => a.enabled);
    if (enabled.length === 0) {
      hideSlashAgentMenu();
      return;
    }

    const query = (agentCmdMatch[1] || '').toLowerCase();
    const matches = enabled.filter((a) =>
      !query ||
      a.id.toLowerCase().includes(query) ||
      a.name.toLowerCase().includes(query) ||
      String(a.role || '').toLowerCase().includes(query)
    );
    if (matches.length === 0) {
      hideSlashAgentMenu();
      return;
    }
    hideSlashCommandPalette();
    state.slashMenuMatches = matches;
    if (state.slashMenuIndex >= matches.length) {
      state.slashMenuIndex = 0;
    }
    renderSlashAgentMenu();
  }

  function handleSlashMenuKeydown(e) {
    const menu = getEl('slash-agent-menu');
    if (!menu || menu.hidden || state.slashMenuMatches.length === 0) {
      return false;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      state.slashMenuIndex = (state.slashMenuIndex + 1) % state.slashMenuMatches.length;
      renderSlashAgentMenu();
      return true;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      state.slashMenuIndex = (state.slashMenuIndex - 1 + state.slashMenuMatches.length) % state.slashMenuMatches.length;
      renderSlashAgentMenu();
      return true;
    }
    if (e.key === 'Tab' || (e.key === 'Enter' && !e.shiftKey)) {
      e.preventDefault();
      const chosen = state.slashMenuMatches[state.slashMenuIndex] || state.slashMenuMatches[0];
      if (chosen) applySlashAgentSelection(chosen);
      return true;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      hideSlashAgentMenu();
      return true;
    }
    return false;
  }

  /* -------------------- Slash Command Palette & Ephemeral /btw Card -------------------- */
  function getAllSlashCommands() {
    const custom = Array.isArray(state.slashWorkflowCommands) ? state.slashWorkflowCommands : [];
    const items = [
      ...SLASH_COMMANDS,
      { cmd: '/agent', args: '<name> <prompt>', desc: 'Dispatch prompt directly to a specific agent', _isAgentPrefix: true },
      ...custom
    ];
    return items;
  }

  function hideSlashCommandPalette() {
    const palette = getEl('chat-slash-palette');
    if (palette) {
      palette.hidden = true;
      palette.classList.add('hidden');
      clearEl(palette);
    }
    state.slashPaletteOpen = false;
    state.slashPaletteMatches = [];
    state.slashPaletteIndex = 0;
  }

  function hideEphemeralSideCard() {
    const card = getEl('chat-ephemeral-card');
    if (!card) return;
    card.hidden = true;
    card.classList.add('hidden');
  }

  function showEphemeralSideCard(title, markdownText) {
    const card = getEl('chat-ephemeral-card');
    const titleEl = getEl('chat-ephemeral-title');
    const bodyEl = getEl('chat-ephemeral-body');
    if (!card || !bodyEl) return;
    if (titleEl) {
      titleEl.textContent = String(title || 'Slash Command Output');
    }
    clearEl(bodyEl);
    const text = String(markdownText || '').trim();
    if (text) {
      bodyEl.appendChild(renderMarkdownNodes(text));
    } else {
      bodyEl.appendChild(el('p', { className: 'text-sm text-muted' }, 'No output returned.'));
    }
    card.hidden = false;
    card.classList.remove('hidden');
  }

  async function applySlashPaletteChoice(item, options) {
    const opts = options || {};
    const input = getEl('chat-input');
    if (!input || !item) return;
    hideSlashCommandPalette();
    if (item._isAgentPrefix) {
      input.value = '/agent ';
      input.focus();
      updateSlashAgentMenu();
      return;
    }
    if (!opts.insertOnly && !item.args) {
      input.value = '';
      await executeSlashCommand(item.cmd, { clearInput: false });
      input.focus();
      return;
    }
    input.value = `${item.cmd} `;
    input.focus();
  }

  function renderSlashCommandPalette() {
    const palette = getEl('chat-slash-palette');
    if (!palette) return;
    clearEl(palette);
    if (state.slashPaletteMatches.length === 0) {
      hideSlashCommandPalette();
      return;
    }
    state.slashPaletteMatches.forEach((item, idx) => {
      const active = idx === state.slashPaletteIndex;
      const label = item.args ? `${item.cmd} ${item.args}` : item.cmd;
      const btn = el('button', {
        type: 'button',
        role: 'option',
        className: `chat-slash-item${active ? ' is-active active' : ''}`,
        'aria-selected': String(active),
        onMouseDown: (e) => {
          e.preventDefault();
          applySlashPaletteChoice(item, { insertOnly: false });
        }
      },
        el('div', { className: 'chat-slash-item-head' },
          el('strong', { className: 'mono' }, label)
        ),
        el('div', { className: 'text-xs text-muted' }, item.desc || '')
      );
      palette.appendChild(btn);
    });
    palette.hidden = false;
    palette.classList.remove('hidden');
    state.slashPaletteOpen = true;
  }

  function openSlashCommandPalette(opts) {
    const options = opts || {};
    const input = getEl('chat-input');
    if (!input || input.disabled) return;
    hideSlashAgentMenu();
    const all = getAllSlashCommands();
    const query = options.showAll
      ? ''
      : String(options.query || '').trim().toLowerCase();
    const matches = query
      ? all.filter((item) =>
          item.cmd.toLowerCase().includes(query)
          || String(item.desc || '').toLowerCase().includes(query)
        )
      : all;
    if (matches.length === 0) {
      hideSlashCommandPalette();
      return;
    }
    state.slashPaletteMatches = matches;
    if (state.slashPaletteIndex >= matches.length) {
      state.slashPaletteIndex = 0;
    }
    renderSlashCommandPalette();
  }

  function updateSlashCommandPalette() {
    const input = getEl('chat-input');
    if (!input || input.disabled) {
      hideSlashCommandPalette();
      return;
    }
    const val = input.value;
    if (/^\s*\/agent(?:\s|$)/i.test(val)) {
      hideSlashCommandPalette();
      return;
    }
    const slashToken = val.match(/^\s*\/([^\s]*)$/);
    if (!slashToken) {
      hideSlashCommandPalette();
      return;
    }
    openSlashCommandPalette({ query: slashToken[1] });
  }

  function handleSlashPaletteKeydown(e) {
    if (!state.slashPaletteOpen || state.slashPaletteMatches.length === 0) {
      return false;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      state.slashPaletteIndex = (state.slashPaletteIndex + 1) % state.slashPaletteMatches.length;
      renderSlashCommandPalette();
      return true;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      state.slashPaletteIndex = (state.slashPaletteIndex - 1 + state.slashPaletteMatches.length) % state.slashPaletteMatches.length;
      renderSlashCommandPalette();
      return true;
    }
    if (e.key === 'Tab') {
      e.preventDefault();
      const chosen = state.slashPaletteMatches[state.slashPaletteIndex] || state.slashPaletteMatches[0];
      if (chosen) applySlashPaletteChoice(chosen, { insertOnly: true });
      return true;
    }
    if (e.key === 'Enter' && !e.shiftKey) {
      const input = getEl('chat-input');
      const trimmed = input ? input.value.trim() : '';
      const chosen = state.slashPaletteMatches[state.slashPaletteIndex] || state.slashPaletteMatches[0];
      if (chosen && (trimmed === chosen.cmd || trimmed.startsWith(`${chosen.cmd} `))) {
        hideSlashCommandPalette();
        return false;
      }
      e.preventDefault();
      if (chosen) {
        applySlashPaletteChoice(chosen, { insertOnly: Boolean(chosen.args) });
      }
      return true;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      hideSlashCommandPalette();
      return true;
    }
    return false;
  }

  async function ensureActiveConversation() {
    if (state.activeConversationId) {
      return state.activeConversationId;
    }
    if (state.conversations.length > 0) {
      const firstId = state.conversations[0].id;
      await selectConversation(firstId);
      return firstId;
    }
    const defaultAgent = state.agents.find((a) => a.id === 'commander' && a.enabled)
      || state.agents.find((a) => a.enabled);
    const agentId = defaultAgent ? defaultAgent.id : 'commander';
    const created = await apiFetch('/api/v1/conversations', {
      method: 'POST',
      body: { title: 'Main Chat', mode: { kind: 'direct', agent_id: agentId } }
    });
    await loadConversations();
    if (created && created.id) {
      await selectConversation(created.id);
      return created.id;
    }
    return state.activeConversationId || null;
  }

  async function executeSlashCommand(rawCmd, opts) {
    const options = opts || {};
    const cmdText = String(rawCmd || '').trim();
    if (!cmdText.startsWith('/')) return null;

    const input = getEl('chat-input');
    const savedInput = options.savedInput !== undefined ? options.savedInput : (input ? input.value : cmdText);
    const agentSelect = getEl('chat-agent-select');
    const selectedAgent = agentSelect ? agentSelect.value : '';

    const convId = options.convId || state.activeConversationId || (await ensureActiveConversation());
    if (!convId) {
      showToast('Select or create a conversation first.', 'warning');
      return null;
    }

    if (options.clearInput && input) {
      input.value = '';
    }
    hideSlashAgentMenu();
    hideSlashCommandPalette();

    const prevCount = state.chatMessages.length;
    try {
      const res = await apiFetch(`/api/v1/conversations/${encodeURIComponent(convId)}/command`, {
        method: 'POST',
        body: {
          command: cmdText,
          agent_id: selectedAgent || null
        }
      });

      const meta = (res && res.metadata) || {};
      const outputText = (res && (res.output || res.content || res.message || res.text)) || '';
      const cmdHead = cmdText.split(/\s+/)[0] || '/command';
      const cmdLower = cmdHead.toLowerCase();
      const cardTitle = (res && res.title) || cmdHead;

      if (meta.thinking_level) {
        const thinkingSel = getEl('thinking-level-select');
        if (thinkingSel) thinkingSel.value = meta.thinking_level;
      }
      if (meta.sandbox_updated || cmdLower === '/sandbox') {
        loadSandboxState({ silent: true }).catch(() => {});
      }
      if (cmdLower === '/doctor') {
        runDoctorChecks({ silent: true, fix: /\bfix\b/i.test(cmdText) }).catch(() => {});
      }

      if (res && res.ephemeral) {
        showEphemeralSideCard(cardTitle, outputText);
        return res;
      }

      if (meta.new_conversation_id) {
        hideEphemeralSideCard();
        await loadConversations();
        await selectConversation(meta.new_conversation_id);
        showToast('Started a fresh conversation.', 'success');
        return res;
      }

      if (state.activeConversationId === convId) {
        await loadChatMessages(convId);
      }
      if (cmdLower === '/compact') {
        await loadConversations();
        showToast(outputText || 'Conversation compacted.', 'success');
      } else if (outputText && state.chatMessages.length === prevCount) {
        showEphemeralSideCard(cardTitle, outputText);
      } else if (outputText && res && res.ok !== false && !res.ephemeral) {
        showToast(`${cmdHead} completed.`, 'info');
      }
      return res;
    } catch (err) {
      if (options.clearInput && input && !input.value.trim()) {
        input.value = savedInput;
      }
      showToast(`Slash command failed: ${err.message}`, 'danger');
      return null;
    }
  }

  /* -------------------- Photo & File Attachments -------------------- */
  function getMaxAttachmentBytes() {
    const srv = state.settingsRaw && state.settingsRaw.server;
    const configured = srv && Number(srv.upload_max_body_bytes || srv.max_body_bytes || 0);
    if (configured > 0) return configured;
    return 16 * 1024 * 1024;
  }

  function readFileAsDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => reject(new Error(`Failed to read ${file.name}`));
      reader.readAsDataURL(file);
    });
  }

  function estimateDataUrlBytes(dataUrl) {
    const idx = dataUrl.indexOf(',');
    if (idx === -1) return dataUrl.length;
    const b64Len = dataUrl.length - idx - 1;
    return Math.floor((b64Len * 3) / 4);
  }

  function downscaleImageDataUrl(dataUrl, targetMaxBytes) {
    return new Promise((resolve) => {
      const img = new Image();
      img.onload = () => {
        try {
          let w = img.naturalWidth || img.width || 1024;
          let h = img.naturalHeight || img.height || 1024;
          const maxDim = 1600;
          if (w > maxDim || h > maxDim) {
            const scale = Math.min(maxDim / w, maxDim / h);
            w = Math.max(1, Math.round(w * scale));
            h = Math.max(1, Math.round(h * scale));
          }
          const canvas = document.createElement('canvas');
          canvas.width = w;
          canvas.height = h;
          const ctx = canvas.getContext('2d');
          if (!ctx) {
            resolve(dataUrl);
            return;
          }
          ctx.drawImage(img, 0, 0, w, h);
          let out = canvas.toDataURL('image/jpeg', 0.85);
          if (estimateDataUrlBytes(out) > targetMaxBytes) {
            const scale2 = 0.7;
            canvas.width = Math.max(1, Math.round(w * scale2));
            canvas.height = Math.max(1, Math.round(h * scale2));
            ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            out = canvas.toDataURL('image/jpeg', 0.72);
          }
          resolve(out);
        } catch (_) {
          resolve(dataUrl);
        }
      };
      img.onerror = () => resolve(dataUrl);
      img.src = dataUrl;
    });
  }

  function renderAttachmentStrip() {
    const strip = getEl('chat-attachment-strip');
    if (!strip) return;
    clearEl(strip);
    if (state.pendingAttachments.length === 0) {
      strip.hidden = true;
      return;
    }
    strip.hidden = false;
    state.pendingAttachments.forEach((att, idx) => {
      const chipNodes = [];
      if (att.kind === 'image' && isSafeImageSrc(att.data_url)) {
        chipNodes.push(el('img', { src: att.data_url, alt: att.name }));
      }
      chipNodes.push(el('span', {
        className: 'attachment-name',
        title: `${att.name} (${fmtBytes(att.size_bytes)})`
      }, `${att.name} (${fmtBytes(att.size_bytes)})`));
      chipNodes.push(el('button', {
        type: 'button',
        'aria-label': `Remove ${att.name}`,
        onClick: () => {
          state.pendingAttachments.splice(idx, 1);
          renderAttachmentStrip();
        }
      }, '\u00d7'));
      strip.appendChild(el('div', { className: 'attachment-chip' }, chipNodes));
    });
  }

  async function handleChatFiles(fileList) {
    if (!fileList || fileList.length === 0) return;
    const maxTotalBytes = getMaxAttachmentBytes();
    const rasterDownscaleThreshold = 1.5 * 1024 * 1024;
    const files = Array.from(fileList);

    for (const file of files) {
      if (state.pendingAttachments.length >= 16) {
        showToast('Maximum 16 attachments per message.', 'warning');
        break;
      }
      const mime = String(file.type || 'application/octet-stream');
      const isRasterImg = mime.startsWith('image/') && mime !== 'image/svg+xml' && mime !== 'image/gif';
      const isImg = mime.startsWith('image/') && mime !== 'image/svg+xml';

      if (!isImg && file.size > maxTotalBytes) {
        showToast(`"${file.name}" exceeds upload limit (${fmtBytes(maxTotalBytes)}).`, 'warning');
        continue;
      }

      try {
        let dataUrl = await readFileAsDataUrl(file);
        let finalMime = mime;
        let finalBytes = file.size;

        if (isRasterImg && (file.size > rasterDownscaleThreshold || estimateDataUrlBytes(dataUrl) > rasterDownscaleThreshold)) {
          dataUrl = await downscaleImageDataUrl(dataUrl, rasterDownscaleThreshold);
          finalMime = 'image/jpeg';
          finalBytes = estimateDataUrlBytes(dataUrl);
        }

        if (estimateDataUrlBytes(dataUrl) > maxTotalBytes) {
          showToast(`"${file.name}" exceeds upload body limit (${fmtBytes(maxTotalBytes)}).`, 'warning');
          continue;
        }

        state.pendingAttachments.push({
          kind: isImg ? 'image' : 'file',
          name: file.name || 'attachment',
          mime_type: finalMime,
          data_url: dataUrl,
          size_bytes: finalBytes
        });
      } catch (err) {
        showToast(err.message || `Failed to attach ${file.name}`, 'danger');
      }
    }
    renderAttachmentStrip();
  }

  async function sendMessage(opts) {
    const options = opts || {};
    const input = $('chat-input');
    const rawContent = input.value.trim();
    const attachmentsToSend = state.pendingAttachments.slice();
    if (!rawContent && attachmentsToSend.length === 0) return;

    const slashAgentParsed = parseSlashAgentCommand(rawContent);
    if (rawContent.startsWith('/') && !slashAgentParsed) {
      const convId = state.activeConversationId || (await ensureActiveConversation().catch(() => null));
      await executeSlashCommand(rawContent, {
        convId,
        clearInput: true,
        savedInput: rawContent
      });
      return;
    }

    const targetConvId = state.activeConversationId;
    if (!targetConvId) return;

    const agentSelect = getEl('chat-agent-select');
    const thinkingSelect = getEl('thinking-level-select');
    let selectedAgent = agentSelect ? agentSelect.value : '';
    let content = rawContent;

    if (slashAgentParsed) {
      selectedAgent = slashAgentParsed.agent.id;
      if (agentSelect) agentSelect.value = selectedAgent;
      content = slashAgentParsed.prompt;
    }

    if (!content && attachmentsToSend.length === 0) {
      showToast('Enter a prompt after /agent <name> or attach a file.', 'warning');
      return;
    }

    const thinkingLevel = (thinkingSelect && thinkingSelect.value) || 'medium';

    // Clear composer immediately & keep it enabled for non-blocking concurrent prompts
    input.value = '';
    hideSlashAgentMenu();
    hideSlashCommandPalette();
    state.pendingAttachments = [];
    renderAttachmentStrip();
    updateComposerEnabled(true);
    input.focus();

    const tempId = `local-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const tempUserMsg = {
      id: tempId,
      conversation_id: targetConvId,
      role: 'user',
      agent_id: selectedAgent || null,
      content,
      attachments: attachmentsToSend,
      created_at: new Date().toISOString()
    };
    clearChatErrorsForConversation(targetConvId);
    state.chatMessages.push(tempUserMsg);
    const chatContainer = getEl('chat-messages');
    if (chatContainer && state.activeConversationId === targetConvId) {
      const empty = chatContainer.querySelector('.empty-state');
      if (empty) empty.remove();
      const nearBottom = chatContainer.scrollHeight - chatContainer.scrollTop - chatContainer.clientHeight < 140;
      chatContainer.appendChild(buildMessageBubble(tempUserMsg, false));
      if (nearBottom) {
        chatContainer.scrollTop = chatContainer.scrollHeight;
      }
    }

    const agenticBtn = getEl('btn-agentic-search-toggle');
    const agenticActive = Boolean(
      state.agenticSearchEnabled
      || (agenticBtn && agenticBtn.getAttribute('aria-pressed') === 'true')
    );
    const agenticAgentSel = getEl('chat-agentic-agent-select');
    const agenticAgentId = agenticAgentSel ? String(agenticAgentSel.value || '').trim() : '';
    const dispatchAgentId = agenticActive ? (agenticAgentId || selectedAgent || null) : (selectedAgent || null);
    const agenticHint = '[Agentic Search Mode — use browser / web_search tools when needed and show live viewport progress]';
    const dispatchContent = (agenticActive && !content.startsWith('/') && !content.includes('[Agentic Search Mode'))
      ? `${agenticHint}\n${content}`
      : content;
    if (agenticActive) {
      state.chatLiveBrowserDismissed = false;
      openChatLiveBrowserDock({ statusText: 'Searching...', pillClass: 'pill-info' });
    }
    try {
      const res = await apiFetch(`/api/v1/conversations/${encodeURIComponent(targetConvId)}/messages`, {
        method: 'POST',
        body: {
          content: dispatchContent,
          agent_id: dispatchAgentId,
          thinking_level: thinkingLevel,
          attachments: attachmentsToSend,
          async_dispatch: true,
          immediate: Boolean(options.immediate)
        }
      });

      if (res && res.user_message) {
        const idx = state.chatMessages.findIndex((m) => m.id === tempId);
        if (idx !== -1) {
          state.chatMessages[idx] = res.user_message;
          if (state.activeConversationId === targetConvId) {
            renderChatMessages();
          }
        }
      }

      const started = attachReturnedTask(res, targetConvId, selectedAgent, null);
      if (!started && state.activeConversationId === targetConvId) {
        await loadChatMessages(targetConvId);
      }
    } catch (err) {
      state.chatMessages = state.chatMessages.filter((m) => m.id !== tempId);
      if (input && !input.value.trim()) {
        input.value = rawContent;
      }
      if (attachmentsToSend.length > 0 && state.pendingAttachments.length === 0) {
        state.pendingAttachments = attachmentsToSend;
        renderAttachmentStrip();
      }
      if (state.activeConversationId === targetConvId) {
        renderChatMessages();
      }
      showChatErrorBanner(targetConvId, 'Message Dispatch Failed', err.message);
      showDeduplicatedErrorToast(`Failed to send message: ${err.message}`);
    }
  }

  async function onChatSend(ev) {
    if (ev && ev.preventDefault) ev.preventDefault();
    return sendMessage({ immediate: false });
  }

  async function onCancelActiveChatTask() {
    const convId = state.activeConversationId;
    if (!convId) return;
    try {
      const res = await apiFetch(`/api/v1/conversations/${encodeURIComponent(convId)}/cancel`, {
        method: 'POST',
        body: {}
      });
      for (const [tid, t] of [...state.activeChatTasks.entries()]) {
        if (t.conversation_id === convId) {
          state.activeChatTasks.delete(tid);
        }
      }
      for (const [tid, b] of [...state.chatStreams.entries()]) {
        if (b.conversationId === convId) {
          removeStreamBucket(tid);
        }
      }
      renderChatTaskBar();
      const count = res && res.cancelled_count !== undefined ? res.cancelled_count : '';
      showToast(
        count !== '' ? `Cancelled ${count} task(s) in this conversation.` : 'Task cancellation requested.',
        'warning'
      );
    } catch (err) {
      showToast(`Cancel failed: ${err.message}`, 'danger');
    }
  }

  function openNewDirectChatModal() {
    const enabledAgents = state.agents.filter((a) => a.enabled);
    const titleInput = el('input', { id: 'modal-direct-title', type: 'text', className: 'input', placeholder: 'Optional title' });
    const agentSelect = el('select', { id: 'modal-direct-agent', className: 'select' },
      enabledAgents.length === 0
        ? el('option', { value: 'commander' }, 'Commander (default)')
        : enabledAgents.map((a) => el('option', { value: a.id }, `${a.name} (${a.role})`))
    );

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const agentId = agentSelect.value;
        const chosen = state.agents.find((a) => a.id === agentId);
        const title = titleInput.value.trim() || `Chat with ${chosen ? chosen.name : agentId}`;
        try {
          const conv = await apiFetch('/api/v1/conversations', {
            method: 'POST',
            body: { title, mode: { kind: 'direct', agent_id: agentId } }
          });
          closeModal();
          await loadConversations();
          if (conv && conv.id) {
            setAgenticSearchEnabled(false);
            await selectConversation(conv.id);
          }
        } catch (err) {
          showToast(`Create chat failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'modal-direct-agent' }, 'Select Agent'),
        agentSelect
      ),
      el('div', { className: 'field' },
        el('label', { for: 'modal-direct-title' }, 'Conversation Title'),
        titleInput
      )
    );

    openModal('New Direct Chat', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Start Chat')
    ]);
  }

  function openNewGroupChatModal() {
    const enabledAgents = state.agents.filter((a) => a.enabled);
    const titleInput = el('input', { id: 'modal-group-title', type: 'text', className: 'input', placeholder: 'Multi-Agent War Room', required: true });
    const checkboxes = enabledAgents.map((a) => {
      const cb = el('input', { type: 'checkbox', value: a.id, checked: a.role === 'COMMANDER' });
      return {
        agent: a,
        cb,
        row: el('label', { className: 'checkbox-row' }, cb, el('span', null, `${a.name} (${a.role})`))
      };
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const participantIds = checkboxes.filter((x) => x.cb.checked).map((x) => x.agent.id);
        if (participantIds.length < 2) {
          showToast('Select at least 2 participant agents.', 'warning');
          return;
        }
        const title = titleInput.value.trim() || 'Group Chat';
        try {
          const conv = await apiFetch('/api/v1/conversations', {
            method: 'POST',
            body: { title, mode: { kind: 'group', participant_agent_ids: participantIds } }
          });
          closeModal();
          await loadConversations();
          if (conv && conv.id) {
            setAgenticSearchEnabled(false);
            await selectConversation(conv.id);
          }
        } catch (err) {
          showToast(`Create group chat failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'modal-group-title' }, 'Group Title'),
        titleInput
      ),
      el('div', { className: 'field' },
        el('label', null, 'Participant Agents (select 2 or more)'),
        el('div', { className: 'checkbox-grid' }, checkboxes.map((x) => x.row))
      )
    );

    openModal('New Group Chat', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Create Group')
    ]);
  }

  function confirmDeleteConversation(conv) {
    openModal('Delete Conversation', [
      el('p', null, `Delete conversation "${conv.title || conv.id}" and its message history?`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/conversations/${encodeURIComponent(conv.id)}`, { method: 'DELETE' });
            cleanupConversationClientState(conv.id);
            const deletedActive = state.activeConversationId === conv.id;
            for (const [tid, t] of [...state.activeChatTasks.entries()]) {
              if (t && t.conversation_id === conv.id) {
                state.activeChatTasks.delete(tid);
              }
            }
            for (const [tid, b] of [...state.chatStreams.entries()]) {
              if (b && b.conversationId === conv.id) {
                removeStreamBucket(tid);
              }
            }
            state.chatErrors.delete(conv.id);
            if (deletedActive) {
              state.activeConversationId = null;
              state.chatMessages = [];
              hideEphemeralSideCard();
            }
            closeModal();
            showToast('Conversation deleted.', 'success');
            await loadConversations();
            if (deletedActive) {
              if (state.activeConversationId) {
                await selectConversation(state.activeConversationId);
              } else {
                renderChatMessages();
                renderChatTaskBar();
                updateComposerEnabled(false);
              }
            }
          } catch (err) {
            showToast(`Delete failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete')
    ]);
  }

  /* -------------------- 3. Agents -------------------- */
  async function loadAgents() {
    const data = await apiFetch('/api/v1/agents');
    state.agents = unwrapList(data, ['agents']);
    populateAgentDropdowns();
    renderAgentsGrid();
  }

  function populateAgentDropdowns() {
    const targets = [$('a2a-filter-agent'), $('memory-filter-agent')];
    for (const sel of targets) {
      if (!sel) continue;
      const prev = sel.value;
      const firstLabel = sel.id === 'a2a-filter-agent' ? 'All Agents' : 'Any Agent';
      clearEl(sel);
      sel.appendChild(el('option', { value: '' }, firstLabel));
      for (const ag of state.agents) {
        sel.appendChild(el('option', { value: ag.id }, `${ag.name} (${ag.role})`));
      }
      sel.value = prev;
    }

    const chatAgentSel = getEl('chat-agent-select');
    if (chatAgentSel) {
      const prev = chatAgentSel.value;
      clearEl(chatAgentSel);
      chatAgentSel.appendChild(el('option', { value: '' }, 'Auto / Group Coordinator'));
      const enabledAgents = state.agents.filter((a) => a.enabled);
      for (const ag of enabledAgents) {
        chatAgentSel.appendChild(el('option', { value: ag.id }, `${ag.name} (${ag.role})`));
      }
      if (prev && [...chatAgentSel.options].some((o) => o.value === prev)) {
        chatAgentSel.value = prev;
      }
    }
    populateOptionalAgentSelect('chat-agentic-agent-select', 'Auto (Active Agent)');
    populateOptionalAgentSelect('sandbox-browser-agent-select', 'Auto (Active Agent)');
    populateScheduleAgentSelects();
    populateWebhookAgentSelect();
  }

  function renderAgentsGrid() {
    const grid = $('agents-grid');
    if (!grid) return;
    clearEl(grid);
    const enabledCount = state.agents.filter((a) => a.enabled).length;
    setText('agents-count-summary', `${state.agents.length} total \u00b7 ${enabledCount} enabled`);

    if (state.agents.length === 0) {
      grid.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, 'No agents registered'),
        el('span', { className: 'text-sm' }, 'Create a specialist agent to begin.')
      ));
      return;
    }

    for (const ag of state.agents) {
      const stats = ag.stats || {};
      const toggleBtn = ag.enabled
        ? el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            disabled: ag.is_protected && ag.role === 'COMMANDER',
            onClick: () => toggleAgentEnabled(ag.id, false)
          }, 'Disable')
        : el('button', {
            type: 'button',
            className: 'btn btn-success btn-sm',
            onClick: () => toggleAgentEnabled(ag.id, true)
          }, 'Enable');

      const card = el('article', { className: 'card agent-card' },
        el('div', { className: 'agent-card-top' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title truncate' }, ag.name),
            el('div', { className: 'text-sm text-muted truncate' }, ag.role_label || ag.role)
          ),
          el('div', { className: 'agent-badges' },
            el('span', { className: `pill ${ag.enabled ? 'pill-success' : 'pill-muted'}` }, ag.enabled ? 'Enabled' : 'Disabled'),
            el('span', { className: 'pill pill-info' }, ag.role),
            ag.sandbox_mode ? el('span', { className: 'pill pill-muted mono' }, ag.sandbox_mode) : null,
            ag.is_protected ? el('span', { className: 'pill pill-warning' }, 'Protected') : null
          )
        ),
        el('p', { className: 'text-sm text-secondary' }, ag.description || 'No description provided.'),
        el('div', { className: 'agent-stats-grid' },
          el('div', { className: 'stat-cell' },
            el('span', { className: 'stat-label' }, 'Current Task'),
            el('span', { className: 'stat-value mono truncate' }, stats.current_task_id ? shortId(stats.current_task_id) : 'Idle')
          ),
          el('div', { className: 'stat-cell' },
            el('span', { className: 'stat-label' }, 'Last Activity'),
            el('span', { className: 'stat-value' }, stats.last_activity_at ? fmtTime(stats.last_activity_at) : 'Never')
          ),
          el('div', { className: 'stat-cell' },
            el('span', { className: 'stat-label' }, 'Completed / Errors'),
            el('span', { className: 'stat-value mono' }, `${stats.completed_tasks || 0} / ${stats.failed_tasks || 0}`)
          ),
          el('div', { className: 'stat-cell' },
            el('span', { className: 'stat-label' }, 'Memory Usage'),
            el('span', { className: 'stat-value mono' }, `${stats.memory_entries || 0} (${fmtBytes(stats.memory_bytes || 0)})`)
          )
        ),
        el('div', { className: 'card-actions' },
          toggleBtn,
          el('button', { type: 'button', className: 'btn btn-secondary btn-sm', onClick: () => openAgentModal(ag) }, 'Edit'),
          el('button', { type: 'button', className: 'btn btn-primary btn-sm', onClick: () => openChatWithAgent(ag) }, 'Open Chat'),
          el('button', { type: 'button', className: 'btn btn-secondary btn-sm', onClick: () => viewAgentMemory(ag.id) }, 'View Memory'),
          !ag.is_protected
            ? el('button', { type: 'button', className: 'btn btn-danger btn-sm', onClick: () => confirmDeleteAgent(ag) }, 'Delete')
            : null
        )
      );
      grid.appendChild(card);
    }
  }

  async function toggleAgentEnabled(agentId, enable) {
    const action = enable ? 'enable' : 'disable';
    try {
      await apiFetch(`/api/v1/agents/${encodeURIComponent(agentId)}/${action}`, { method: 'POST', body: {} });
      showToast(`Agent ${enable ? 'enabled' : 'disabled'}.`, 'success');
      await loadAgents();
    } catch (err) {
      showToast(`Failed to ${action} agent: ${err.message}`, 'danger');
    }
  }

  async function openChatWithAgent(agent) {
    try {
      await loadConversations('');
      const existing = state.conversations.find((c) => c.mode && c.mode.kind === 'direct' && c.mode.agent_id === agent.id);
      if (existing) {
        state.activeConversationId = existing.id;
      } else {
        const created = await apiFetch('/api/v1/conversations', {
          method: 'POST',
          body: { title: `Direct: ${agent.name}`, mode: { kind: 'direct', agent_id: agent.id } }
        });
        if (created && created.id) state.activeConversationId = created.id;
      }
      switchSection('chat');
    } catch (err) {
      showToast(`Failed to open chat: ${err.message}`, 'danger');
    }
  }

  function viewAgentMemory(agentId) {
    state.memoryFilters.ownerAgentId = agentId;
    const sel = $('memory-filter-agent');
    if (sel) sel.value = agentId;
    switchSection('memory');
  }

  async function enableAllAgents() {
    try {
      const res = await apiFetch('/api/v1/agents/enable-all', { method: 'POST', body: {} });
      const count = res && res.enabled_count !== undefined ? res.enabled_count : 0;
      showToast(`Booted / enabled ${count} agent(s).`, 'success');
      await loadAgents();
    } catch (err) {
      showToast(`Enable all agents failed: ${err.message}`, 'danger');
    }
  }

  function openAgentModal(existing) {
    const isEdit = Boolean(existing);
    const ag = existing || {
      name: '',
      description: '',
      role: 'CUSTOM',
      role_label: 'Specialist',
      system_prompt: 'You are a helpful specialist agent.',
      model_config: {
        provider: 'ps5lm',
        model_override: null,
        api_mode: 'chat_completions',
        temperature: 0.4,
        max_output_tokens: 1024,
        context_limit: 8192,
        thinking_level: 'medium'
      },
      enabled_tools: [],
      denied_tools: [],
      sandbox_mode: null,
      memory_permissions: {
        read_own: true,
        write_own: true,
        read_shared: true,
        write_shared: true,
        read_user_prefs: true,
        write_user_prefs: false,
        cross_agent_read: false
      },
      max_task_duration_secs: 0,
      max_reasoning_iterations: 0,
      priority: 5,
      can_communicate: true
    };
    const mc = ag.model_config || {};

    const nameIn = el('input', { id: 'ag-name', type: 'text', className: 'input', value: ag.name, required: true });
    const roleSel = el('select', { id: 'ag-role', className: 'select' },
      ['COMMANDER', 'RESEARCHER', 'PROGRAMMER', 'ANALYST', 'EXECUTOR', 'CUSTOM'].map((r) =>
        el('option', { value: r, selected: ag.role === r }, r)
      )
    );
    const roleLabelIn = el('input', { id: 'ag-role-label', type: 'text', className: 'input', value: ag.role_label || '' });
    const descIn = el('input', { id: 'ag-desc', type: 'text', className: 'input', value: ag.description || '' });
    const promptIn = el('textarea', { id: 'ag-prompt', className: 'input', rows: 4 }, ag.system_prompt || '');
    const modelOverrideIn = el('input', {
      id: 'ag-model-override',
      type: 'text',
      className: 'input mono',
      value: mc.model_override || '',
      placeholder: 'Inherit global default'
    });
    const apiModeSel = el('select', { id: 'ag-api-mode', className: 'select' },
      ['chat_completions', 'responses', 'anthropic_messages', 'completions', 'system_one'].map((m) =>
        el('option', { value: m, selected: mc.api_mode === m }, m)
      )
    );
    const currentThinking = String(mc.thinking_level || 'medium').toLowerCase();
    const thinkingSel = el('select', { id: 'agent-model-thinking', className: 'select' },
      ['off', 'low', 'medium', 'high', 'xhigh', 'max'].map((lvl) =>
        el('option', { value: lvl, selected: currentThinking === lvl }, lvl)
      )
    );
    const currentSandboxMode = String(ag.sandbox_mode || '').trim().toLowerCase();
    const sandboxModeSel = el('select', { id: 'agent-sandbox-mode', className: 'select' }, [
      el('option', { value: '', selected: !currentSandboxMode }, 'Inherit Global Default'),
      el('option', { value: 'read_only', selected: currentSandboxMode === 'read_only' }, 'Read Only'),
      el('option', { value: 'workspace_write', selected: currentSandboxMode === 'workspace_write' }, 'Workspace Write'),
      el('option', { value: 'full_sandbox', selected: currentSandboxMode === 'full_sandbox' }, 'Full Sandbox')
    ]);
    const deniedToolsIn = el('input', {
      id: 'agent-denied-tools',
      type: 'text',
      className: 'input mono',
      value: Array.isArray(ag.denied_tools) ? ag.denied_tools.join(', ') : '',
      placeholder: 'e.g. sandbox_exec, write_file'
    });
    const tempIn = el('input', {
      id: 'ag-temperature',
      type: 'number',
      min: 0,
      max: 2,
      step: 0.05,
      className: 'input mono',
      value: mc.temperature ?? 0.4
    });
    const ctxLimitIn = el('input', {
      id: 'ag-context-limit',
      type: 'number',
      min: 0,
      max: 1048576,
      step: 1,
      className: 'input mono',
      value: mc.context_limit ?? 8192
    });
    const maxOutIn = el('input', {
      id: 'ag-max-output-tokens',
      type: 'number',
      min: 0,
      max: 1048576,
      step: 1,
      className: 'input mono',
      value: mc.max_output_tokens ?? 1024
    });
    const maxDurIn = el('input', {
      id: 'ag-max-dur',
      type: 'number',
      min: 0,
      max: 86400,
      step: 1,
      className: 'input mono',
      value: ag.max_task_duration_secs ?? 0
    });
    const maxIterIn = el('input', {
      id: 'ag-max-reasoning-iters',
      type: 'number',
      min: 0,
      max: 1000,
      step: 1,
      className: 'input mono',
      value: ag.max_reasoning_iterations ?? 0
    });
    const priorityIn = el('input', { id: 'ag-priority', type: 'number', min: -10, max: 100, className: 'input mono', value: ag.priority !== undefined ? ag.priority : 5 });
    const canCommCb = el('input', { id: 'ag-can-comm', type: 'checkbox', checked: ag.can_communicate !== false });

    const toolChoices = state.tools.length > 0
      ? state.tools
      : (ag.enabled_tools || []).map((id) => ({ id, name: id }));
    const toolCbs = toolChoices.map((t) => {
      const cb = el('input', { type: 'checkbox', value: t.id, checked: (ag.enabled_tools || []).includes(t.id) });
      return { id: t.id, cb, row: el('label', { className: 'checkbox-row' }, cb, el('span', { className: 'mono text-sm' }, t.id)) };
    });

    const memPermKeys = [
      ['read_own', 'Read Own Memory'],
      ['write_own', 'Write Own Memory'],
      ['read_shared', 'Read Shared Memory'],
      ['write_shared', 'Write Shared Memory'],
      ['read_user_prefs', 'Read User Prefs'],
      ['write_user_prefs', 'Write User Prefs'],
      ['cross_agent_read', 'Cross-Agent Read']
    ];
    const memPerms = ag.memory_permissions || {};
    const memCbs = memPermKeys.map(([key, label]) => {
      const cb = el('input', { type: 'checkbox', checked: Boolean(memPerms[key]) });
      return { key, cb, row: el('label', { className: 'checkbox-row' }, cb, el('span', { className: 'text-sm' }, label)) };
    });

    let agentFormSubmitting = false;
    let saveBtn = null;

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        if (agentFormSubmitting) return;
        const cleanName = nameIn.value.trim();
        if (!cleanName) {
          showToast('Agent name is required.', 'warning');
          nameIn.focus();
          return;
        }
        const maxOutputTokens = Number(maxOutIn.value);
        const contextLimit = Number(ctxLimitIn.value);
        const maxTaskDuration = Number(maxDurIn.value);
        const maxReasoningIters = Number(maxIterIn.value);
        const temperature = Number(tempIn.value);

        if (Number.isNaN(maxOutputTokens) || maxOutputTokens < 0) {
          showToast('Max Output Tokens must be 0 (Unlimited) or greater.', 'warning');
          return;
        }
        if (Number.isNaN(contextLimit) || contextLimit < 0) {
          showToast('Context Limit must be 0 (Default/Unlimited) or greater.', 'warning');
          return;
        }
        if (maxOutputTokens > 0 && contextLimit > 0 && maxOutputTokens >= contextLimit) {
          showToast('Max Output Tokens must be less than Context Limit when both are > 0.', 'warning');
          return;
        }
        if (Number.isNaN(maxTaskDuration) || maxTaskDuration < 0) {
          showToast('Max Task Duration must be 0 (Unlimited) or greater.', 'warning');
          return;
        }
        if (Number.isNaN(maxReasoningIters) || maxReasoningIters < 0) {
          showToast('Max Reasoning Iterations must be 0 (Unlimited) or greater.', 'warning');
          return;
        }

        const memoryPermissions = {};
        for (const item of memCbs) memoryPermissions[item.key] = item.cb.checked;

        const deniedTools = [...new Set(
          deniedToolsIn.value
            .split(/[,\r\n]+/)
            .map((s) => s.trim())
            .filter(Boolean)
        )];

        const payload = {
          name: cleanName,
          description: descIn.value.trim(),
          role: roleSel.value,
          role_label: roleLabelIn.value.trim() || roleSel.value,
          system_prompt: promptIn.value,
          model_config: {
            provider: mc.provider || 'ps5lm',
            model_override: modelOverrideIn.value.trim() || null,
            api_mode: apiModeSel.value,
            temperature: Number.isNaN(temperature) ? 0.4 : temperature,
            max_output_tokens: Math.floor(maxOutputTokens),
            context_limit: Math.floor(contextLimit),
            thinking_level: thinkingSel.value || 'medium'
          },
          enabled_tools: toolCbs.filter((t) => t.cb.checked).map((t) => t.id),
          denied_tools: deniedTools,
          sandbox_mode: sandboxModeSel.value || null,
          memory_permissions: memoryPermissions,
          max_task_duration_secs: Math.floor(maxTaskDuration),
          max_reasoning_iterations: Math.floor(maxReasoningIters),
          priority: Number(priorityIn.value) || 0,
          can_communicate: canCommCb.checked
        };

        agentFormSubmitting = true;
        if (saveBtn) saveBtn.disabled = true;
        try {
          if (isEdit) {
            await apiFetch(`/api/v1/agents/${encodeURIComponent(ag.id)}`, { method: 'PUT', body: payload });
            showToast(`Updated agent "${payload.name}".`, 'success');
          } else {
            await apiFetch('/api/v1/agents', { method: 'POST', body: payload });
            showToast(`Created agent "${payload.name}".`, 'success');
          }
          closeAgentDialog();
          await loadAgents();
        } catch (err) {
          showToast(`Save agent failed: ${err.message}`, 'danger');
        } finally {
          agentFormSubmitting = false;
          if (saveBtn) saveBtn.disabled = false;
        }
      }
    },
      el('div', { className: 'form-grid' },
        el('div', { className: 'field' }, el('label', { for: 'ag-name' }, 'Name'), nameIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-role' }, 'Role'), roleSel),
        el('div', { className: 'field' }, el('label', { for: 'ag-role-label' }, 'Role Label'), roleLabelIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-desc' }, 'Description'), descIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-model-override' }, 'Model Override'), modelOverrideIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-api-mode' }, 'API Mode'), apiModeSel),
        el('div', { className: 'field' }, el('label', { for: 'agent-model-thinking' }, 'Thinking Level'), thinkingSel),
        el('div', { className: 'field' }, el('label', { for: 'agent-sandbox-mode' }, 'Sandbox Mode'), sandboxModeSel),
        el('div', { className: 'field' }, el('label', { for: 'ag-temperature' }, 'Temperature'), tempIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-context-limit' }, 'Context Limit (0 = Default)'), ctxLimitIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-max-output-tokens' }, 'Max Output Tokens (0 = Unlimited)'), maxOutIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-max-dur' }, 'Max Duration secs (0 = Unlimited)'), maxDurIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-max-reasoning-iters' }, 'Max Reasoning Iters (0 = Unlimited)'), maxIterIn),
        el('div', { className: 'field' }, el('label', { for: 'ag-priority' }, 'Priority'), priorityIn)
      ),
      el('div', { className: 'field' }, el('label', { for: 'agent-denied-tools' }, 'Denied Tools (comma-separated, overrides enabled tools)'), deniedToolsIn),
      el('div', { className: 'field' }, el('label', { for: 'ag-prompt' }, 'System Prompt'), promptIn),
      el('label', { className: 'checkbox-row' }, canCommCb, el('span', null, 'Can Communicate with Other Agents (A2A)')),
      el('div', { className: 'field' },
        el('label', null, 'Enabled Tools'),
        el('div', { className: 'checkbox-grid' }, toolCbs.map((t) => t.row))
      ),
      el('div', { className: 'field' },
        el('label', null, 'Memory Permissions'),
        el('div', { className: 'checkbox-grid' }, memCbs.map((m) => m.row))
      )
    );

    saveBtn = el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, isEdit ? 'Save Changes' : 'Create Agent');
    openAgentDialog(isEdit ? `Edit Agent: ${ag.name}` : 'Create Agent', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeAgentDialog }, 'Cancel'),
      saveBtn
    ]);
  }

  function confirmDeleteAgent(ag) {
    openModal('Delete Agent', [
      el('p', null, `Delete agent "${ag.name}"? This also removes its runtime registration.`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/agents/${encodeURIComponent(ag.id)}`, { method: 'DELETE' });
            closeModal();
            showToast(`Agent "${ag.name}" deleted.`, 'success');
            await loadAgents();
          } catch (err) {
            showToast(`Delete agent failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete Agent')
    ]);
  }

  /* -------------------- 4. Agent Communication (A2A) -------------------- */
  async function loadA2aMessages() {
    const params = new URLSearchParams();
    const taskId = ($('a2a-filter-task').value || '').trim();
    const agentId = $('a2a-filter-agent').value;
    if (taskId) params.set('task_id', taskId);
    if (agentId) params.set('agent_id', agentId);
    const qs = params.toString();
    const data = await apiFetch(`/api/v1/agent-messages${qs ? `?${qs}` : ''}`);
    state.a2aMessages = unwrapList(data, ['messages', 'envelopes']);
    renderA2aList();
  }

  function renderA2aList() {
    const container = $('a2a-list');
    clearEl(container);
    const typeFilter = $('a2a-filter-type').value;
    const rows = typeFilter
      ? state.a2aMessages.filter((m) => String(m.message_type).toUpperCase() === typeFilter)
      : state.a2aMessages;

    if (rows.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No agent-to-agent messages match the current filters'),
        el('span', { className: 'text-sm' }, 'Multi-agent delegation and responses will appear here.')
      ));
      return;
    }

    const tbody = el('tbody');
    for (const m of rows) {
      tbody.appendChild(el('tr', null,
        el('td', { className: 'mono text-sm' }, fmtTime(m.timestamp)),
        el('td', null, agentDisplayName(m.sender_agent_id)),
        el('td', null, agentDisplayName(m.recipient_agent_id)),
        el('td', null, el('span', { className: 'pill pill-info' }, m.message_type)),
        el('td', null, el('span', { className: `pill ${statusPillClass(m.status)}` }, m.status)),
        el('td', { className: 'mono text-sm' }, m.task_id ? shortId(m.task_id) : '\u2014'),
        el('td', null, el('div', { className: 'text-sm' }, m.content))
      ));
    }

    container.appendChild(el('div', { className: 'table-scroll' },
      el('table', { className: 'data-table' },
        el('thead', null, el('tr', null,
          el('th', null, 'Time'),
          el('th', null, 'Sender'),
          el('th', null, 'Recipient'),
          el('th', null, 'Type'),
          el('th', null, 'Status'),
          el('th', null, 'Task ID'),
          el('th', null, 'Content')
        )),
        tbody
      )
    ));
  }

  /* -------------------- 5. Tasks -------------------- */
  async function loadTasks() {
    const data = await apiFetch('/api/v1/tasks');
    state.tasks = unwrapList(data, ['tasks']);
    for (const t of state.tasks) {
      if (!t || !t.id) continue;
      const tid = String(t.id);
      const st = String(t.status || '').toUpperCase();
      if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(st)) {
        removeStreamBucket(tid);
      }
      if (t.conversation_id) {
        upsertActiveChatTask({
          task_id: t.id,
          conversation_id: t.conversation_id,
          agent_id: t.assigned_agent_id,
          status: t.status,
          progress_pct: t.progress_pct,
          progress_note: t.progress_note
        });
      } else if (state.activeChatTasks.has(tid)) {
        upsertActiveChatTask(Object.assign({}, state.activeChatTasks.get(tid), {
          status: t.status,
          progress_pct: t.progress_pct,
          progress_note: t.progress_note
        }));
      }
    }
    renderChatTaskBar();
    renderTasks();
  }

  function renderTasks() {
    const container = $('tasks-container');
    clearEl(container);
    const f = state.taskFilter;
    const rows = state.tasks.filter((t) => {
      const s = String(t.status || '').toUpperCase();
      if (f === 'all') return true;
      if (f === 'active') return ['PENDING', 'ASSIGNED', 'RUNNING', 'WAITING'].includes(s);
      return s === f;
    });

    setText('tasks-count-summary', `${rows.length} of ${state.tasks.length} tasks`);

    if (rows.length === 0) {
      const hasFiltered = state.tasks.length > 0 && f !== 'all';
      container.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, hasFiltered ? `No ${String(f).toLowerCase()} tasks` : 'No tasks found'),
        el('span', { className: 'text-sm' },
          hasFiltered
            ? `Switch filter from "${f}" to view the ${state.tasks.length} recorded task(s).`
            : 'Send a message in Main Chat to create and run a task.'
        )
      ));
      return;
    }

    const tbody = el('tbody');
    for (const t of rows) {
      const s = String(t.status || '').toUpperCase();
      const terminal = ['COMPLETED', 'FAILED', 'CANCELLED'].includes(s);
      tbody.appendChild(el('tr', null,
        el('td', { className: 'mono text-sm', title: t.id }, shortId(t.id)),
        el('td', null, el('div', { className: 'text-sm' }, t.user_request || '\u2014')),
        el('td', null, agentDisplayName(t.assigned_agent_id)),
        el('td', { className: 'mono text-sm' }, t.parent_task_id ? shortId(t.parent_task_id) : '\u2014'),
        el('td', { className: 'num' }, t.priority ?? 0),
        el('td', null, el('span', { className: `pill ${statusPillClass(s)}` }, s)),
        el('td', { className: 'num' }, `${t.progress_pct ?? 0}%`),
        el('td', { className: 'mono text-sm' }, fmtTime(t.created_at)),
        el('td', null,
          el('div', { className: 'card-actions' },
            el('button', { type: 'button', className: 'btn btn-secondary btn-sm', onClick: () => inspectTask(t) }, 'Inspect'),
            !terminal
              ? el('button', { type: 'button', className: 'btn btn-danger btn-sm', onClick: () => cancelTaskById(t.id) }, 'Cancel')
              : null
          )
        )
      ));
    }

    container.appendChild(el('div', { className: 'table-scroll' },
      el('table', { className: 'data-table' },
        el('thead', null, el('tr', null,
          el('th', null, 'ID'),
          el('th', null, 'Request'),
          el('th', null, 'Agent'),
          el('th', null, 'Parent'),
          el('th', { className: 'num' }, 'Pri'),
          el('th', null, 'Status'),
          el('th', { className: 'num' }, 'Prog'),
          el('th', null, 'Created'),
          el('th', null, 'Actions')
        )),
        tbody
      )
    ));
  }

  async function inspectTask(task) {
    let events = [];
    try {
      const evData = await apiFetch(`/api/v1/tasks/${encodeURIComponent(task.id)}/events`);
      events = unwrapList(evData, ['events']);
    } catch (_) {
      events = [];
    }

    const bodyNodes = [
      el('div', { className: 'agent-stats-grid' },
        el('div', { className: 'stat-cell' }, el('span', { className: 'stat-label' }, 'Task ID'), el('span', { className: 'stat-value mono' }, task.id)),
        el('div', { className: 'stat-cell' }, el('span', { className: 'stat-label' }, 'Assigned Agent'), el('span', { className: 'stat-value' }, agentDisplayName(task.assigned_agent_id))),
        el('div', { className: 'stat-cell' }, el('span', { className: 'stat-label' }, 'Status'), el('span', { className: 'stat-value' }, `${task.status} (${task.progress_pct}%)`)),
        el('div', { className: 'stat-cell' }, el('span', { className: 'stat-label' }, 'Timestamps'), el('span', { className: 'stat-value text-sm' }, `${fmtDateTime(task.created_at)}`))
      ),
      el('div', { className: 'field' },
        el('label', null, 'User Request'),
        el('pre', { className: 'code-box' }, task.user_request || '')
      ),
      task.result ? el('div', { className: 'field' },
        el('label', null, 'Result'),
        el('pre', { className: 'code-box' }, task.result)
      ) : null,
      task.error_info ? el('div', { className: 'field' },
        el('label', null, 'Error Info'),
        el('pre', { className: 'code-box' }, task.error_info)
      ) : null,
      el('div', { className: 'field' },
        el('label', null, `Task Events (${events.length})`),
        events.length === 0
          ? el('span', { className: 'text-sm text-muted' }, 'No recorded task events.')
          : el('pre', { className: 'code-box' }, events.map((e) => `${fmtTime(e.timestamp)} [${e.event_type}] ${e.details}`).join('\n'))
      )
    ];

    openModal(`Task Inspection`, bodyNodes, [
      el('button', { type: 'button', className: 'btn btn-primary', onClick: closeModal }, 'Close')
    ]);
  }

  async function cancelTaskById(taskId) {
    try {
      await apiFetch(`/api/v1/tasks/${encodeURIComponent(taskId)}/cancel`, { method: 'POST', body: {} });
      showToast('Task cancelled.', 'warning');
      await loadTasks();
    } catch (err) {
      showToast(`Cancel task failed: ${err.message}`, 'danger');
    }
  }

  async function rehydrateActiveChatTasks(convId) {
    const seq = ++state.taskRehydrateSeq;
    const targetConv = convId || state.activeConversationId || '';
    const knownAtStart = new Set(state.activeChatTasks.keys());
    const streamsAtStart = new Set(state.chatStreams.keys());
    try {
      const url = targetConv
        ? `/api/v1/tasks?status=active&conversation_id=${encodeURIComponent(targetConv)}&limit=100`
        : '/api/v1/tasks?status=active&limit=100';
      const data = await apiFetch(url);
      if (seq !== state.taskRehydrateSeq) return;
      if (targetConv && state.activeConversationId && state.activeConversationId !== targetConv) return;

      const list = unwrapList(data, ['tasks']);
      const activeIds = new Set();
      const terminalIds = new Set();

      for (const t of list) {
        if (!t || !t.id) continue;
        const tid = String(t.id).trim();
        if (!tid) continue;
        const st = String(t.status || '').toUpperCase();
        if (['PENDING', 'ASSIGNED', 'RUNNING', 'WAITING'].includes(st)) {
          activeIds.add(tid);
          if (t.conversation_id) {
            upsertActiveChatTask({
              task_id: tid,
              conversation_id: t.conversation_id,
              agent_id: t.assigned_agent_id,
              status: t.status,
              progress_pct: t.progress_pct,
              progress_note: t.progress_note
            });
          }
        } else if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(st)) {
          terminalIds.add(tid);
        }
      }

      let cleanedActiveConv = false;
      for (const tid of terminalIds) {
        const activeEntry = state.activeChatTasks.get(tid);
        const streamBucket = state.chatStreams.get(tid);
        if (
          (activeEntry && activeEntry.conversation_id === state.activeConversationId)
          || (streamBucket && streamBucket.conversationId === state.activeConversationId)
        ) {
          cleanedActiveConv = true;
        }
        markChatTaskTerminal(tid);
      }

      for (const [tid, entry] of [...state.activeChatTasks.entries()]) {
        const staleInScope = knownAtStart.has(tid)
          && (!targetConv || entry.conversation_id === targetConv)
          && !activeIds.has(tid);
        if (terminalIds.has(tid) || staleInScope) {
          if (entry.conversation_id === state.activeConversationId) {
            cleanedActiveConv = true;
          }
          state.activeChatTasks.delete(tid);
        }
      }
      for (const [tid, bucket] of [...state.chatStreams.entries()]) {
        const staleStream = streamsAtStart.has(tid)
          && !tid.includes(':')
          && (!targetConv || bucket.conversationId === targetConv)
          && !activeIds.has(tid);
        if ((!targetConv || bucket.conversationId === targetConv) && (terminalIds.has(tid) || staleStream)) {
          if (bucket.conversationId === state.activeConversationId) {
            cleanedActiveConv = true;
          }
          removeStreamBucket(tid);
        }
      }

      renderChatTaskBar();
      if (cleanedActiveConv && state.currentSection === 'chat' && state.activeConversationId) {
        await loadChatMessages(state.activeConversationId);
      }
    } catch (_) {
      // Best-effort rehydration of background chat tasks
    }
  }

  /* -------------------- 5b. Schedules & Autonomous Tasks -------------------- */
  function getLocalTimezoneOffsetMins() {
    const mins = 0 - new Date().getTimezoneOffset();
    return mins === 0 ? 0 : mins;
  }

  function isoToDatetimeLocalInput(iso) {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return '';
      const pad = (n) => String(n).padStart(2, '0');
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    } catch (_) {
      return '';
    }
  }

  function datetimeLocalInputToIso(localVal) {
    const trimmed = String(localVal || '').trim();
    if (!trimmed) return null;
    const d = new Date(trimmed);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  function formatIntervalHuman(rawSecs) {
    const secs = Number(rawSecs);
    if (!Number.isFinite(secs) || secs <= 0) return 'Custom interval';
    if (secs % 86400 === 0 && secs >= 86400) return `${secs / 86400}d`;
    if (secs % 3600 === 0 && secs >= 3600) return `${secs / 3600}h`;
    if (secs % 60 === 0 && secs >= 60) return `${secs / 60}m`;
    return `${secs}s`;
  }

  function formatScheduleCadence(sched) {
    if (!sched) return 'Schedule';
    const kind = String(sched.kind || 'once').toLowerCase();
    if (kind === 'once') {
      return sched.run_at ? `Once \u00b7 ${fmtDateTime(sched.run_at)}` : 'Once';
    }
    if (kind === 'interval') {
      return `Every ${formatIntervalHuman(sched.interval_secs)}`;
    }
    if (kind === 'daily') {
      return `Daily \u00b7 ${sched.daily_time || '09:00'}`;
    }
    if (kind === 'cron') {
      return `Cron \u00b7 ${sched.cron_expr || '* * * * *'}`;
    }
    return kind;
  }

  function populateScheduleAgentSelects(preserveAgentId) {
    const selectIds = ['schedule-ai-agent', 'sched-agent'];
    const enabledAgents = state.agents.filter((a) => a.enabled);

    for (const selId of selectIds) {
      const sel = getEl(selId);
      if (!sel) continue;
      const desired = selId === 'sched-agent' && preserveAgentId !== undefined
        ? (preserveAgentId || '')
        : (sel.value || '');

      clearEl(sel);
      sel.appendChild(el('option', { value: '' }, 'Auto (Commander)'));
      for (const ag of enabledAgents) {
        sel.appendChild(el('option', { value: ag.id }, `${ag.name} (${ag.role})`));
      }

      if (desired && ![...sel.options].some((o) => o.value === desired)) {
        const disabledMatch = state.agents.find((a) => a.id === desired);
        const label = disabledMatch
          ? `${disabledMatch.name} (${disabledMatch.role}${disabledMatch.enabled ? '' : ', disabled'})`
          : desired;
        sel.appendChild(el('option', { value: desired }, label));
      }
      sel.value = [...sel.options].some((o) => o.value === desired) ? desired : '';
    }
  }

  function updateSchedulesNavBadge() {
    const badge = getEl('nav-badge-schedules');
    if (!badge) return;
    const enabledCount = state.schedules.filter((s) => Boolean(s.enabled)).length;
    badge.textContent = String(enabledCount);
  }

  function upsertScheduleRecord(raw) {
    const sched = (raw && raw.schedule) ? raw.schedule : raw;
    if (!sched || !sched.id) return;
    const idx = state.schedules.findIndex((s) => s.id === sched.id);
    if (idx !== -1) {
      state.schedules[idx] = sched;
    } else {
      state.schedules.unshift(sched);
    }
    updateSchedulesNavBadge();
    if (state.currentSection === 'schedules') {
      renderSchedules();
    }
  }

  async function refreshSchedulesCount() {
    try {
      const data = await apiFetch('/api/v1/schedules');
      state.schedules = unwrapList(data, ['schedules']);
      updateSchedulesNavBadge();
      if (state.currentSection === 'schedules') {
        renderSchedules();
      }
    } catch (_) {
      // Best-effort badge refresh
    }
  }

  async function loadSchedules() {
    populateScheduleAgentSelects();
    const data = await apiFetch('/api/v1/schedules');
    state.schedules = unwrapList(data, ['schedules']);
    updateSchedulesNavBadge();
    renderSchedules();
  }

  function syncScheduleFilterButtons() {
    document.querySelectorAll('.schedule-filter-pills [data-sched-filter]').forEach((btn) => {
      const active = btn.dataset.schedFilter === state.scheduleFilter;
      btn.classList.toggle('is-active', active);
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-selected', String(active));
    });
  }

  function renderSchedules() {
    const container = getEl('schedules-container');
    if (!container) return;
    clearEl(container);
    syncScheduleFilterButtons();

    const filter = state.scheduleFilter || 'all';
    const isFailed = (s) => String(s.last_status || '').toLowerCase() === 'failed' || Boolean(s.last_error);

    const rows = state.schedules.filter((s) => {
      if (filter === 'enabled') return Boolean(s.enabled);
      if (filter === 'paused') return !s.enabled;
      if (filter === 'failed') return isFailed(s);
      return true;
    });

    const total = state.schedules.length;
    const enabledCount = state.schedules.filter((s) => Boolean(s.enabled)).length;
    const failedCount = state.schedules.filter(isFailed).length;
    setText(
      'schedules-stats',
      `${rows.length} of ${total} schedule(s) \u00b7 ${enabledCount} enabled${failedCount > 0 ? ` \u00b7 ${failedCount} failed` : ''}`
    );

    if (rows.length === 0) {
      container.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, total === 0 ? 'No autonomous schedules configured' : 'No schedules match this filter'),
        el('span', { className: 'text-sm' },
          total === 0
            ? 'Use the AI Schedule Builder above or click "+ New Schedule" to create a recurring or timed background task.'
            : 'Try selecting "All" or another status pill above.')
      ));
      return;
    }

    for (const sched of rows) {
      const lastSt = String(sched.last_status || '').toLowerCase();
      let statusLabel = 'Enabled';
      let statusClass = 'schedule-status-badge is-enabled status-enabled';
      if (lastSt === 'running') {
        statusLabel = 'Running';
        statusClass = 'schedule-status-badge is-running status-running';
      } else if (!sched.enabled) {
        statusLabel = 'Paused';
        statusClass = 'schedule-status-badge is-paused status-paused';
      } else if (isFailed(sched)) {
        statusLabel = 'Failed';
        statusClass = 'schedule-status-badge is-failed status-failed';
      }

      const targetLabel = sched.target_agent_id
        ? agentDisplayName(sched.target_agent_id)
        : 'Auto (Commander)';
      const nextRunLabel = sched.next_run_at
        ? fmtDateTime(sched.next_run_at)
        : (sched.enabled ? 'Pending calculation' : 'Paused');
      const lastRunLabel = sched.last_run_at ? fmtDateTime(sched.last_run_at) : 'Never';
      const runSummary = `${sched.run_count ?? 0} run(s)${sched.last_status ? ` \u00b7 ${sched.last_status}` : ''}`;

      const cardChildren = [
        el('div', { className: 'schedule-card-header' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'schedule-card-title' }, sched.title || shortId(sched.id))
          ),
          el('div', { className: 'agent-badges' },
            el('span', { className: 'schedule-cadence-badge' }, formatScheduleCadence(sched)),
            el('span', { className: statusClass }, statusLabel)
          )
        ),
        el('div', { className: 'text-sm text-secondary' }, sched.prompt || ''),
        el('div', { className: 'schedule-meta-grid' },
          el('div', null,
            el('div', { className: 'stat-label' }, 'Target Agent'),
            el('div', { className: 'text-sm' }, targetLabel)
          ),
          el('div', null,
            el('div', { className: 'stat-label' }, 'Next Run'),
            el('div', { className: 'text-sm mono' }, nextRunLabel)
          ),
          el('div', null,
            el('div', { className: 'stat-label' }, 'Last Run'),
            el('div', { className: 'text-sm mono' }, lastRunLabel)
          ),
          el('div', null,
            el('div', { className: 'stat-label' }, 'Execution History'),
            el('div', { className: 'text-sm mono' }, runSummary)
          )
        )
      ];

      if (sched.last_error) {
        cardChildren.push(
          el('div', { className: 'msg-error-banner', role: 'alert' },
            el('div', { className: 'min-w-0' },
              el('strong', null, 'Last Error: '),
              el('span', null, sched.last_error)
            )
          )
        );
      }

      cardChildren.push(
        el('div', { className: 'schedule-card-actions' },
          el('button', {
            type: 'button',
            className: 'btn btn-primary btn-sm',
            onClick: (e) => runScheduleNow(sched.id, e.currentTarget)
          }, 'Run Now'),
          el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            onClick: (e) => toggleSchedule(sched.id, !sched.enabled, e.currentTarget)
          }, sched.enabled ? 'Pause' : 'Resume'),
          el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            onClick: () => openScheduleModal(sched)
          }, 'Edit'),
          el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            disabled: !sched.conversation_id,
            title: sched.conversation_id
              ? 'Open schedule conversation in Main Chat'
              : 'Run schedule once to initialize its conversation thread',
            onClick: () => openScheduleConversation(sched.conversation_id, sched.last_error)
          }, 'Open Chat'),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteSchedule(sched)
          }, 'Delete')
        )
      );

      container.appendChild(el('article', {
        className: 'card schedule-card',
        dataset: { scheduleId: sched.id }
      }, cardChildren));
    }
  }

  function syncScheduleKindFields() {
    const kindSel = getEl('sched-kind');
    const kind = kindSel ? kindSel.value : 'once';
    document.querySelectorAll('#modal-schedule [data-kind]').forEach((field) => {
      field.hidden = field.dataset.kind !== kind;
    });
  }

  function openScheduleModal(scheduleOrNull, draftOverrides) {
    const modal = getEl('modal-schedule');
    if (!modal) return;
    const isEdit = Boolean(scheduleOrNull && scheduleOrNull.id);
    const src = scheduleOrNull || draftOverrides || {};
    const kind = String(src.kind || 'once').toLowerCase();

    setText('modal-schedule-title', isEdit ? `Edit Schedule: ${src.title || shortId(src.id)}` : 'Create Schedule');

    const editIdEl = getEl('sched-edit-id');
    const titleEl = getEl('sched-title');
    const promptEl = getEl('sched-prompt');
    const kindEl = getEl('sched-kind');
    const runAtEl = getEl('sched-run-at');
    const intervalEl = getEl('sched-interval-secs');
    const dailyEl = getEl('sched-daily-time');
    const cronEl = getEl('sched-cron-expr');
    const tzEl = getEl('sched-tz-offset');
    const enabledEl = getEl('sched-enabled');

    if (editIdEl) editIdEl.value = isEdit ? String(src.id) : '';
    if (titleEl) titleEl.value = src.title || '';
    if (promptEl) promptEl.value = src.prompt || '';
    populateScheduleAgentSelects(src.target_agent_id || '');
    if (kindEl) kindEl.value = ['once', 'interval', 'daily', 'cron'].includes(kind) ? kind : 'once';

    if (runAtEl) {
      const defaultRunIso = new Date(Date.now() + 15 * 60 * 1000).toISOString();
      runAtEl.value = isoToDatetimeLocalInput(src.run_at || defaultRunIso);
    }
    if (intervalEl) {
      intervalEl.value = String(src.interval_secs || 3600);
    }
    if (dailyEl) {
      dailyEl.value = src.daily_time || '09:00';
    }
    if (cronEl) {
      cronEl.value = src.cron_expr || '0 9 * * *';
    }
    if (tzEl) {
      const hasOffset = src.timezone_offset_mins !== undefined && src.timezone_offset_mins !== null;
      tzEl.value = String(hasOffset ? src.timezone_offset_mins : getLocalTimezoneOffsetMins());
    }
    if (enabledEl) {
      enabledEl.checked = src.enabled !== undefined ? Boolean(src.enabled) : true;
    }

    syncScheduleKindFields();
    modal.hidden = false;
    if (titleEl) {
      window.setTimeout(() => titleEl.focus(), 20);
    }
  }

  function closeScheduleModal() {
    const modal = getEl('modal-schedule');
    if (modal) modal.hidden = true;
  }

  async function saveScheduleModal() {
    const editId = ((getEl('sched-edit-id') && getEl('sched-edit-id').value) || '').trim();
    const title = ((getEl('sched-title') && getEl('sched-title').value) || '').trim();
    const prompt = ((getEl('sched-prompt') && getEl('sched-prompt').value) || '').trim();
    const targetAgentId = ((getEl('sched-agent') && getEl('sched-agent').value) || '').trim() || null;
    const kind = ((getEl('sched-kind') && getEl('sched-kind').value) || 'once').toLowerCase();
    const rawTz = parseInt((getEl('sched-tz-offset') && getEl('sched-tz-offset').value) || '', 10);
    const tzOffset = Number.isNaN(rawTz) ? getLocalTimezoneOffsetMins() : Math.max(-720, Math.min(840, rawTz));
    const enabled = Boolean(getEl('sched-enabled') && getEl('sched-enabled').checked);

    if (!title) {
      showToast('Schedule title is required.', 'warning');
      return;
    }
    if (!prompt) {
      showToast('Task prompt is required.', 'warning');
      return;
    }

    let runAtIso = null;
    let intervalSecs = null;
    let dailyTime = null;
    let cronExpr = null;

    if (kind === 'once') {
      runAtIso = datetimeLocalInputToIso(getEl('sched-run-at') && getEl('sched-run-at').value);
      if (!runAtIso) {
        showToast('Please select a valid Run At date & time.', 'warning');
        return;
      }
    } else if (kind === 'interval') {
      intervalSecs = Math.floor(Number((getEl('sched-interval-secs') && getEl('sched-interval-secs').value) || 0));
      if (!Number.isFinite(intervalSecs) || intervalSecs < 1) {
        showToast('Interval must be at least 1 second.', 'warning');
        return;
      }
    } else if (kind === 'daily') {
      dailyTime = ((getEl('sched-daily-time') && getEl('sched-daily-time').value) || '').trim();
      if (!/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(dailyTime)) {
        showToast('Daily time must be in HH:MM format (00:00 - 23:59).', 'warning');
        return;
      }
    } else if (kind === 'cron') {
      cronExpr = ((getEl('sched-cron-expr') && getEl('sched-cron-expr').value) || '').trim();
      if (!cronExpr || cronExpr.split(/\s+/).length !== 5) {
        showToast('Cron expression must have 5 fields (min hour dom mon dow).', 'warning');
        return;
      }
    }

    const payload = {
      title,
      prompt,
      target_agent_id: targetAgentId,
      kind,
      run_at: kind === 'once' ? runAtIso : null,
      interval_secs: kind === 'interval' ? intervalSecs : null,
      daily_time: kind === 'daily' ? dailyTime : null,
      cron_expr: kind === 'cron' ? cronExpr : null,
      timezone_offset_mins: tzOffset,
      enabled
    };

    const saveBtn = getEl('btn-sched-save');
    if (saveBtn) saveBtn.disabled = true;
    try {
      const path = editId
        ? `/api/v1/schedules/${encodeURIComponent(editId)}`
        : '/api/v1/schedules';
      const method = editId ? 'PUT' : 'POST';
      const res = await apiFetch(path, { method, body: payload });
      const saved = (res && res.schedule) ? res.schedule : res;
      closeScheduleModal();
      if (saved && saved.id) {
        upsertScheduleRecord(saved);
      } else {
        await loadSchedules();
      }
      showToast(editId ? `Updated schedule "${title}".` : `Created schedule "${title}".`, 'success');
    } catch (err) {
      showToast(`Save schedule failed: ${err.message}`, 'danger');
    } finally {
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function renderAiSchedulePreview(draft, createdRecord) {
    const preview = getEl('schedule-ai-preview');
    if (!preview) return;
    clearEl(preview);
    if (!draft && !createdRecord) {
      preview.hidden = true;
      return;
    }

    const effective = draft || createdRecord;
    preview.hidden = false;

    const actions = [];
    if (createdRecord && createdRecord.id) {
      if (createdRecord.conversation_id) {
        actions.push(el('button', {
          type: 'button',
          className: 'btn btn-primary btn-sm',
          onClick: () => openScheduleConversation(createdRecord.conversation_id, createdRecord.last_error)
        }, 'Open Chat'));
      }
      actions.push(el('button', {
        type: 'button',
        className: 'btn btn-secondary btn-sm',
        onClick: () => openScheduleModal(createdRecord)
      }, 'Edit Created Schedule'));
    } else if (draft) {
      actions.push(el('button', {
        type: 'button',
        className: 'btn btn-primary btn-sm',
        onClick: (e) => createScheduleFromDraft(draft, e.currentTarget)
      }, 'Confirm & Create'));
      actions.push(el('button', {
        type: 'button',
        className: 'btn btn-secondary btn-sm',
        onClick: () => openScheduleModal(null, draft)
      }, 'Open in Editor'));
    }
    actions.push(el('button', {
      type: 'button',
      className: 'btn btn-ghost btn-sm',
      onClick: () => {
        state.aiScheduleDraft = null;
        renderAiSchedulePreview(null, null);
      }
    }, 'Dismiss'));

    const badges = [
      el('span', { className: 'schedule-cadence-badge' }, formatScheduleCadence(effective))
    ];
    if (draft && draft.source) {
      badges.push(el('span', { className: 'pill pill-muted' }, String(draft.source)));
    }
    if (createdRecord && createdRecord.id) {
      badges.push(el('span', { className: 'schedule-status-badge is-enabled' }, 'Created'));
    }

    preview.appendChild(el('div', { className: 'schedule-card-header' },
      el('div', null,
        el('strong', null, createdRecord ? 'Schedule Created Automatically' : 'Parsed Schedule Preview')
      ),
      el('div', { className: 'agent-badges' }, badges)
    ));

    preview.appendChild(el('div', { className: 'schedule-ai-preview-grid' },
      el('div', null,
        el('div', { className: 'stat-label' }, 'Title'),
        el('div', { className: 'text-sm' }, effective.title || 'Scheduled Task')
      ),
      el('div', null,
        el('div', { className: 'stat-label' }, 'Type / Kind'),
        el('div', { className: 'text-sm mono' }, String(effective.kind || 'once'))
      ),
      el('div', null,
        el('div', { className: 'stat-label' }, 'Cadence'),
        el('div', { className: 'text-sm mono' }, formatScheduleCadence(effective))
      ),
      el('div', null,
        el('div', { className: 'stat-label' }, 'Target Agent'),
        el('div', { className: 'text-sm' }, effective.target_agent_id ? agentDisplayName(effective.target_agent_id) : 'Auto (Commander)')
      )
    ));

    preview.appendChild(el('div', { className: 'field' },
      el('div', { className: 'stat-label' }, 'Task Prompt'),
      el('div', { className: 'text-sm text-secondary' }, effective.prompt || '')
    ));

    if (draft && draft.explanation) {
      preview.appendChild(el('div', { className: 'text-sm text-muted' }, draft.explanation));
    }

    preview.appendChild(el('div', { className: 'card-actions' }, actions));
  }

  async function parseScheduleAi() {
    const promptInput = getEl('schedule-ai-prompt');
    const agentSel = getEl('schedule-ai-agent');
    const autoCb = getEl('schedule-ai-auto-create');
    const parseBtn = getEl('btn-schedule-ai-parse');

    const prompt = ((promptInput && promptInput.value) || '').trim();
    if (!prompt) {
      showToast('Describe a schedule in plain English first.', 'warning');
      return;
    }

    const targetAgentId = ((agentSel && agentSel.value) || '').trim() || null;
    const autoCreate = Boolean(autoCb && autoCb.checked);
    const tzOffset = getLocalTimezoneOffsetMins();

    if (parseBtn) {
      parseBtn.disabled = true;
      parseBtn.textContent = 'Parsing\u2026';
    }
    try {
      const res = await apiFetch('/api/v1/schedules/ai', {
        method: 'POST',
        body: {
          prompt,
          timezone_offset_mins: tzOffset,
          target_agent_id: targetAgentId,
          auto_create: autoCreate
        }
      });
      const draft = (res && res.draft) ? res.draft : null;
      const created = (res && res.created) ? res.created : null;
      state.aiScheduleDraft = created ? null : draft;

      if (created && created.id) {
        upsertScheduleRecord(created);
        if (promptInput) promptInput.value = '';
        showToast(`Created schedule "${created.title}".`, 'success');
      }
      renderAiSchedulePreview(draft, created);
      if (!created && !draft) {
        showToast('Could not parse schedule draft from response.', 'warning');
      }
    } catch (err) {
      showToast(`AI schedule parse failed: ${err.message}`, 'danger');
    } finally {
      if (parseBtn) {
        parseBtn.disabled = false;
        parseBtn.textContent = 'Parse Schedule';
      }
    }
  }

  async function createScheduleFromDraft(draft, triggerBtn) {
    if (!draft) return;
    if (triggerBtn) triggerBtn.disabled = true;
    const kind = String(draft.kind || 'once').toLowerCase();
    const payload = {
      title: String(draft.title || 'Scheduled Task').trim(),
      prompt: String(draft.prompt || '').trim(),
      target_agent_id: draft.target_agent_id || null,
      kind,
      run_at: kind === 'once' ? (draft.run_at || null) : null,
      interval_secs: kind === 'interval' ? Number(draft.interval_secs || 3600) : null,
      daily_time: kind === 'daily' ? (draft.daily_time || '09:00') : null,
      cron_expr: kind === 'cron' ? (draft.cron_expr || '0 9 * * *') : null,
      timezone_offset_mins: Number.isFinite(Number(draft.timezone_offset_mins))
        ? Number(draft.timezone_offset_mins)
        : getLocalTimezoneOffsetMins(),
      enabled: true
    };
    try {
      const res = await apiFetch('/api/v1/schedules', { method: 'POST', body: payload });
      const saved = (res && res.schedule) ? res.schedule : res;
      if (saved && saved.id) {
        upsertScheduleRecord(saved);
      } else {
        await loadSchedules();
      }
      state.aiScheduleDraft = null;
      const promptInput = getEl('schedule-ai-prompt');
      if (promptInput) promptInput.value = '';
      renderAiSchedulePreview(null, null);
      showToast(`Created schedule "${payload.title}".`, 'success');
    } catch (err) {
      showToast(`Create schedule failed: ${err.message}`, 'danger');
    } finally {
      if (triggerBtn && triggerBtn.isConnected) triggerBtn.disabled = false;
    }
  }

  async function toggleSchedule(id, nextEnabled, triggerBtn) {
    if (!id) return;
    if (triggerBtn) triggerBtn.disabled = true;
    try {
      const body = nextEnabled === undefined ? {} : { enabled: Boolean(nextEnabled) };
      const res = await apiFetch(`/api/v1/schedules/${encodeURIComponent(id)}/toggle`, {
        method: 'POST',
        body
      });
      const updated = (res && res.schedule) ? res.schedule : res;
      if (updated && updated.id) {
        upsertScheduleRecord(updated);
        showToast(updated.enabled ? `Resumed "${updated.title}".` : `Paused "${updated.title}".`, 'success');
      } else {
        await loadSchedules();
      }
    } catch (err) {
      showToast(`Toggle schedule failed: ${err.message}`, 'danger');
    } finally {
      if (triggerBtn && triggerBtn.isConnected) triggerBtn.disabled = false;
    }
  }

  async function runScheduleNow(id, triggerBtn) {
    if (!id) return;
    if (triggerBtn) triggerBtn.disabled = true;
    try {
      const res = await apiFetch(`/api/v1/schedules/${encodeURIComponent(id)}/run-now`, {
        method: 'POST',
        body: {}
      });
      const updated = (res && res.schedule) ? res.schedule : res;
      if (updated && updated.id) {
        upsertScheduleRecord(updated);
      } else {
        await loadSchedules();
      }
      showToast('Triggered immediate schedule run.', 'info');
    } catch (err) {
      showToast(`Run schedule failed: ${err.message}`, 'danger');
    } finally {
      if (triggerBtn && triggerBtn.isConnected) triggerBtn.disabled = false;
    }
  }

  async function deleteSchedule(id) {
    if (!id) return;
    await apiFetch(`/api/v1/schedules/${encodeURIComponent(id)}`, { method: 'DELETE' });
    state.schedules = state.schedules.filter((s) => s.id !== id);
    updateSchedulesNavBadge();
    if (state.currentSection === 'schedules') {
      renderSchedules();
    }
  }

  function confirmDeleteSchedule(sched) {
    if (!sched || !sched.id) return;
    openModal('Delete Schedule', [
      el('p', null, `Delete schedule "${sched.title || shortId(sched.id)}"? This stops future scheduled runs cleanly.`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await deleteSchedule(sched.id);
            closeModal();
            showToast(`Deleted schedule "${sched.title || shortId(sched.id)}".`, 'success');
          } catch (err) {
            showToast(`Delete schedule failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete Schedule')
    ]);
  }

  function openScheduleConversation(conversationId, optionalLastError) {
    if (!conversationId) {
      showToast('No conversation created yet. Click "Run Now" to run this schedule.', 'info');
      return;
    }
    state.activeConversationId = conversationId;
    if (optionalLastError) {
      showChatErrorBanner(conversationId, 'Last Scheduled Run Error', optionalLastError);
    }
    switchSection('chat');
  }

  /* -------------------- 6. Memory -------------------- */
  async function loadMemories() {
    const params = new URLSearchParams();
    const scope = $('memory-filter-scope').value;
    const ns = ($('memory-filter-namespace').value || '').trim();
    const owner = $('memory-filter-agent').value;
    if (scope) params.set('scope', scope);
    if (ns) params.set('namespace', ns);
    if (owner) params.set('owner_agent_id', owner);
    const qs = params.toString();
    const data = await apiFetch(`/api/v1/memories${qs ? `?${qs}` : ''}`);
    state.memories = unwrapList(data, ['memories', 'items']);
    renderMemories();
  }

  function renderMemories() {
    const grid = $('memory-list');
    clearEl(grid);
    if (state.memories.length === 0) {
      grid.appendChild(el('div', { className: 'card empty-state' },
        el('strong', null, 'No memories found'),
        el('span', { className: 'text-sm' }, 'Add a memory entry or adjust your filters.')
      ));
      return;
    }

    for (const mem of state.memories) {
      grid.appendChild(el('article', { className: 'card agent-card' },
        el('div', { className: 'agent-card-top' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title truncate' }, mem.key || shortId(mem.id)),
            el('div', { className: 'text-sm text-muted mono' }, `${mem.scope} \u00b7 ${mem.namespace || 'default'}`)
          ),
          el('span', { className: 'pill pill-info' }, `Imp ${mem.importance ?? 5}`)
        ),
        el('div', { className: 'text-sm text-secondary' }, mem.content || mem.summary || ''),
        Array.isArray(mem.tags) && mem.tags.length > 0
          ? el('div', { className: 'agent-badges' }, mem.tags.map((t) => el('span', { className: 'pill pill-muted' }, t)))
          : null,
        el('div', { className: 'card-actions' },
          el('span', { className: 'text-sm text-muted', style: 'margin-right:auto' },
            `${mem.owner_agent_id ? agentDisplayName(mem.owner_agent_id) : 'Shared'} \u00b7 ${fmtTime(mem.updated_at || mem.created_at)}`
          ),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => deleteMemory(mem.id)
          }, 'Delete')
        )
      ));
    }
  }

  function openAddMemoryModal() {
    const scopeSel = el('select', { id: 'mem-scope', className: 'select' },
      ['agent', 'project', 'user_preference', 'conversation', 'task'].map((s) => el('option', { value: s }, s))
    );
    const nsIn = el('input', { id: 'mem-ns', type: 'text', className: 'input', value: 'global', required: true });
    const ownerSel = el('select', { id: 'mem-owner', className: 'select' },
      [el('option', { value: '' }, 'None (Shared)')].concat(
        state.agents.map((a) => el('option', { value: a.id }, a.name))
      )
    );
    const keyIn = el('input', { id: 'mem-key', type: 'text', className: 'input', required: true, placeholder: 'e.g. coding_style' });
    const contentIn = el('textarea', { id: 'mem-content', className: 'input', rows: 4, required: true });
    const impIn = el('input', { id: 'mem-imp', type: 'number', min: 1, max: 10, className: 'input mono', value: 7 });
    const tagsIn = el('input', { id: 'mem-tags', type: 'text', className: 'input', placeholder: 'rust, ps5, preference' });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const tags = tagsIn.value.split(',').map((s) => s.trim()).filter(Boolean);
        try {
          await apiFetch('/api/v1/memories', {
            method: 'POST',
            body: {
              scope: scopeSel.value,
              namespace: nsIn.value.trim() || 'global',
              owner_agent_id: ownerSel.value || null,
              key: keyIn.value.trim(),
              content: contentIn.value.trim(),
              importance: Number(impIn.value) || 5,
              tags
            }
          });
          closeModal();
          showToast('Memory saved.', 'success');
          await loadMemories();
        } catch (err) {
          showToast(`Add memory failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'form-grid' },
        el('div', { className: 'field' }, el('label', { for: 'mem-scope' }, 'Scope'), scopeSel),
        el('div', { className: 'field' }, el('label', { for: 'mem-ns' }, 'Namespace'), nsIn),
        el('div', { className: 'field' }, el('label', { for: 'mem-owner' }, 'Owner Agent'), ownerSel),
        el('div', { className: 'field' }, el('label', { for: 'mem-imp' }, 'Importance (1\u201310)'), impIn)
      ),
      el('div', { className: 'field' }, el('label', { for: 'mem-key' }, 'Key'), keyIn),
      el('div', { className: 'field' }, el('label', { for: 'mem-content' }, 'Content'), contentIn),
      el('div', { className: 'field' }, el('label', { for: 'mem-tags' }, 'Tags (comma-separated)'), tagsIn)
    );

    openModal('Add Memory Entry', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Save Memory')
    ]);
  }

  async function deleteMemory(id) {
    try {
      await apiFetch(`/api/v1/memories/${encodeURIComponent(id)}`, { method: 'DELETE' });
      showToast('Memory deleted.', 'success');
      await loadMemories();
    } catch (err) {
      showToast(`Delete memory failed: ${err.message}`, 'danger');
    }
  }

  function openImportMemoriesModal() {
    const fileIn = el('input', {
      id: 'mem-import-file',
      type: 'file',
      accept: '.json,application/json',
      className: 'input'
    });
    const jsonIn = el('textarea', {
      id: 'mem-import-json',
      className: 'input mono',
      rows: 10,
      placeholder: '[{"scope":"shared","namespace":"default","key":"note","content":"..."}] or {"memories":[...]}',
      required: true
    });

    fileIn.addEventListener('change', () => {
      const f = fileIn.files && fileIn.files[0];
      if (!f) return;
      const reader = new FileReader();
      reader.onload = () => {
        jsonIn.value = typeof reader.result === 'string' ? reader.result : '';
      };
      reader.onerror = () => showToast('Failed to read selected JSON file.', 'danger');
      reader.readAsText(f);
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const raw = jsonIn.value.trim();
        if (!raw) {
          showToast('Paste JSON or select a JSON file first.', 'warning');
          return;
        }
        let parsed;
        try {
          parsed = JSON.parse(raw);
        } catch (err) {
          showToast(`Invalid JSON: ${err.message}`, 'danger');
          return;
        }
        try {
          const res = await apiFetch('/api/v1/memories/import', { method: 'POST', body: parsed });
          closeModal();
          const count = res && res.imported !== undefined ? res.imported : 0;
          showToast(`Imported ${count} memory item(s).`, 'success');
          await loadMemories();
        } catch (err) {
          showToast(`Import memories failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'mem-import-file' }, 'Upload JSON File (optional)'),
        fileIn
      ),
      el('div', { className: 'field' },
        el('label', { for: 'mem-import-json' }, 'Memories JSON Payload'),
        jsonIn
      )
    );

    openModal('Import Memories JSON', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Import Memories')
    ]);
  }

  async function exportMemories() {
    try {
      const data = await apiFetch('/api/v1/memories/export');
      const jsonText = JSON.stringify(data, null, 2);
      const blob = new Blob([jsonText], { type: 'application/json;charset=utf-8' });
      const blobUrl = URL.createObjectURL(blob);
      const a = el('a', {
        href: blobUrl,
        download: 'ps5-agent-memories.json'
      });
      document.body.appendChild(a);
      try {
        a.click();
      } finally {
        a.remove();
        window.setTimeout(() => URL.revokeObjectURL(blobUrl), 1000);
      }
      showToast('Memories exported.', 'success');
    } catch (err) {
      showToast(`Export memories failed: ${err.message}`, 'danger');
    }
  }

  /* -------------------- 7. Tools -------------------- */
  async function loadTools() {
    const data = await apiFetch('/api/v1/tools');
    state.tools = unwrapList(data, ['tools']);
    renderTools();
  }

  async function loadToolExecutions() {
    const data = await apiFetch('/api/v1/tool-executions');
    state.toolExecutions = unwrapList(data, ['executions', 'tool_executions']);
    renderToolExecutions();
  }

  function renderTools() {
    const listEl = $('tools-list');
    const testerSel = $('tool-tester-select');
    if (!listEl || !testerSel) return;

    clearEl(listEl);
    const prevSel = testerSel.value;
    clearEl(testerSel);

    setText('tools-count-summary', `${state.tools.filter((t) => t.enabled).length}/${state.tools.length} enabled`);

    if (state.tools.length === 0) {
      listEl.appendChild(el('div', { className: 'empty-state' }, el('strong', null, 'No tools registered')));
      return;
    }

    for (const tool of state.tools) {
      testerSel.appendChild(el('option', { value: tool.id }, `${tool.id} (${tool.category})`));
      const isCustomTool = !BUILTIN_TOOL_IDS.has(tool.id) && !String(tool.id || '').startsWith('mcp:');

      listEl.appendChild(el('article', { className: 'tool-card' },
        el('div', { className: 'agent-card-top' },
          el('div', { className: 'min-w-0' },
            el('h3', { className: 'card-title mono truncate' }, tool.id),
            el('span', { className: 'text-sm text-muted' }, `${tool.name} \u00b7 ${tool.category}`)
          ),
          el('span', { className: `pill ${tool.enabled ? 'pill-success' : 'pill-muted'}` }, tool.enabled ? 'Enabled' : 'Disabled')
        ),
        el('p', { className: 'text-sm text-secondary' }, tool.description || ''),
        el('div', { className: 'agent-badges' },
          isCustomTool ? el('span', { className: 'pill pill-info' }, 'Custom') : null,
          tool.requires_approval ? el('span', { className: 'pill pill-warning' }, 'Approval Required') : null,
          tool.is_mutating ? el('span', { className: 'pill pill-danger' }, 'Mutating') : null,
          ...(tool.required_permissions || []).map((p) => el('span', { className: 'pill pill-muted' }, p))
        ),
        el('div', { className: 'card-actions' },
          el('button', {
            type: 'button',
            className: `btn btn-sm ${tool.enabled ? 'btn-secondary' : 'btn-success'}`,
            onClick: () => updateToolPolicy(tool.id, { enabled: !tool.enabled })
          }, tool.enabled ? 'Disable' : 'Enable'),
          el('button', {
            type: 'button',
            className: `btn btn-sm ${tool.requires_approval ? 'btn-warning' : 'btn-secondary'}`,
            onClick: () => updateToolPolicy(tool.id, { requires_approval: !tool.requires_approval })
          }, tool.requires_approval ? 'Approval: Req' : 'Approval: Auto'),
          el('button', {
            type: 'button',
            className: 'btn btn-primary btn-sm',
            onClick: () => selectToolForTesting(tool)
          }, 'Test Tool'),
          isCustomTool ? el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteCustomTool(tool)
          }, 'Delete') : null
        )
      ));
    }

    if (prevSel && state.tools.some((t) => t.id === prevSel)) {
      testerSel.value = prevSel;
    } else if (state.tools.length > 0) {
      selectToolForTesting(state.tools[0], true);
    }
  }

  function selectToolForTesting(tool, skipScroll) {
    $('tool-tester-select').value = tool.id;
    const sample = {};
    const props = tool.input_schema && tool.input_schema.properties;
    if (props && typeof props === 'object') {
      for (const [k, schema] of Object.entries(props)) {
        if (schema && schema.default !== undefined) sample[k] = schema.default;
        else if (schema && schema.type === 'number') sample[k] = 0;
        else if (schema && schema.type === 'boolean') sample[k] = false;
        else sample[k] = '';
      }
    }
    $('tool-tester-args').value = JSON.stringify(sample, null, 2);
    if (!skipScroll) {
      $('tool-tester-card').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }

  async function updateToolPolicy(toolId, patch) {
    try {
      await apiFetch(`/api/v1/tools/${encodeURIComponent(toolId)}`, {
        method: 'PATCH',
        body: patch
      });
      if (patch.enabled !== undefined) {
        showToast(`Tool "${toolId}" ${patch.enabled ? 'enabled' : 'disabled'}.`, 'success');
      } else if (patch.requires_approval !== undefined) {
        showToast(
          `Tool "${toolId}" approval set to ${patch.requires_approval ? 'Required' : 'Auto'}.`,
          'success'
        );
      } else {
        showToast(`Updated policy for "${toolId}".`, 'success');
      }
      await loadTools();
    } catch (err) {
      showToast(`Update tool policy failed: ${err.message}`, 'danger');
    }
  }

  async function toggleTool(toolId, enabled) {
    return updateToolPolicy(toolId, { enabled });
  }

  function confirmDeleteCustomTool(tool) {
    openModal('Delete Custom Tool', [
      el('p', null, `Delete custom tool "${tool.id}" (${tool.name})? This removes its saved JSON definition.`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/tools/${encodeURIComponent(tool.id)}`, { method: 'DELETE' });
            closeModal();
            showToast(`Custom tool "${tool.id}" deleted.`, 'success');
            await loadTools();
          } catch (err) {
            showToast(`Delete custom tool failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete Tool')
    ]);
  }

  function openAddCustomToolModal() {
    const idIn = el('input', {
      id: 'ct-id',
      type: 'text',
      className: 'input mono',
      placeholder: 'my_custom_tool',
      required: true
    });
    const nameIn = el('input', {
      id: 'ct-name',
      type: 'text',
      className: 'input',
      placeholder: 'My Custom Tool',
      required: true
    });
    const catIn = el('input', { id: 'ct-category', type: 'text', className: 'input', value: 'custom' });
    const descIn = el('input', {
      id: 'ct-desc',
      type: 'text',
      className: 'input',
      placeholder: 'Describe what this tool does for agents'
    });
    const kindSel = el('select', { id: 'ct-kind', className: 'select' },
      el('option', { value: 'http' }, 'http (HTTP Endpoint / Webhook)'),
      el('option', { value: 'template' }, 'template (Prompt / String Template)')
    );
    const urlIn = el('input', {
      id: 'ct-url',
      type: 'text',
      className: 'input mono',
      placeholder: 'https://api.example.com/v1/query?q={{query}}'
    });
    const methodSel = el('select', { id: 'ct-method', className: 'select' },
      ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map((m) => el('option', { value: m }, m))
    );
    const headersIn = el('textarea', {
      id: 'ct-headers',
      className: 'input mono',
      rows: 2,
      placeholder: '{"Accept": "application/json"}'
    }, '{}');
    const bodyTmplIn = el('textarea', {
      id: 'ct-body-template',
      className: 'input mono',
      rows: 3,
      placeholder: '{"query": "{{query}}"}'
    });
    const allowLanCb = el('input', { id: 'ct-allow-lan', type: 'checkbox', checked: false });
    const tmplIn = el('textarea', {
      id: 'ct-template',
      className: 'input mono',
      rows: 3,
      placeholder: 'Formatted result for {{input}}'
    });
    const schemaIn = el('textarea', {
      id: 'ct-input-schema',
      className: 'input mono',
      rows: 5
    }, JSON.stringify({
      type: 'object',
      properties: {
        input: { type: 'string', description: 'Tool input parameter' }
      },
      required: []
    }, null, 2));
    const reqApprovalCb = el('input', { id: 'ct-requires-approval', type: 'checkbox', checked: false });
    const timeoutIn = el('input', {
      id: 'ct-timeout',
      type: 'number',
      min: 1,
      max: 300,
      step: 1,
      className: 'input mono',
      value: 20
    });

    const httpGroup = el('div', { className: 'stack-form' },
      el('div', { className: 'form-grid' },
        el('div', { className: 'field' }, el('label', { for: 'ct-url' }, 'URL'), urlIn),
        el('div', { className: 'field' }, el('label', { for: 'ct-method' }, 'HTTP Method'), methodSel)
      ),
      el('div', { className: 'field' }, el('label', { for: 'ct-headers' }, 'Headers (JSON object)'), headersIn),
      el('div', { className: 'field' }, el('label', { for: 'ct-body-template' }, 'Body Template (optional)'), bodyTmplIn),
      el('label', { className: 'checkbox-row' }, allowLanCb, el('span', { className: 'text-sm' }, 'Allow Private / LAN Addresses'))
    );
    const tmplGroup = el('div', { className: 'field', hidden: true },
      el('label', { for: 'ct-template' }, 'Output Template (uses {{arg}} placeholders)'),
      tmplIn
    );

    const syncKindFields = () => {
      const isHttp = kindSel.value === 'http';
      httpGroup.hidden = !isHttp;
      tmplGroup.hidden = isHttp;
    };
    kindSel.addEventListener('change', syncKindFields);
    syncKindFields();

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const id = idIn.value.trim();
        const name = nameIn.value.trim();
        if (!id || !/^[a-zA-Z0-9_-]+$/.test(id)) {
          showToast('Tool ID is required (letters, numbers, _ or - only).', 'warning');
          return;
        }
        if (!name) {
          showToast('Tool name is required.', 'warning');
          return;
        }
        if (BUILTIN_TOOL_IDS.has(id) || id.startsWith('mcp:')) {
          showToast('Choose a custom ID that does not collide with built-in or MCP tools.', 'warning');
          return;
        }

        let inputSchema;
        try {
          inputSchema = JSON.parse(schemaIn.value.trim() || '{"type":"object","properties":{}}');
          if (!inputSchema || typeof inputSchema !== 'object' || Array.isArray(inputSchema)) {
            throw new Error('must be a JSON object');
          }
        } catch (err) {
          showToast(`Invalid Input Schema JSON: ${err.message}`, 'danger');
          return;
        }

        const kind = kindSel.value === 'template' ? 'template' : 'http';
        let headers = {};
        if (kind === 'http') {
          if (!urlIn.value.trim()) {
            showToast('URL is required for HTTP custom tools.', 'warning');
            return;
          }
          try {
            headers = JSON.parse(headersIn.value.trim() || '{}');
            if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
              throw new Error('must be a JSON object');
            }
          } catch (err) {
            showToast(`Invalid Headers JSON: ${err.message}`, 'danger');
            return;
          }
        }

        const timeoutSecs = Math.max(1, Math.floor(Number(timeoutIn.value) || 20));
        const def = {
          id,
          name,
          category: catIn.value.trim() || 'custom',
          description: descIn.value.trim(),
          kind,
          input_schema: inputSchema,
          url: kind === 'http' ? urlIn.value.trim() : null,
          method: kind === 'http' ? methodSel.value : 'GET',
          headers,
          body_template: kind === 'http' && bodyTmplIn.value.trim() ? bodyTmplIn.value : null,
          template: kind === 'template' ? tmplIn.value : null,
          enabled: true,
          requires_approval: reqApprovalCb.checked,
          timeout_secs: timeoutSecs,
          allow_private_lan: kind === 'http' ? allowLanCb.checked : false
        };

        try {
          await apiFetch('/api/v1/tools', { method: 'POST', body: def });
          closeModal();
          showToast(`Custom tool "${id}" added.`, 'success');
          await loadTools();
        } catch (err) {
          showToast(`Add custom tool failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'form-grid' },
        el('div', { className: 'field' }, el('label', { for: 'ct-id' }, 'Tool ID'), idIn),
        el('div', { className: 'field' }, el('label', { for: 'ct-name' }, 'Display Name'), nameIn),
        el('div', { className: 'field' }, el('label', { for: 'ct-category' }, 'Category'), catIn),
        el('div', { className: 'field' }, el('label', { for: 'ct-kind' }, 'Tool Kind'), kindSel),
        el('div', { className: 'field' }, el('label', { for: 'ct-timeout' }, 'Timeout (secs)'), timeoutIn)
      ),
      el('div', { className: 'field' }, el('label', { for: 'ct-desc' }, 'Description'), descIn),
      httpGroup,
      tmplGroup,
      el('div', { className: 'field' }, el('label', { for: 'ct-input-schema' }, 'Input Schema (JSON Schema)'), schemaIn),
      el('label', { className: 'checkbox-row' }, reqApprovalCb, el('span', { className: 'text-sm' }, 'Requires Approval Before Execution'))
    );

    openModal('Add Custom Tool', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Save Tool')
    ]);
  }

  function openImportToolsModal() {
    const fileIn = el('input', {
      id: 'tools-import-file',
      type: 'file',
      accept: '.json,application/json',
      className: 'input'
    });
    const jsonIn = el('textarea', {
      id: 'tools-import-json',
      className: 'input mono',
      rows: 10,
      placeholder: 'Paste CustomToolDefinition JSON, [...], {"tools":[...]}, or OpenAI {"type":"function","function":{...}}',
      required: true
    });

    fileIn.addEventListener('change', () => {
      const f = fileIn.files && fileIn.files[0];
      if (!f) return;
      const reader = new FileReader();
      reader.onload = () => {
        jsonIn.value = typeof reader.result === 'string' ? reader.result : '';
      };
      reader.onerror = () => showToast('Failed to read selected JSON file.', 'danger');
      reader.readAsText(f);
    });

    const form = el('form', {
      className: 'stack-form',
      onSubmit: async (e) => {
        e.preventDefault();
        const raw = jsonIn.value.trim();
        if (!raw) {
          showToast('Paste tool JSON or choose a JSON file first.', 'warning');
          return;
        }
        let parsed;
        try {
          parsed = JSON.parse(raw);
        } catch (err) {
          showToast(`Invalid JSON: ${err.message}`, 'danger');
          return;
        }
        try {
          const created = await apiFetch('/api/v1/tools', { method: 'POST', body: parsed });
          closeModal();
          const count = Array.isArray(created) ? created.length : 1;
          showToast(`Imported ${count} custom tool(s).`, 'success');
          await loadTools();
        } catch (err) {
          showToast(`Import tools failed: ${err.message}`, 'danger');
        }
      }
    },
      el('div', { className: 'field' },
        el('label', { for: 'tools-import-file' }, 'Upload JSON File (optional)'),
        fileIn
      ),
      el('div', { className: 'field' },
        el('label', { for: 'tools-import-json' }, 'Tools JSON Payload'),
        jsonIn
      )
    );

    openModal('Import Custom Tools', [form], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', { type: 'button', className: 'btn btn-primary', onClick: () => form.requestSubmit() }, 'Import Tools')
    ]);
  }

  async function onExecuteToolTest(ev) {
    ev.preventDefault();
    const toolId = $('tool-tester-select').value;
    if (!toolId) return;
    let args;
    try {
      args = JSON.parse($('tool-tester-args').value || '{}');
    } catch (err) {
      showToast(`Invalid JSON arguments: ${err.message}`, 'danger');
      return;
    }

    const btn = $('tool-tester-run-btn');
    const out = $('tool-tester-output');
    btn.disabled = true;
    setText('tool-tester-status', 'Executing\u2026');

    try {
      const res = await apiFetch(`/api/v1/tools/${encodeURIComponent(toolId)}/execute`, {
        method: 'POST',
        body: { arguments: args }
      });
      out.hidden = false;
      out.textContent = prettyJson(res);
      setText('tool-tester-status', 'Completed');
      await loadToolExecutions();
    } catch (err) {
      out.hidden = false;
      out.textContent = err.message;
      setText('tool-tester-status', 'Failed');
    } finally {
      btn.disabled = false;
    }
  }

  function renderToolExecutions() {
    const container = $('tool-executions-list');
    clearEl(container);
    if (state.toolExecutions.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No recent tool executions')
      ));
      return;
    }

    const tbody = el('tbody');
    for (const ex of state.toolExecutions) {
      tbody.appendChild(el('tr', null,
        el('td', { className: 'mono text-sm' }, fmtTime(ex.created_at)),
        el('td', { className: 'mono' }, ex.tool_id),
        el('td', null, agentDisplayName(ex.agent_id)),
        el('td', null, el('span', { className: `pill ${ex.success ? 'pill-success' : 'pill-danger'}` }, ex.success ? 'SUCCESS' : 'ERROR')),
        el('td', { className: 'num' }, `${ex.duration_ms ?? 0} ms`),
        el('td', null,
          el('details', { className: 'msg-chip' },
            el('summary', null, ex.error ? `Error: ${ex.error}` : 'Inspect Input / Output'),
            el('pre', null, `Input:\n${prettyJson(ex.input_json)}\n\nOutput:\n${prettyJson(ex.output_json || ex.error || '')}`)
          )
        )
      ));
    }

    container.appendChild(el('div', { className: 'table-scroll' },
      el('table', { className: 'data-table' },
        el('thead', null, el('tr', null,
          el('th', null, 'Time'),
          el('th', null, 'Tool'),
          el('th', null, 'Agent'),
          el('th', null, 'Result'),
          el('th', { className: 'num' }, 'Duration'),
          el('th', null, 'Payload')
        )),
        tbody
      )
    ));
  }

  /* -------------------- 7b. Sandbox, Doctor, Browser Console, Brain & Webhooks -------------------- */
  function splitDomainLines(rawText) {
    return [...new Set(
      String(rawText || '')
        .split(/[\r\n,]+/)
        .map((s) => s.trim())
        .filter(Boolean)
    )];
  }

  function renderSandboxState(dto) {
    if (!dto) return;
    state.sandboxState = dto;

    const statusEl = getEl('sandbox-overall-status');
    if (statusEl) {
      const modeLabel = dto.mode || 'full_sandbox';
      const netLabel = dto.network_access ? 'Net On' : 'Net Off';
      statusEl.className = `pill ${dto.network_access ? 'pill-info' : 'pill-success'}`;
      statusEl.textContent = `${modeLabel} \u00b7 ${netLabel}`;
    }

    const setVal = (id, val) => {
      const node = getEl(id);
      if (node && val !== undefined && val !== null) node.value = String(val);
    };
    const setChk = (id, val) => {
      const node = getEl(id);
      if (node) node.checked = Boolean(val);
    };

    setVal('sandbox-mode-select', dto.mode || 'full_sandbox');
    setVal('sandbox-approval-select', dto.approval_policy || 'on_request');
    setChk('sandbox-network-access', dto.network_access);
    setChk('sandbox-allow-local-net', dto.allow_local_network);
    setChk('sandbox-allow-host-shell', dto.allow_host_shell);
    setChk('sandbox-inject-brain', dto.inject_brain_files !== false);
    setVal('sandbox-search-provider', dto.web_search_provider || 'duckduckgo');
    setVal('sandbox-searxng-url', dto.searxng_url || '');
    setVal('sandbox-remote-browser-url', dto.remote_browser_url || '');
    setVal('sandbox-exec-timeout-secs', dto.exec_timeout_secs ?? 30);
    setVal('sandbox-max-exec-bytes', dto.max_exec_output_bytes ?? 65536);
    setVal('sandbox-heartbeat-secs', dto.heartbeat_interval_secs ?? 60);
    setVal('sandbox-max-brain-chars', dto.max_brain_context_chars ?? 12000);
    setVal('sandbox-allowed-domains', Array.isArray(dto.allowed_domains) ? dto.allowed_domains.join('\n') : '');
    setVal('sandbox-blocked-domains', Array.isArray(dto.blocked_domains) ? dto.blocked_domains.join('\n') : '');
  }

  async function loadSandboxState(opts) {
    const options = opts || {};
    try {
      const dto = await apiFetch('/api/v1/sandbox');
      renderSandboxState(dto);
      return dto;
    } catch (err) {
      if (!options.silent) {
        setText('sandbox-policy-status', `Load failed: ${err.message}`);
        showToast(`Failed to load sandbox state: ${err.message}`, 'danger');
      }
      return null;
    }
  }

  async function onSaveSandboxPolicy(ev) {
    ev.preventDefault();
    const saveBtn = getEl('sandbox-save-btn');
    if (saveBtn && saveBtn.disabled) return;

    const timeoutSecs = Number((getEl('sandbox-exec-timeout-secs') || {}).value);
    const maxExecBytes = Number((getEl('sandbox-max-exec-bytes') || {}).value);
    const heartbeatSecs = Number((getEl('sandbox-heartbeat-secs') || {}).value);
    const maxBrainChars = Number((getEl('sandbox-max-brain-chars') || {}).value);

    if (Number.isNaN(timeoutSecs) || timeoutSecs < 0) {
      showToast('Exec timeout must be 0 or greater.', 'warning');
      return;
    }
    if (Number.isNaN(maxExecBytes) || maxExecBytes < 0) {
      showToast('Max exec output bytes must be 0 or greater.', 'warning');
      return;
    }
    if (Number.isNaN(heartbeatSecs) || heartbeatSecs < 0) {
      showToast('Heartbeat interval must be 0 or greater.', 'warning');
      return;
    }
    if (Number.isNaN(maxBrainChars) || maxBrainChars < 0) {
      showToast('Max brain context chars must be 0 or greater.', 'warning');
      return;
    }

    const payload = {
      mode: (getEl('sandbox-mode-select') || {}).value || 'full_sandbox',
      approval_policy: (getEl('sandbox-approval-select') || {}).value || 'on_request',
      network_access: Boolean((getEl('sandbox-network-access') || {}).checked),
      allow_local_network: Boolean((getEl('sandbox-allow-local-net') || {}).checked),
      allow_host_shell: Boolean((getEl('sandbox-allow-host-shell') || {}).checked),
      inject_brain_files: Boolean((getEl('sandbox-inject-brain') || {}).checked),
      web_search_provider: (getEl('sandbox-search-provider') || {}).value || 'duckduckgo',
      searxng_url: String((getEl('sandbox-searxng-url') || {}).value || '').trim(),
      remote_browser_url: String((getEl('sandbox-remote-browser-url') || {}).value || '').trim(),
      exec_timeout_secs: Math.floor(timeoutSecs),
      max_exec_output_bytes: Math.floor(maxExecBytes),
      heartbeat_interval_secs: Math.floor(heartbeatSecs),
      max_brain_context_chars: Math.floor(maxBrainChars),
      allowed_domains: splitDomainLines((getEl('sandbox-allowed-domains') || {}).value),
      blocked_domains: splitDomainLines((getEl('sandbox-blocked-domains') || {}).value)
    };

    if (saveBtn) saveBtn.disabled = true;
    setText('sandbox-policy-status', 'Saving sandbox policy...');
    try {
      const updated = await apiFetch('/api/v1/sandbox', {
        method: 'PUT',
        body: payload
      });
      renderSandboxState(updated);
      setText('sandbox-policy-status', 'Sandbox policy saved.');
      showToast('Sandbox policy saved.', 'success');
    } catch (err) {
      setText('sandbox-policy-status', `Save failed: ${err.message}`);
      showToast(`Save sandbox policy failed: ${err.message}`, 'danger');
    } finally {
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function updateDoctorNavBadge(report) {
    const badge = getEl('nav-badge-doctor');
    if (!badge) return;
    if (!report) {
      badge.hidden = true;
      badge.textContent = '0';
      return;
    }
    const failCount = Number(report.fail_count || 0);
    const warnCount = Number(report.warn_count || 0);
    const issues = failCount + warnCount;
    if (issues <= 0) {
      badge.hidden = true;
      badge.textContent = '0';
      return;
    }
    badge.className = `nav-badge ${failCount > 0 ? 'badge-danger' : 'badge-warning'}`;
    badge.textContent = String(issues);
    badge.hidden = false;
  }

  function renderDoctorReport(report) {
    if (!report) return;
    state.doctorReport = report;
    updateDoctorNavBadge(report);

    const overall = String(report.overall_status || 'pass').toLowerCase();
    const summaryPill = getEl('doctor-summary-pill');
    if (summaryPill) {
      summaryPill.className = `pill ${overall === 'fail' ? 'pill-danger' : overall === 'warn' ? 'pill-warning' : 'pill-success'}`;
      summaryPill.textContent = `${report.pass_count ?? 0} Pass \u00b7 ${report.warn_count ?? 0} Warn \u00b7 ${report.fail_count ?? 0} Fail`;
    }
    setText('doctor-checked-at', report.checked_at ? fmtTime(report.checked_at) : '\u2014');

    const listEl = getEl('doctor-checks-list');
    if (!listEl) return;
    clearEl(listEl);

    const checks = Array.isArray(report.checks) ? report.checks : [];
    if (checks.length === 0) {
      listEl.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No diagnostic checks reported'),
        el('span', { className: 'text-sm' }, 'Click Run Diagnostics to execute health checks.')
      ));
      return;
    }

    for (const chk of checks) {
      const st = String(chk.status || 'pass').toLowerCase();
      const badgeClass = st === 'fail' ? 'is-fail' : st === 'warn' ? 'is-warn' : 'is-pass';
      listEl.appendChild(el('article', { className: 'doctor-check-card' },
        el('div', { className: 'card-header' },
          el('div', { className: 'min-w-0' },
            el('strong', { className: 'text-sm' }, chk.title || chk.id || 'Check'),
            chk.category ? el('div', { className: 'text-xs text-muted' }, chk.category) : null
          ),
          el('div', { className: 'inline-flex gap-sm' },
            chk.fixable ? el('span', { className: 'pill pill-muted' }, 'Auto-fix') : null,
            el('span', { className: `doctor-status-badge ${badgeClass}` }, st.toUpperCase())
          )
        ),
        el('p', { className: 'text-sm' }, chk.summary || ''),
        chk.remediation
          ? el('div', { className: 'text-xs text-muted mono' }, chk.remediation)
          : null
      ));
    }
  }

  async function runDoctorChecks(opts) {
    const options = opts || {};
    const runBtn = getEl('btn-run-doctor');
    if (runBtn && !options.silent) runBtn.disabled = true;
    try {
      const report = await apiFetch('/api/v1/doctor');
      renderDoctorReport(report);
      if (!options.silent) {
        showToast('Diagnostics complete.', report && report.fail_count > 0 ? 'warning' : 'success');
      }
      return report;
    } catch (err) {
      if (!options.silent) {
        showToast(`Diagnostics failed: ${err.message}`, 'danger');
      }
      return null;
    } finally {
      if (runBtn && !options.silent) runBtn.disabled = false;
    }
  }

  async function fixDoctorIssues() {
    const fixBtn = getEl('btn-fix-doctor');
    if (fixBtn) fixBtn.disabled = true;
    try {
      const res = await apiFetch('/api/v1/doctor/fix', { method: 'POST', body: {} });
      const report = (res && res.report) || res;
      if (report) renderDoctorReport(report);
      const fixes = res && Array.isArray(res.applied_fixes) ? res.applied_fixes : [];
      showToast(
        fixes.length > 0 ? `Applied ${fixes.length} fix(es): ${fixes.join(', ')}` : 'No auto-fixes needed.',
        fixes.length > 0 ? 'success' : 'info'
      );
      return res;
    } catch (err) {
      showToast(`Auto-fix failed: ${err.message}`, 'danger');
      return null;
    } finally {
      if (fixBtn) fixBtn.disabled = false;
    }
  }

  function renderBrowserSessionSelect() {
    const sel = getEl('browser-session-select');
    if (!sel) return;
    const prev = state.activeBrowserSessionId || sel.value;
    clearEl(sel);
    if (state.browserSessions.length === 0) {
      sel.appendChild(el('option', { value: '' }, 'No active browser sessions'));
      state.activeBrowserSessionId = '';
      return;
    }
    for (const s of state.browserSessions) {
      const label = `${shortId(s.session_id)} \u00b7 ${s.title || s.url || 'untitled'} (${s.elements_count ?? 0} els)`;
      sel.appendChild(el('option', { value: s.session_id }, label));
    }
    const validPrev = prev && state.browserSessions.some((s) => s.session_id === prev);
    state.activeBrowserSessionId = validPrev ? prev : state.browserSessions[0].session_id;
    sel.value = state.activeBrowserSessionId;
  }

  async function loadBrowserSessions(opts) {
    const options = opts || {};
    try {
      const data = await apiFetch('/api/v1/sandbox/browser/sessions');
      state.browserSessions = unwrapList(data, ['sessions']);
      renderBrowserSessionSelect();
      syncLiveTabsFromSessionsList(state.browserSessions);
      const statusBar = getEl('browser-status-bar');
      if (statusBar && state.browserSessions.length === 0 && !options.keepStatus) {
        statusBar.textContent = 'Idle \u2014 open a URL to start a sandboxed browser session.';
      }
      return state.browserSessions;
    } catch (err) {
      if (!options.silent) {
        showToast(`Failed to load browser sessions: ${err.message}`, 'danger');
      }
      return [];
    }
  }

  function renderBrowserSnapshot(res, actionName) {
    if (!res) return;
    if (res.session_id) {
      state.activeBrowserSessionId = res.session_id;
      const sel = getEl('browser-session-select');
      if (sel && [...sel.options].some((o) => o.value === res.session_id)) {
        sel.value = res.session_id;
      }
      const sid = String(res.session_id);
      const curTab = state.liveBrowserTabs.get(sid) || {
        session_id: sid,
        url: '',
        title: '',
        agent_id: '',
        action: actionName || '',
        status: 'opened',
        x_pct: 50,
        y_pct: 18,
        w_pct: null,
        h_pct: null,
        scroll_y: 0,
        selector: '',
        elements: [],
        conversation_id: '',
        task_id: '',
        hasLoadedPreview: false,
        updatedAtMs: Date.now()
      };
      if (res.url) curTab.url = String(res.url);
      if (res.title) curTab.title = String(res.title);
      if (Array.isArray(res.elements)) curTab.elements = res.elements;
      if (res.cursor && typeof res.cursor === 'object') {
        const cx = clampPct(res.cursor.x_pct);
        const cy = clampPct(res.cursor.y_pct);
        if (cx !== null) curTab.x_pct = cx;
        if (cy !== null) curTab.y_pct = cy;
        if (typeof res.cursor.scroll_y === 'number') curTab.scroll_y = res.cursor.scroll_y;
        if (res.cursor.last_action) curTab.action = res.cursor.last_action;
        if (res.cursor.last_selector) curTab.selector = res.cursor.last_selector;
        if (res.cursor.agent_id) curTab.agent_id = res.cursor.agent_id;
      }
      if (actionName) curTab.action = actionName;
      curTab.updatedAtMs = Date.now();
      state.liveBrowserTabs.set(sid, curTab);
      if (!state.liveBrowserOrder.includes(sid)) {
        state.liveBrowserOrder.unshift(sid);
      }
      state.activeLiveBrowserSessionId = sid;
      renderAllLiveBrowserViewports({
        refreshPreview: actionName !== 'links' && actionName !== 'text',
        immediatePreview: actionName === 'open' || actionName === 'back' || actionName === 'snapshot',
        ripple: actionName === 'click' || actionName === 'submit'
      });
    }
    if (res.url) {
      const urlIn = getEl('browser-url-input');
      if (urlIn && actionName === 'open') urlIn.value = res.url;
    }

    const statusBar = getEl('browser-status-bar');
    if (statusBar) {
      const parts = [
        res.session_id ? `Session ${shortId(res.session_id)}` : null,
        res.status ? `HTTP ${res.status}` : null,
        res.url || null
      ].filter(Boolean);
      statusBar.textContent = parts.length > 0 ? parts.join(' \u00b7 ') : 'Browser ready';
    }

    const metaEl = getEl('browser-snapshot-meta');
    if (metaEl) {
      const elsCount = Array.isArray(res.elements) ? res.elements.length : (res.elements_count ?? 0);
      const linksCount = Array.isArray(res.links) ? res.links.length : (res.links_count ?? 0);
      metaEl.textContent = `${res.title || 'Untitled'} \u00b7 ${elsCount} element(s) \u00b7 ${linksCount} link(s) \u00b7 ${res.cookie_count ?? 0} cookie(s)`;
    }

    const elsList = getEl('browser-elements-list');
    if (elsList && (Array.isArray(res.elements) || Array.isArray(res.links))) {
      clearEl(elsList);
      if (Array.isArray(res.links) && actionName === 'links') {
        if (res.links.length === 0) {
          elsList.appendChild(el('div', { className: 'text-sm text-muted' }, 'No links found on page.'));
        } else {
          for (const lnk of res.links) {
            const refVal = lnk.ref || '';
            const hrefVal = lnk.resolved_url || lnk.href || '';
            elsList.appendChild(el('div', {
              className: 'browser-ref-row',
              onClick: () => {
                const refIn = getEl('browser-ref-input');
                if (refIn && refVal) refIn.value = refVal;
                const urlIn = getEl('browser-url-input');
                if (urlIn && hrefVal) urlIn.value = hrefVal;
              }
            },
              el('span', { className: 'browser-ref-badge' }, refVal || 'link'),
              el('div', { className: 'min-w-0 flex-1' },
                el('div', { className: 'text-sm truncate' }, lnk.text || '(untitled link)'),
                el('div', { className: 'text-xs text-muted mono truncate' }, hrefVal)
              )
            ));
          }
        }
      } else if (Array.isArray(res.elements)) {
        if (res.elements.length === 0) {
          elsList.appendChild(el('div', { className: 'text-sm text-muted' }, 'No interactive elements found in snapshot.'));
        } else {
          for (const item of res.elements) {
            const refVal = item.ref || '';
            const tagLabel = [item.tag, item.type, item.role].filter(Boolean).join('/');
            const summaryText = item.text || item.placeholder || item.value || item.name || item.href || '';
            elsList.appendChild(el('div', {
              className: 'browser-ref-row',
              onClick: () => {
                const refIn = getEl('browser-ref-input');
                if (refIn && refVal) {
                  refIn.value = refVal;
                  refIn.focus();
                }
              }
            },
              el('span', { className: 'browser-ref-badge' }, refVal || '?'),
              el('div', { className: 'min-w-0 flex-1' },
                el('div', { className: 'text-xs mono text-muted' }, tagLabel || 'element'),
                el('div', { className: 'text-sm truncate' }, summaryText || '(no label)')
              )
            ));
          }
        }
      }
    }

    const pageTextEl = getEl('browser-page-text');
    if (pageTextEl && typeof res.text === 'string') {
      pageTextEl.textContent = res.text || '(empty text)';
    }
  }

  async function runBrowserAction(actionName, opts) {
    const options = opts || {};
    const sid = options.sessionId !== undefined
      ? options.sessionId
      : (state.activeBrowserSessionId || (getEl('browser-session-select') || {}).value || '');
    const urlVal = String((getEl('browser-url-input') || {}).value || '').trim();
    const refVal = String((getEl('browser-ref-input') || {}).value || '').trim();
    const fillVal = String((getEl('browser-fill-value') || {}).value || '');

    if (actionName === 'open' && !urlVal) {
      showToast('Enter a URL to open in the sandboxed browser.', 'warning');
      return null;
    }
    if (['snapshot', 'text', 'links', 'back', 'fill', 'click', 'submit'].includes(actionName) && !sid) {
      if (!options.silentEmpty) {
        showToast('Open a URL or select an active browser session first.', 'warning');
      }
      return null;
    }
    if (['fill', 'click', 'submit'].includes(actionName) && !refVal) {
      showToast(`Enter an element ref for browser ${actionName}.`, 'warning');
      return null;
    }

    const body = { action: actionName };
    if (sid) body.session_id = sid;
    if (actionName === 'open') body.url = urlVal;
    if (['fill', 'click', 'submit'].includes(actionName)) body.ref = refVal;
    if (actionName === 'fill') body.value = fillVal;

    try {
      const res = await apiFetch('/api/v1/sandbox/browser/action', {
        method: 'POST',
        body
      });
      renderBrowserSnapshot(res, actionName);

      if (actionName === 'open') {
        await loadBrowserSessions({ silent: true, keepStatus: true });
      }
      if (['open', 'back', 'fill', 'click', 'submit'].includes(actionName) && res && res.session_id) {
        const snap = await apiFetch('/api/v1/sandbox/browser/action', {
          method: 'POST',
          body: { action: 'snapshot', session_id: res.session_id }
        }).catch(() => null);
        if (snap) renderBrowserSnapshot(snap, 'snapshot');
      }
      if (['open', 'snapshot', 'back', 'click', 'submit'].includes(actionName) && res && res.session_id) {
        const txt = await apiFetch('/api/v1/sandbox/browser/action', {
          method: 'POST',
          body: { action: 'text', session_id: res.session_id }
        }).catch(() => null);
        if (txt) renderBrowserSnapshot(txt, 'text');
      }
      return res;
    } catch (err) {
      const statusBar = getEl('browser-status-bar');
      if (statusBar) statusBar.textContent = `Error: ${err.message}`;
      showToast(`Browser ${actionName} failed: ${err.message}`, 'danger');
      return null;
    }
  }

  async function closeActiveBrowserSession() {
    const sid = state.activeBrowserSessionId || (getEl('browser-session-select') || {}).value || '';
    if (!sid) {
      showToast('No active browser session to close.', 'warning');
      return;
    }
    try {
      await apiFetch(`/api/v1/sandbox/browser/sessions/${encodeURIComponent(sid)}`, {
        method: 'DELETE'
      });
      if (state.activeBrowserSessionId === sid) {
        state.activeBrowserSessionId = '';
      }
      handleBrowserActivityEvent({
        session_id: sid,
        action: 'close',
        status: 'closed',
        timestamp: new Date().toISOString()
      });
      const elsList = getEl('browser-elements-list');
      if (elsList) clearEl(elsList);
      setText('browser-snapshot-meta', 'Session closed.');
      setText('browser-page-text', '');
      await loadBrowserSessions();
      showToast('Closed browser session.', 'success');
    } catch (err) {
      showToast(`Close browser session failed: ${err.message}`, 'danger');
    }
  }

  function renderBrainFilesList() {
    const listEl = getEl('brain-files-list');
    if (!listEl) return;
    clearEl(listEl);

    if (state.brainFiles.length === 0) {
      listEl.appendChild(el('div', { className: 'text-sm text-muted' }, 'No brain files found.'));
      return;
    }
    for (const f of state.brainFiles) {
      const active = f.name === state.activeBrainFile;
      listEl.appendChild(el('button', {
        type: 'button',
        role: 'option',
        className: `btn btn-secondary btn-sm${active ? ' is-active active' : ''}`,
        'aria-selected': String(active),
        onClick: () => selectBrainFile(f.name)
      },
        el('span', { className: 'mono truncate' }, f.name),
        el('span', { className: 'text-xs text-muted' }, f.exists ? fmtBytes(f.size_bytes || 0) : 'new')
      ));
    }
  }

  async function selectBrainFile(fileName, opts) {
    const options = opts || {};
    if (!fileName) return;
    if (!options.force && state.brainDirty && state.activeBrainFile && state.activeBrainFile !== fileName) {
      openModal('Unsaved Brain Edits', [
        el('p', null, `Discard unsaved changes to "${state.activeBrainFile}" and open "${fileName}"?`)
      ], [
        el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Keep Editing'),
        el('button', {
          type: 'button',
          className: 'btn btn-primary',
          onClick: () => {
            closeModal();
            selectBrainFile(fileName, { force: true });
          }
        }, 'Discard & Switch')
      ]);
      return;
    }

    state.activeBrainFile = fileName;
    renderBrainFilesList();
    setText('brain-current-name', fileName);
    setText('brain-save-status', 'Loading...');

    try {
      const detail = await apiFetch(`/api/v1/brain/${encodeURIComponent(fileName)}`);
      const textarea = getEl('brain-editor-textarea');
      if (textarea) {
        textarea.value = detail && typeof detail.content === 'string' ? detail.content : '';
      }
      state.brainDirty = false;
      const meta = detail && detail.updated_at ? `Loaded \u00b7 ${fmtTime(detail.updated_at)}` : `Loaded \u00b7 ${fmtBytes((detail && detail.size_bytes) || 0)}`;
      setText('brain-save-status', meta);
    } catch (err) {
      setText('brain-save-status', `Load failed: ${err.message}`);
      showToast(`Load ${fileName} failed: ${err.message}`, 'danger');
    }
  }

  async function loadBrainFiles(opts) {
    const options = opts || {};
    try {
      const data = await apiFetch('/api/v1/brain');
      state.brainFiles = unwrapList(data, ['files']);
      const fallback = state.brainFiles[0] ? state.brainFiles[0].name : '';
      const target = state.activeBrainFile && state.brainFiles.some((f) => f.name === state.activeBrainFile)
        ? state.activeBrainFile
        : fallback;
      renderBrainFilesList();
      if (target && !options.listOnly && !state.brainDirty) {
        await selectBrainFile(target, { force: true });
      }
      return state.brainFiles;
    } catch (err) {
      if (!options.silent) {
        showToast(`Failed to load brain files: ${err.message}`, 'danger');
      }
      return [];
    }
  }

  async function saveActiveBrainFile() {
    const fileName = state.activeBrainFile;
    const textarea = getEl('brain-editor-textarea');
    if (!fileName || !textarea) {
      showToast('Select a brain file first.', 'warning');
      return;
    }
    const saveBtn = getEl('btn-brain-save');
    if (saveBtn) saveBtn.disabled = true;
    setText('brain-save-status', 'Saving...');
    try {
      const res = await apiFetch('/api/v1/brain', {
        method: 'PUT',
        body: { name: fileName, content: textarea.value }
      });
      state.brainDirty = false;
      setText('brain-save-status', `Saved \u00b7 ${fmtBytes((res && res.size_bytes) || textarea.value.length)}`);
      showToast(`Saved ${fileName}.`, 'success');
      await loadBrainFiles({ listOnly: true, silent: true });
    } catch (err) {
      setText('brain-save-status', `Save failed: ${err.message}`);
      showToast(`Save ${fileName} failed: ${err.message}`, 'danger');
    } finally {
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function populateWebhookAgentSelect() {
    const sel = getEl('webhook-agent-select');
    if (!sel) return;
    const prev = sel.value;
    clearEl(sel);
    sel.appendChild(el('option', { value: '' }, 'Default (Commander)'));
    for (const ag of state.agents) {
      sel.appendChild(el('option', { value: ag.id }, `${ag.name} (${ag.role})`));
    }
    if (prev && [...sel.options].some((o) => o.value === prev)) {
      sel.value = prev;
    }
  }

  function populateWebhookConvSelect() {
    const sel = getEl('webhook-conv-select');
    if (!sel) return;
    const prev = sel.value;
    clearEl(sel);
    sel.appendChild(el('option', { value: '' }, 'New / Latest Conversation'));
    for (const c of state.conversations) {
      sel.appendChild(el('option', { value: c.id }, `${c.title || shortId(c.id)}`));
    }
    if (prev && [...sel.options].some((o) => o.value === prev)) {
      sel.value = prev;
    }
  }

  function hideWebhookCreatedBanner() {
    const banner = getEl('webhook-created-banner');
    if (banner) banner.hidden = true;
    setText('webhook-created-url', '');
    setText('webhook-created-secret', '');
  }

  function renderWebhooksList() {
    const container = getEl('webhooks-list');
    if (!container) return;
    clearEl(container);

    if (state.webhooks.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' },
        el('strong', null, 'No inbound webhooks'),
        el('span', { className: 'text-sm' }, 'Create a webhook above to trigger agents over signed HTTP POST.')
      ));
      return;
    }

    for (const wh of state.webhooks) {
      const inboundUrl = `${window.location.origin}/api/v1/webhooks/inbound/${encodeURIComponent(wh.id)}`;
      container.appendChild(el('article', { className: 'webhook-row' },
        el('div', { className: 'min-w-0 flex-1' },
          el('div', { className: 'inline-flex gap-sm' },
            el('strong', null, wh.name || wh.id),
            el('span', { className: `pill ${wh.enabled ? 'pill-success' : 'pill-muted'}` }, wh.enabled ? 'Enabled' : 'Disabled'),
            el('span', { className: 'pill pill-info' }, agentDisplayName(wh.target_agent_id))
          ),
          el('div', { className: 'text-xs mono text-muted truncate' }, inboundUrl),
          el('div', { className: 'text-xs text-muted' },
            `Secret: ${wh.secret_prefix || '***'} \u00b7 Triggers: ${wh.trigger_count ?? 0}${wh.last_triggered_at ? ` \u00b7 Last: ${fmtTime(wh.last_triggered_at)}` : ''}`
          ),
          wh.prompt_template
            ? el('div', { className: 'text-xs text-secondary truncate' }, `Template: ${wh.prompt_template}`)
            : null
        ),
        el('div', { className: 'card-actions' },
          el('button', {
            type: 'button',
            className: 'btn btn-secondary btn-sm',
            onClick: (e) => copyTextWithFeedback(e.currentTarget, inboundUrl, 'Copy URL')
          }, 'Copy URL'),
          el('button', {
            type: 'button',
            className: 'btn btn-danger btn-sm',
            onClick: () => confirmDeleteWebhook(wh)
          }, 'Delete')
        )
      ));
    }
  }

  async function loadWebhooks(opts) {
    const options = opts || {};
    try {
      const data = await apiFetch('/api/v1/webhooks');
      state.webhooks = unwrapList(data, ['webhooks']);
      renderWebhooksList();
      populateWebhookAgentSelect();
      populateWebhookConvSelect();
      return state.webhooks;
    } catch (err) {
      if (!options.silent) {
        showToast(`Failed to load webhooks: ${err.message}`, 'danger');
      }
      return [];
    }
  }

  async function onCreateWebhook(ev) {
    ev.preventDefault();
    const btn = getEl('btn-webhook-create');
    if (btn && btn.disabled) return;

    const nameIn = getEl('webhook-name-input');
    const agentSel = getEl('webhook-agent-select');
    const convSel = getEl('webhook-conv-select');
    const tmplIn = getEl('webhook-template-input');

    const cleanName = nameIn ? nameIn.value.trim() : '';
    if (!cleanName) {
      showToast('Webhook name is required.', 'warning');
      if (nameIn) nameIn.focus();
      return;
    }
    let targetAgentId = agentSel ? agentSel.value : '';
    if (!targetAgentId) {
      const cmdr = state.agents.find((a) => a.role === 'COMMANDER' && a.enabled)
        || state.agents.find((a) => a.id === 'commander')
        || state.agents.find((a) => a.enabled);
      targetAgentId = cmdr ? cmdr.id : 'commander';
    }

    const payload = {
      name: cleanName,
      target_agent_id: targetAgentId,
      conversation_id: convSel && convSel.value ? convSel.value : null,
      prompt_template: tmplIn ? tmplIn.value : '',
      enabled: true
    };

    if (btn) btn.disabled = true;
    try {
      const res = await apiFetch('/api/v1/webhooks', {
        method: 'POST',
        body: payload
      });
      const wh = res && res.webhook ? res.webhook : res;
      const secret = (res && res.secret) || '';
      const banner = getEl('webhook-created-banner');
      if (banner && wh && wh.id) {
        setText('webhook-created-url', `${window.location.origin}/api/v1/webhooks/inbound/${encodeURIComponent(wh.id)}`);
        setText('webhook-created-secret', secret || '(hidden)');
        banner.hidden = false;
      }
      if (nameIn) nameIn.value = '';
      if (tmplIn) tmplIn.value = '';
      await loadWebhooks({ silent: true });
      showToast(`Created webhook "${cleanName}". Copy the secret now.`, 'success');
    } catch (err) {
      showToast(`Create webhook failed: ${err.message}`, 'danger');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  function confirmDeleteWebhook(wh) {
    openModal('Delete Webhook', [
      el('p', null, `Delete inbound webhook "${wh.name || wh.id}"? External triggers using this ID will stop immediately.`)
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch(`/api/v1/webhooks/${encodeURIComponent(wh.id)}`, { method: 'DELETE' });
            closeModal();
            showToast('Webhook deleted.', 'success');
            await loadWebhooks({ silent: true });
          } catch (err) {
            showToast(`Delete webhook failed: ${err.message}`, 'danger');
          }
        }
      }, 'Delete')
    ]);
  }

  async function loadSandboxTab() {
    await Promise.allSettled([
      loadSandboxState(),
      runDoctorChecks({ silent: true }),
      loadBrowserSessions({ silent: true }),
      loadBrainFiles({ silent: true }),
      loadWebhooks({ silent: true })
    ]);
  }

  /* -------------------- 8. Logs -------------------- */
  async function loadRuntimeLogs() {
    const lvl = $('logs-level-filter').value;
    const data = await apiFetch(`/api/v1/logs${lvl ? `?level=${encodeURIComponent(lvl)}` : ''}`);
    state.runtimeLogs = unwrapList(data, ['logs', 'entries']);
    renderRuntimeLogs();
  }

  async function loadAuditLogs() {
    const data = await apiFetch('/api/v1/audit-logs');
    state.auditLogs = unwrapList(data, ['audit_logs', 'logs', 'entries']);
    renderAuditLogs();
  }

  function renderRuntimeLogs() {
    const container = $('logs-container');
    clearEl(container);
    const search = ($('logs-search-input').value || '').trim().toLowerCase();
    const rows = state.runtimeLogs.filter((l) => {
      if (!search) return true;
      return `${l.target} ${l.message} ${l.level}`.toLowerCase().includes(search);
    });

    if (rows.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' }, el('strong', null, 'No runtime logs matching filter')));
      return;
    }

    const tbody = el('tbody');
    for (const l of rows) {
      tbody.appendChild(el('tr', null,
        el('td', { className: 'num text-sm' }, l.seq ?? ''),
        el('td', { className: 'mono text-sm' }, fmtTime(l.timestamp)),
        el('td', null, el('span', { className: `pill ${statusPillClass(l.level)}` }, l.level)),
        el('td', { className: 'mono text-sm' }, l.target),
        el('td', { className: 'mono text-sm' }, l.message)
      ));
    }

    container.appendChild(el('div', { className: 'table-scroll' },
      el('table', { className: 'data-table' },
        el('thead', null, el('tr', null,
          el('th', { className: 'num' }, '#'),
          el('th', null, 'Time'),
          el('th', null, 'Level'),
          el('th', null, 'Target'),
          el('th', null, 'Message')
        )),
        tbody
      )
    ));
  }

  function renderAuditLogs() {
    const container = $('audit-logs-container');
    clearEl(container);
    const search = ($('logs-search-input').value || '').trim().toLowerCase();
    const rows = state.auditLogs.filter((l) => {
      if (!search) return true;
      return `${l.actor} ${l.action} ${l.target} ${l.outcome}`.toLowerCase().includes(search);
    });

    if (rows.length === 0) {
      container.appendChild(el('div', { className: 'empty-state' }, el('strong', null, 'No security audit logs recorded')));
      return;
    }

    const tbody = el('tbody');
    for (const a of rows) {
      tbody.appendChild(el('tr', null,
        el('td', { className: 'mono text-sm' }, fmtTime(a.timestamp)),
        el('td', null, a.actor),
        el('td', { className: 'mono text-sm' }, a.action),
        el('td', { className: 'mono text-sm' }, a.target),
        el('td', null, el('span', { className: `pill ${statusPillClass(a.severity)}` }, a.severity)),
        el('td', null, el('span', { className: `pill ${statusPillClass(a.outcome)}` }, a.outcome)),
        el('td', null, a.details_json ? el('pre', { className: 'code-box', style: 'margin:0' }, prettyJson(a.details_json)) : '\u2014')
      ));
    }

    container.appendChild(el('div', { className: 'table-scroll' },
      el('table', { className: 'data-table' },
        el('thead', null, el('tr', null,
          el('th', null, 'Time'),
          el('th', null, 'Actor'),
          el('th', null, 'Action'),
          el('th', null, 'Target'),
          el('th', null, 'Severity'),
          el('th', null, 'Outcome'),
          el('th', null, 'Details')
        )),
        tbody
      )
    ));
  }

  /* -------------------- 9. Settings & PS5LM Sync -------------------- */
  function renderInferenceCapabilities(caps) {
    if (!caps || typeof caps !== 'object') return;
    state.inferenceCapabilities = caps;
    const modelText = caps.loaded_model || (caps.healthy ? 'No model loaded' : 'Unreachable');
    setText('ps5lm-cap-model', modelText);
    if (caps.loaded_model) {
      setText('ov-loaded-model', caps.loaded_model);
    }
    setText('ps5lm-cap-ctx', caps.context_length ? String(caps.context_length) : '\u2014');
    setText('ps5lm-cap-slots', caps.total_slots !== null && caps.total_slots !== undefined ? String(caps.total_slots) : '\u2014');
    setText('ps5lm-cap-vision', caps.healthy ? (caps.supports_vision ? 'Yes' : 'No') : '\u2014');
    setText('ps5lm-cap-thinking', caps.healthy ? (caps.supports_thinking ? 'Yes' : 'No') : '\u2014');
    setText('ps5lm-cap-synced', caps.synced_at ? fmtDateTime(caps.synced_at) : '\u2014');
  }

  async function loadInferenceCapabilities() {
    const caps = await apiFetch('/api/v1/inference/capabilities');
    renderInferenceCapabilities(caps);
    return caps;
  }

  function setInputValue(id, value) {
    const node = getEl(id);
    if (node) node.value = value;
  }

  function applySettingsFormValues(cfg) {
    const inf = cfg.inference || cfg;
    const srv = cfg.server || cfg.runtime || {};
    const db = cfg.database || {};
    const ag = cfg.agents || cfg;
    const mem = cfg.memory || {};
    const log = cfg.logging || cfg;
    const sec = cfg.security || cfg.runtime || {};

    setInputValue('cfg-base-url', inf.base_url || 'http://127.0.0.1:8081/v1');
    setInputValue('cfg-model', inf.model || 'qwen-loaded');
    setInputValue('cfg-api-mode', inf.api_mode || 'chat_completions');
    setInputValue('cfg-thinking-level', String(inf.thinking_level || 'medium').toLowerCase());
    setInputValue('cfg-auto-sync-ps5lm', inf.auto_sync_from_ps5lm === false ? 'false' : 'true');
    setInputValue('cfg-auto-sync-interval-secs', inf.auto_sync_interval_secs ?? 30);
    setInputValue('cfg-stream-enabled', inf.stream_enabled === false ? 'false' : 'true');
    setInputValue('cfg-timeout-secs', inf.timeout_secs ?? 120);
    setInputValue('cfg-connect-timeout-secs', inf.connect_timeout_secs ?? 5);
    setInputValue('cfg-max-retries', inf.max_retries ?? 2);
    setInputValue('cfg-context-limit', inf.context_limit ?? 8192);
    setInputValue('cfg-max-output-tokens', inf.max_output_tokens ?? 1024);

    setInputValue('cfg-prefill-kv-cache', inf.prefill_kv_cache === false ? 'false' : 'true');
    setInputValue('cfg-cache-prompt', inf.cache_prompt === false ? 'false' : 'true');
    const normKv = (v) => String(v || '').trim().toLowerCase();
    setInputValue('set-inf-cache-type-k', normKv(inf.cache_type_k));
    setInputValue('set-inf-cache-type-v', normKv(inf.cache_type_v));
    const flashAttnEl = getEl('set-inf-flash-attn');
    if (flashAttnEl) flashAttnEl.checked = Boolean(inf.flash_attn);
    const smartRoutingEl = getEl('set-inf-smart-tool-routing');
    if (smartRoutingEl) smartRoutingEl.checked = inf.smart_tool_routing !== false;
    setInputValue('cfg-temperature', inf.temperature ?? 0.4);
    setInputValue('cfg-dynatemp-range', inf.dynatemp_range ?? 0);
    setInputValue('cfg-dynatemp-exponent', inf.dynatemp_exponent ?? 1);
    setInputValue('cfg-top-k', inf.top_k ?? 40);
    setInputValue('cfg-top-p', inf.top_p ?? 0.95);
    setInputValue('cfg-min-p', inf.min_p ?? 0.05);
    setInputValue('cfg-typical-p', inf.typical_p ?? 1);
    setInputValue('cfg-xtc-probability', inf.xtc_probability ?? 0);
    setInputValue('cfg-xtc-threshold', inf.xtc_threshold ?? 0.1);
    setInputValue('cfg-top-n-sigma', inf.top_n_sigma ?? -1);
    setInputValue('cfg-repeat-penalty', inf.repeat_penalty ?? 1.05);
    setInputValue('cfg-repeat-last-n', inf.repeat_last_n ?? 256);
    setInputValue('cfg-presence-penalty', inf.presence_penalty ?? 0);
    setInputValue('cfg-frequency-penalty', inf.frequency_penalty ?? 0);
    setInputValue('cfg-dry-multiplier', inf.dry_multiplier ?? 0.8);
    setInputValue('cfg-dry-base', inf.dry_base ?? 1.75);
    setInputValue('cfg-dry-allowed-length', inf.dry_allowed_length ?? 2);
    setInputValue('cfg-dry-penalty-last-n', inf.dry_penalty_last_n ?? -1);
    setInputValue('cfg-mirostat', String(inf.mirostat ?? 0));
    setInputValue('cfg-mirostat-tau', inf.mirostat_tau ?? 5);
    setInputValue('cfg-mirostat-eta', inf.mirostat_eta ?? 0.1);
    setInputValue('cfg-seed', inf.seed ?? -1);
    setInputValue('cfg-ignore-eos', inf.ignore_eos === true ? 'true' : 'false');
    setInputValue('cfg-n-keep', inf.n_keep ?? 0);
    setInputValue('cfg-id-slot', inf.id_slot ?? -1);
    setInputValue('cfg-samplers', inf.samplers || '');
    setInputValue('cfg-reasoning-format', inf.reasoning_format ?? 'deepseek');
    setInputValue('cfg-reasoning-mode', inf.reasoning_mode ?? '');
    setInputValue('cfg-timings-per-token', inf.timings_per_token === true ? 'true' : 'false');
    setInputValue('cfg-stop-sequences', Array.isArray(inf.stop_sequences) ? inf.stop_sequences.join('\n') : '');
    setInputValue('cfg-grammar', inf.grammar || '');
    setInputValue('cfg-custom-extra-body-json', inf.custom_extra_body_json || '');

    setInputValue('cfg-upload-max-body-bytes', srv.upload_max_body_bytes ?? 67108864);

    setInputValue('cfg-max-agents', ag.max_agents ?? 0);
    setInputValue('cfg-max-active-tasks', ag.max_active_tasks ?? 0);
    setInputValue('cfg-max-queued-tasks', ag.max_queued_tasks ?? 0);
    setInputValue('cfg-max-concurrent-inference', ag.max_concurrent_inference ?? 4);
    setInputValue('cfg-max-messages-per-task', ag.max_messages_per_task ?? 0);
    setInputValue('cfg-max-reasoning-iterations', ag.max_reasoning_iterations ?? 0);
    setInputValue('cfg-default-task-timeout-secs', ag.default_task_timeout_secs ?? 0);
    setInputValue('cfg-allow-temp-specialists', ag.allow_temporary_specialists === false ? 'false' : 'true');
    setInputValue('cfg-on-disable-policy', ag.on_disable_policy || 'cancel');

    setInputValue('cfg-require-approval-mutating', sec.require_approval_for_mutating === false ? 'false' : 'true');
    setInputValue('cfg-rate-limit-per-minute', sec.rate_limit_per_minute ?? 240);
    setInputValue('cfg-session-ttl-hours', sec.session_ttl_hours ?? 24);

    setInputValue('cfg-max-memories-per-agent', mem.max_memories_per_agent ?? 0);
    setInputValue('cfg-archive-after-messages', mem.archive_after_messages ?? 40);

    setInputValue('cfg-retention-days', db.retention_days ?? 30);
    setInputValue('cfg-max-rows-per-table', db.max_rows_per_table ?? 10000);

    setInputValue('cfg-logging-level', String(log.level || cfg.logging_level || 'info').toLowerCase());
    setInputValue('cfg-admin-username', sec.admin_username || 'admin');
    setInputValue('cfg-auth-enabled', sec.auth_enabled === false ? 'false' : 'true');
  }

  function updateModelDatalist(models) {
    const input = getEl('cfg-model');
    if (!input) return;
    let dl = getEl('cfg-model-datalist');
    if (!dl) {
      dl = el('datalist', { id: 'cfg-model-datalist' });
      input.insertAdjacentElement('afterend', dl);
      input.setAttribute('list', 'cfg-model-datalist');
    }
    clearEl(dl);
    for (const m of models) {
      if (m) dl.appendChild(el('option', { value: m }));
    }
  }

  async function loadSettings(opts) {
    const options = opts || {};
    const requests = [apiFetch('/api/v1/settings')];
    if (!options.skipCapabilities) {
      requests.push(apiFetch('/api/v1/inference/capabilities').catch(() => null));
      requests.push(apiFetch('/api/v1/inference/models').catch(() => null));
    }

    const [cfg, caps, modelsResp] = await Promise.all(requests);
    state.settingsRaw = cfg || {};
    applySettingsFormValues(state.settingsRaw);

    if (caps) {
      renderInferenceCapabilities(caps);
    }
    if (modelsResp && Array.isArray(modelsResp.models)) {
      state.inferenceModels = modelsResp.models;
      updateModelDatalist(modelsResp.models);
    }
  }

  async function onSyncPs5Lm() {
    const btn = getEl('btn-sync-ps5lm');
    const prevText = btn ? btn.textContent : '';
    if (btn) {
      btn.disabled = true;
      btn.textContent = 'Syncing\u2026';
    }
    try {
      const caps = await apiFetch('/api/v1/inference/sync', { method: 'POST', body: {} });
      renderInferenceCapabilities(caps);
      await loadSettings({ skipCapabilities: true });
      if (caps && caps.healthy) {
        showToast(`Synced from PS5LM: ${caps.loaded_model || 'OK'} (n_ctx ${caps.context_length || '\u2014'})`, 'success');
      } else {
        showToast('PS5LM probed, but server reported unhealthy/unreachable.', 'warning');
      }
    } catch (err) {
      showToast(`PS5LM sync failed: ${err.message}`, 'danger');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = prevText || 'Sync Settings from PS5LM Now';
      }
    }
  }

  function readNonNegInt(id, label) {
    const node = getEl(id);
    if (!node) return 0;
    const raw = String(node.value || '').trim();
    const num = Number(raw);
    if (raw === '' || Number.isNaN(num) || !Number.isFinite(num) || num < 0) {
      throw new Error(`${label} must be 0 (Unlimited) or a positive integer.`);
    }
    return Math.floor(num);
  }

  function readFloatInRange(id, label, minVal, maxVal) {
    const raw = ($(id).value || '').trim();
    const num = Number(raw);
    if (raw === '' || !Number.isFinite(num) || num < minVal || (maxVal !== null && num > maxVal)) {
      throw new Error(maxVal === null ? `${label} must be at least ${minVal}.` : `${label} must be between ${minVal} and ${maxVal}.`);
    }
    return num;
  }

  function readIntInRange(id, label, minVal, maxVal) {
    const raw = ($(id).value || '').trim();
    const num = Number(raw);
    if (raw === '' || !Number.isInteger(num) || num < minVal || (maxVal !== null && num > maxVal)) {
      throw new Error(maxVal === null ? `${label} must be an integer >= ${minVal}.` : `${label} must be an integer between ${minVal} and ${maxVal}.`);
    }
    return num;
  }

  async function onSaveSettings(ev) {
    ev.preventDefault();
    const base = state.settingsRaw && typeof state.settingsRaw === 'object'
      ? JSON.parse(JSON.stringify(state.settingsRaw))
      : {};

    let inferenceUpdate;
    let serverUpdate;
    let agentsUpdate;
    let memoryUpdate;
    let databaseUpdate;
    let securityUpdate;
    let loggingLevel;

    try {
      const baseUrl = $('cfg-base-url').value.trim();
      const model = $('cfg-model').value.trim();
      if (!baseUrl) throw new Error('PS5LM Base URL is required.');
      if (!model) throw new Error('Model is required.');

      const timeoutSecs = readNonNegInt('cfg-timeout-secs', 'Inference Timeout');
      const connectTimeoutSecs = readNonNegInt('cfg-connect-timeout-secs', 'Connect Timeout');
      if (connectTimeoutSecs < 1 || connectTimeoutSecs > 300) {
        throw new Error('Connect Timeout must be between 1 and 300 seconds.');
      }
      const maxRetries = readNonNegInt('cfg-max-retries', 'Max Inference Retries');
      const autoSyncIntervalSecs = readNonNegInt('cfg-auto-sync-interval-secs', 'Auto-Sync Interval');
      const contextLimit = readNonNegInt('cfg-context-limit', 'Context Limit');
      if (contextLimit !== 0 && (contextLimit < 512 || contextLimit > 1048576)) {
        throw new Error('Context Limit must be 0 (Default/Unlimited) or between 512 and 1048576.');
      }
      const maxOutputTokens = readNonNegInt('cfg-max-output-tokens', 'Max Output Tokens');
      if (maxOutputTokens !== 0 && maxOutputTokens < 64) {
        throw new Error('Max Output Tokens must be 0 (Unlimited) or at least 64.');
      }
      if (maxOutputTokens > 0 && contextLimit > 0 && maxOutputTokens >= contextLimit) {
        throw new Error('Max Output Tokens must be less than Context Limit when both are > 0.');
      }

      const uploadMaxBodyBytes = readNonNegInt('cfg-upload-max-body-bytes', 'Max Upload Body');
      if (uploadMaxBodyBytes !== 0 && (uploadMaxBodyBytes < 1024 || uploadMaxBodyBytes > 268435456)) {
        throw new Error('Max Upload Body must be 0 (256 MiB max) or between 1024 and 268435456 bytes.');
      }

      const sessionTtlHours = readNonNegInt('cfg-session-ttl-hours', 'Session TTL');
      if (sessionTtlHours < 1 || sessionTtlHours > 8760) {
        throw new Error('Session TTL must be between 1 and 8760 hours.');
      }

      const adminUsername = $('cfg-admin-username').value.trim();
      if (!adminUsername) throw new Error('Admin Username cannot be empty.');

      const extraBodyRaw = $('cfg-custom-extra-body-json').value.trim();
      if (extraBodyRaw) {
        let parsedExtra;
        try {
          parsedExtra = JSON.parse(extraBodyRaw);
        } catch (err) {
          throw new Error(`Custom Extra Body JSON is invalid: ${err.message}`);
        }
        if (!parsedExtra || typeof parsedExtra !== 'object' || Array.isArray(parsedExtra)) {
          throw new Error('Custom Extra Body JSON must be a JSON object.');
        }
      }

      const stopSequences = ($('cfg-stop-sequences').value || '')
        .split(/\r?\n/)
        .map((s) => s.trim())
        .filter(Boolean);

      inferenceUpdate = {
        base_url: baseUrl,
        model,
        api_mode: $('cfg-api-mode').value,
        thinking_level: $('cfg-thinking-level').value || 'medium',
        auto_sync_from_ps5lm: $('cfg-auto-sync-ps5lm').value !== 'false',
        auto_sync_interval_secs: autoSyncIntervalSecs,
        stream_enabled: $('cfg-stream-enabled').value !== 'false',
        timeout_secs: timeoutSecs,
        connect_timeout_secs: connectTimeoutSecs,
        max_retries: maxRetries,
        context_limit: contextLimit,
        max_output_tokens: maxOutputTokens,
        prefill_kv_cache: $('cfg-prefill-kv-cache').value !== 'false',
        cache_prompt: $('cfg-cache-prompt').value !== 'false',
        cache_type_k: String((getEl('set-inf-cache-type-k') || {}).value || '').trim().toLowerCase(),
        cache_type_v: String((getEl('set-inf-cache-type-v') || {}).value || '').trim().toLowerCase(),
        flash_attn: Boolean((getEl('set-inf-flash-attn') || {}).checked),
        smart_tool_routing: (getEl('set-inf-smart-tool-routing') || { checked: true }).checked !== false,
        temperature: readFloatInRange('cfg-temperature', 'Temperature', 0, 2),
        dynatemp_range: readFloatInRange('cfg-dynatemp-range', 'Dynamic Temp Range', 0, 2),
        dynatemp_exponent: readFloatInRange('cfg-dynatemp-exponent', 'Dynamic Temp Exponent', 0, 10),
        top_k: readIntInRange('cfg-top-k', 'Top-K', -1, 10000),
        top_p: readFloatInRange('cfg-top-p', 'Top-P Nucleus', 0, 1),
        min_p: readFloatInRange('cfg-min-p', 'Min-P', 0, 1),
        typical_p: readFloatInRange('cfg-typical-p', 'Locally Typical P', 0, 1),
        xtc_probability: readFloatInRange('cfg-xtc-probability', 'XTC Probability', 0, 1),
        xtc_threshold: readFloatInRange('cfg-xtc-threshold', 'XTC Threshold', 0, 0.5),
        top_n_sigma: readFloatInRange('cfg-top-n-sigma', 'Top-N Sigma', -1, 100),
        repeat_penalty: readFloatInRange('cfg-repeat-penalty', 'Repeat Penalty', 0, 3),
        repeat_last_n: readIntInRange('cfg-repeat-last-n', 'Repeat Last N', -1, 131072),
        presence_penalty: readFloatInRange('cfg-presence-penalty', 'Presence Penalty', -2, 2),
        frequency_penalty: readFloatInRange('cfg-frequency-penalty', 'Frequency Penalty', -2, 2),
        dry_multiplier: readFloatInRange('cfg-dry-multiplier', 'DRY Multiplier', 0, 10),
        dry_base: readFloatInRange('cfg-dry-base', 'DRY Base', 1, 10),
        dry_allowed_length: readIntInRange('cfg-dry-allowed-length', 'DRY Allowed Length', 0, 100),
        dry_penalty_last_n: readIntInRange('cfg-dry-penalty-last-n', 'DRY Penalty Last N', -1, 131072),
        mirostat: readIntInRange('cfg-mirostat', 'Mirostat Mode', 0, 2),
        mirostat_tau: readFloatInRange('cfg-mirostat-tau', 'Mirostat Tau', 0, 20),
        mirostat_eta: readFloatInRange('cfg-mirostat-eta', 'Mirostat Eta', 0, 1),
        seed: readIntInRange('cfg-seed', 'RNG Seed', -1, null),
        ignore_eos: $('cfg-ignore-eos').value === 'true',
        n_keep: readIntInRange('cfg-n-keep', 'Prompt Tokens to Keep', -1, 131072),
        id_slot: readIntInRange('cfg-id-slot', 'Pinned Server Slot', -1, 64),
        samplers: ($('cfg-samplers').value || '').trim(),
        reasoning_format: ($('cfg-reasoning-format').value || '').trim(),
      reasoning_mode: ($('cfg-reasoning-mode') ? ($('cfg-reasoning-mode').value || '').trim() : ''),
        timings_per_token: $('cfg-timings-per-token').value === 'true',
        stop_sequences: stopSequences,
        grammar: $('cfg-grammar').value || '',
        custom_extra_body_json: extraBodyRaw
      };

      serverUpdate = {
        upload_max_body_bytes: uploadMaxBodyBytes
      };

      agentsUpdate = {
        max_agents: readNonNegInt('cfg-max-agents', 'Max Agents'),
        max_active_tasks: readNonNegInt('cfg-max-active-tasks', 'Max Active Tasks'),
        max_queued_tasks: readNonNegInt('cfg-max-queued-tasks', 'Max Queued Tasks'),
        max_concurrent_inference: readNonNegInt('cfg-max-concurrent-inference', 'Max Concurrent Inference'),
        max_messages_per_task: readNonNegInt('cfg-max-messages-per-task', 'Max Messages Per Task'),
        max_reasoning_iterations: readNonNegInt('cfg-max-reasoning-iterations', 'Max Reasoning Iterations'),
        default_task_timeout_secs: readNonNegInt('cfg-default-task-timeout-secs', 'Default Task Timeout'),
        allow_temporary_specialists: $('cfg-allow-temp-specialists').value !== 'false',
        on_disable_policy: $('cfg-on-disable-policy').value || 'cancel'
      };

      memoryUpdate = {
        max_memories_per_agent: readNonNegInt('cfg-max-memories-per-agent', 'Max Memories Per Agent'),
        archive_after_messages: readNonNegInt('cfg-archive-after-messages', 'Archive After Messages')
      };

      databaseUpdate = {
        retention_days: readNonNegInt('cfg-retention-days', 'Database Retention'),
        max_rows_per_table: readNonNegInt('cfg-max-rows-per-table', 'Max Rows Per Table')
      };

      loggingLevel = $('cfg-logging-level').value || 'info';

      securityUpdate = {
        require_approval_for_mutating: $('cfg-require-approval-mutating').value !== 'false',
        rate_limit_per_minute: readNonNegInt('cfg-rate-limit-per-minute', 'API Rate Limit'),
        session_ttl_hours: sessionTtlHours,
        admin_username: adminUsername,
        auth_enabled: $('cfg-auth-enabled').value !== 'false'
      };
    } catch (validationErr) {
      showToast(validationErr.message, 'warning');
      return;
    }

    const payload = Object.assign({}, base, {
      inference: Object.assign({}, base.inference || {}, inferenceUpdate),
      server: Object.assign({}, base.server || {}, serverUpdate),
      agents: Object.assign({}, base.agents || {}, agentsUpdate),
      memory: Object.assign({}, base.memory || {}, memoryUpdate),
      database: Object.assign({}, base.database || {}, databaseUpdate),
      logging: Object.assign({}, base.logging || {}, { level: loggingLevel }),
      security: Object.assign({}, base.security || {}, securityUpdate),
      runtime: Object.assign({}, base.runtime || {}, {
        upload_max_body_bytes: serverUpdate.upload_max_body_bytes,
        auth_enabled: securityUpdate.auth_enabled,
        rate_limit_per_minute: securityUpdate.rate_limit_per_minute,
        require_approval_for_mutating: securityUpdate.require_approval_for_mutating,
        inference_model: inferenceUpdate.model,
        log_level: loggingLevel
      })
    });

    const formEl = ev.currentTarget || getEl('settings-form');
    const submitBtn = formEl ? formEl.querySelector('button[type="submit"]') : null;
    if (submitBtn) submitBtn.disabled = true;
    try {
      const updated = await apiFetch('/api/v1/settings', { method: 'PUT', body: payload });
      state.settingsRaw = updated || payload;
      applySettingsFormValues(state.settingsRaw);
      showToast('Configuration saved.', 'success');
    } catch (err) {
      showToast(`Save settings failed: ${err.message}`, 'danger');
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  }

  async function onCheckpointDb() {
    try {
      await apiFetch('/api/v1/system/checkpoint', { method: 'POST', body: {} });
      showToast('Database checkpoint completed.', 'success');
    } catch (err) {
      showToast(`Checkpoint failed: ${err.message}`, 'danger');
    }
  }

  function setPasswordFormError(message) {
    const form = getEl('change-password-form');
    let errEl = getEl('pw-error');
    if (!errEl && form) {
      errEl = el('div', { id: 'pw-error', className: 'alert alert-danger', role: 'alert', hidden: true });
      form.prepend(errEl);
    }
    if (!errEl) return;
    if (message) {
      errEl.textContent = String(message);
      errEl.hidden = false;
    } else {
      errEl.textContent = '';
      errEl.hidden = true;
    }
  }

  async function onChangePassword(ev) {
    ev.preventDefault();
    const form = getEl('change-password-form');
    const submitBtn = form ? form.querySelector('button[type="submit"]') : null;
    const currentPassword = $('pw-current').value;
    const newPassword = $('pw-new').value;
    const confirm = $('pw-confirm').value;

    setPasswordFormError('');
    if (!currentPassword) {
      const msg = 'Current password is required.';
      setPasswordFormError(msg);
      showToast(msg, 'warning');
      return;
    }
    if (!newPassword) {
      const msg = 'New password is required.';
      setPasswordFormError(msg);
      showToast(msg, 'warning');
      return;
    }
    if (newPassword !== confirm) {
      const msg = 'New password and confirmation do not match.';
      setPasswordFormError(msg);
      showToast(msg, 'warning');
      return;
    }

    if (submitBtn) submitBtn.disabled = true;
    try {
      await apiFetch('/api/v1/auth/password', {
        method: 'POST',
        body: { current_password: currentPassword, new_password: newPassword },
        skipAuthRedirect: true
      });
      if (form) form.reset();
      setPasswordFormError('');
      showToast('Password updated.', 'success');
    } catch (err) {
      const msg = `Password update failed: ${err.message}`;
      setPasswordFormError(msg);
      showToast(msg, 'danger');
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  }

  function confirmGracefulShutdown() {
    openModal('Confirm Graceful Shutdown', [
      el('div', { className: 'alert alert-warning' },
        'This will stop active inference loops, checkpoint the database, and terminate the PS5 agent server.'
      )
    ], [
      el('button', { type: 'button', className: 'btn btn-secondary', onClick: closeModal }, 'Cancel'),
      el('button', {
        type: 'button',
        className: 'btn btn-danger',
        onClick: async () => {
          try {
            await apiFetch('/api/v1/system/shutdown', { method: 'POST', body: {} });
            closeModal();
            showToast('Graceful shutdown initiated.', 'warning');
          } catch (err) {
            showToast(`Shutdown failed: ${err.message}`, 'danger');
          }
        }
      }, 'Shut Down Server')
    ]);
  }

  /* -------------------- Live SSE & Fallback Polling -------------------- */
  function setSsePill(status) {
    const pill = $('sse-status-pill');
    if (!pill) return;
    if (status === 'live') {
      pill.textContent = 'SSE Live';
      pill.className = 'pill pill-success';
    } else if (status === 'reconnecting') {
      pill.textContent = 'Polling Fallback';
      pill.className = 'pill pill-warning';
    } else {
      pill.textContent = 'Offline';
      pill.className = 'pill pill-danger';
    }
  }

  function connectSse() {
    closeSse();
    if (!state.authenticated) return;

    const es = new EventSource('/api/v1/events');
    state.sse = es;

    es.onopen = () => {
      const reconnecting = state.sseWasConnectedOnce || state.sseNeedsResync;
      state.sseConnected = true;
      state.sseWasConnectedOnce = true;
      state.sseRetryMs = 1500;
      setSsePill('live');
      if (reconnecting) {
        state.sseNeedsResync = false;
        loadApprovals().catch(() => {});
        loadAgents().catch(() => {});
        if (state.currentSection === 'chat' && state.activeConversationId) {
          loadChatMessages(state.activeConversationId).catch(() => {});
          rehydrateActiveChatTasks(state.activeConversationId).catch(() => {});
        } else {
          loadSection(state.currentSection).catch(() => {});
        }
      }
    };

    const dispatchRaw = (rawData) => {
      if (!rawData) return;
      try {
        const parsed = JSON.parse(rawData);
        handleStreamEvent(parsed);
      } catch (_) {
        // Ignore keepalive comments/non-JSON heartbeats
      }
    };

    es.onmessage = (ev) => dispatchRaw(ev.data);

    const namedEvents = [
      'chat_token', 'chat_reasoning', 'chat_completed',
      'tool_call_started', 'tool_call_finished', 'task_updated', 'task_failed',
      'agent_message_sent', 'approval_requested', 'approval_resolved',
      'agent_updated', 'system_alert', 'schedule_updated',
      'files_changed', 'ps5_lm_synced', 'ps5lm_synced', 'continuation_triggered',
      'sandbox_updated', 'webhook_triggered', 'browser_activity'
    ];
    for (const evtName of namedEvents) {
      es.addEventListener(evtName, (ev) => {
        try {
          const parsed = JSON.parse(ev.data);
          if (!parsed.type) parsed.type = evtName;
          handleStreamEvent(parsed);
        } catch (_) {}
      });
    }

    es.onerror = () => {
      state.sseConnected = false;
      state.sseNeedsResync = true;
      setSsePill('reconnecting');
      es.close();
      state.sse = null;
      if (state.sseReconnectTimer) {
        window.clearTimeout(state.sseReconnectTimer);
        state.sseReconnectTimer = null;
      }
      if (state.authenticated) {
        const delay = state.sseRetryMs;
        state.sseRetryMs = Math.min(15000, state.sseRetryMs * 1.6);
        state.sseReconnectTimer = window.setTimeout(connectSse, delay);
      }
    };
  }

  function closeSse() {
    if (state.sseReconnectTimer) {
      window.clearTimeout(state.sseReconnectTimer);
      state.sseReconnectTimer = null;
    }
    if (state.chatStreamTimerId !== null) {
      window.clearTimeout(state.chatStreamTimerId);
      state.chatStreamTimerId = null;
    }
    if (state.chatStreamRafId !== null) {
      window.cancelAnimationFrame(state.chatStreamRafId);
      state.chatStreamRafId = null;
    }
    if (state.sse) {
      state.sse.close();
      state.sse = null;
    }
    state.sseConnected = false;
  }

  function recordCompletedChatTaskId(taskId) {
    const tid = taskId ? String(taskId).trim() : '';
    if (!tid || !state.completedChatTaskIds) return;
    if (state.completedChatTaskIds.has(tid)) {
      state.completedChatTaskIds.delete(tid);
    } else if (state.completedChatTaskIds.size >= 512) {
      const oldest = state.completedChatTaskIds.values().next().value;
      if (oldest !== undefined) state.completedChatTaskIds.delete(oldest);
    }
    state.completedChatTaskIds.add(tid);
  }

  function isChatTaskTerminal(taskId) {
    const tid = taskId ? String(taskId).trim() : '';
    return Boolean(tid && state.completedChatTaskIds && state.completedChatTaskIds.has(tid));
  }

  function markChatTaskTerminal(taskId) {
    const tid = taskId ? String(taskId).trim() : '';
    if (!tid) return;
    recordCompletedChatTaskId(tid);
    state.activeChatTasks.delete(tid);
    if (String(taskId) !== tid) state.activeChatTasks.delete(String(taskId));
    removeStreamBucket(tid);
  }

  function ensureStreamBucket(ev) {
    const rawTaskId = ev && ev.task_id ? String(ev.task_id).trim() : '';
    const convId = (ev && ev.conversation_id) || state.activeConversationId || 'default';
    const agentId = (ev && ev.agent_id) || '';
    if (rawTaskId && isChatTaskTerminal(rawTaskId)) {
      return {
        taskId: rawTaskId,
        conversationId: (ev && ev.conversation_id) || state.activeConversationId,
        agentId,
        text: '',
        reasoning: '',
        toolCalls: [],
        bubbleEl: null,
        dirty: false,
        ephemeral: true,
        createdAt: new Date().toISOString()
      };
    }
    const taskId = rawTaskId || `${convId}:${agentId || 'assistant'}`;
    let bucket = state.chatStreams.get(taskId);
    if (!bucket) {
      pruneChatStreamBuckets();
      bucket = {
        taskId,
        conversationId: (ev && ev.conversation_id) || state.activeConversationId,
        agentId,
        text: '',
        reasoning: '',
        toolCalls: [],
        bubbleEl: null,
        dirty: false,
        completed: false,
        updatedAtMs: Date.now(),
        createdAt: new Date().toISOString()
      };
      state.chatStreams.set(taskId, bucket);
      updateActiveStreamsBadge();
    } else {
      bucket.updatedAtMs = Date.now();
      if (ev && ev.conversation_id && (!bucket.conversationId || bucket.conversationId !== ev.conversation_id)) {
        bucket.conversationId = ev.conversation_id;
      }
      if (agentId && !bucket.agentId) {
        bucket.agentId = agentId;
      }
    }
    return bucket;
  }

  function removeStreamBucket(taskId) {
    const tid = taskId ? String(taskId).trim() : '';
    if (!tid) return;
    const bucket = state.chatStreams.get(tid) || state.chatStreams.get(String(taskId));
    if (bucket) {
      if (bucket.bubbleEl && bucket.bubbleEl.isConnected) {
        bucket.bubbleEl.remove();
      }
      state.chatStreams.delete(tid);
      if (String(taskId) !== tid) state.chatStreams.delete(String(taskId));
      updateActiveStreamsBadge();
    }
  }

  function sweepOrphanClosingThinkTag(bucket) {
    if (!bucket || bucket.orphanThinkSwept || typeof bucket.text !== 'string' || !bucket.text) return;
    const lower = bucket.text.toLowerCase();
    const closeTags = ['</think>', '</seed:think>'];
    let bestCloseIdx = -1;
    let bestCloseLen = 0;
    for (const tag of closeTags) {
      const idx = lower.indexOf(tag);
      if (idx !== -1 && (bestCloseIdx === -1 || idx < bestCloseIdx)) {
        bestCloseIdx = idx;
        bestCloseLen = tag.length;
      }
    }
    if (bestCloseIdx === -1) return;
    const beforeClose = lower.slice(0, bestCloseIdx);
    if (beforeClose.includes('<think>') || beforeClose.includes('<seed:think>')) {
      bucket.orphanThinkSwept = true;
      return;
    }
    const thoughtChunk = bucket.text.slice(0, bestCloseIdx).trim();
    const visibleRemainder = bucket.text.slice(bestCloseIdx + bestCloseLen).replace(/^\s*\n+/, '');
    if (thoughtChunk) {
      bucket.reasoning = bucket.reasoning ? `${bucket.reasoning}\n${thoughtChunk}` : thoughtChunk;
    }
    bucket.text = visibleRemainder;
    bucket.orphanThinkSwept = true;
  }

  function handleStreamEvent(ev) {
    if (!ev || !ev.type) return;
    switch (ev.type) {
      case 'chat_token': {
        const b = ensureStreamBucket(ev);
        b.text += ev.delta || '';
        sweepOrphanClosingThinkTag(b);
        b.dirty = true;
        scheduleStreamFlush();
        break;
      }
      case 'chat_reasoning': {
        const b = ensureStreamBucket(ev);
        b.reasoning += ev.delta || '';
        b.dirty = true;
        scheduleStreamFlush();
        break;
      }
      case 'browser_activity': {
        handleBrowserActivityEvent(ev);
        break;
      }
      case 'chat_completed': {
        const tid = ev.task_id ? String(ev.task_id).trim() : '';
        const bucket = tid ? (state.chatStreams.get(tid) || state.chatStreams.get(String(ev.task_id))) : null;
        if (bucket) {
          bucket.completed = true;
          bucket.updatedAtMs = Date.now();
        }
        if (ev.timings) {
          cacheMessageTimings(ev.message_id, tid, ev.conversation_id, ev.timings);
        }
        const cachedReasoning = bucket && bucket.reasoning ? bucket.reasoning : '';
        const finalReasoning = (typeof ev.reasoning === 'string' && ev.reasoning) ? ev.reasoning : cachedReasoning;
        if (tid) {
          markChatTaskTerminal(tid);
          if (state.sandboxAgenticTaskIds && state.sandboxAgenticTaskIds.has(tid)) {
            const sbSt = getEl('sandbox-live-browser-status');
            if (sbSt) {
              sbSt.className = 'pill pill-success';
              sbSt.textContent = 'Completed';
            }
            if (state.currentSection === 'sandbox') {
              loadBrowserSessions({ silent: true, keepStatus: true }).catch(() => {});
            }
          }
        }
        restoreChatLiveStatusFromSearching(ev.conversation_id, tid);
        renderChatTaskBar();
        if (state.currentSection === 'chat' && ev.conversation_id === state.activeConversationId) {
          loadChatMessages(ev.conversation_id, {
            messageId: ev.message_id,
            fallbackReasoning: finalReasoning
          }).catch(() => {});
        }
        break;
      }
      case 'tool_call_started': {
        const b = ensureStreamBucket(ev);
        b.toolCalls.push({
          tool_id: ev.tool_id,
          arguments_json: ev.arguments_json,
          running: true,
          success: true
        });
        b.dirty = true;
        scheduleStreamFlush();
        break;
      }
      case 'tool_call_finished': {
        const b = ensureStreamBucket(ev);
        const item = [...b.toolCalls].reverse().find((t) => t.tool_id === ev.tool_id && t.running);
        if (item) {
          item.running = false;
          item.success = Boolean(ev.success);
          item.duration_ms = ev.duration_ms;
        } else {
          b.toolCalls.push({
            tool_id: ev.tool_id,
            running: false,
            success: Boolean(ev.success),
            duration_ms: ev.duration_ms
          });
        }
        b.dirty = true;
        scheduleStreamFlush();
        if (state.currentSection === 'tools') {
          loadToolExecutions().catch(() => {});
        }
        break;
      }
      case 'task_updated': {
        const tid = ev.task_id ? String(ev.task_id).trim() : '';
        const st = String(ev.status || '').toUpperCase();
        const existingTask = tid ? state.activeChatTasks.get(tid) : null;
        const streamBucket = tid ? state.chatStreams.get(tid) : null;
        const convId = ev.conversation_id
          || (existingTask && existingTask.conversation_id)
          || (streamBucket && streamBucket.conversationId)
          || '';
        if (['COMPLETED', 'FAILED', 'CANCELLED', 'BLOCKED_LOOP'].includes(st)) {
          if (st === 'FAILED' || st === 'BLOCKED_LOOP') {
            const detail = ev.error || ev.message || ev.error_info || ev.progress_note || '';
            if (detail) {
              showChatErrorBanner(convId || state.activeConversationId, 'Task Failed', detail);
              showDeduplicatedErrorToast(`Task failed: ${detail}`);
            }
          }
          if (tid) {
            markChatTaskTerminal(tid);
            if (state.sandboxAgenticTaskIds && state.sandboxAgenticTaskIds.has(tid)) {
              const sbSt = getEl('sandbox-live-browser-status');
              if (sbSt) {
                const isOk = st === 'COMPLETED';
                sbSt.className = isOk ? 'pill pill-success' : 'pill pill-danger';
                sbSt.textContent = isOk ? 'Completed' : (st === 'CANCELLED' ? 'Cancelled' : 'Failed');
              }
              if (state.currentSection === 'sandbox') {
                loadBrowserSessions({ silent: true, keepStatus: true }).catch(() => {});
              }
            }
          }
          restoreChatLiveStatusFromSearching(convId, tid);
          renderChatTaskBar();
          if (st === 'COMPLETED' && streamBucket && state.currentSection === 'chat' && convId === state.activeConversationId) {
            loadChatMessages(convId).catch(() => {});
          }
        } else if (tid && convId && !isChatTaskTerminal(tid)) {
          updateChatTaskBar(Object.assign({}, ev, { conversation_id: convId }));
        }
        if (state.currentSection === 'tasks') {
          loadTasks().catch(() => {});
        }
        break;
      }
      case 'task_failed': {
        const tid = ev.task_id ? String(ev.task_id).trim() : '';
        const existingTask = tid ? state.activeChatTasks.get(tid) : null;
        const streamBucket = tid ? state.chatStreams.get(tid) : null;
        const convId = ev.conversation_id
          || (existingTask && existingTask.conversation_id)
          || (streamBucket && streamBucket.conversationId)
          || '';
        const detail = ev.error || ev.message || ev.error_info || ev.progress_note || 'Background task failed.';
        clearChatPendingForConversation(convId, tid);
        if (tid) {
          markChatTaskTerminal(tid);
          renderChatTaskBar();
          if (state.sandboxAgenticTaskIds && state.sandboxAgenticTaskIds.has(tid)) {
            const sbSt = getEl('sandbox-live-browser-status');
            if (sbSt) {
              sbSt.className = 'pill pill-danger';
              sbSt.textContent = 'Failed';
            }
          }
        }
        restoreChatLiveStatusFromSearching(convId, tid);
        showChatErrorBanner(convId || state.activeConversationId, 'Task Failed', detail);
        showDeduplicatedErrorToast(`Task failed: ${detail}`);
        if (state.currentSection === 'tasks') {
          loadTasks().catch(() => {});
        }
        break;
      }
      case 'agent_message_sent': {
        if (state.currentSection === 'a2a') {
          loadA2aMessages().catch(() => {});
        }
        break;
      }
      case 'approval_requested': {
        showToast(`Approval needed: ${ev.operation} (${agentDisplayName(ev.agent_id)})`, 'warning');
        loadApprovals().catch(() => {});
        break;
      }
      case 'approval_resolved': {
        loadApprovals().catch(() => {});
        break;
      }
      case 'agent_updated': {
        loadAgents().catch(() => {});
        break;
      }
      case 'files_changed': {
        const changedPath = sanitizeWorkspacePath(ev.path || '');
        if (state.currentSection === 'files') {
          loadFilesSection().catch(() => {});
          if (changedPath && state.selectedFilePath && changedPath === state.selectedFilePath && !state.fileEditMode) {
            previewWorkspaceFile(state.selectedFilePath, { autoRunHtml: state.filesHtmlPreviewActive }).catch(() => {});
          }
        }
        const brainName = changedPath ? pathBasename(changedPath) : '';
        if (
          state.currentSection === 'sandbox'
          || (brainName && state.brainFiles.some((f) => f.name === brainName))
        ) {
          loadBrainFiles({ listOnly: state.brainDirty, silent: true }).catch(() => {});
        }
        break;
      }
      case 'sandbox_updated': {
        loadSandboxState({ silent: true }).catch(() => {});
        if (ev.mode) {
          showToast(`Sandbox policy updated (${ev.mode}).`, 'info');
        }
        break;
      }
      case 'webhook_triggered': {
        if (state.currentSection === 'sandbox') {
          loadWebhooks({ silent: true }).catch(() => {});
        }
        if (ev.conversation_id && ev.conversation_id === state.activeConversationId && state.currentSection === 'chat') {
          rehydrateActiveChatTasks(ev.conversation_id).catch(() => {});
        }
        showToast(`Webhook triggered: ${ev.name || ev.webhook_id || 'webhook'}`, 'info');
        break;
      }
      case 'ps5_lm_synced':
      case 'ps5lm_synced': {
        if (ev.loaded_model) {
          setText('ov-loaded-model', ev.loaded_model);
          setText('ps5lm-cap-model', ev.loaded_model);
        }
        if (ev.context_length !== undefined && ev.context_length !== null) {
          setText('ps5lm-cap-ctx', String(ev.context_length));
        }
        if (ev.total_slots !== undefined && ev.total_slots !== null) {
          setText('ps5lm-cap-slots', String(ev.total_slots));
        }
        setText('ps5lm-cap-synced', fmtDateTime(new Date().toISOString()));
        loadInferenceCapabilities().catch(() => {});
        if (state.currentSection === 'settings') {
          loadSettings({ skipCapabilities: true }).catch(() => {});
        }
        showToast(
          ev.loaded_model
            ? `PS5LM synced: ${ev.loaded_model}`
            : 'PS5LM capabilities updated.',
          'info'
        );
        break;
      }
      case 'continuation_triggered': {
        if (ev.task_id) {
          const b = ensureStreamBucket(ev);
          b.dirty = true;
          scheduleStreamFlush();
          const tracked = state.activeChatTasks.get(String(ev.task_id));
          if (tracked) {
            tracked.progress_note = `Auto-continuing: ${ev.reason || 'more steps'}`;
            renderChatTaskBar();
          }
        }
        break;
      }
      case 'schedule_updated': {
        const sched = ev.schedule || (ev.id ? ev : null);
        if (sched) {
          upsertScheduleRecord(sched);
        } else {
          refreshSchedulesCount();
        }
        break;
      }
      case 'system_alert': {
        const lvl = String(ev.level || '').toLowerCase();
        const rawMsg = String(ev.message || 'System alert');
        if (ev.resync || ev.resync_required) {
          state.sseNeedsResync = false;
          loadApprovals().catch(() => {});
          loadAgents().catch(() => {});
          if (state.currentSection === 'chat' && state.activeConversationId) {
            loadChatMessages(state.activeConversationId).catch(() => {});
            rehydrateActiveChatTasks(state.activeConversationId).catch(() => {});
          } else {
            loadSection(state.currentSection).catch(() => {});
          }
          showToast(rawMsg || 'Live stream lagged; resynchronized state.', 'warning');
          break;
        }
        const chatErrMatch = rawMsg.match(/Chat task failed\s*\(([^)]+)\)\s*:\s*([\s\S]*)/i);
        const alertConvId = ev.conversation_id
          || (chatErrMatch && chatErrMatch[1] ? chatErrMatch[1].trim() : '');
        const alertDetail = (chatErrMatch && chatErrMatch[2] ? chatErrMatch[2].trim() : '') || rawMsg;
        if (lvl === 'error' || Boolean(chatErrMatch)) {
          if (alertConvId) {
            clearChatPendingForConversation(alertConvId, ev.task_id || null);
          } else if (state.currentSection === 'chat' && state.activeConversationId) {
            clearChatPendingForConversation(state.activeConversationId, ev.task_id || null);
          }
          showChatErrorBanner(alertConvId || state.activeConversationId, 'Inference / System Error', alertDetail);
          showDeduplicatedErrorToast(rawMsg);
        } else {
          const tone = (lvl === 'warn' || lvl === 'warning') ? 'warning' : 'info';
          showToast(rawMsg, tone);
        }
        break;
      }
    }
  }

  function startPolling() {
    stopPolling();
    state.fallbackPollTimer = window.setInterval(() => {
      if (!state.authenticated || state.sseConnected) return;
      loadApprovals().catch(() => {});
      loadSection(state.currentSection).catch(() => {});
    }, 5000);

    state.metricsPollTimer = window.setInterval(() => {
      if (!state.authenticated) return;
      if (state.currentSection === 'overview') {
        loadMetrics().catch(() => {});
      }
    }, 10000);
  }

  function stopPolling() {
    if (state.fallbackPollTimer) {
      window.clearInterval(state.fallbackPollTimer);
      state.fallbackPollTimer = null;
    }
    if (state.metricsPollTimer) {
      window.clearInterval(state.metricsPollTimer);
      state.metricsPollTimer = null;
    }
  }

  /* -------------------- Initialization & Event Wiring -------------------- */
  async function bootstrapAuthenticatedSession() {
    connectSse();
    startPolling();
    await Promise.allSettled([
      loadAgents(),
      loadTools(),
      loadApprovals(),
      refreshSchedulesCount(),
      runDoctorChecks({ silent: true })
    ]);
    switchSection(state.currentSection || 'overview');
  }

  async function checkSessionOnBoot() {
    try {
      const session = await apiFetch('/api/v1/auth/session', { skipAuthRedirect: true });
      if (session && (session.csrf_token || session.csrfToken)) {
        state.csrfToken = session.csrf_token || session.csrfToken;
        sessionStorage.setItem(CSRF_KEY, state.csrfToken);
      }
      if (session && session.username) {
        state.username = session.username;
        sessionStorage.setItem(USER_KEY, state.username);
      }
      state.authenticated = true;
      showAppShell();
      await bootstrapAuthenticatedSession();
    } catch (err) {
      if (err && err.status === 404) {
        try {
          await apiFetch('/api/v1/metrics', { skipAuthRedirect: true });
          state.authenticated = true;
          showAppShell();
          await bootstrapAuthenticatedSession();
          return;
        } catch (_) {
          // Fall through to login overlay
        }
      }
      handleUnauthorized('');
    }
  }

  function wireEvents() {
    $('login-form').addEventListener('submit', onLoginSubmit);
    $('login-toggle-pw').addEventListener('click', () => {
      const pw = $('login-password');
      const btn = $('login-toggle-pw');
      const showing = pw.type === 'text';
      pw.type = showing ? 'password' : 'text';
      btn.textContent = showing ? 'Show' : 'Hide';
      btn.setAttribute('aria-pressed', String(!showing));
    });

    $('logout-btn').addEventListener('click', onLogout);
    $('mobile-menu-btn').addEventListener('click', () => setMobileDrawer(true));
    $('sidebar-close-btn').addEventListener('click', () => setMobileDrawer(false));
    $('sidebar-backdrop').addEventListener('click', () => setMobileDrawer(false));
    const sidebarCollapseBtn = getEl('sidebar-collapse-btn');
    const sidebarExpandBtn = getEl('sidebar-expand-btn');
    if (sidebarCollapseBtn) sidebarCollapseBtn.addEventListener('click', () => setSidebarCollapsed(true));
    if (sidebarExpandBtn) sidebarExpandBtn.addEventListener('click', () => setSidebarCollapsed(false));
    setSidebarCollapsed(state.sidebarCollapsed);
    $('refresh-btn').addEventListener('click', () => loadSection(state.currentSection));
    $('topbar-approvals-btn').addEventListener('click', () => switchSection('overview'));

    document.querySelectorAll('.nav-item[data-section]').forEach((btn) => {
      btn.addEventListener('click', () => switchSection(btn.dataset.section));
    });

    document.querySelectorAll('[data-quick]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const action = btn.dataset.quick;
        if (action === 'create-agent') {
          switchSection('agents');
          await loadTools().catch(() => {});
          openAgentModal(null);
        } else if (action === 'checkpoint') {
          await onCheckpointDb();
        } else {
          switchSection(action);
        }
      });
    });

    // Chat
    $('new-direct-chat-btn').addEventListener('click', openNewDirectChatModal);
    $('new-group-chat-btn').addEventListener('click', openNewGroupChatModal);
    const rawOutputToggle = getEl('chat-raw-output-toggle');
    if (rawOutputToggle) {
      rawOutputToggle.checked = state.showRawOutput;
      rawOutputToggle.addEventListener('change', () => setRawOutputMode(rawOutputToggle.checked));
    }
    const chatForkBtn = getEl('chat-fork-conv-btn');
    if (chatForkBtn) {
      chatForkBtn.addEventListener('click', () => forkConversation(null));
    }
    $('chat-sidebar-toggle').addEventListener('click', () => {
      const sb = $('chat-sidebar');
      const open = sb.classList.toggle('is-open');
      $('chat-sidebar-toggle').setAttribute('aria-expanded', String(open));
    });
    let searchDebounce = null;
    $('chat-search-input').addEventListener('input', () => {
      window.clearTimeout(searchDebounce);
      searchDebounce = window.setTimeout(() => loadConversations(), 240);
    });
    $('chat-composer-form').addEventListener('submit', onChatSend);
    const chatInput = $('chat-input');
    chatInput.addEventListener('input', () => {
      updateSlashAgentMenu();
      updateSlashCommandPalette();
    });
    chatInput.addEventListener('keydown', (e) => {
      if (handleSlashMenuKeydown(e)) return;
      if (handleSlashPaletteKeydown(e)) return;
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        $('chat-composer-form').requestSubmit();
      }
    });
    const chatSlashBtn = getEl('chat-slash-btn');
    if (chatSlashBtn) {
      chatSlashBtn.addEventListener('click', () => {
        if (state.slashPaletteOpen) {
          hideSlashCommandPalette();
        } else {
          openSlashCommandPalette({ showAll: true });
          chatInput.focus();
        }
      });
    }
    const ephemeralCloseBtn = getEl('chat-ephemeral-close-btn');
    if (ephemeralCloseBtn) {
      ephemeralCloseBtn.addEventListener('click', hideEphemeralSideCard);
    }
    chatInput.addEventListener('paste', (e) => {
      const cb = e.clipboardData;
      if (!cb) return;
      const files = [];
      if (cb.files && cb.files.length > 0) {
        for (const f of cb.files) files.push(f);
      } else if (cb.items && cb.items.length > 0) {
        for (const item of cb.items) {
          if (item.kind === 'file') {
            const f = item.getAsFile();
            if (f) files.push(f);
          }
        }
      }
      if (files.length > 0) {
        const hasText = cb.getData && Boolean(cb.getData('text/plain'));
        if (!hasText) e.preventDefault();
        handleChatFiles(files);
      }
    });

    const btnChatAttach = getEl('btn-chat-attach');
    const chatFileInput = getEl('chat-file-input');
    if (btnChatAttach && chatFileInput) {
      btnChatAttach.addEventListener('click', () => {
        if (!chatInput.disabled) chatFileInput.click();
      });
      chatFileInput.addEventListener('change', () => {
        if (chatFileInput.files && chatFileInput.files.length > 0) {
          handleChatFiles(chatFileInput.files);
          chatFileInput.value = '';
        }
      });
    }

    const composerWrap = getEl('chat-composer-wrap');
    const dropHint = getEl('chat-drop-hint');
    if (composerWrap) {
      composerWrap.addEventListener('dragenter', (e) => {
        e.preventDefault();
        if (chatInput.disabled) return;
        composerWrap.classList.add('chat-drop-active');
        if (dropHint) dropHint.hidden = false;
      });
      composerWrap.addEventListener('dragover', (e) => {
        e.preventDefault();
        if (chatInput.disabled) return;
        composerWrap.classList.add('chat-drop-active');
        if (dropHint) dropHint.hidden = false;
      });
      composerWrap.addEventListener('dragleave', (e) => {
        if (!composerWrap.contains(e.relatedTarget)) {
          composerWrap.classList.remove('chat-drop-active');
          if (dropHint) dropHint.hidden = true;
        }
      });
      composerWrap.addEventListener('drop', (e) => {
        e.preventDefault();
        composerWrap.classList.remove('chat-drop-active');
        if (dropHint) dropHint.hidden = true;
        if (!chatInput.disabled && e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0) {
          handleChatFiles(e.dataTransfer.files);
        }
      });
    }

    $('chat-cancel-task-btn').addEventListener('click', onCancelActiveChatTask);
    const chatSendImmediateBtn = getEl('chat-send-immediate-btn');
    if (chatSendImmediateBtn) {
      chatSendImmediateBtn.addEventListener('click', () => sendMessage({ immediate: true }));
    }

    // Agents
    $('create-agent-btn').addEventListener('click', async () => {
      if (state.tools.length === 0) await loadTools().catch(() => {});
      openAgentModal(null);
    });
    const enableAllAgentsBtn = getEl('enable-all-agents-btn');
    if (enableAllAgentsBtn) enableAllAgentsBtn.addEventListener('click', enableAllAgents);

    // A2A
    $('a2a-apply-btn').addEventListener('click', () => {
      loadA2aMessages().catch((err) => showToast(`Load A2A messages failed: ${err.message}`, 'danger'));
    });
    $('a2a-filter-type').addEventListener('change', renderA2aList);

    // Tasks
    $('tasks-filter-pills').addEventListener('click', (e) => {
      const btn = e.target.closest('[data-task-filter]');
      if (!btn) return;
      state.taskFilter = btn.dataset.taskFilter;
      document.querySelectorAll('[data-task-filter]').forEach((b) => {
        b.classList.toggle('is-active', b === btn);
      });
      renderTasks();
    });

    // Schedules & Autonomous Tasks
    const btnRefreshSchedules = getEl('btn-refresh-schedules');
    const btnNewSchedule = getEl('btn-new-schedule');
    const btnScheduleAiParse = getEl('btn-schedule-ai-parse');
    const scheduleAiPrompt = getEl('schedule-ai-prompt');
    if (btnRefreshSchedules) {
      btnRefreshSchedules.addEventListener('click', () => {
        loadSchedules().catch((err) => showToast(`Refresh schedules failed: ${err.message}`, 'danger'));
      });
    }
    if (btnNewSchedule) {
      btnNewSchedule.addEventListener('click', () => openScheduleModal(null));
    }
    if (btnScheduleAiParse) {
      btnScheduleAiParse.addEventListener('click', () => parseScheduleAi());
    }
    if (scheduleAiPrompt) {
      scheduleAiPrompt.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          parseScheduleAi();
        }
      });
    }
    document.querySelectorAll('.schedule-example-chip').forEach((chip) => {
      chip.addEventListener('click', () => {
        const promptInput = getEl('schedule-ai-prompt');
        if (promptInput) {
          promptInput.value = chip.dataset.prompt || chip.textContent.trim();
          promptInput.focus();
        }
        parseScheduleAi();
      });
    });
    const scheduleFilterPills = document.querySelector('.schedule-filter-pills');
    if (scheduleFilterPills) {
      scheduleFilterPills.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-sched-filter]');
        if (!btn) return;
        state.scheduleFilter = btn.dataset.schedFilter || 'all';
        renderSchedules();
      });
    }

    // Memory
    $('memory-apply-btn').addEventListener('click', () => {
      loadMemories().catch((err) => showToast(`Load memories failed: ${err.message}`, 'danger'));
    });
    $('add-memory-btn').addEventListener('click', openAddMemoryModal);
    const importMemoriesBtn = getEl('import-memories-btn');
    const exportMemoriesBtn = getEl('export-memories-btn');
    if (importMemoriesBtn) importMemoriesBtn.addEventListener('click', openImportMemoriesModal);
    if (exportMemoriesBtn) exportMemoriesBtn.addEventListener('click', exportMemories);

    // Tools
    const addCustomToolBtn = getEl('add-custom-tool-btn');
    const importToolsBtn = getEl('import-tools-btn');
    if (addCustomToolBtn) addCustomToolBtn.addEventListener('click', openAddCustomToolModal);
    if (importToolsBtn) importToolsBtn.addEventListener('click', openImportToolsModal);
    $('tool-tester-select').addEventListener('change', () => {
      const found = state.tools.find((t) => t.id === $('tool-tester-select').value);
      if (found) selectToolForTesting(found, true);
    });
    $('tool-tester-form').addEventListener('submit', onExecuteToolTest);

    // Workspace Files
    const btnFilesRefresh = getEl('btn-files-refresh');
    const btnFilesMkdir = getEl('btn-files-mkdir');
    const btnFilesUpload = getEl('btn-files-upload');
    const filesUploadInput = getEl('files-upload-input');
    if (btnFilesRefresh) btnFilesRefresh.addEventListener('click', () => loadFilesSection(state.filesCurrentPath));
    if (btnFilesMkdir) btnFilesMkdir.addEventListener('click', openMkdirModal);
    if (btnFilesUpload && filesUploadInput) {
      btnFilesUpload.addEventListener('click', () => filesUploadInput.click());
      filesUploadInput.addEventListener('change', () => {
        if (filesUploadInput.files && filesUploadInput.files.length > 0) {
          handleWorkspaceUploadFiles(filesUploadInput.files);
          filesUploadInput.value = '';
        }
      });
    }
    const btnFileViewScript = getEl('btn-file-view-script');
    const btnFileEdit = getEl('btn-file-edit');
    const btnFileSave = getEl('btn-file-save');
    const btnFileCancelEdit = getEl('btn-file-cancel-edit');
    if (btnFileViewScript) btnFileViewScript.addEventListener('click', viewSelectedFileAsScript);
    if (btnFileEdit) btnFileEdit.addEventListener('click', enterFileEditMode);
    if (btnFileSave) btnFileSave.addEventListener('click', saveEditedWorkspaceFile);
    if (btnFileCancelEdit) btnFileCancelEdit.addEventListener('click', cancelFileEditMode);

    // Extensions
    const extTabSkills = getEl('ext-tab-skills');
    const extTabMcp = getEl('ext-tab-mcp');
    const extTabPlugins = getEl('ext-tab-plugins');
    if (extTabSkills) extTabSkills.addEventListener('click', () => setExtensionsTab('skills'));
    if (extTabMcp) extTabMcp.addEventListener('click', () => setExtensionsTab('mcp'));
    if (extTabPlugins) extTabPlugins.addEventListener('click', () => setExtensionsTab('plugins'));
    const btnImportSkill = getEl('btn-import-skill');
    const btnAddMcp = getEl('btn-add-mcp');
    const btnImportPlugin = getEl('btn-import-plugin');
    const btnRefreshExt = getEl('btn-refresh-extensions');
    if (btnImportSkill) btnImportSkill.addEventListener('click', openImportSkillModal);
    if (btnAddMcp) btnAddMcp.addEventListener('click', openAddMcpModal);
    if (btnImportPlugin) btnImportPlugin.addEventListener('click', openImportPluginModal);
    if (btnRefreshExt) btnRefreshExt.addEventListener('click', () => loadExtensionsSection());

    // Sandbox & Doctor
    const sandboxPolicyForm = getEl('sandbox-policy-form');
    if (sandboxPolicyForm) sandboxPolicyForm.addEventListener('submit', onSaveSandboxPolicy);
    const btnRunDoctor = getEl('btn-run-doctor');
    const btnFixDoctor = getEl('btn-fix-doctor');
    if (btnRunDoctor) btnRunDoctor.addEventListener('click', () => runDoctorChecks({ fix: false }));
    if (btnFixDoctor) btnFixDoctor.addEventListener('click', fixDoctorIssues);

    // Sandboxed Browser Console
    const browserSessionSel = getEl('browser-session-select');
    if (browserSessionSel) {
      browserSessionSel.addEventListener('change', () => {
        state.activeBrowserSessionId = browserSessionSel.value;
        if (state.activeBrowserSessionId) {
          if (state.liveBrowserTabs && state.liveBrowserTabs.has(state.activeBrowserSessionId)) {
            selectLiveBrowserTab(state.activeBrowserSessionId);
          }
          runBrowserAction('snapshot', { sessionId: state.activeBrowserSessionId, silentEmpty: true });
        }
      });
    }
    const btnBrowserRefresh = getEl('btn-browser-refresh-sessions');
    const btnBrowserClose = getEl('btn-browser-close-session');
    const btnBrowserOpen = getEl('btn-browser-open');
    const browserUrlIn = getEl('browser-url-input');
    const btnBrowserSnapshot = getEl('btn-browser-snapshot');
    const btnBrowserBack = getEl('btn-browser-back');
    const btnBrowserLinks = getEl('btn-browser-links');
    const btnBrowserFill = getEl('btn-browser-fill');
    const btnBrowserClick = getEl('btn-browser-click');
    const btnBrowserSubmit = getEl('btn-browser-submit');
    if (btnBrowserRefresh) btnBrowserRefresh.addEventListener('click', () => loadBrowserSessions());
    if (btnBrowserClose) btnBrowserClose.addEventListener('click', closeActiveBrowserSession);
    if (btnBrowserOpen) btnBrowserOpen.addEventListener('click', () => runBrowserAction('open'));
    if (browserUrlIn) {
      browserUrlIn.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          runBrowserAction('open');
        }
      });
    }
    if (btnBrowserSnapshot) btnBrowserSnapshot.addEventListener('click', () => runBrowserAction('snapshot'));
    if (btnBrowserBack) btnBrowserBack.addEventListener('click', () => runBrowserAction('back'));
    if (btnBrowserLinks) btnBrowserLinks.addEventListener('click', () => runBrowserAction('links'));
    if (btnBrowserFill) btnBrowserFill.addEventListener('click', () => runBrowserAction('fill'));
    if (btnBrowserClick) btnBrowserClick.addEventListener('click', () => runBrowserAction('click'));
    if (btnBrowserSubmit) btnBrowserSubmit.addEventListener('click', () => runBrowserAction('submit'));

    // Brain Files Editor
    const brainTextarea = getEl('brain-editor-textarea');
    const btnBrainReload = getEl('btn-brain-reload');
    const btnBrainSave = getEl('btn-brain-save');
    if (brainTextarea) {
      brainTextarea.addEventListener('input', () => {
        state.brainDirty = true;
        setText('brain-save-status', 'Unsaved changes');
      });
    }
    if (btnBrainReload) {
      btnBrainReload.addEventListener('click', () => {
        if (state.activeBrainFile) {
          selectBrainFile(state.activeBrainFile, { force: true });
        } else {
          loadBrainFiles();
        }
      });
    }
    if (btnBrainSave) btnBrainSave.addEventListener('click', saveActiveBrainFile);

    // Inbound Webhooks
    const webhookForm = getEl('webhook-create-form');
    const webhookBannerDismiss = getEl('btn-webhook-banner-dismiss');
    if (webhookForm) webhookForm.addEventListener('submit', onCreateWebhook);
    if (webhookBannerDismiss) webhookBannerDismiss.addEventListener('click', hideWebhookCreatedBanner);

    // Logs
    document.querySelectorAll('[data-log-tab]').forEach((btn) => {
      btn.addEventListener('click', () => {
        state.logsTab = btn.dataset.logTab;
        document.querySelectorAll('[data-log-tab]').forEach((b) => b.classList.toggle('is-active', b === btn));
        const isAudit = state.logsTab === 'audit';
        $('logs-container').hidden = isAudit;
        $('audit-logs-container').hidden = !isAudit;
        $('logs-level-wrap').hidden = isAudit;
        loadSection('logs');
      });
    });
    $('logs-level-filter').addEventListener('change', () => {
      loadRuntimeLogs().catch((err) => showToast(`Load logs failed: ${err.message}`, 'danger'));
    });
    $('logs-search-input').addEventListener('input', () => {
      if (state.logsTab === 'audit') renderAuditLogs();
      else renderRuntimeLogs();
    });

    // Settings & PS5LM Sync
    const btnSyncPs5Lm = getEl('btn-sync-ps5lm');
    if (btnSyncPs5Lm) btnSyncPs5Lm.addEventListener('click', onSyncPs5Lm);
    const btnImportKv = getEl('btn-import-llamacpp-kv');
    if (btnImportKv) btnImportKv.addEventListener('click', onImportLlamaCppKv);
    $('settings-form').addEventListener('submit', onSaveSettings);

    // Agentic Search & Live Browser Viewport controls
    const btnAgenticToggle = getEl('btn-agentic-search-toggle');
    if (btnAgenticToggle) {
      btnAgenticToggle.addEventListener('click', () => {
        const next = !state.agenticSearchEnabled;
        setAgenticSearchEnabled(next);
        if (next && state.liveBrowserTabs && state.liveBrowserTabs.size > 0) {
          openChatLiveBrowserDock();
        }
      });
    }
    const btnCloseLiveDock = getEl('btn-chat-live-browser-close');
    if (btnCloseLiveDock) btnCloseLiveDock.addEventListener('click', hideChatLiveBrowserDock);
    const btnLiveDockToggle = getEl('btn-chat-live-browser-toggle');
    if (btnLiveDockToggle) {
      btnLiveDockToggle.addEventListener('click', () => toggleChatLiveBrowserDock());
    }
    syncChatLiveBrowserToggleBtn();

    const btnSandboxAgenticRun = getEl('btn-sandbox-agentic-run');
    if (btnSandboxAgenticRun) btnSandboxAgenticRun.addEventListener('click', runSandboxAgenticSearch);
    const sandboxAgenticInput = getEl('sandbox-browser-agentic-input');
    if (sandboxAgenticInput) {
      sandboxAgenticInput.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter' && !ev.shiftKey) {
          ev.preventDefault();
          runSandboxAgenticSearch();
        }
      });
    }
    $('checkpoint-db-btn').addEventListener('click', onCheckpointDb);
    $('change-password-form').addEventListener('submit', onChangePassword);
    $('shutdown-btn').addEventListener('click', confirmGracefulShutdown);

    // Modals & Escape
    $('modal-close-btn').addEventListener('click', closeModal);
    $('modal-overlay').addEventListener('click', (e) => {
      if (e.target === $('modal-overlay')) closeModal();
    });

    const agentModal = getEl('agent-modal');
    const agentModalClose = getEl('agent-modal-close');
    if (agentModalClose) agentModalClose.addEventListener('click', closeAgentDialog);
    if (agentModal) {
      agentModal.addEventListener('click', (e) => {
        if (e.target === agentModal) closeAgentDialog();
      });
    }

    const extModal = getEl('extension-modal');
    const extModalClose = getEl('extension-modal-close');
    if (extModalClose) extModalClose.addEventListener('click', closeExtensionModal);
    if (extModal) {
      extModal.addEventListener('click', (e) => {
        if (e.target === extModal) closeExtensionModal();
      });
    }

    const lightboxModal = getEl('image-lightbox-modal');
    const lightboxClose = getEl('lightbox-close');
    const lightboxDone = getEl('lightbox-done-btn');
    if (lightboxClose) lightboxClose.addEventListener('click', closeImageLightbox);
    if (lightboxDone) lightboxDone.addEventListener('click', closeImageLightbox);
    if (lightboxModal) {
      lightboxModal.addEventListener('click', (e) => {
        if (e.target === lightboxModal) closeImageLightbox();
      });
    }

    const schedModal = getEl('modal-schedule');
    const schedModalClose = getEl('btn-modal-schedule-close');
    const schedCancelBtn = getEl('btn-sched-cancel');
    const schedSaveBtn = getEl('btn-sched-save');
    const schedKindSel = getEl('sched-kind');
    if (schedModalClose) schedModalClose.addEventListener('click', closeScheduleModal);
    if (schedCancelBtn) schedCancelBtn.addEventListener('click', closeScheduleModal);
    if (schedSaveBtn) schedSaveBtn.addEventListener('click', () => saveScheduleModal());
    if (schedKindSel) schedKindSel.addEventListener('change', syncScheduleKindFields);
    if (schedModal) {
      schedModal.addEventListener('click', (e) => {
        if (e.target === schedModal) closeScheduleModal();
      });
    }

    document.addEventListener('click', (e) => {
      if (!state.slashPaletteOpen) return;
      const pal = getEl('chat-slash-palette');
      const btn = getEl('chat-slash-btn');
      const inp = getEl('chat-input');
      if (
        pal
        && !pal.contains(e.target)
        && (!btn || !btn.contains(e.target))
        && (!inp || !inp.contains(e.target))
      ) {
        hideSlashCommandPalette();
      }
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        if (schedModal && !schedModal.hidden) closeScheduleModal();
        if (lightboxModal && !lightboxModal.hidden) closeImageLightbox();
        if (extModal && !extModal.hidden) closeExtensionModal();
        if (agentModal && !agentModal.hidden) closeAgentDialog();
        if (!$('modal-overlay').hidden) closeModal();
        hideSlashAgentMenu();
        hideSlashCommandPalette();
        setMobileDrawer(false);
      }
    });

    window.addEventListener('resize', () => {
      if (!state.activeLiveBrowserSessionId) return;
      if (state.liveOverlayResizeTimer) {
        window.clearTimeout(state.liveOverlayResizeTimer);
      }
      state.liveOverlayResizeTimer = window.setTimeout(() => {
        state.liveOverlayResizeTimer = null;
        const activeTab = state.activeLiveBrowserSessionId
          ? (state.liveBrowserTabs.get(state.activeLiveBrowserSessionId) || null)
          : null;
        if (!activeTab) return;
        renderLiveSvgOverlay('chat-live-browser-overlay', activeTab, { ripple: false });
        renderLiveSvgOverlay('sandbox-live-browser-overlay', activeTab, { ripple: false });
      }, 120);
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    wireEvents();
    checkSessionOnBoot();
  });
})();
