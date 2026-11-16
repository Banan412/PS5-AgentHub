/**
 * In-Process Zero-Dependency Mock HTTP and SSE Server for PS5-AgentHub E2E Testing
 * Fully implements contracts for inference SSE stream (~120 tok/s), dashboard assets,
 * YouTube oEmbed metadata, DuckDuckGo rate limiting & fallback, sandbox error sessions,
 * and task cancellation/eviction.
 */

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const DEFAULT_ASSET_BASE = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets');

export function createMockServer(options = {}) {
  const state = {
    conversations: new Map(),
    tasks: new Map(), // taskId -> { id, convId, status: 'running'|'cancelled'|'completed' }
    tokens: new Map(), // taskId -> { cancelled: boolean, createdAt: number }
    sessions: new Map(), // sessionId -> { id, url, status, html, createdAt }
    requestLogs: [],
    customHandlers: new Map()
  };

  const server = http.createServer(async (req, res) => {
    const parsedUrl = new URL(req.url, `http://${req.headers.host || '127.0.0.1'}`);
    const pathname = parsedUrl.pathname;
    const method = req.method.toUpperCase();

    state.requestLogs.push({
      method,
      pathname,
      query: Object.fromEntries(parsedUrl.searchParams),
      headers: req.headers,
      timestamp: Date.now()
    });

    // CORS & Common Headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');

    if (method === 'OPTIONS') {
      res.statusCode = 204;
      res.end();
      return;
    }

    // Read request body if present
    let body = '';
    let payloadTooLarge = false;
    req.setEncoding('utf8');
    for await (const chunk of req) {
      if (!payloadTooLarge) {
        body += chunk;
        if (body.length > 5 * 1024 * 1024) { // 5MB limit
          payloadTooLarge = true;
          // Drain stream
          req.resume();
        }
      }
    }

    if (payloadTooLarge) {
      res.statusCode = 413;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'Payload Too Large' }));
      return;
    }

    let parsedBody = null;
    if (body && req.headers['content-type']?.includes('application/json')) {
      try {
        parsedBody = JSON.parse(body);
      } catch {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: { code: 'malformed_json', message: 'Invalid JSON' } }));
        return;
      }
    }

    // 1. Static Dashboard Assets
    if (method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
      try {
        const content = fs.readFileSync(path.join(DEFAULT_ASSET_BASE, 'index.html'), 'utf8');
        res.statusCode = 200;
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(content);
      } catch (err) {
        res.statusCode = 500;
        res.end(err.message);
      }
      return;
    }

    if (method === 'GET' && pathname === '/styles.css') {
      try {
        const content = fs.readFileSync(path.join(DEFAULT_ASSET_BASE, 'styles.css'), 'utf8');
        res.statusCode = 200;
        res.setHeader('Content-Type', 'text/css; charset=utf-8');
        res.end(content);
      } catch (err) {
        res.statusCode = 500;
        res.end(err.message);
      }
      return;
    }

    if (method === 'GET' && pathname === '/app.js') {
      try {
        const content = fs.readFileSync(path.join(DEFAULT_ASSET_BASE, 'app.js'), 'utf8');
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
        res.end(content);
      } catch (err) {
        res.statusCode = 500;
        res.end(err.message);
      }
      return;
    }

    // 2. Health & Telemetry
    if (method === 'GET' && pathname === '/api/v1/health') {
      const simulateOffline = parsedUrl.searchParams.get('simulate_offline') === 'true';
      if (simulateOffline) {
        res.statusCode = 503;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ status: 'degraded', healthy: false, ps5lm_reachable: false }));
        return;
      }
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({
        status: 'ok',
        healthy: true,
        version: '0.1.0',
        uptime_secs: 420,
        storage_driver: 'sqlite',
        ps5lm_reachable: true,
        ps5lm_loaded_model: 'qwen2.5-7b-instruct',
        network_degraded: false
      }));
      return;
    }

    if (method === 'GET' && pathname === '/api/v1/inference/capabilities') {
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({
        loaded_model: 'qwen2.5-7b-instruct',
        context_length: 32768,
        supports_streaming: true,
        max_output_tokens: 4096
      }));
      return;
    }

    // 3. SSE Events Stream
    if (method === 'GET' && pathname === '/api/v1/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
      res.write(`event: heartbeat\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
      // Keep open for 200ms then gracefully close for test stability
      setTimeout(() => {
        if (!res.writableEnded) res.end();
      }, 200);
      return;
    }

    // 4. Chat Completions & Streaming (~120 tok/s)
    if (method === 'POST' && (pathname === '/api/v1/chat/completions' || pathname === '/v1/chat/completions')) {
      const isStreaming = Boolean(parsedBody?.stream);
      const tokenCount = Number(parsedBody?.max_tokens || 150);
      const simulateRepetition = Boolean(parsedBody?.simulate_repetition);
      const promptEvalDelayMs = Number(parsedBody?.prompt_eval_ms ?? 15);
      const targetTokPerSec = Number(parsedBody?.target_tok_per_sec ?? 120);
      const tokenIntervalMs = Math.max(1, Math.round(1000 / targetTokPerSec)); // ~8.33ms for 120 tok/s

      if (!isStreaming) {
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({
          id: 'chatcmpl_' + randomUUID(),
          object: 'chat.completion',
          model: parsedBody?.model || 'qwen2.5-7b-instruct',
          choices: [{
            index: 0,
            message: { role: 'assistant', content: 'Mock assistant response.' },
            finish_reason: 'stop'
          }],
          usage: { prompt_tokens: 16, completion_tokens: tokenCount, total_tokens: 16 + tokenCount },
          timings: {
            predicted_n: tokenCount,
            predicted_ms: tokenCount * tokenIntervalMs,
            predicted_per_second: targetTokPerSec,
            prompt_n: 16,
            prompt_ms: promptEvalDelayMs,
            prompt_per_second: 640.0
          }
        }));
        return;
      }

      // Streaming SSE mode
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });

      // TTFT prompt evaluation delay
      if (promptEvalDelayMs > 0) {
        await new Promise(r => setTimeout(r, promptEvalDelayMs));
      }

      let emittedTokens = 0;
      const sendToken = (text) => {
        if (res.writableEnded) return;
        const chunk = {
          id: 'chatcmpl_chunk_' + randomUUID(),
          object: 'chat.completion.chunk',
          model: 'qwen2.5-7b-instruct',
          choices: [{
            index: 0,
            delta: { content: text },
            finish_reason: null
          }]
        };
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        emittedTokens++;
      };

      if (simulateRepetition) {
        // Emit repeating phrase
        const phrase = 'The quick brown fox jumps over the lazy dog. ';
        for (let i = 0; i < 8; i++) {
          if (res.writableEnded) break;
          sendToken(phrase);
          await new Promise(r => setTimeout(r, 10));
        }
      } else {
        // Emit steady tokens pacing to target decode rate (~120 tok/s)
        const targetIntervalMs = 1000 / targetTokPerSec;
        const streamStartTime = Date.now();
        for (let i = 0; i < tokenCount; i++) {
          if (res.writableEnded) break;
          sendToken(` tok_${i}`);
          const expectedElapsed = (i + 1) * targetIntervalMs;
          const actualElapsed = Date.now() - streamStartTime;
          const sleepMs = expectedElapsed - actualElapsed;
          if (sleepMs > 6) {
            await new Promise(r => setTimeout(r, sleepMs));
          } else if (i % 8 === 0) {
            await new Promise(r => setImmediate(r));
          }
        }
      }

      if (!res.writableEnded) {
        // Final finish chunk and [DONE]
        const finalChunk = {
          id: 'chatcmpl_final',
          object: 'chat.completion.chunk',
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
          timings: {
            predicted_n: emittedTokens,
            predicted_per_second: targetTokPerSec
          }
        };
        res.write(`data: ${JSON.stringify(finalChunk)}\n\n`);
        res.write('data: [DONE]\n\n');
        res.end();
      }
      return;
    }

    // 5. Conversations & Messages
    if (method === 'POST' && pathname === '/api/v1/conversations') {
      const convId = 'conv-' + randomUUID();
      state.conversations.set(convId, { id: convId, createdAt: Date.now() });
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, conversation_id: convId }));
      return;
    }

    const messageMatch = pathname.match(/^\/api\/v1\/conversations\/([^/]+)\/messages$/);
    if (method === 'POST' && messageMatch) {
      const convId = messageMatch[1];
      const taskId = 'task-' + randomUUID();
      state.tasks.set(taskId, { id: taskId, convId, status: 'running' });
      state.tokens.set(taskId, { taskId, cancelled: false, createdAt: Date.now() });
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, task_id: taskId, conversation_id: convId }));
      return;
    }

    // 6. Cancellation & Token Eviction
    const cancelConvMatch = pathname.match(/^\/api\/v1\/conversations\/([^/]+)\/cancel$/);
    if (method === 'POST' && cancelConvMatch) {
      const convId = cancelConvMatch[1];
      for (const [taskId, task] of state.tasks.entries()) {
        if (task.convId === convId && task.status === 'running') {
          task.status = 'cancelled';
          // Evict cancellation token from tokens map (mirrors runtime fix)
          state.tokens.delete(taskId);
        }
      }
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, message: 'Tasks cancelled and tokens evicted' }));
      return;
    }

    const cancelTaskMatch = pathname.match(/^\/api\/v1\/tasks\/([^/]+)\/cancel$/);
    if (method === 'POST' && cancelTaskMatch) {
      const taskId = cancelTaskMatch[1];
      const task = state.tasks.get(taskId);
      if (!task) {
        res.statusCode = 404;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: false, error: 'task not found' }));
        return;
      }
      task.status = 'cancelled';
      // Evict token
      state.tokens.delete(taskId);
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, taskId, evicted: true }));
      return;
    }

    // 7. Sandbox Browser Navigation & Preview
    if (method === 'POST' && pathname === '/api/v1/sandbox/browser/navigate') {
      const targetUrl = parsedBody?.url || '';
      const sessionId = 'sess-' + randomUUID();
      const isInvalid = targetUrl.includes('invalid') || targetUrl.includes('error') || targetUrl.includes('broken') || targetUrl.includes('<script>');

      if (isInvalid) {
        // Record error session in store.sessions (Feature 9)
        const errorSession = {
          id: sessionId,
          url: targetUrl,
          status: 'error',
          error_message: 'Navigation failed: host could not be resolved',
          createdAt: Date.now()
        };
        state.sessions.set(sessionId, errorSession);
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: true, session_id: sessionId, status: 'error', error: errorSession.error_message }));
        return;
      }

      const activeSession = {
        id: sessionId,
        url: targetUrl,
        status: 'loaded',
        createdAt: Date.now()
      };
      state.sessions.set(sessionId, activeSession);
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, session_id: sessionId, status: 'loaded' }));
      return;
    }

    if (method === 'GET' && pathname === '/api/v1/sandbox/browser/sessions') {
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({
        ok: true,
        sessions: Array.from(state.sessions.values())
      }));
      return;
    }

    if (method === 'GET' && pathname === '/api/v1/sandbox/browser/preview') {
      const sessionId = parsedUrl.searchParams.get('session_id');

      // Security check: validate session_id length and characters (no path traversal)
      if (sessionId && (sessionId.length > 128 || !/^[a-zA-Z0-9._:-]+$/.test(sessionId))) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: { code: 'invalid_input', message: 'Invalid session_id' } }));
        return;
      }

      let session = null;
      if (sessionId) {
        session = state.sessions.get(sessionId);
        if (!session) {
          res.statusCode = 404;
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: 'session not found' }));
          return;
        }
      } else {
        // Default to most recent session
        const allSessions = Array.from(state.sessions.values());
        session = allSessions[allSessions.length - 1] || null;
      }

      // Feature 9: Always return HTTP 200 for preview with CSP headers
      res.statusCode = 200;
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.setHeader('X-Browser-Session-Id', session ? session.id : 'none');
      res.setHeader('X-Browser-Url', session ? session.url : 'about:blank');
      res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; sandbox");

      if (session && session.status === 'error') {
        // Sanitized error preview HTML
        const safeUrl = session.url.replace(/[<>"'&]/g, (c) => `&#${c.charCodeAt(0)};`);
        const safeError = (session.error_message || 'Navigation Error').replace(/[<>"'&]/g, (c) => `&#${c.charCodeAt(0)};`);
        res.end(`<!DOCTYPE html>
<html>
<head><title>Navigation Error</title>
<style>body{background:#0b0f17;color:#e6edf7;font-family:sans-serif;padding:24px;}</style>
</head>
<body>
  <div class="error-card" style="border:1px solid #1c2638;padding:16px;border-radius:8px;background:#131a26;">
    <h3 style="color:#ef4444;margin:0 0 8px;">Navigation Failed</h3>
    <p>Target URL: <code style="color:#38bdf8;">${safeUrl}</code></p>
    <p style="color:#94a3b8;">${safeError}</p>
    <small style="color:#64748b;">Timestamp: ${new Date(session.createdAt).toISOString()}</small>
  </div>
</body>
</html>`);
      } else {
        res.end(`<!DOCTYPE html><html><body><h3>Browser Preview</h3><p>Loaded</p></body></html>`);
      }
      return;
    }

    const deleteSessionMatch = pathname.match(/^\/api\/v1\/sandbox\/browser\/sessions\/([^/]+)$/);
    if (method === 'DELETE' && deleteSessionMatch) {
      const sessId = deleteSessionMatch[1];
      state.sessions.delete(sessId);
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, deleted: sessId }));
      return;
    }

    // 8. Multi-Provider Search Fallback Pipeline
    if (method === 'GET' && pathname === '/api/v1/search') {
      const query = parsedUrl.searchParams.get('query');
      const provider = parsedUrl.searchParams.get('provider') || 'duckduckgo';
      const simulate429 = req.headers['x-simulate-rate-limit'] === 'true' || provider === 'duckduckgo_ratelimit';
      const simulate503 = req.headers['x-simulate-error-503'] === 'true';

      if (!query || !query.trim()) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: false, error: "missing required string 'query'" }));
        return;
      }

      if (simulate429) {
        res.statusCode = 429;
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Retry-After', '1');
        res.end(JSON.stringify({ ok: false, error: 'Too Many Requests' }));
        return;
      }

      if (simulate503) {
        res.statusCode = 503;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: false, error: 'Service Unavailable' }));
        return;
      }

      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({
        ok: true,
        provider,
        count: 5,
        results: [
          { title: `${query} result 1`, url: 'https://example.com/1', snippet: `Info about ${query}` },
          { title: `${query} result 2`, url: 'https://example.com/2', snippet: `Details on ${query}` },
          { title: `${query} result 3 (YouTube)`, url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', snippet: `Video guide for ${query}` }
        ]
      }));
      return;
    }

    // 9. YouTube oEmbed
    if (method === 'GET' && (pathname === '/youtube/oembed' || pathname === '/oembed')) {
      const urlParam = parsedUrl.searchParams.get('url') || '';
      if (req.headers['x-simulate-rate-limit'] === 'true') {
        res.statusCode = 429;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: 'Too Many Requests' }));
        return;
      }
      if (urlParam.includes('invalid') || urlParam.includes('404')) {
        res.statusCode = 404;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: 'Video unavailable' }));
        return;
      }
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({
        title: 'PlayStation 5 Update Overview',
        author_name: 'PlayStation',
        author_url: 'https://www.youtube.com/@PlayStation',
        type: 'video',
        provider_name: 'YouTube',
        html: '<iframe src="https://www.youtube.com/embed/dQw4w9WgXcQ" width="640" height="360"></iframe>'
      }));
      return;
    }

    // 10. Protected routes (Auth / CSRF check)
    if (method === 'POST' && pathname === '/api/v1/settings') {
      const authHeader = req.headers['authorization'];
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        res.statusCode = 401;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: 'unauthorized' }));
        return;
      }
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, updated: parsedBody }));
      return;
    }

    // Default 404
    res.statusCode = 404;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'Not Found' }));
  });

  return {
    server,
    state,
    start() {
      return new Promise((resolve, reject) => {
        server.listen(options.port || 0, '127.0.0.1', () => {
          resolve(this);
        });
        server.on('error', reject);
      });
    },
    stop() {
      return new Promise((resolve) => {
        server.close(resolve);
      });
    },
    getPort() {
      return server.address().port;
    },
    getUrl() {
      return `http://127.0.0.1:${this.getPort()}`;
    }
  };
}
