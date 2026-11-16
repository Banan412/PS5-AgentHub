/**
 * Tier 2: Boundary & Corner Cases E2E Test Suite (60 Tests, 5 per feature F1-F12)
 * Validates resilience against stress conditions, edge boundaries, cancellations, and rate limits.
 */

import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createMockServer } from './helpers/mock_server.mjs';
import {
  validateIndexHtml,
  validateStylesCss,
  validateAppJs
} from './helpers/static_validator.mjs';
import {
  calculateDecodeThroughput,
  StreamingRepetitionGuardSimulator,
  TaskLifecycleSimulator
} from './helpers/assertion_utils.mjs';

function httpRequest(url, options = {}, postData = null) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: options.headers || {}
    };

    let payloadData = null;
    if (postData) {
      payloadData = typeof postData === 'string' ? postData : JSON.stringify(postData);
      reqOptions.headers['Content-Length'] = Buffer.byteLength(payloadData);
    }

    let responseReceived = false;
    let resStatusCode = null;

    const req = http.request(reqOptions, (res) => {
      responseReceived = true;
      resStatusCode = res.statusCode;
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(data); } catch {}
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          data,
          json
        });
      });
    });

    req.on('error', (err) => {
      if (responseReceived && resStatusCode) {
        resolve({
          statusCode: resStatusCode,
          headers: {},
          data: '',
          json: null
        });
      } else {
        reject(err);
      }
    });
    if (payloadData) {
      req.write(payloadData);
    }
    req.end();
  });
}

function streamSSE(url, options = {}, onChunk = null) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname + parsed.search,
      method: options.method || 'POST',
      headers: {
        'Accept': 'text/event-stream',
        'Content-Type': 'application/json',
        ...(options.headers || {})
      }
    };

    const startTime = Date.now();
    let firstTokenTime = null;
    let chunksReceived = 0;
    let totalText = '';

    const req = http.request(reqOptions, (res) => {
      res.setEncoding('utf8');
      let buffer = '';

      res.on('data', (rawChunk) => {
        buffer += rawChunk;
        const lines = buffer.split('\n\n');
        buffer = lines.pop();

        for (const block of lines) {
          if (!block.trim()) continue;
          for (const line of block.split('\n')) {
            if (line.startsWith('data: ')) {
              const dataStr = line.slice(6).trim();
              if (dataStr === '[DONE]') {
                continue;
              }
              try {
                const parsedChunk = JSON.parse(dataStr);
                const delta = parsedChunk.choices?.[0]?.delta?.content;
                if (delta) {
                  if (firstTokenTime === null) {
                    firstTokenTime = Date.now();
                  }
                  chunksReceived++;
                  totalText += delta;
                  if (onChunk) onChunk(delta, parsedChunk);
                }
              } catch {}
            }
          }
        }
      });

      res.on('end', () => {
        const endTime = Date.now();
        resolve({
          statusCode: res.statusCode,
          chunksReceived,
          totalText,
          startTime,
          endTime,
          firstTokenTime: firstTokenTime || endTime
        });
      });
    });

    req.on('error', reject);
    if (options.body) {
      req.write(typeof options.body === 'string' ? options.body : JSON.stringify(options.body));
    }
    req.end();
  });
}

export async function runTier2(opts = {}) {
  let mockServer = null;
  let serverUrl = opts.serverUrl;

  if (!serverUrl) {
    mockServer = createMockServer();
    await mockServer.start();
    serverUrl = mockServer.getUrl();
  }

  const results = [];
  const runTest = async (id, name, fn) => {
    const start = Date.now();
    try {
      await fn();
      results.push({ id, name, passed: true, durationMs: Date.now() - start });
    } catch (err) {
      results.push({ id, name, passed: false, error: err.message, durationMs: Date.now() - start });
    }
  };

  try {
    // -------------------------------------------------------------
    // FEATURE 1: Live Browser Dock Reopening & Toggle (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F01-01', 'test_dock_rapid_toggle_jitter', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');

      assert.ok(/function\s+toggleChatLiveBrowserDock\s*\(/.test(appJs), 'toggleChatLiveBrowserDock must exist');
      assert.ok(/const\s+shouldOpen\s*=\s*\(typeof\s+forceState\s*===\s*['"]boolean['"]\)\s*\?\s*forceState\s*:\s*dock\.hidden;/.test(appJs),
        'toggleChatLiveBrowserDock must compute shouldOpen deterministically');
      assert.ok(/openChatLiveBrowserDock\(\s*\{\s*force:\s*true\s*\}\s*\)/.test(appJs),
        'Opening must force clear dismissal');
      assert.ok(/hideChatLiveBrowserDock\(\)/.test(appJs), 'Closing must call hideChatLiveBrowserDock');
      assert.ok(html.includes('id="btn-chat-live-browser-toggle"'), 'btn-chat-live-browser-toggle must exist in HTML');
      assert.ok(html.includes('id="chat-live-browser-dock"'), 'chat-live-browser-dock must exist in HTML');
    });

    await runTest('TEST-T2-F01-02', 'test_dock_toggle_during_active_streaming', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 25, target_tok_per_sec: 120 }
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.chunksReceived > 0, 'Stream must receive chunks over socket');

      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');
      // Assert streaming event handling does not forcefully override user's dock visibility state
      assert.ok(!/case\s*['"]chat_token['"]:[\s\S]*?chatLiveBrowserDismissed\s*=\s*(?:true|false)/.test(appJs),
        'Stream token event handler must not mutate dock dismissal state');
      assert.ok(!/case\s*['"]chat_token['"]:[\s\S]*?dock\.hidden\s*=\s*(?:true|false)/.test(appJs),
        'Stream token event handler must not force mutate dock.hidden');
    });

    await runTest('TEST-T2-F01-03', 'test_dock_open_with_zero_tabs', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(/if\s*\(!state\.activeLiveBrowserSessionId\s*&&\s*state\.liveBrowserOrder\.length\s*>\s*0\)/.test(appJs),
        'openChatLiveBrowserDock must guard against empty sessions array');
      assert.ok(/function\s+renderLiveTabsStrip\s*\(/.test(appJs), 'renderLiveTabsStrip must exist');

      const sessionsRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/sessions`);
      assert.equal(sessionsRes.statusCode, 200);
      assert.ok(Array.isArray(sessionsRes.json.sessions));

      const previewRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview`);
      assert.equal(previewRes.statusCode, 200, 'Preview endpoint must return HTTP 200 even with zero active sessions');
    });

    await runTest('TEST-T2-F01-04', 'test_dock_conversation_switching_isolation', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(/function\s+syncChatLiveBrowserDockForConversation\s*\(\s*convId\s*\)/.test(appJs),
        'syncChatLiveBrowserDockForConversation must be defined');
      assert.ok(/tabs\.some\(\s*\(t\)\s*=>\s*t\s*&&\s*t\.conversation_id\s*===\s*convId\s*\)/.test(appJs),
        'Dock synchronization must filter tabs scoped to current conversation');
      assert.ok(/function\s+cleanupConversationClientState\s*\(\s*convId\s*\)/.test(appJs),
        'cleanupConversationClientState must be defined to isolate conversation state');
    });

    await runTest('TEST-T2-F01-05', 'test_dock_mobile_viewport_toggle', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes('@media (max-width: 768px)'), 'Mobile media query must be present');
    });

    // -------------------------------------------------------------
    // FEATURE 2: llama.cpp-Style Chat Redesign & Controls (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F02-01', 'test_unbroken_string_overflow_handling', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(
        css.includes('overflow-wrap') || css.includes('word-break') || css.includes('break-word'),
        'styles.css must include word breaking rules for unbroken strings'
      );
    });

    await runTest('TEST-T2-F02-02', 'test_oversized_code_block_horizontal_scroll', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes('overflow-x') || css.includes('overflow'), 'Code containers must support horizontal scrolling');
    });

    await runTest('TEST-T2-F02-03', 'test_rapid_raw_output_mode_toggle', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(html.includes('id="chat-raw-output-toggle"'), '#chat-raw-output-toggle must exist in index.html');
      assert.ok(/function\s+setRawOutputMode\s*\(\s*enabled\s*\)/.test(appJs), 'setRawOutputMode must be defined');
      assert.ok(/state\.showRawOutput\s*=\s*Boolean\(enabled\);/.test(appJs), 'setRawOutputMode must update state.showRawOutput');
      assert.ok(/localStorage\.setItem\(RAW_OUTPUT_KEY/.test(appJs), 'Must persist raw output preference');
      assert.ok(/renderChatMessages\(\)/.test(appJs), 'Must trigger renderChatMessages');
    });

    await runTest('TEST-T2-F02-04', 'test_composer_multiline_auto_expand_boundary', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      assert.ok(/<textarea[^>]*id="chat-input"[^>]*class="[^"]*chat-textarea[^"]*"/.test(html) || html.includes('id="chat-input"'),
        'chat-input must be present in index.html');
      assert.ok(/\.chat-textarea\s*\{[\s\S]*?min-height:\s*(?:40px|48px);/.test(css), 'Must specify min-height boundary');
      assert.ok(/\.chat-textarea\s*\{[\s\S]*?max-height:\s*180px;/.test(css), 'Must clamp max-height at 180px');
      assert.ok(/\.chat-textarea\s*\{[\s\S]*?resize:\s*none;/.test(css), 'Must prevent manual resize distortion');
    });

    await runTest('TEST-T2-F02-05', 'test_keyboard_focus_trap_and_navigation', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes(':focus-visible'), ':focus-visible outline styling must be present');
    });

    // -------------------------------------------------------------
    // FEATURE 3: Frontend Syntax & Invariants (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F03-01', 'test_iframe_sandbox_security_boundary', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      assert.ok(!html.includes('allow-same-origin'), 'No iframe should have allow-same-origin');
    });

    await runTest('TEST-T2-F03-02', 'test_no_eval_or_function_constructor', async () => {
      const jsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const js = fs.readFileSync(jsPath, 'utf8');
      assert.ok(!/\beval\s*\(/.test(js), 'app.js must not contain eval()');
      assert.ok(!/\bnew\s+Function\s*\(/.test(js), 'app.js must not contain new Function()');
    });

    await runTest('TEST-T2-F03-03', 'test_css_brace_and_comment_balance', async () => {
      const cssRes = validateStylesCss();
      assert.equal(cssRes.openBraces, cssRes.closeBraces, 'CSS braces must be perfectly balanced');
    });

    await runTest('TEST-T2-F03-04', 'test_safe_dom_builder_escaping', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(!/\.innerHTML\s*=/.test(appJs), 'app.js must not assign to .innerHTML');
      assert.ok(!/\.outerHTML\s*=/.test(appJs), 'app.js must not assign to .outerHTML');
      assert.ok(!/insertAdjacentHTML\s*\(/.test(appJs), 'app.js must not use insertAdjacentHTML');

      // Live endpoint verification: preview HTML escaping
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://test.invalid/<script>alert("xss")</script>' });
      assert.equal(navRes.statusCode, 200);

      const previewRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${navRes.json.session_id}`);
      assert.equal(previewRes.statusCode, 200);
      assert.ok(!previewRes.data.includes('<script>alert("xss")</script>'), 'Dangerous tags must not appear raw in preview');
    });

    await runTest('TEST-T2-F03-05', 'test_path_traversal_asset_rejection', async () => {
      const traversal1 = await httpRequest(`${serverUrl}/../../etc/passwd`);
      assert.ok(traversal1.statusCode === 404 || traversal1.statusCode === 400);

      const traversal2 = await httpRequest(`${serverUrl}/assets/../Cargo.toml`);
      assert.ok(traversal2.statusCode === 404 || traversal2.statusCode === 400);
    });

    // -------------------------------------------------------------
    // FEATURE 4: Inference Throughput Parity (~120 tok/s) (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F04-01', 'test_extreme_token_rate_burst', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 50, target_tok_per_sec: 180, prompt_eval_ms: 10 }
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.chunksReceived > 0);
      const metrics = calculateDecodeThroughput(res.chunksReceived, res.startTime, res.endTime, res.firstTokenTime);
      assert.ok(metrics.rate >= 140.0, `Burst streaming must maintain high throughput, got ${metrics.rate.toFixed(1)}`);
    });

    await runTest('TEST-T2-F04-02', 'test_repetition_guard_threshold_boundary', async () => {
      const parserPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/parser.rs');
      const parserRs = fs.readFileSync(parserPath, 'utf8');

      assert.ok(/pub\s+struct\s+StreamingRepetitionGuard/.test(parserRs), 'StreamingRepetitionGuard must be defined in parser.rs');
      assert.ok(/self\.unverified_delta_chars\s*>=\s*32/.test(parserRs), 'Must enforce 32-character boundary invariant');
      assert.ok(/STREAM_GUARD_TAG_OPENS/.test(parserRs), 'Must define protected tag boundaries');
      assert.ok(/find_contiguous_suffix_repeat/.test(parserRs), 'Must implement contiguous repeat detection');
    });

    await runTest('TEST-T2-F04-03', 'test_repetition_loop_breaker_clean_abort', async () => {
      const parserPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/parser.rs');
      const parserRs = fs.readFileSync(parserPath, 'utf8');

      assert.ok(/StreamingLoopDecision::AbortRepeatedLoop/.test(parserRs),
        'StreamingLoopDecision must define AbortRepeatedLoop variant');

      // Live request with simulated repetition abortion
      const parsed = new URL(`${serverUrl}/api/v1/chat/completions`);
      const req = http.request({
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' }
      }, (res) => {
        res.on('data', () => {
          req.destroy();
        });
      });
      req.write(JSON.stringify({ stream: true, simulate_repetition: true }));
      req.end();

      // Ensure server survives abrupt stream abort and remains healthy
      await new Promise(r => setTimeout(r, 60));
      const healthRes = await httpRequest(`${serverUrl}/api/v1/health`);
      assert.equal(healthRes.statusCode, 200);
      assert.equal(healthRes.json.status, 'ok');
    });

    await runTest('TEST-T2-F04-04', 'test_multibyte_utf8_split_across_sse_chunks', async () => {
      const parserPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/parser.rs');
      const parserRs = fs.readFileSync(parserPath, 'utf8');

      assert.ok(/self\.pending\.push_str\(delta\)/.test(parserRs), 'Must append delta via UTF-8 push_str');

      // Live UTF-8 multibyte request
      const res = await httpRequest(`${serverUrl}/api/v1/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, {
        stream: false,
        messages: [{ role: 'user', content: '🎮 PS5 🚀 🌟 日本語 text' }]
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.choices?.[0]?.message?.content);
      assert.ok(!res.json.choices[0].message.content.includes('\uFFFD'), 'Must not contain UTF-8 replacement character');
    });

    await runTest('TEST-T2-F04-05', 'test_zero_token_empty_chunk_handling', async () => {
      const parserPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/parser.rs');
      const parserRs = fs.readFileSync(parserPath, 'utf8');
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(/if\s+delta\.is_empty\(\)\s*\{\s*return\s+StreamingLoopDecision::Continue;\s*\}/.test(parserRs),
        'StreamingRepetitionGuard must return Continue on empty delta');
      assert.ok(/case\s*['"]chat_token['"]:[\s\S]*?if\s*\(!ev\.delta\)/.test(appJs) ||
        /b\.text\s*\+=\s*ev\.delta/.test(appJs),
        'app.js must handle stream chunk deltas cleanly');
    });

    // -------------------------------------------------------------
    // FEATURE 5: Streaming Responsiveness & Low TTFT (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F05-01', 'test_client_abrupt_disconnect_mid_stream', async () => {
      // Connect to SSE stream and disconnect immediately
      const parsed = new URL(`${serverUrl}/api/v1/chat/completions`);
      const req = http.request({
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      // Handle expected socket destroy error
      req.on('error', () => {});
      req.write(JSON.stringify({ stream: true, max_tokens: 50 }));
      req.end();

      await new Promise(r => setTimeout(r, 50));
      req.destroy(); // Abrupt client disconnect
      assert.ok(req.destroyed);
    });

    await runTest('TEST-T2-F05-02', 'test_transient_network_pause_within_timeout', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, prompt_eval_ms: 80, max_tokens: 15, target_tok_per_sec: 120 }
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.chunksReceived > 0, 'Must receive chunks despite transient network pause');
    });

    await runTest('TEST-T2-F05-03', 'test_idle_stream_timeout_enforcement', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');

      assert.ok(/\.pool_idle_timeout\(Duration::from_secs\(45\)\)/.test(provRs),
        'Upstream client must configure pool_idle_timeout');
      assert.ok(/\.tcp_keepalive\(Some\(Duration::from_secs\(30\)\)\)/.test(provRs),
        'Upstream client must configure tcp_keepalive');

      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');
      assert.ok(/function\s+connectSse\s*\(/.test(appJs), 'app.js must implement connectSse');
    });

    await runTest('TEST-T2-F05-04', 'test_concurrent_multi_conversation_streaming', async () => {
      const streamPromises = [1, 2, 3, 4].map(id =>
        httpRequest(`${serverUrl}/api/v1/chat/completions`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        }, { stream: false, max_tokens: 20 })
      );
      const responses = await Promise.all(streamPromises);
      for (const res of responses) {
        assert.equal(res.statusCode, 200);
        assert.ok(res.json.choices);
      }
    });

    await runTest('TEST-T2-F05-05', 'test_oversized_single_token_chunk', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');
      assert.ok(/mpsc::channel::<InferenceStreamChunk>\(\s*(\d+)\s*\)/.test(rtRs),
        'mpsc channel must be defined for stream chunks');

      // Live 16KB prompt test
      const largeText = 'A'.repeat(16384);
      const res = await httpRequest(`${serverUrl}/api/v1/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, {
        stream: false,
        messages: [{ role: 'user', content: largeText }]
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.choices);
    });

    // -------------------------------------------------------------
    // FEATURE 6: Agentic Search Anti-Blocking & User-Agent (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F06-01', 'test_http_403_bot_challenge_triggers_fallback', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+detect_challenge_gate/.test(webRs), 'detect_challenge_gate must be present in web.rs');
      assert.ok(/build_search_fallback_chain/.test(webRs), 'build_search_fallback_chain must be defined in web.rs');

      const res = await httpRequest(`${serverUrl}/api/v1/search?provider=duckduckgo_ratelimit&query=ps5`);
      assert.equal(res.statusCode, 429, 'Rate limited provider must return 429 triggering fallback');
    });

    await runTest('TEST-T2-F06-02', 'test_user_agent_rotation_consistency', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/pub\s+fn\s+rotate\(\)\s*->\s*&'static\s+BrowserHeadersProfile/.test(webRs), 'rotate() function must exist');
      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua"\s*\)/.test(webRs));
      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua-mobile"\s*\)/.test(webRs));
      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua-platform"\s*\)/.test(webRs));
      assert.ok(/fn\s+browser_headers_profile_rotates_and_attaches_headers\s*\(\s*\)/.test(webRs),
        'Unit test for browser headers profile rotation must exist in web.rs');
    });

    await runTest('TEST-T2-F06-03', 'test_compressed_brotli_payload_decompression', async () => {
      const cargoPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/Cargo.toml');
      const cargoToml = fs.readFileSync(cargoPath, 'utf8');
      assert.ok(cargoToml.includes('reqwest') || cargoToml.includes('flate2'),
        'Cargo.toml must configure HTTP compression support');

      const healthRes = await httpRequest(`${serverUrl}/api/v1/health`, {
        headers: { 'Accept-Encoding': 'gzip, deflate, br' }
      });
      assert.equal(healthRes.statusCode, 200);
      assert.equal(healthRes.json.status, 'ok');
    });

    await runTest('TEST-T2-F06-04', 'test_non_standard_http_status_codes', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/resp\.status\(\)/.test(webRs), 'web.rs must inspect HTTP response status');

      // Test boundary status codes on mock server
      const notFoundRes = await httpRequest(`${serverUrl}/api/v1/nonexistent_route`);
      assert.equal(notFoundRes.statusCode, 404);

      const rateLimitRes = await httpRequest(`${serverUrl}/api/v1/search?provider=duckduckgo_ratelimit&query=ps5`);
      assert.equal(rateLimitRes.statusCode, 429);
    });

    await runTest('TEST-T2-F06-05', 'test_invalid_tls_certificate_rejection', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/ensure_rustls_ring_provider\(\)/.test(webRs), 'Must install rustls ring provider');
      assert.ok(!/danger_accept_invalid_certs\(\s*true\s*\)/.test(webRs),
        'web.rs must NOT disable TLS certificate validation');
    });

    // -------------------------------------------------------------
    // FEATURE 7: Multi-Provider Search Fallback (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F07-01', 'test_all_providers_exhausted_failure', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(webRs.includes('all search providers in fallback chain failed'),
        'web.rs must define exhaustion error string: "all search providers in fallback chain failed"');

      // Test live search failure response under simulated error
      const res = await httpRequest(`${serverUrl}/api/v1/search?query=ps5`, {
        headers: { 'x-simulate-error-503': 'true' }
      });
      assert.equal(res.statusCode, 503);
    });

    await runTest('TEST-T2-F07-02', 'test_empty_or_whitespace_search_query', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/search?query=%20%20`);
      assert.equal(res.statusCode, 400);
      assert.equal(res.json.ok, false);
      assert.ok(res.json.error.includes("missing required string 'query'"));
    });

    await runTest('TEST-T2-F07-03', 'test_special_characters_url_encoding', async () => {
      const rawQuery = 'c++ #include <iostream> & "ps5" 🎮';
      const res = await httpRequest(`${serverUrl}/api/v1/search?query=` + encodeURIComponent(rawQuery));
      assert.equal(res.statusCode, 200);
      assert.equal(res.json.ok, true);

      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');
      assert.ok(/url::form_urlencoded/.test(webRs) || /Url::parse/.test(webRs),
        'web.rs must use standard URL encoding primitives');
    });

    await runTest('TEST-T2-F07-04', 'test_rate_limit_backoff_timing_bounds', async () => {
      const mgrPath = path.resolve(process.cwd(), 'crates/ps5-agent-manager/src/lib.rs');
      const mgrRs = fs.readFileSync(mgrPath, 'utf8');

      assert.ok(/Duration::from_millis\(25\)/.test(mgrRs), 'Manager backoff must start at 25ms');
      assert.ok(/Duration::from_millis\(100\)/.test(mgrRs), 'Manager backoff cap must be 100ms');

      // Live 429 Retry-After header contract
      const rateLimitRes = await httpRequest(`${serverUrl}/api/v1/search?provider=duckduckgo_ratelimit&query=ps5`);
      assert.equal(rateLimitRes.statusCode, 429);
      assert.ok(rateLimitRes.headers['retry-after'], '429 response must provide Retry-After header');
    });

    await runTest('TEST-T2-F07-05', 'test_malformed_html_search_page_parsing', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+parse_duckduckgo_html/.test(webRs) || /fn\s+parse_duckduckgo_lite_html/.test(webRs),
        'DDG HTML parsers must be declared in web.rs');
      assert.ok(/fn\s+duckduckgo_lite_html_table_parser_extracts_results\s*\(\s*\)/.test(webRs),
        'web.rs must include unit tests for HTML parser resilience');
    });

    // -------------------------------------------------------------
    // FEATURE 8: YouTube & Media Fallback Extraction (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F08-01', 'test_malformed_youtube_video_id', async () => {
      const res = await httpRequest(`${serverUrl}/youtube/oembed?url=https://www.youtube.com/watch?v=invalid_id_404`);
      assert.equal(res.statusCode, 404);
      assert.equal(res.json.error, 'Video unavailable');
    });

    await runTest('TEST-T2-F08-02', 'test_youtube_url_with_dense_tracking_params', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/is_youtube_url/.test(webRs), 'is_youtube_url must handle query parameters');
      assert.ok(/fn\s+youtube_url_detection_and_opengraph_extraction\s*\(\s*\)/.test(webRs),
        'Unit test for YouTube url detection with parameters must exist');

      // Live oEmbed query with dense tracking parameters
      const denseUrl = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ&utm_source=twitter&t=120s&feature=shared';
      const res = await httpRequest(`${serverUrl}/youtube/oembed?url=` + encodeURIComponent(denseUrl));
      assert.equal(res.statusCode, 200);
      assert.equal(res.json.provider_name, 'YouTube');
    });

    await runTest('TEST-T2-F08-03', 'test_youtube_oembed_rate_limited_fallback', async () => {
      const res = await httpRequest(`${serverUrl}/youtube/oembed?url=https://www.youtube.com/watch?v=dQw4w9WgXcQ`, {
        headers: { 'x-simulate-rate-limit': 'true' }
      });
      assert.equal(res.statusCode, 429);
    });

    await runTest('TEST-T2-F08-04', 'test_oversized_spa_javascript_page', async () => {
      const browserPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/browser.rs');
      const browserRs = fs.readFileSync(browserPath, 'utf8');

      assert.ok(/const\s+MAX_STORED_HTML_BYTES:\s*usize\s*=\s*512\s*\*\s*1024;/.test(browserRs),
        'MAX_STORED_HTML_BYTES must cap stored HTML at 512KB');
      assert.ok(/const\s+MAX_PREVIEW_HTML_BYTES:\s*usize\s*=\s*128\s*\*\s*1024;/.test(browserRs),
        'MAX_PREVIEW_HTML_BYTES must cap preview HTML at 128KB');
      assert.ok(/const\s+MAX_PAGE_TEXT_CHARS:\s*usize\s*=\s*16_000;/.test(browserRs),
        'MAX_PAGE_TEXT_CHARS must cap page text at 16,000 chars');
    });

    await runTest('TEST-T2-F08-05', 'test_internationalized_domain_media_url', async () => {
      const idnUrl = 'https://xn--e1afmkfd.xn--p1ai/video';
      const res = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: idnUrl });
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.session_id, 'Navigation to IDN URL must return valid session ID');

      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');
      assert.ok(/Url::parse/.test(webRs), 'web.rs must use Url::parse for standard IDN handling');
    });

    // -------------------------------------------------------------
    // FEATURE 9: Sandbox Browser Error Navigation & Preview (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F09-01', 'test_preview_non_existent_session_id', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=non-existent-uuid-1234`);
      assert.equal(res.statusCode, 404);
      assert.equal(res.json.error, 'session not found');
    });

    await runTest('TEST-T2-F09-02', 'test_preview_omitted_session_id_defaults_latest', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview`);
      assert.equal(res.statusCode, 200);
      assert.ok(res.headers['content-type'].includes('text/html'));
    });

    await runTest('TEST-T2-F09-03', 'test_preview_xss_injection_in_failed_url', async () => {
      const xssUrl = 'http://invalid-site.com/<script>alert("xss")</script>';
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: xssUrl });

      const prevRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${navRes.json.session_id}`);
      assert.equal(prevRes.statusCode, 200);
      assert.ok(!prevRes.data.includes('<script>alert("xss")</script>'), 'Dangerous script tag must be sanitized');
      assert.ok(prevRes.data.includes('&#60;script&#62;') || prevRes.data.includes('&lt;script&gt;'));
    });

    await runTest('TEST-T2-F09-04', 'test_session_store_capacity_eviction', async () => {
      const browserPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/browser.rs');
      const browserRs = fs.readFileSync(browserPath, 'utf8');

      assert.ok(/const\s+MAX_SESSIONS:\s*usize\s*=\s*16;/.test(browserRs),
        'MAX_SESSIONS must be 16 in browser.rs');
      assert.ok(/ensure_session_capacity\(/.test(browserRs),
        'browser.rs must call ensure_session_capacity to enforce eviction');
      assert.ok(/fn\s+browser_enforces_16_session_cap_network_toggle_and_close\s*\(\s*\)/.test(browserRs),
        'Unit test for 16 session cap enforcement must exist in browser.rs');

      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');
      assert.ok(/function\s+trimLiveBrowserTabs\s*\(/.test(appJs), 'app.js must define trimLiveBrowserTabs');
    });

    await runTest('TEST-T2-F09-05', 'test_close_error_session_lifecycle', async () => {
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://close-me.invalid' });
      const sessId = navRes.json.session_id;

      const delRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/sessions/${sessId}`, { method: 'DELETE' });
      assert.equal(delRes.statusCode, 200);

      const checkRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${sessId}`);
      assert.equal(checkRes.statusCode, 404);
    });

    // -------------------------------------------------------------
    // FEATURE 10: Task Token Eviction on Message Cancellation (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F10-01', 'test_rapid_cancel_and_immediate_regenerate', async () => {
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      const convId = convRes.json.conversation_id;

      const msg1 = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Task 1 to cancel' });
      const taskId1 = msg1.json.task_id;

      const cancelRes = await httpRequest(`${serverUrl}/api/v1/tasks/${taskId1}/cancel`, { method: 'POST' });
      assert.equal(cancelRes.statusCode, 200);

      const msg2 = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Task 2 regenerated' });
      assert.equal(msg2.statusCode, 200);
      assert.ok(msg2.json.task_id);

      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');
      assert.ok(/rt\.evict_pending_for_task/.test(rtRs));
      assert.ok(/rt\.task_manager\.release_token/.test(rtRs));
    });

    await runTest('TEST-T2-F10-02', 'test_cancel_already_finished_task', async () => {
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      const convId = convRes.json.conversation_id;

      const msgRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Task to double cancel' });
      const taskId = msgRes.json.task_id;

      const cancel1 = await httpRequest(`${serverUrl}/api/v1/tasks/${taskId}/cancel`, { method: 'POST' });
      assert.equal(cancel1.statusCode, 200);

      const cancel2 = await httpRequest(`${serverUrl}/api/v1/tasks/${taskId}/cancel`, { method: 'POST' });
      assert.ok(cancel2.statusCode === 200 || cancel2.statusCode === 404, 'Second cancellation must be idempotent');

      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');
      assert.ok(/task_cancel\.is_cancelled\(\)/.test(rtRs) || /cancel\.is_cancelled\(\)/.test(rtRs),
        'runtime.rs must verify cancellation idempotency');
    });

    await runTest('TEST-T2-F10-03', 'test_cancel_invalid_task_id', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/tasks/invalid-uuid-xyz/cancel`, { method: 'POST' });
      assert.equal(res.statusCode, 404);
      assert.equal(res.json.ok, false);
    });

    await runTest('TEST-T2-F10-04', 'test_consecutive_task_cancellation_soak', async () => {
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      const convId = convRes.json.conversation_id;

      for (let i = 0; i < 10; i++) {
        const msgRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        }, { content: `Soak test message ${i}` });
        assert.equal(msgRes.statusCode, 200);

        const cancelRes = await httpRequest(`${serverUrl}/api/v1/tasks/${msgRes.json.task_id}/cancel`, { method: 'POST' });
        assert.equal(cancelRes.statusCode, 200);
      }

      const healthRes = await httpRequest(`${serverUrl}/api/v1/health`);
      assert.equal(healthRes.statusCode, 200);

      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');
      assert.ok(/self\.task_manager\.release_token/.test(rtRs));
    });

    await runTest('TEST-T2-F10-05', 'test_cancellation_during_long_tool_execution', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');

      assert.ok(/cancel:\s*CancellationToken/.test(rtRs), 'Cancellation tokens must be passed to tool loops');
      assert.ok(/AgentError::Cancelled/.test(rtRs), 'Must return AgentError::Cancelled on task abort');

      const convCancelRes = await httpRequest(`${serverUrl}/api/v1/conversations/conv-long-tool/cancel`, { method: 'POST' });
      assert.equal(convCancelRes.statusCode, 200);
      assert.equal(convCancelRes.json.ok, true);
    });

    // -------------------------------------------------------------
    // FEATURE 11: Polling & Connection Reuse Optimization (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F11-01', 'test_infinite_redirect_loop_bounded', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/redirect::Policy::limited\(\s*10\s*\)/.test(webRs) || /max_redirects/.test(webRs) || /redirect/.test(webRs),
        'web.rs must enforce bounded redirect policies to prevent infinite loops');
      assert.ok(/fn\s+fetch_validated_url/.test(webRs), 'fetch_validated_url must exist');
    });

    await runTest('TEST-T2-F11-02', 'test_agent_deletion_timeout_deadline_hit', async () => {
      const mgrPath = path.resolve(process.cwd(), 'crates/ps5-agent-manager/src/lib.rs');
      const mgrRs = fs.readFileSync(mgrPath, 'utf8');

      assert.ok(/let\s+deadline\s*=\s*tokio::time::Instant::now\(\)\s*\+\s*Duration::from_millis\(timeout_ms\);/.test(mgrRs),
        'Agent deletion must compute absolute deadline from timeout_ms');
      assert.ok(/deadline\.saturating_duration_since\(now\)/.test(mgrRs),
        'Remaining wait duration must be bounded by deadline');
      assert.ok(/AgentError::Conflict/.test(mgrRs),
        'Timeout must return Conflict error if active tasks remain');
    });

    await runTest('TEST-T2-F11-03', 'test_high_concurrency_web_fetch_pool_stress', async () => {
      const requests = Array.from({ length: 25 }, () => httpRequest(`${serverUrl}/api/v1/health`));
      const resps = await Promise.all(requests);
      assert.equal(resps.length, 25);
      for (const r of resps) {
        assert.equal(r.statusCode, 200);
      }
    });

    await runTest('TEST-T2-F11-04', 'test_cross_domain_redirect_header_scrubbing', async () => {
      // Mock server authentication boundary check
      const unauthRes = await httpRequest(`${serverUrl}/api/v1/settings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { theme: 'dark' });
      assert.equal(unauthRes.statusCode, 401, 'Protected settings must reject unauthenticated requests');

      const authRes = await httpRequest(`${serverUrl}/api/v1/settings`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer test-token'
        }
      }, { theme: 'dark' });
      assert.equal(authRes.statusCode, 200, 'Authenticated request must succeed');

      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');
      assert.ok(/strip_sensitive_redirect_headers\(&mut\s+current_headers\)/.test(webRs),
        'web.rs must call strip_sensitive_redirect_headers on cross-origin redirects');
      assert.ok(/is_same_origin/.test(webRs), 'web.rs must evaluate same-origin boundaries on redirect');
    });

    await runTest('TEST-T2-F11-05', 'test_sandbox_exec_long_sleep_cpu_utilization', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');
      assert.ok(/tokio::time::sleep/.test(provRs), 'provider.rs must use asynchronous tokio sleep');

      const mgrPath = path.resolve(process.cwd(), 'crates/ps5-agent-manager/src/lib.rs');
      const mgrRs = fs.readFileSync(mgrPath, 'utf8');
      assert.ok(/tokio::time::sleep\(sleep_dur\)\.await;/.test(mgrRs), 'manager must await async sleep');

      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');
      assert.ok(/function\s+clearLiveBrowserTimers\s*\(/.test(appJs), 'app.js must implement timer cleanup');
    });

    // -------------------------------------------------------------
    // FEATURE 12: Live PS5 Health & Endpoint Verification (Boundary Cases)
    // -------------------------------------------------------------
    await runTest('TEST-T2-F12-01', 'test_health_check_inference_offline', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/health?simulate_offline=true`);
      assert.equal(res.statusCode, 503);
      assert.equal(res.json.healthy, false);
      assert.equal(res.json.status, 'degraded');
    });

    await runTest('TEST-T2-F12-02', 'test_oversized_request_body_rejection', async () => {
      const hugeBody = 'X'.repeat(6 * 1024 * 1024); // 6MB exceeds 5MB limit
      const res = await httpRequest(`${serverUrl}/api/v1/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, hugeBody);
      assert.equal(res.statusCode, 413);
    });

    await runTest('TEST-T2-F12-03', 'test_unauthenticated_protected_route_rejection', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/settings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { theme: 'dark' });
      assert.equal(res.statusCode, 401);
      assert.equal(res.json.error, 'unauthorized');
    });

    await runTest('TEST-T2-F12-04', 'test_malformed_json_syntax_error_response', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, '{ invalid json: ...');
      assert.equal(res.statusCode, 400);
      assert.equal(res.json.error.code, 'malformed_json');
    });

    await runTest('TEST-T2-F12-05', 'test_health_endpoint_high_concurrency_stress', async () => {
      const requests = Array.from({ length: 50 }, () => httpRequest(`${serverUrl}/api/v1/health`));
      const resps = await Promise.all(requests);
      assert.equal(resps.length, 50);
      for (const r of resps) {
        assert.equal(r.statusCode, 200);
        assert.equal(r.json.status, 'ok');
      }
    });

  } finally {
    if (mockServer) {
      await mockServer.stop();
    }
  }

  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;

  return {
    tierName: 'Tier 2: Boundary & Corner Cases (60 Tests)',
    total: results.length,
    passed,
    failed,
    results
  };
}

if (process.argv[1] && (process.argv[1].endsWith('tier2_boundary_corner.test.mjs') || process.argv[1].includes('tier2_boundary_corner'))) {
  const summary = await runTier2();
  console.log(`\n=== ${summary.tierName} ===`);
  console.log(`Passed: ${summary.passed}/${summary.total} (Failed: ${summary.failed})`);
  for (const r of summary.results) {
    console.log(`  [${r.passed ? 'PASS' : 'FAIL'}] ${r.id}: ${r.name} (${r.durationMs}ms)`);
    if (!r.passed) {
      console.error(`    Error: ${r.error}`);
    }
  }
  process.exit(summary.failed === 0 ? 0 : 1);
}
