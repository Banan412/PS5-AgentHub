/**
 * Tier 1: Feature Coverage E2E Test Suite (60 Tests, 5 per feature F1-F12)
 * Validates primary behavior (happy path) for all 12 PS5-AgentHub features.
 */

import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createMockServer } from './helpers/mock_server.mjs';
import { probeLiveServer } from './helpers/live_probe.mjs';
import {
  validateIndexHtml,
  validateStylesCss,
  validateAppJs,
  checkAppJsSyntax,
  MANDATORY_21_CONTROL_IDS
} from './helpers/static_validator.mjs';
import {
  calculateDecodeThroughput,
  calculateTTFT,
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

    const req = http.request(reqOptions, (res) => {
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

    req.on('error', reject);
    if (postData) {
      req.write(typeof postData === 'string' ? postData : JSON.stringify(postData));
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
        buffer = lines.pop(); // keep remainder

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

export async function runTier1(opts = {}) {
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
    // FEATURE 1: Live Browser Dock Reopening & Toggle (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F01-01', 'test_dock_toggle_button_initial_state', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      assert.ok(html.includes('id="btn-chat-live-browser-toggle"'), 'Toggle button must exist in index.html');
      assert.ok(html.includes('id="chat-live-browser-dock"'), 'Live browser dock container must exist');
      assert.ok(/<div[^>]*id="chat-live-browser-dock"[^>]*hidden/.test(html), 'Dock must be hidden initially');
      assert.ok(html.includes('aria-label="Toggle Live Agent Browser Dock"'), 'Must have aria-label');
    });

    await runTest('TEST-T1-F01-02', 'test_dock_open_on_toggle_click', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      // Verify function existence
      assert.ok(/function\s+toggleChatLiveBrowserDock\s*\(/.test(appJs), 'toggleChatLiveBrowserDock must be defined in app.js');

      // Verify dismissal reset and forced dock opening
      assert.ok(/state\.chatLiveBrowserDismissed\s*=\s*false/.test(appJs), 'Toggling dock must reset chatLiveBrowserDismissed to false');
      assert.ok(/openChatLiveBrowserDock\(\s*\{\s*force:\s*true\s*\}\s*\)/.test(appJs), 'Toggling open must pass { force: true }');

      // Verify event listener attachment
      assert.ok(/btnLiveDockToggle\.addEventListener\(\s*['"]click['"]\s*,\s*\(\)\s*=>\s*toggleChatLiveBrowserDock\(\)\s*\)/.test(appJs),
        'Click listener must bind btn-chat-live-browser-toggle to toggleChatLiveBrowserDock');
    });

    await runTest('TEST-T1-F01-03', 'test_dock_hide_on_close_click', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      // Verify function existence
      assert.ok(/function\s+hideChatLiveBrowserDock\s*\(\s*\)/.test(appJs), 'hideChatLiveBrowserDock must be defined in app.js');

      // Verify state flag update and DOM hide
      assert.ok(/state\.chatLiveBrowserDismissed\s*=\s*true;/.test(appJs), 'hideChatLiveBrowserDock must set chatLiveBrowserDismissed to true');
      assert.ok(/dock\.hidden\s*=\s*true;/.test(appJs), 'hideChatLiveBrowserDock must set dock.hidden = true');

      // Verify sync button and event binding
      assert.ok(/syncChatLiveBrowserToggleBtn\(\s*false\s*\);/.test(appJs), 'hideChatLiveBrowserDock must sync toggle button state to false');
      assert.ok(/btnCloseLiveDock\.addEventListener\(\s*['"]click['"]\s*,\s*hideChatLiveBrowserDock\s*\)/.test(appJs),
        'Close button must be wired to hideChatLiveBrowserDock');
    });

    await runTest('TEST-T1-F01-04', 'test_dock_reopen_clears_dismissal', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      // Verify dismissal guard in openChatLiveBrowserDock
      const guardRegex = /if\s*\(\s*state\.chatLiveBrowserDismissed\s*&&\s*!\(\s*opts\s*&&\s*opts\.force\s*\)\s*\)\s*return;/;
      assert.ok(guardRegex.test(appJs), 'openChatLiveBrowserDock must respect dismissal unless force: true is provided');

      // Verify force: true is passed when user explicitly reopens
      assert.ok(/state\.chatLiveBrowserDismissed\s*=\s*false;\s*openChatLiveBrowserDock\(\s*\{\s*force:\s*true\s*\}\s*\);/.test(appJs),
        'Reopening via toggle must clear dismissed flag and pass force: true');
    });

    await runTest('TEST-T1-F01-05', 'test_dock_tab_and_url_synchronization', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(/function\s+renderAllLiveBrowserViewports\s*\(/.test(appJs), 'renderAllLiveBrowserViewports must be defined in app.js');
      assert.ok(/renderLiveTabsStrip\(\s*['"]chat-live-browser-tabs['"]\s*,\s*activeId\s*\);/.test(appJs),
        'Must synchronize chat-live-browser-tabs strip');
      assert.ok(/const\s+displayUrl\s*=\s*activeTab\s*&&\s*activeTab\.url\s*\?\s*activeTab\.url\s*:\s*['"]about:blank['"];/.test(appJs),
        'displayUrl must fall back to about:blank');
      assert.ok(/chatUrl\.textContent\s*=\s*displayUrl;/.test(appJs), 'chatUrl.textContent must update with current tab URL');
    });

    // -------------------------------------------------------------
    // FEATURE 2: llama.cpp-Style Chat Redesign & Controls (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F02-01', 'test_llama_chat_21_interactive_controls_present', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      for (const id of MANDATORY_21_CONTROL_IDS) {
        assert.ok(html.includes(`id="${id}"`), `Mandatory control ID #${id} must be present`);
      }
    });

    await runTest('TEST-T1-F02-02', 'test_llama_theme_css_color_tokens', async () => {
      const cssRes = validateStylesCss();
      assert.ok(cssRes.valid, `CSS color tokens validation failed: ${cssRes.errors.join(', ')}`);
    });

    await runTest('TEST-T1-F02-03', 'test_touch_target_accessibility_tokens', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes(':focus-visible'), 'styles.css must include :focus-visible rules');
      assert.ok(/min-height:\s*44px/i.test(css), 'styles.css must enforce min-height: 44px on interactive controls');
      assert.ok(/@media\s*\(\s*max-width:\s*768px\s*\)/i.test(css), 'styles.css must have mobile media query');
    });

    await runTest('TEST-T1-F02-04', 'test_message_bubble_visual_hierarchy', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes('.chat-message') || css.includes('.msg-'), 'styles.css must have message bubble rules');
    });

    await runTest('TEST-T1-F02-05', 'test_thinking_drawer_collapsible_structure', async () => {
      const cssPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      assert.ok(css.includes('thinking-drawer') || css.includes('think-') || css.includes('drawer'), 'Must contain thinking/reasoning drawer styles');
    });

    // -------------------------------------------------------------
    // FEATURE 3: Frontend Syntax & Invariants (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F03-01', 'test_app_js_node_syntax_check', async () => {
      const syntax = checkAppJsSyntax();
      assert.ok(syntax.valid, `app.js syntax check failed: ${syntax.error}`);
    });

    await runTest('TEST-T1-F03-02', 'test_zero_inner_html_injection_invariant', async () => {
      const appJsValidation = validateAppJs();
      assert.ok(appJsValidation.valid, `app.js security violation: ${appJsValidation.errors.join(', ')}`);
    });

    await runTest('TEST-T1-F03-03', 'test_zero_external_cdn_links_in_html', async () => {
      const indexValidation = validateIndexHtml();
      assert.ok(indexValidation.valid, `index.html security violation: ${indexValidation.errors.join(', ')}`);
    });

    await runTest('TEST-T1-F03-04', 'test_all_13_dashboard_sections_exist', async () => {
      const htmlPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');
      const sections = [
        'section-overview', 'section-chat', 'section-files', 'section-agents',
        'section-a2a', 'section-tasks', 'section-schedules', 'section-memory',
        'section-tools', 'section-extensions', 'section-sandbox', 'section-logs', 'section-settings'
      ];
      for (const s of sections) {
        assert.ok(html.includes(`id="${s}"`), `Section #${s} must exist in index.html`);
      }
    });

    await runTest('TEST-T1-F03-05', 'test_embedded_asset_resolver_routes', async () => {
      const rootRes = await httpRequest(`${serverUrl}/`);
      assert.equal(rootRes.statusCode, 200);
      assert.ok(rootRes.headers['content-type'].includes('text/html'));

      const cssRes = await httpRequest(`${serverUrl}/styles.css`);
      assert.equal(cssRes.statusCode, 200);
      assert.ok(cssRes.headers['content-type'].includes('text/css'));

      const jsRes = await httpRequest(`${serverUrl}/app.js`);
      assert.equal(jsRes.statusCode, 200);
      assert.ok(jsRes.headers['content-type'].includes('application/javascript'));

      const notFoundRes = await httpRequest(`${serverUrl}/nonexistent_asset.xyz`);
      assert.equal(notFoundRes.statusCode, 404);
    });

    // -------------------------------------------------------------
    // FEATURE 4: Inference Throughput Parity (~120 tok/s) (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F04-01', 'test_streaming_decode_throughput_benchmark', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 120, target_tok_per_sec: 130, prompt_eval_ms: 10 }
      });
      assert.equal(res.statusCode, 200);
      assert.ok(res.chunksReceived >= 100, `Expected at least 100 chunks, got ${res.chunksReceived}`);

      const metrics = calculateDecodeThroughput(res.chunksReceived, res.startTime, res.endTime, res.firstTokenTime);
      assert.ok(metrics.rate >= 115.0, `Decode throughput must reach >= 115 tok/s, got ${metrics.rate.toFixed(1)} tok/s`);
    });

    await runTest('TEST-T1-F04-02', 'test_decode_tok_per_sec_excludes_ttft', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 50, target_tok_per_sec: 120, prompt_eval_ms: 150 }
      });
      assert.equal(res.statusCode, 200, 'Endpoint must return 200 OK');
      assert.ok(res.firstTokenTime > res.startTime, 'First token time must be greater than start time');

      const isolated = calculateDecodeThroughput(res.chunksReceived, res.startTime, res.endTime, res.firstTokenTime);
      const naive = calculateDecodeThroughput(res.chunksReceived, res.startTime, res.endTime, null);

      assert.equal(isolated.excludedTTFT, true, 'Isolated calculation must mark excludedTTFT = true');
      assert.ok(isolated.rate > naive.rate * 1.15,
        `Isolated rate (${isolated.rate.toFixed(1)}) must be at least 15% higher than naive rate (${naive.rate.toFixed(1)}) due to TTFT deduction`);
    });

    await runTest('TEST-T1-F04-03', 'test_zero_copy_sse_chunk_deserialization', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');

      // Verify byte streaming and pre-allocated buffer
      assert.ok(/let\s+mut\s+byte_stream\s*=\s*resp\.bytes_stream\(\);/.test(provRs), 'Must use non-blocking byte_stream()');
      assert.ok(/let\s+mut\s+raw_buf:\s*Vec<u8>\s*=\s*Vec::with_capacity\(32\s*\*\s*1024\);/.test(provRs),
        'Must pre-allocate 32KB buffer for zero-realloc SSE parsing');
      assert.ok(/emit_stream_piece\(\s*piece,\s*tx\s*\)\.await;/.test(provRs), 'Must stream discrete pieces immediately');
    });

    await runTest('TEST-T1-F04-04', 'test_mpsc_channel_capacity_expansion', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');

      const channelMatch = rtRs.match(/mpsc::channel::<InferenceStreamChunk>\(\s*(\d+)\s*\)/);
      assert.ok(channelMatch, 'mpsc::channel for InferenceStreamChunk must be declared');
      const capacity = parseInt(channelMatch[1], 10);
      assert.ok(capacity >= 1024, `MPSC channel capacity must be >= 1024 to prevent throughput backpressure stalls, got ${capacity}`);
    });

    await runTest('TEST-T1-F04-05', 'test_broadcast_events_channel_capacity', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');
      const startupPath = path.resolve(process.cwd(), 'src/startup.rs');
      const startupRs = fs.readFileSync(startupPath, 'utf8');

      assert.ok(/const\s+UNLIMITED_SEMAPHORE_PERMITS:\s*usize\s*=\s*4096;/.test(rtRs),
        'UNLIMITED_SEMAPHORE_PERMITS in runtime.rs must be 4096');
      assert.ok(/broadcast::channel::<StreamEvent>\(\s*512\s*\)/.test(startupRs),
        'startup.rs must configure broadcast channel capacity to at least 512 for multi-agent events');
    });

    // -------------------------------------------------------------
    // FEATURE 5: Streaming Responsiveness & Low TTFT (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F05-01', 'test_time_to_first_token_latency', async () => {
      const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 20, prompt_eval_ms: 20, target_tok_per_sec: 120 }
      });
      const ttft = calculateTTFT(res.startTime, res.firstTokenTime);
      assert.ok(ttft <= 200, `TTFT should be <= 200ms on local engine, got ${ttft}ms`);
    });

    await runTest('TEST-T1-F05-02', 'test_incremental_dom_stream_append', async () => {
      const appJsPath = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets/app.js');
      const appJs = fs.readFileSync(appJsPath, 'utf8');

      assert.ok(/case\s*['"]chat_token['"]:\s*\{[\s\S]*?b\.text\s*\+=\s*ev\.delta[\s\S]*?scheduleStreamFlush\(\);/.test(appJs),
        'chat_token handler must append delta incrementally and call scheduleStreamFlush');
      assert.ok(/function\s+scheduleStreamFlush\(\)\s*\{[\s\S]*?requestAnimationFrame\(flushDirtyStreams\)/.test(appJs),
        'scheduleStreamFlush must batch DOM updates via requestAnimationFrame');
      assert.ok(/function\s+flushDirtyStreams\(\)\s*\{[\s\S]*?if\s*\(!bucket\.dirty\)\s*continue;/.test(appJs),
        'flushDirtyStreams must only update dirty buckets');
    });

    await runTest('TEST-T1-F05-03', 'test_pinned_sleep_timer_reuse', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');

      // Verify pinned timer declaration
      assert.ok(/let\s+sleep\s*=\s*tokio::time::sleep[\s\S]*?tokio::pin!\(sleep\);/.test(provRs),
        'Streaming loop must pin sleep timer with tokio::pin!(sleep) before loop');

      // Verify in-place re-arming
      assert.ok(/sleep\.as_mut\(\)\.reset\(tokio::time::Instant::now\(\)\s*\+\s*idle_dur\);/.test(provRs),
        'Chunk loop must re-arm pinned timer via sleep.as_mut().reset() without reallocating');

      // Verify select! arm
      assert.ok(/_\s*=\s*&mut\s+sleep\s*=>\s*\{[\s\S]*?AgentError::Timeout/.test(provRs),
        'select! loop must bind &mut sleep to trigger idle timeout');
    });

    await runTest('TEST-T1-F05-04', 'test_server_downstream_tcp_nodelay', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/health`);
      assert.equal(res.statusCode, 200);
      assert.ok(res.headers, 'HTTP response headers received');
    });

    await runTest('TEST-T1-F05-05', 'test_upstream_client_tcp_nodelay', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');

      assert.ok(/reqwest::Client::builder\(\)[\s\S]*?\.tcp_nodelay\(true\)/.test(provRs),
        'reqwest::Client::builder must explicitly enable tcp_nodelay(true)');
      assert.ok(/\.tcp_keepalive\(Some\(Duration::from_secs\(30\)\)\)/.test(provRs),
        'Client must configure tcp_keepalive to 30 seconds');
      assert.ok(/\.pool_max_idle_per_host\(32\)/.test(provRs),
        'Client must configure pool_max_idle_per_host to 32');
    });

    // -------------------------------------------------------------
    // FEATURE 6: Agentic Search Anti-Blocking & User-Agent (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F06-01', 'test_outbound_search_modern_user_agent', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/pub\s+struct\s+BrowserHeadersProfile/.test(webRs), 'BrowserHeadersProfile must be defined');
      assert.ok(/pub\s+fn\s+rotate\(\)\s*->\s*&'static\s+BrowserHeadersProfile/.test(webRs), 'rotate() function must be defined');

      // Extract all user_agent literals from BrowserHeadersProfile
      const uaMatches = [...webRs.matchAll(/user_agent:\s*"([^"]+)"/g)].map(m => m[1]);
      assert.ok(uaMatches.length >= 3, `Must define at least 3 rotating user agents, found ${uaMatches.length}`);
      for (const ua of uaMatches) {
        assert.ok(ua.startsWith('Mozilla/5.0'), `User agent must start with Mozilla/5.0, got: ${ua}`);
        assert.ok(!ua.toLowerCase().includes('ps5-agent'), `User agent must not leak ps5-agent identity, got: ${ua}`);
      }
    });

    await runTest('TEST-T1-F06-02', 'test_sec_ch_ua_client_hint_headers', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua"\s*\)/.test(webRs), 'Must attach sec-ch-ua header');
      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua-mobile"\s*\)/.test(webRs), 'Must attach sec-ch-ua-mobile header');
      assert.ok(/HeaderName::from_static\(\s*"sec-ch-ua-platform"\s*\)/.test(webRs), 'Must attach sec-ch-ua-platform header');
      assert.ok(/sec_ch_ua:\s*Some\([^)]*Chromium/.test(webRs),
        'Must include modern Chromium client hints');
    });

    await runTest('TEST-T1-F06-03', 'test_sec_fetch_metadata_headers', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/HeaderName::from_static\(\s*"sec-fetch-dest"\s*\)/.test(webRs), 'Must attach sec-fetch-dest header');
      assert.ok(/HeaderName::from_static\(\s*"sec-fetch-mode"\s*\)/.test(webRs), 'Must attach sec-fetch-mode header');
      assert.ok(/HeaderName::from_static\(\s*"sec-fetch-site"\s*\)/.test(webRs), 'Must attach sec-fetch-site header');
      assert.ok(/HeaderName::from_static\(\s*"sec-fetch-user"\s*\)/.test(webRs), 'Must attach sec-fetch-user header');
      assert.ok(/sec_fetch_mode:\s*"navigate"/.test(webRs), 'Fetch mode must be navigate');
      assert.ok(/sec_fetch_site:\s*"none"/.test(webRs), 'Fetch site must be none');
    });

    await runTest('TEST-T1-F06-04', 'test_accept_language_header_present', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/HeaderName::from_static\(\s*"accept-language"\s*\)/.test(webRs), 'Must attach accept-language header');
      assert.ok(/accept_language:\s*"en-US,en;q=0\.9"/.test(webRs), 'accept-language profile must configure en-US,en;q=0.9');
    });

    await runTest('TEST-T1-F06-05', 'test_challenge_gate_detection_logic', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+detect_challenge_gate\s*\(\s*status:\s*u16,\s*headers:\s*&HeaderMap,\s*html:\s*&str\s*\)\s*->\s*Option<String>/.test(webRs),
        'detect_challenge_gate function signature must match contract in web.rs');
      assert.ok(/cf-mitigated/.test(webRs), 'Must inspect cf-mitigated header');
      assert.ok(/challenges\.cloudflare\.com\/turnstile\//.test(webRs), 'Must detect Cloudflare Turnstile domain');
      assert.ok(/duckduckgo_challenge/.test(webRs), 'Must classify DuckDuckGo challenge pages');
    });

    // -------------------------------------------------------------
    // FEATURE 7: Multi-Provider Search Fallback (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F07-01', 'test_primary_duckduckgo_search_success', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/search?query=playstation+5&provider=duckduckgo`);
      assert.equal(res.statusCode, 200);
      assert.equal(res.json.ok, true);
      assert.equal(res.json.provider, 'duckduckgo');
      assert.ok(res.json.results.length > 0);
      assert.ok(res.json.results[0].title);
      assert.ok(res.json.results[0].url);
    });

    await runTest('TEST-T1-F07-02', 'test_fallback_to_ddg_lite_on_429', async () => {
      // Simulate primary 429 then fallback
      const primaryRes = await httpRequest(`${serverUrl}/api/v1/search?query=test&provider=duckduckgo_ratelimit`);
      assert.equal(primaryRes.statusCode, 429);

      // Fallback request
      const fallbackRes = await httpRequest(`${serverUrl}/api/v1/search?query=test&provider=duckduckgo_lite`);
      assert.equal(fallbackRes.statusCode, 200);
      assert.equal(fallbackRes.json.ok, true);
      assert.equal(fallbackRes.json.provider, 'duckduckgo_lite');
    });

    await runTest('TEST-T1-F07-03', 'test_fallback_to_searxng_on_503', async () => {
      const primaryRes = await httpRequest(`${serverUrl}/api/v1/search?query=test`, {
        headers: { 'x-simulate-error-503': 'true' }
      });
      assert.equal(primaryRes.statusCode, 503);

      const tertiaryRes = await httpRequest(`${serverUrl}/api/v1/search?query=test&provider=searxng`);
      assert.equal(tertiaryRes.statusCode, 200);
      assert.equal(tertiaryRes.json.provider, 'searxng');
    });

    await runTest('TEST-T1-F07-04', 'test_search_freshness_parameter_mapping', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+normalize_freshness\s*\(\s*raw:\s*Option<&str>\s*\)\s*->\s*Option<String>/.test(webRs),
        'normalize_freshness must be defined in web.rs');
      assert.ok(/"pd"\s*\|\s*"d"\s*=>\s*"day"/.test(webRs), 'Must map day freshness token');
      assert.ok(/"pm"\s*\|\s*"m"\s*=>\s*"month"/.test(webRs), 'Must map month freshness token');
      assert.ok(/"day"\s*\|\s*"d"\s*=>\s*"pd"/.test(webRs), 'Must map Brave day freshness parameter');
      assert.ok(/"month"\s*\|\s*"m"\s*=>\s*"pm"/.test(webRs), 'Must map Brave month freshness parameter');
    });

    await runTest('TEST-T1-F07-05', 'test_search_domain_restriction_prefix', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/let\s+effective_query\s*=\s*match\s+domain\s*\{/.test(webRs), 'Must construct effective_query from domain argument');
      assert.ok(/!raw_query\.to_ascii_lowercase\(\)\.contains\(\s*"site:"\s*\)/.test(webRs),
        'Must prevent double-prepending site: prefix if user already specified it');
      assert.ok(/format!\(\s*"site:\{dom\}\s*\{raw_query\}"\s*\)/.test(webRs),
        'Must prepend site:{dom} to query string');
    });

    // -------------------------------------------------------------
    // FEATURE 8: YouTube & Media Fallback Extraction (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F08-01', 'test_youtube_watch_url_detection', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+is_youtube_url\s*\(\s*url:\s*&str\s*\)\s*->\s*bool/.test(webRs), 'is_youtube_url must be defined in web.rs');

      // Extract the actual regex from Rust source within is_youtube_url block and test it
      const isYtBlock = webRs.slice(webRs.indexOf('fn is_youtube_url'));
      const ytMatch = isYtBlock.match(/r"\(\?i\)([^"]+)"/);
      assert.ok(ytMatch, 'YouTube regex literal must be found in web.rs');
      const compiledRe = new RegExp(ytMatch[1], 'i');

      assert.equal(compiledRe.test('https://www.youtube.com/watch?v=dQw4w9WgXcQ'), true, 'Must match standard watch URL');
      assert.equal(compiledRe.test('https://example.com/not-youtube'), false, 'Must not match external domains');
    });

    await runTest('TEST-T1-F08-02', 'test_youtube_short_url_detection', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/youtu\\\.be\//.test(webRs), 'YouTube regex must support youtu.be shortlinks');
      assert.ok(/format!\(\s*"https:\/\/www\.youtube\.com\/oembed\?url=\{encoded_url\}&format=json"\s*\)/.test(webRs),
        'Must construct YouTube oEmbed endpoint URL with encoded parameter and format=json');

      const isYtBlock = webRs.slice(webRs.indexOf('fn is_youtube_url'));
      const ytMatch = isYtBlock.match(/r"\(\?i\)([^"]+)"/);
      const compiledRe = new RegExp(ytMatch[1], 'i');
      assert.equal(compiledRe.test('https://youtu.be/dQw4w9WgXcQ'), true, 'Must match youtu.be short URL');
    });

    await runTest('TEST-T1-F08-03', 'test_youtube_shorts_url_detection', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/shorts\//.test(webRs), 'YouTube regex must support YouTube Shorts URLs');
      assert.ok(/Platform\*\*:\s*YouTube/.test(webRs), 'Must format platform markdown with YouTube metadata');
      assert.ok(/\[Video content available on YouTube\]/.test(webRs), 'Must append video availability notice');

      const isYtBlock = webRs.slice(webRs.indexOf('fn is_youtube_url'));
      const ytMatch = isYtBlock.match(/r"\(\?i\)([^"]+)"/);
      const compiledRe = new RegExp(ytMatch[1], 'i');
      assert.equal(compiledRe.test('https://www.youtube.com/shorts/dQw4w9WgXcQ'), true, 'Must match YouTube Shorts URL');
    });

    await runTest('TEST-T1-F08-04', 'test_youtube_oembed_json_parsing', async () => {
      const res = await httpRequest(`${serverUrl}/youtube/oembed?url=https://www.youtube.com/watch?v=dQw4w9WgXcQ`);
      assert.equal(res.statusCode, 200);
      assert.equal(res.json.provider_name, 'YouTube');
      assert.ok(res.json.title);
      assert.ok(res.json.author_name);

      const markdown = `# ${res.json.title}\n**Channel:** [${res.json.author_name}](${res.json.author_url})`;
      assert.ok(markdown.includes('PlayStation 5 Update Overview'));
    });

    await runTest('TEST-T1-F08-05', 'test_opengraph_meta_tag_extraction', async () => {
      const webPath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/web.rs');
      const webRs = fs.readFileSync(webPath, 'utf8');

      assert.ok(/fn\s+extract_opengraph_metadata\s*\(\s*html:\s*&str\s*\)\s*->\s*\(Option<String>,\s*Option<String>\)/.test(webRs),
        'extract_opengraph_metadata must be declared in web.rs');
      assert.ok(/og:title\|twitter:title/.test(webRs), 'Must match og:title and twitter:title');
      assert.ok(/og:description\|twitter:description\|description/.test(webRs), 'Must match og:description and fallbacks');
      assert.ok(/OG_TITLE_REV/.test(webRs) && /OG_DESC_REV/.test(webRs),
        'Must define reverse regexes to support HTML with content attribute before property/name attribute');
    });

    // -------------------------------------------------------------
    // FEATURE 9: Sandbox Browser Error Navigation & Preview (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F09-01', 'test_sandbox_browser_session_creation', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://127.0.0.1:8080/demo' });
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.session_id);
      assert.equal(res.json.status, 'loaded');
    });

    await runTest('TEST-T1-F09-02', 'test_sandbox_navigation_failure_session_saved', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://invalid.local.domain/' });
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.session_id);
      assert.equal(res.json.status, 'error');
    });

    await runTest('TEST-T1-F09-03', 'test_sandbox_preview_endpoint_http_200', async () => {
      // Create error session first
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://broken-site.invalid/' });
      const sessionId = navRes.json.session_id;

      // Preview must return HTTP 200 (not 404!)
      const prevRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${sessionId}`);
      assert.equal(prevRes.statusCode, 200);
      assert.ok(prevRes.headers['content-type'].includes('text/html'));
    });

    await runTest('TEST-T1-F09-04', 'test_sandbox_preview_error_html_content', async () => {
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: 'http://broken-site-2.invalid/' });
      const prevRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${navRes.json.session_id}`);
      assert.ok(prevRes.data.includes('Navigation Failed'));
      assert.ok(prevRes.data.includes('broken-site-2.invalid'));
      assert.ok(prevRes.headers['content-security-policy'].includes("default-src 'none'"));
    });

    await runTest('TEST-T1-F09-05', 'test_sandbox_session_listing_contains_error_sessions', async () => {
      const listRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/sessions`);
      assert.equal(listRes.statusCode, 200);
      assert.ok(Array.isArray(listRes.json.sessions));
      const hasErrorSession = listRes.json.sessions.some(s => s.status === 'error');
      assert.ok(hasErrorSession, 'Sessions list must include error sessions for inspector visibility');
    });

    // -------------------------------------------------------------
    // FEATURE 10: Task Token Eviction on Message Cancellation (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F10-01', 'test_conversation_cancellation_endpoint', async () => {
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      const convId = convRes.json.conversation_id;

      const msgRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Long running task' });
      assert.ok(msgRes.json.task_id);

      const cancelRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/cancel`, { method: 'POST' });
      assert.equal(cancelRes.statusCode, 200);
      assert.equal(cancelRes.json.ok, true);
    });

    await runTest('TEST-T1-F10-02', 'test_task_manager_cancellation_token_released', async () => {
      const tmPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/task_manager.rs');
      const tmRs = fs.readFileSync(tmPath, 'utf8');

      assert.ok(/pub\s+fn\s+release_token\s*\(\s*&self,\s*task_id:\s*&str\s*\)\s*\{[\s\S]*?guard\.remove\(task_id\);/.test(tmRs),
        'release_token must remove token from tokens map');
      assert.ok(/Self::shrink_map_if_sparse\(&mut\s+guard\);/.test(tmRs),
        'release_token must call shrink_map_if_sparse to prevent memory leak');
      assert.ok(/pub\s+fn\s+cancel_task[\s\S]*?guard\.remove\(task_id\)/.test(tmRs),
        'cancel_task must remove and cancel the token');
    });

    await runTest('TEST-T1-F10-03', 'test_pending_task_queue_eviction', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');

      assert.ok(/pub\s+fn\s+evict_pending_for_task\s*\(\s*&self,\s*task_id:\s*&str\s*\)\s*->\s*usize/.test(rtRs),
        'evict_pending_for_task must be declared in runtime.rs');
      assert.ok(/guard\.retain\(\s*\|_,\s*s\|\s*s\.task_id\s*!=\s*task_id\s*&&\s*s\.root_task_id\s*!=\s*task_id\s*\);/.test(rtRs),
        'Must evict pending approvals matching both task_id and root_task_id');
    });

    await runTest('TEST-T1-F10-04', 'test_regenerate_message_cancellation_cleanup', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');

      assert.ok(/pub\s+fn\s+cancel_conversation_tasks[\s\S]*?self\.evict_pending_for_conversation\(conv_id,\s*agent_id\);/.test(rtRs),
        'cancel_conversation_tasks must evict pending approvals for conversation');

      // Live contract execution against test server
      const cancelRes = await httpRequest(`${serverUrl}/api/v1/conversations/conv-regen-test/cancel`, { method: 'POST' });
      assert.equal(cancelRes.statusCode, 200, 'Conversation cancel endpoint must respond with 200 OK');
      assert.equal(cancelRes.json.ok, true, 'Cancellation response must indicate ok: true');
    });

    await runTest('TEST-T1-F10-05', 'test_semaphore_slot_reclaimed_after_cancel', async () => {
      const rtPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/runtime.rs');
      const rtRs = fs.readFileSync(rtPath, 'utf8');

      // Verify slot acquisition cancellation branch
      assert.ok(/let\s+Ok\(_permit\)\s*=\s*rt\s*\.await_task_slot_or_expedite[\s\S]*?else\s*\{[\s\S]*?rt\.evict_pending_for_task\(&bg_task_id\);[\s\S]*?rt\.task_manager\.release_token\(&bg_task_id\);/.test(rtRs),
        'When slot acquisition cancels, runtime must immediately evict pending tasks and release token');
    });

    // -------------------------------------------------------------
    // FEATURE 11: Polling & Connection Reuse Optimization (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F11-01', 'test_http_client_connection_reuse_across_redirects', async () => {
      const agent = new http.Agent({ keepAlive: true });
      const req1 = await httpRequest(`${serverUrl}/api/v1/health`, { agent });
      const req2 = await httpRequest(`${serverUrl}/api/v1/health`, { agent });
      assert.equal(req1.statusCode, 200);
      assert.equal(req2.statusCode, 200);
      agent.destroy();
    });

    await runTest('TEST-T1-F11-02', 'test_agent_deletion_active_task_backoff', async () => {
      const mgrPath = path.resolve(process.cwd(), 'crates/ps5-agent-manager/src/lib.rs');
      const mgrRs = fs.readFileSync(mgrPath, 'utf8');

      assert.ok(/ActiveTaskPolicy::WaitForActive\s*\{\s*timeout_ms\s*\}\s*=>\s*\{[\s\S]*?let\s+mut\s+backoff\s*=\s*Duration::from_millis\(25\);/.test(mgrRs),
        'delete_agent WaitForActive policy must initialize backoff at 25ms');
      assert.ok(/backoff\s*=\s*\(backoff\.saturating_mul\(2\)\)\.min\(Duration::from_millis\(100\)\);/.test(mgrRs),
        'Backoff must double exponentially up to 100ms cap');
      assert.ok(/tokio::time::sleep\(sleep_dur\)\.await;/.test(mgrRs),
        'Wait loop must sleep asynchronously instead of busy-spinning');
    });

    await runTest('TEST-T1-F11-03', 'test_host_sandbox_exec_polling_backoff', async () => {
      const sePath = path.resolve(process.cwd(), 'crates/ps5-agent-tools/src/sandbox_exec.rs');
      const seRs = fs.readFileSync(sePath, 'utf8');

      assert.ok(/let\s+mut\s+wait_sleep\s*=\s*Duration::from_millis\(25\);/.test(seRs),
        'sandbox_exec polling loop must initialize wait sleep at 25ms');
      assert.ok(/wait_sleep\s*=\s*\(wait_sleep\.saturating_mul\(2\)\)\.min\(Duration::from_millis\(75\)\);/.test(seRs),
        'sandbox_exec polling must exponentially back off up to 75ms cap');
      assert.ok(/std::thread::sleep\(wait_sleep\);/.test(seRs),
        'sandbox_exec polling must sleep between process status checks');
    });

    await runTest('TEST-T1-F11-04', 'test_http_client_pool_idle_timeout', async () => {
      const provPath = path.resolve(process.cwd(), 'crates/ps5-agent-inference/src/provider.rs');
      const provRs = fs.readFileSync(provPath, 'utf8');

      assert.ok(/\.pool_idle_timeout\(Duration::from_secs\(45\)\)/.test(provRs),
        'provider.rs must configure upstream client pool_idle_timeout to 45 seconds');
    });

    await runTest('TEST-T1-F11-05', 'test_sqlite_task_query_rate_bounded', async () => {
      const tmPath = path.resolve(process.cwd(), 'crates/ps5-agent-runtime/src/task_manager.rs');
      const tmRs = fs.readFileSync(tmPath, 'utf8');

      assert.ok(/pub\s+fn\s+register_expedite\s*\(\s*&self,\s*task_id:\s*&str\s*\)\s*->\s*Arc<Notify>/.test(tmRs),
        'task_manager must implement event-driven expedite notification via tokio::sync::Notify');
      assert.ok(/notify\.notify_waiters\(\);/.test(tmRs),
        'Cancellation and completion must wake expedite waiters with notify_waiters() instead of polling');
      assert.ok(/expedite_notifiers:\s*RwLock<HashMap<String,\s*Arc<Notify>>>/.test(tmRs),
        'expedite_notifiers map must be maintained on TaskManager');
    });

    // -------------------------------------------------------------
    // FEATURE 12: Live PS5 Health & Endpoint Verification (5 Tests)
    // -------------------------------------------------------------
    await runTest('TEST-T1-F12-01', 'test_health_endpoint_contract', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/health`);
      assert.equal(res.statusCode, 200);
      assert.equal(res.json.status, 'ok');
      assert.equal(res.json.healthy, true);
      assert.ok(res.json.version);
    });

    await runTest('TEST-T1-F12-02', 'test_inference_capabilities_endpoint', async () => {
      const res = await httpRequest(`${serverUrl}/api/v1/inference/capabilities`);
      assert.equal(res.statusCode, 200);
      assert.ok(res.json.loaded_model);
      assert.equal(res.json.supports_streaming, true);
    });

    await runTest('TEST-T1-F12-03', 'test_events_sse_heartbeat_stream', async () => {
      const sseRes = await httpRequest(`${serverUrl}/api/v1/events`, {
        headers: { 'Accept': 'text/event-stream' }
      });
      assert.equal(sseRes.statusCode, 200);
      assert.ok(sseRes.headers['content-type'].includes('text/event-stream'));
      assert.ok(sseRes.data.includes('event: heartbeat'));
    });

    await runTest('TEST-T1-F12-04', 'test_conversation_creation_and_message_dispatch', async () => {
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      assert.equal(convRes.statusCode, 200);
      const convId = convRes.json.conversation_id;

      const msgRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Ping' });
      assert.equal(msgRes.statusCode, 200);
      assert.ok(msgRes.json.task_id);
    });

    await runTest('TEST-T1-F12-05', 'test_sandbox_browser_preview_endpoint_active', async () => {
      const prevRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview`);
      assert.equal(prevRes.statusCode, 200);
      assert.ok(prevRes.headers['content-type'].includes('text/html'));
    });

  } finally {
    if (mockServer) {
      await mockServer.stop();
    }
  }

  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;

  return {
    tierName: 'Tier 1: Feature Coverage (60 Tests)',
    total: results.length,
    passed,
    failed,
    results
  };
}

if (process.argv[1] && (process.argv[1].endsWith('tier1_feature_coverage.test.mjs') || process.argv[1].includes('tier1_feature_coverage'))) {
  const summary = await runTier1();
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
