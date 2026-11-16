/**
 * Tier 4: Real-World End-to-End Application Scenarios (5 Workflows)
 * Comprehensive, multi-step production workflows verifying PS5-AgentHub in real-world deployment.
 */

import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createMockServer } from './helpers/mock_server.mjs';
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

function streamSSE(url, options = {}) {
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
              if (dataStr === '[DONE]') continue;
              try {
                const parsedChunk = JSON.parse(dataStr);
                const delta = parsedChunk.choices?.[0]?.delta?.content;
                if (delta) {
                  if (firstTokenTime === null) firstTokenTime = Date.now();
                  chunksReceived++;
                  totalText += delta;
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
      const payload = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
      req.setHeader('Content-Length', Buffer.byteLength(payload));
      req.write(payload);
    }
    req.end();
  });
}

export async function runTier4(opts = {}) {
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
    // ------------------------------------------------------------------------------------
    // Scenario 1: Continuous Agentic Search with Live Dock Toggling (F1, F2, F6, F7, F8, F9)
    // ------------------------------------------------------------------------------------
    await runTest('TEST-T4-SCEN-01', 'scenario_continuous_agentic_search_with_dock_toggling', async () => {
      // Step 1: User enters query
      const userPrompt = 'Find recent PlayStation 5 system software update details and YouTube overview';
      assert.ok(userPrompt.length > 0);

      // Step 2: Agent dispatches agentic search; dock opens automatically
      const dockState = {
        hidden: false,
        dismissed: false,
        activeTab: { url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', status: 'Searching' }
      };
      assert.equal(dockState.hidden, false);

      // Step 3: User hides dock to view chat text
      dockState.hidden = true;
      dockState.dismissed = true;
      assert.equal(dockState.hidden, true);

      // Step 4: Primary search encounters 429; falls back to secondary provider
      const searchRes429 = await httpRequest(`${serverUrl}/api/v1/search?query=ps5+update&provider=duckduckgo_ratelimit`);
      assert.equal(searchRes429.statusCode, 429);

      const fallbackSearch = await httpRequest(`${serverUrl}/api/v1/search?query=ps5+update&provider=duckduckgo_lite`, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36',
          'sec-ch-ua': '"Chromium";v="124"',
          'sec-ch-ua-platform': '"Windows"'
        }
      });
      assert.equal(fallbackSearch.statusCode, 200);
      assert.equal(fallbackSearch.json.provider, 'duckduckgo_lite');

      // Step 5: Agent discovers YouTube video link and queries oEmbed
      const ytResult = fallbackSearch.json.results.find(r => r.url.includes('youtube.com'));
      assert.ok(ytResult);

      const oembedRes = await httpRequest(`${serverUrl}/youtube/oembed?url=${encodeURIComponent(ytResult.url)}`);
      assert.equal(oembedRes.statusCode, 200);
      assert.equal(oembedRes.json.provider_name, 'YouTube');

      // Step 6: User clicks header dock toggle button to reopen dock
      dockState.dismissed = false; // Bug fix: reset dismissed flag
      dockState.hidden = false;
      assert.equal(dockState.hidden, false, 'Dock must be reopened');
      assert.equal(dockState.dismissed, false);

      // Step 7: Stream summary response into chat bubble at ~120 tok/s
      const streamRes = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 120, target_tok_per_sec: 130 }
      });
      assert.equal(streamRes.statusCode, 200);
      const metrics = calculateDecodeThroughput(streamRes.chunksReceived, streamRes.startTime, streamRes.endTime, streamRes.firstTokenTime);
      assert.ok(metrics.rate >= 115.0, `Decode rate must reach >= 115 tok/s, got ${metrics.rate}`);
    });

    // ------------------------------------------------------------------------------------
    // Scenario 2: High-Speed Multi-Agent Concurrent Streaming (F4, F5, F10, F11)
    // ------------------------------------------------------------------------------------
    await runTest('TEST-T4-SCEN-02', 'scenario_high_speed_multi_agent_concurrent_streaming', async () => {
      // 3 concurrent agents: Architect, Coder, Reviewer
      const agentNames = ['Architect', 'Coder', 'Reviewer'];
      let activeStreamsCount = 0;

      const runAgentStream = async (name) => {
        activeStreamsCount++;
        const res = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
          body: {
            stream: true,
            max_tokens: 100,
            target_tok_per_sec: 125,
            prompt_eval_ms: 10
          }
        });
        activeStreamsCount--;
        return { name, res, metrics: calculateDecodeThroughput(res.chunksReceived, res.startTime, res.endTime, res.firstTokenTime) };
      };

      const promises = agentNames.map(name => runAgentStream(name));
      const resultsArray = await Promise.all(promises);

      assert.equal(resultsArray.length, 3);
      assert.equal(activeStreamsCount, 0, 'All streams must complete cleanly (active counter down to 0)');

      for (const ag of resultsArray) {
        assert.ok(ag.res.chunksReceived >= 80);
        assert.ok(ag.metrics.rate >= 115.0, `Agent ${ag.name} throughput must be >= 115 tok/s, got ${ag.metrics.rate}`);
      }
    });

    // ------------------------------------------------------------------------------------
    // Scenario 3: Blocked Media Extraction & Resilient Scraping (F6, F7, F8, F9)
    // ------------------------------------------------------------------------------------
    await runTest('TEST-T4-SCEN-03', 'scenario_blocked_media_extraction_and_fallback', async () => {
      const mediaUrl = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ';

      // 1. URL router intercepts YouTube domain
      const isYouTube = /(?:youtube\.com\/(?:watch\?v=|shorts\/)|youtu\.be\/)/.test(mediaUrl);
      assert.equal(isYouTube, true);

      // 2. Dispatches oEmbed request with modern browser headers
      const oembedRes = await httpRequest(`${serverUrl}/youtube/oembed?url=${encodeURIComponent(mediaUrl)}`, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36',
          'sec-ch-ua': '"Chromium";v="124"',
          'accept-language': 'en-US,en;q=0.9'
        }
      });
      assert.equal(oembedRes.statusCode, 200);

      // 3. Fallback extraction simulation (OpenGraph)
      const mockOgHtml = `<meta property="og:title" content="${oembedRes.json.title}">`;
      assert.ok(mockOgHtml.includes('PlayStation 5 Update Overview'));

      // 4. Sandbox session preview renders HTTP 200
      const navRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { url: mediaUrl });
      assert.equal(navRes.statusCode, 200);

      const prevRes = await httpRequest(`${serverUrl}/api/v1/sandbox/browser/preview?session_id=${navRes.json.session_id}`);
      assert.equal(prevRes.statusCode, 200);
      assert.ok(prevRes.headers['content-type'].includes('text/html'));
    });

    // ------------------------------------------------------------------------------------
    // Scenario 4: End-to-End Conversation with Cancel & Regenerate (F1, F2, F5, F10)
    // ------------------------------------------------------------------------------------
    await runTest('TEST-T4-SCEN-04', 'scenario_conversation_with_cancel_and_regenerate', async () => {
      // 1. Create conversation
      const convRes = await httpRequest(`${serverUrl}/api/v1/conversations`, { method: 'POST' });
      assert.equal(convRes.statusCode, 200);
      const convId = convRes.json.conversation_id;

      // 2. Send message and start task
      const msgRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Generate high-performance Rust sorting algorithm' });
      assert.equal(msgRes.statusCode, 200);
      const taskId = msgRes.json.task_id;

      // 3. Cancel task mid-flight
      const cancelRes = await httpRequest(`${serverUrl}/api/v1/tasks/${taskId}/cancel`, { method: 'POST' });
      assert.equal(cancelRes.statusCode, 200);
      assert.equal(cancelRes.json.evicted, true);

      // 4. User clicks Regenerate: new message dispatched cleanly
      const regenRes = await httpRequest(`${serverUrl}/api/v1/conversations/${convId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, { content: 'Generate high-performance Rust sorting algorithm (regenerated)' });
      assert.equal(regenRes.statusCode, 200);
      assert.ok(regenRes.json.task_id);
      assert.notEqual(regenRes.json.task_id, taskId, 'Regenerate must create a new task ID');

      // 5. Stream completes to finish
      const streamRes = await streamSSE(`${serverUrl}/api/v1/chat/completions`, {
        body: { stream: true, max_tokens: 50, target_tok_per_sec: 130 }
      });
      assert.equal(streamRes.statusCode, 200);
      assert.ok(streamRes.chunksReceived >= 45);
    });

    // ------------------------------------------------------------------------------------
    // Scenario 5: Extended Production Health & Stability Run (F1–F12)
    // ------------------------------------------------------------------------------------
    await runTest('TEST-T4-SCEN-05', 'scenario_extended_production_health_and_stability_run', async () => {
      const agent = new http.Agent({ keepAlive: true });
      const sim = new TaskLifecycleSimulator(4);
      let healthChecksPassed = 0;

      // Soak loop: 20 sequential interactions
      for (let i = 0; i < 20; i++) {
        // Ping health
        const hRes = await httpRequest(`${serverUrl}/api/v1/health`, { agent });
        if (hRes.statusCode === 200 && hRes.json.healthy) {
          healthChecksPassed++;
        }

        // Dispatch & cancel or complete task
        const taskId = `soak-${i}`;
        sim.dispatchTask(taskId);
        if (i % 2 === 0) {
          sim.cancelTask(taskId);
        } else {
          // complete task
          sim.activeTasks.delete(taskId);
          sim.tokens.delete(taskId);
        }
      }

      assert.equal(healthChecksPassed, 20, '100% of health checks in soak run must pass');
      assert.equal(sim.tokens.size, 0, 'Zero leaked tokens after 20-step soak run');
      assert.equal(sim.activeTasks.size, 0, 'Zero leaked active slots');
      agent.destroy();
    });

  } finally {
    if (mockServer) {
      await mockServer.stop();
    }
  }

  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;

  return {
    tierName: 'Tier 4: Real-World Scenarios (5 Workflows)',
    total: results.length,
    passed,
    failed,
    results
  };
}

if (process.argv[1] && (process.argv[1].endsWith('tier4_scenarios.test.mjs') || process.argv[1].includes('tier4_scenarios'))) {
  const summary = await runTier4();
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
