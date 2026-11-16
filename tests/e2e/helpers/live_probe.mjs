/**
 * Live Server Probe for PS5-AgentHub
 * Probes for an active instance running on port 8080 or process.env.PS5_AGENT_URL.
 */

import http from 'node:http';

/**
 * Checks whether a live PS5-AgentHub server is active.
 *
 * @param {string} [targetUrl]
 * @param {number} [timeoutMs]
 * @returns {Promise<{ isLive: boolean, url: string, health: object|null, error?: string }>}
 */
export async function probeLiveServer(
  targetUrl = process.env.PS5_AGENT_URL || 'http://127.0.0.1:8080',
  timeoutMs = 600
) {
  return new Promise((resolve) => {
    let resolved = false;
    let req;

    const timer = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        if (req) req.destroy();
        resolve({
          isLive: false,
          url: targetUrl,
          health: null,
          error: `Connection timed out after ${timeoutMs}ms`
        });
      }
    }, timeoutMs);

    try {
      const parsed = new URL(targetUrl);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || 80,
        path: '/api/v1/health',
        method: 'GET',
        headers: { Accept: 'application/json' }
      };

      req = http.request(options, (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => { body += chunk; });
        res.on('end', () => {
          clearTimeout(timer);
          if (!resolved) {
            resolved = true;
            try {
              const parsedJson = JSON.parse(body);
              resolve({
                isLive: res.statusCode === 200,
                url: targetUrl,
                health: parsedJson
              });
            } catch {
              resolve({
                isLive: res.statusCode === 200,
                url: targetUrl,
                health: null,
                error: 'Invalid JSON in health response'
              });
            }
          }
        });
      });

      req.on('error', (err) => {
        clearTimeout(timer);
        if (!resolved) {
          resolved = true;
          resolve({
            isLive: false,
            url: targetUrl,
            health: null,
            error: err.message
          });
        }
      });

      req.end();
    } catch (err) {
      clearTimeout(timer);
      resolve({
        isLive: false,
        url: targetUrl,
        health: null,
        error: err.message
      });
    }
  });
}
