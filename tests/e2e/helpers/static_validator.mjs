/**
 * Static Asset Validator for PS5-AgentHub Dashboard Assets
 * Validates DOM IDs, CSS color tokens, security invariants, and JavaScript syntax.
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';

const DEFAULT_ASSET_BASE = path.resolve(process.cwd(), 'crates/ps5-agent-dashboard/assets');

export const MANDATORY_21_CONTROL_IDS = [
  'chat-agent-select',
  'thinking-level-select',
  'btn-agentic-search-toggle',
  'chat-agentic-agent-select',
  'chat-slash-btn',
  'btn-chat-attach',
  'chat-active-streams',
  'chat-composer-form',
  'chat-input',
  'chat-send-btn',
  'chat-send-immediate-btn',
  'chat-raw-output-toggle',
  'chat-fork-conv-btn',
  'chat-active-task-bar',
  'chat-ephemeral-card',
  'chat-live-browser-dock',
  'btn-chat-live-browser-close',
  'chat-live-browser-frame',
  'chat-live-browser-overlay',
  'chat-live-browser-tabs',
  'chat-live-browser-status',
  'chat-live-browser-url',
  'btn-chat-live-browser-toggle'
];

export const MANDATORY_13_SECTIONS = [
  'section-overview',
  'section-chat',
  'section-files',
  'section-agents',
  'section-a2a',
  'section-tasks',
  'section-schedules',
  'section-memory',
  'section-tools',
  'section-extensions',
  'section-sandbox',
  'section-logs',
  'section-settings'
];

export const MANDATORY_COLOR_TOKENS = [
  '#0b0f17', // app background
  '#131a26', // card background
  '#1c2638', // elevated background
  '#e6edf7'  // text primary
];

/**
 * Validates index.html against project and security contracts.
 * @param {string} [customPath]
 */
export function validateIndexHtml(customPath) {
  const filePath = customPath || path.join(DEFAULT_ASSET_BASE, 'index.html');
  const html = fs.readFileSync(filePath, 'utf8');
  const errors = [];

  // Check 21 interactive IDs
  for (const id of MANDATORY_21_CONTROL_IDS) {
    const pattern = new RegExp(`id=["']${id}["']`);
    if (!pattern.test(html)) {
      errors.push(`Missing mandatory control ID: #${id}`);
    }
  }

  // Check 13 mandatory sections
  for (const sec of MANDATORY_13_SECTIONS) {
    const pattern = new RegExp(`id=["']${sec}["']`);
    if (!pattern.test(html)) {
      errors.push(`Missing mandatory section ID: #${sec}`);
    }
  }

  // Security invariant: Zero external CDN links (crates/ps5-agent-dashboard/src/lib.rs:37-39)
  if (html.includes('https://')) {
    errors.push('Found forbidden https:// link in index.html');
  }
  if (html.includes('href="http')) {
    errors.push('Found forbidden href="http link in index.html');
  }
  if (html.includes('src="http')) {
    errors.push('Found forbidden src="http link in index.html');
  }

  // Security invariant: No allow-same-origin on iframes
  if (html.includes('allow-same-origin')) {
    errors.push('Found forbidden allow-same-origin attribute on iframe in index.html');
  }

  return {
    valid: errors.length === 0,
    errors,
    filePath,
    length: html.length
  };
}

/**
 * Validates styles.css against theme contracts and CSS invariants.
 * @param {string} [customPath]
 */
export function validateStylesCss(customPath) {
  const filePath = customPath || path.join(DEFAULT_ASSET_BASE, 'styles.css');
  const css = fs.readFileSync(filePath, 'utf8');
  const errors = [];

  // Check color tokens
  for (const token of MANDATORY_COLOR_TOKENS) {
    if (!css.toLowerCase().includes(token.toLowerCase())) {
      errors.push(`Missing required CSS theme color token: ${token}`);
    }
  }

  // Check accessibility & responsive requirements
  if (!css.includes(':focus-visible')) {
    errors.push('Missing required :focus-visible rule in styles.css');
  }

  if (!/min-height:\s*44px/i.test(css)) {
    errors.push('Missing required min-height: 44px touch target rule in styles.css');
  }

  if (!/@media\s*\(\s*max-width:\s*768px\s*\)/i.test(css)) {
    errors.push('Missing required @media (max-width: 768px) responsive query in styles.css');
  }

  // Check balanced braces
  const openBraces = (css.match(/\{/g) || []).length;
  const closeBraces = (css.match(/\}/g) || []).length;
  if (openBraces !== closeBraces) {
    errors.push(`Unbalanced CSS braces: ${openBraces} '{' vs ${closeBraces} '}'`);
  }

  return {
    valid: errors.length === 0,
    errors,
    filePath,
    length: css.length,
    openBraces,
    closeBraces
  };
}

/**
 * Validates app.js against strict XSS and code safety invariants.
 * @param {string} [customPath]
 */
export function validateAppJs(customPath) {
  const filePath = customPath || path.join(DEFAULT_ASSET_BASE, 'app.js');
  const js = fs.readFileSync(filePath, 'utf8');
  const errors = [];

  // Strict check: No innerHTML
  if (/\.innerHTML\b/.test(js)) {
    errors.push('Forbidden .innerHTML usage found in app.js');
  }

  // Strict check: No outerHTML
  if (/\.outerHTML\b/.test(js)) {
    errors.push('Forbidden .outerHTML usage found in app.js');
  }

  // Strict check: No insertAdjacentHTML
  if (/insertAdjacentHTML\b/.test(js)) {
    errors.push('Forbidden insertAdjacentHTML usage found in app.js');
  }

  // Strict check: No eval or Function constructor
  if (/\beval\s*\(/.test(js)) {
    errors.push('Forbidden eval() call found in app.js');
  }

  if (/\bnew\s+Function\s*\(/.test(js)) {
    errors.push('Forbidden new Function() constructor found in app.js');
  }

  return {
    valid: errors.length === 0,
    errors,
    filePath,
    length: js.length
  };
}

/**
 * Executes `node --check app.js` to ensure zero syntax errors.
 * @param {string} [customPath]
 */
export function checkAppJsSyntax(customPath) {
  const filePath = customPath || path.join(DEFAULT_ASSET_BASE, 'app.js');
  try {
    execSync(`node --check "${filePath}"`, { stdio: 'pipe' });
    return { valid: true };
  } catch (err) {
    return {
      valid: false,
      error: err.stderr ? err.stderr.toString('utf8') : err.message
    };
  }
}
