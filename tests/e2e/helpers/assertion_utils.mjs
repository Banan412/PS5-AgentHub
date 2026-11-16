/**
 * Assertion & Calculation Utilities for PS5-AgentHub E2E Test Suite
 * Zero external dependencies - pure Node.js standard library.
 */

import assert from 'node:assert/strict';

/**
 * Calculates decode throughput in tokens per second.
 * When firstTokenTimeMs is provided, TTFT (prompt evaluation) is excluded:
 * decode_tok_per_sec = (tokenCount - 1) / (endTimeMs - firstTokenTimeMs) * 1000
 * When firstTokenTimeMs is not provided:
 * decode_tok_per_sec = tokenCount / (endTimeMs - startTimeMs) * 1000
 *
 * @param {number} tokenCount
 * @param {number} startTimeMs
 * @param {number} endTimeMs
 * @param {number|null} [firstTokenTimeMs]
 * @returns {{ rate: number, durationSec: number, excludedTTFT: boolean }}
 */
export function calculateDecodeThroughput(tokenCount, startTimeMs, endTimeMs, firstTokenTimeMs = null) {
  if (tokenCount <= 0) {
    return { rate: 0, durationSec: 0, excludedTTFT: false };
  }

  if (firstTokenTimeMs !== null && firstTokenTimeMs !== undefined && firstTokenTimeMs > startTimeMs) {
    const decodeDurationSec = (endTimeMs - firstTokenTimeMs) / 1000;
    if (decodeDurationSec <= 0) {
      return { rate: Infinity, durationSec: 0, excludedTTFT: true };
    }
    // (predicted_n - 1) tokens generated during the decode window after the first token arrived
    const rate = (tokenCount - 1) / decodeDurationSec;
    return { rate, durationSec: decodeDurationSec, excludedTTFT: true };
  }

  const totalDurationSec = (endTimeMs - startTimeMs) / 1000;
  if (totalDurationSec <= 0) {
    return { rate: Infinity, durationSec: 0, excludedTTFT: false };
  }
  const rate = tokenCount / totalDurationSec;
  return { rate, durationSec: totalDurationSec, excludedTTFT: false };
}

/**
 * Calculates Time-To-First-Token (TTFT) latency in milliseconds.
 *
 * @param {number} startTimeMs
 * @param {number} firstTokenTimeMs
 * @returns {number}
 */
export function calculateTTFT(startTimeMs, firstTokenTimeMs) {
  return Math.max(0, firstTokenTimeMs - startTimeMs);
}

/**
 * Simulates the cadence-optimized StreamingRepetitionGuard logic
 * mirroring crates/ps5-agent-inference/src/parser.rs:1788-1870.
 *
 * Invariants:
 * 1. Checks cadence: unverified delta accumulation >= 32 chars or sentence/newline boundary (\n, ., ?, !).
 * 2. Ignores tokens inside code fences (```).
 * 3. Truncates repeated prose loops cleanly at the loop boundary.
 */
export class StreamingRepetitionGuardSimulator {
  constructor(options = {}) {
    this.maxWindow = options.maxWindow || 512;
    this.visibleAcc = '';
    this.unverifiedDeltaLen = 0;
    this.inFence = false;
    this.checksTriggered = 0;
    this.abortedClean = null;
  }

  /**
   * Pushes a token delta and returns loop decision.
   * @param {string} delta
   * @returns {{ decision: 'Continue' | 'AbortRepeatedLoop', truncatedVisible?: string }}
   */
  pushDelta(delta) {
    if (this.abortedClean !== null) {
      return { decision: 'AbortRepeatedLoop', truncatedVisible: this.abortedClean };
    }

    if (delta.includes('```')) {
      const fenceCount = (delta.match(/```/g) || []).length;
      if (fenceCount % 2 === 1) {
        this.inFence = !this.inFence;
      }
    }

    this.visibleAcc += delta;
    this.unverifiedDeltaLen += delta.length;

    // Cadence check: trigger detection only at newline/punctuation or when >= 32 chars unverified
    const hasBoundary = /[\n.!?]/.test(delta);
    if (!this.inFence && (hasBoundary || this.unverifiedDeltaLen >= 32)) {
      this.checksTriggered++;
      this.unverifiedDeltaLen = 0;

      const loopDetected = this._detectProseRepetition(this.visibleAcc);
      if (loopDetected) {
        this.abortedClean = this._truncateProseLoop(this.visibleAcc, loopDetected);
        return { decision: 'AbortRepeatedLoop', truncatedVisible: this.abortedClean };
      }
    }

    return { decision: 'Continue' };
  }

  /**
   * Evaluates at end of stream.
   */
  finish() {
    if (this.abortedClean !== null) {
      return { decision: 'AbortRepeatedLoop', truncatedVisible: this.abortedClean };
    }
    this.checksTriggered++;
    const loopDetected = this._detectProseRepetition(this.visibleAcc);
    if (loopDetected) {
      this.abortedClean = this._truncateProseLoop(this.visibleAcc, loopDetected);
      return { decision: 'AbortRepeatedLoop', truncatedVisible: this.abortedClean };
    }
    return { decision: 'Continue' };
  }

  _detectProseRepetition(text) {
    // Look for repeating patterns of length >= 12 chars repeated >= 3 times
    const minPatternLen = 12;
    const maxPatternLen = 120;
    for (let len = minPatternLen; len <= Math.min(maxPatternLen, Math.floor(text.length / 3)); len++) {
      const candidate = text.slice(text.length - len);
      const prev1 = text.slice(text.length - 2 * len, text.length - len);
      const prev2 = text.slice(text.length - 3 * len, text.length - 2 * len);
      if (candidate === prev1 && prev1 === prev2) {
        return candidate;
      }
    }
    return null;
  }

  _truncateProseLoop(text, pattern) {
    const firstIdx = text.indexOf(pattern);
    if (firstIdx !== -1) {
      return text.slice(0, firstIdx + pattern.length).trimEnd();
    }
    return text;
  }
}

/**
 * Simulates task token eviction and permit lifecycle
 * mirroring crates/ps5-agent-runtime/src/runtime.rs and crates/ps5-agent-manager/src/lib.rs.
 */
export class TaskLifecycleSimulator {
  constructor(concurrencyLimit = 4) {
    this.concurrencyLimit = concurrencyLimit;
    this.tokens = new Map(); // taskId -> token info
    this.pendingQueue = [];  // queue of taskIds
    this.activeTasks = new Set();
  }

  dispatchTask(taskId) {
    const token = { taskId, cancelled: false, createdAt: Date.now() };
    this.tokens.set(taskId, token);

    if (this.activeTasks.size < this.concurrencyLimit) {
      this.activeTasks.add(taskId);
      return { status: 'running', token };
    } else {
      this.pendingQueue.push(taskId);
      return { status: 'pending', token };
    }
  }

  cancelTask(taskId) {
    const token = this.tokens.get(taskId);
    if (token) {
      token.cancelled = true;
    }

    // Evict from pending queue if queued
    const pendingIdx = this.pendingQueue.indexOf(taskId);
    if (pendingIdx !== -1) {
      this.pendingQueue.splice(pendingIdx, 1);
    }

    // Release slot if active
    if (this.activeTasks.has(taskId)) {
      this.activeTasks.delete(taskId);
      // Promote next pending task
      if (this.pendingQueue.length > 0) {
        const nextId = this.pendingQueue.shift();
        this.activeTasks.add(nextId);
      }
    }

    // Release token from token map (prevent leak)
    this.tokens.delete(taskId);
    return { ok: true, activeCount: this.activeTasks.size, leakedToken: this.tokens.has(taskId) };
  }
}

/**
 * Formats a clean ASCII table for test run summaries.
 */
export function formatTable(headers, rows) {
  const colWidths = headers.map((h, i) => {
    const maxRowWidth = rows.reduce((max, r) => Math.max(max, String(r[i] ?? '').length), 0);
    return Math.max(h.length, maxRowWidth);
  });

  const sep = '+-' + colWidths.map(w => '-'.repeat(w)).join('-+-') + '-+';
  const formatRow = (r) => '| ' + r.map((c, i) => String(c ?? '').padEnd(colWidths[i])).join(' | ') + ' |';

  const out = [];
  out.push(sep);
  out.push(formatRow(headers));
  out.push(sep);
  for (const r of rows) {
    out.push(formatRow(r));
  }
  out.push(sep);
  return out.join('\n');
}
