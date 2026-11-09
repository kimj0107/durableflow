import { setTimeout as sleep } from 'node:timers/promises';
import { LeaseLostError, ShutdownTimeoutError } from './errors.js';

function duration(value, name, minimum = 1) {
  if (!Number.isSafeInteger(value) || value < minimum || value > 2_147_483_647) {
    throw new RangeError(`${name} must be an integer from ${minimum} to 2147483647`);
  }
  return value;
}

export function exponentialBackoff({
  baseMs = 1000, maxMs = 60_000, jitter = 0.2, random = Math.random,
} = {}) {
  duration(baseMs, 'baseMs', 0);
  duration(maxMs, 'maxMs', 0);
  if (typeof jitter !== 'number' || !Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new RangeError('jitter must be between 0 and 1');
  }
  if (typeof random !== 'function') throw new TypeError('random must be a function');
  return (attempt) => {
    if (!Number.isSafeInteger(attempt) || attempt < 1) {
      throw new RangeError('attempt must be a positive safe integer');
    }
    const sample = random();
    if (!Number.isFinite(sample) || sample < 0 || sample > 1) {
      throw new RangeError('random must return a number between 0 and 1');
    }
    const capped = Math.min(maxMs, baseMs * 2 ** Math.min(attempt - 1, 52));
    return Math.min(maxMs, Math.round(capped * (1 - jitter + 2 * jitter * sample)));
  };
}

function untilAborted(promise, signal) {
  return new Promise((resolve, reject) => {
    const aborted = () => {
      signal.removeEventListener('abort', aborted);
      reject(signal.reason);
    };
    if (signal.aborted) {
      // Observe a late handler rejection even when ownership was already lost.
      Promise.resolve(promise).catch(() => {});
      reject(signal.reason);
      return;
    }
    signal.addEventListener('abort', aborted, { once: true });
    Promise.resolve(promise).then(
      (value) => { signal.removeEventListener('abort', aborted); resolve(value); },
      (error) => { signal.removeEventListener('abort', aborted); reject(error); },
    );
  });
}

export class Worker {
  #queue;
  #handlers;
  #options;
  #started = false;

  constructor(queue, handlers, {
    concurrency = 4,
    leaseMs = 30_000,
    heartbeatMs = Math.max(1, Math.floor(leaseMs / 3)),
    pollIntervalMs = 100,
    shutdownTimeoutMs = 30_000,
    retryDelay = exponentialBackoff(),
    wait = (ms, signal) => sleep(ms, undefined, { signal }),
  } = {}) {
    if (!queue || ['claim', 'renew', 'ack', 'fail'].some((method) => typeof queue[method] !== 'function')) {
      throw new TypeError('queue must support claim, renew, ack, and fail');
    }
    if (!handlers || typeof handlers !== 'object') throw new TypeError('handlers must be an object');
    this.#handlers = new Map(Object.entries(handlers));
    if (this.#handlers.size === 0 || [...this.#handlers.values()].some((handler) => typeof handler !== 'function')) {
      throw new TypeError('handlers must contain at least one function');
    }
    duration(concurrency, 'concurrency');
    if (concurrency > 1000) throw new RangeError('concurrency must be <= 1000');
    duration(leaseMs, 'leaseMs', 2);
    duration(heartbeatMs, 'heartbeatMs');
    if (heartbeatMs >= leaseMs) throw new RangeError('heartbeatMs must be less than leaseMs');
    duration(pollIntervalMs, 'pollIntervalMs');
    duration(shutdownTimeoutMs, 'shutdownTimeoutMs', 0);
    if (typeof retryDelay !== 'function' || typeof wait !== 'function') {
      throw new TypeError('retryDelay and wait must be functions');
    }
    this.#queue = queue;
    this.#options = { concurrency, leaseMs, heartbeatMs, pollIntervalMs, shutdownTimeoutMs, retryDelay, wait };
  }

  async run({ signal } = {}) {
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal');
    if (this.#started) throw new Error('A Worker instance can only run once');
    this.#started = true;
    const options = this.#options;
    const stopClaims = new AbortController();
    const stopDeadline = new AbortController();
    const active = new Set();
    const stats = { completed: 0, retried: 0, failed: 0, leaseLost: 0, abandoned: 0 };
    let fatal;
    let deadline = Promise.resolve();
    const abortActive = (reason) => {
      for (const controller of active) controller.abort(reason);
    };
    const stop = () => {
      if (stopClaims.signal.aborted) return;
      stopClaims.abort();
      let waiting;
      try { waiting = options.wait(options.shutdownTimeoutMs, stopDeadline.signal); }
      catch (error) {
        fatal ??= error;
        abortActive(error);
        return;
      }
      deadline = Promise.resolve(waiting).then(
        () => abortActive(new ShutdownTimeoutError()),
        (error) => {
          if (!stopDeadline.signal.aborted) {
            fatal ??= error;
            abortActive(error);
          }
        },
      );
    };
    signal?.addEventListener('abort', stop, { once: true });
    if (signal?.aborted) stop();
    const lane = async () => {
      while (!stopClaims.signal.aborted) {
        const job = this.#queue.claim({ leaseMs: options.leaseMs });
        if (job) {
          await this.#execute(job, active, stats);
        } else {
          try { await options.wait(options.pollIntervalMs, stopClaims.signal); }
          catch (error) { if (!stopClaims.signal.aborted) throw error; }
        }
      }
    };
    try {
      await Promise.all(Array.from({ length: options.concurrency }, () => lane().catch((error) => {
        fatal ??= error;
        stop();
        abortActive(error);
      })));
    } finally {
      signal?.removeEventListener('abort', stop);
      stopDeadline.abort();
      await deadline;
    }
    if (fatal) throw fatal;
    return stats;
  }

  async #execute(job, active, stats) {
    const options = this.#options;
    const controller = new AbortController();
    const stopHeartbeat = new AbortController();
    active.add(controller);
    let heartbeatError;
    let handlerCompleted = false;
    const heartbeat = (async () => {
      try {
        while (!stopHeartbeat.signal.aborted) {
          await options.wait(options.heartbeatMs, stopHeartbeat.signal);
          if (!stopHeartbeat.signal.aborted) {
            this.#queue.renew(job.id, job.leaseToken, { leaseMs: options.leaseMs });
          }
        }
      } catch (error) {
        if (!stopHeartbeat.signal.aborted) {
          heartbeatError = error;
          controller.abort(error);
        }
      }
    })();
    try {
      const handler = this.#handlers.get(job.type);
      const result = Promise.resolve().then(() => {
        controller.signal.throwIfAborted();
        if (!handler) throw new Error(`No handler registered for ${job.type}`);
        return handler(job, { signal: controller.signal });
      });
      await untilAborted(result, controller.signal);
      handlerCompleted = true;
      controller.signal.throwIfAborted();
      this.#queue.ack(job.id, job.leaseToken);
      stats.completed++;
    } catch (error) {
      if (error instanceof LeaseLostError) {
        controller.abort(error);
        stats.leaseLost++;
      } else if (heartbeatError) {
        throw heartbeatError;
      } else if (controller.signal.aborted) {
        stats.abandoned++;
      } else if (handlerCompleted) {
        throw error;
      } else {
        const retryDelayMs = options.retryDelay(job.attempts);
        if (!Number.isSafeInteger(retryDelayMs) || retryDelayMs < 0) {
          throw new RangeError('retryDelay must return a nonnegative safe integer');
        }
        const message = error instanceof Error ? error.message : String(error ?? 'Handler failed');
        try {
          const updated = this.#queue.fail(job.id, job.leaseToken, new Error(message), { retryDelayMs });
          if (updated.status === 'failed') stats.failed++;
          else stats.retried++;
        } catch (failure) {
          if (!(failure instanceof LeaseLostError)) throw failure;
          controller.abort(failure);
          stats.leaseLost++;
        }
      }
    } finally {
      stopHeartbeat.abort();
      await heartbeat;
      active.delete(controller);
    }
  }
}
