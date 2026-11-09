import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { test } from 'node:test';
import { Queue, Worker, LeaseLostError, ShutdownTimeoutError, exponentialBackoff } from '../src/index.js';

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

async function eventually(predicate) {
  for (let turn = 0; turn < 100; turn++) {
    if (predicate()) return;
    await setImmediate();
  }
  assert.fail('Expected state was not reached');
}

function fixture(t) {
  let now = 1000;
  const timers = new Set();
  const queue = new Queue(':memory:', { clock: () => now });
  const stop = new AbortController();
  t.after(() => { stop.abort(); queue.close(); });
  const wait = (ms, signal) => new Promise((resolve, reject) => {
    const timer = { due: now + ms, done: () => { cleanup(); resolve(); } };
    const abort = () => { cleanup(); reject(signal.reason); };
    function cleanup() {
      timers.delete(timer);
      signal.removeEventListener('abort', abort);
    }
    if (signal.aborted) { reject(signal.reason); return; }
    timers.add(timer);
    signal.addEventListener('abort', abort, { once: true });
  });
  return {
    queue, stop, timers,
    worker(handlers, options = {}) {
      return new Worker(queue, handlers, {
        concurrency: 1, leaseMs: 100, heartbeatMs: 20,
        pollIntervalMs: 5, shutdownTimeoutMs: 50, wait, ...options,
      });
    },
    async advance(ms) {
      now += ms;
      for (const timer of [...timers]) if (timer.due <= now) timer.done();
      await setImmediate();
    },
  };
}

test('renewal cannot resurrect expired leases and replay preserves deduplication', () => {
  let now = 100;
  const queue = new Queue(':memory:', { clock: () => now });
  try {
    const original = queue.enqueue('work', {}, { maxAttempts: 1, idempotencyKey: 'key' });
    const claimed = queue.claim({ leaseMs: 10 });
    now = 105;
    assert.equal(queue.renew(claimed.id, claimed.leaseToken, { leaseMs: 10 }).leaseExpiresAt, 115);
    assert.equal(queue.renew(claimed.id, claimed.leaseToken, { leaseMs: 1 }).leaseExpiresAt, 115);
    now = 115;
    assert.throws(() => queue.renew(claimed.id, claimed.leaseToken), LeaseLostError);
    assert.equal(queue.retryFailed(original.id), null);
    queue.recoverExpired();
    assert.equal(queue.failed()[0].id, original.id);
    assert.throws(() => queue.failed({ limit: 1001 }), RangeError);
    assert.throws(() => queue.retryFailed(original.id, { maxAttempts: 0 }), TypeError);
    const replay = queue.retryFailed(original.id, { maxAttempts: 2, delayMs: 5 });
    assert.equal(replay.attempts, 0);
    assert.equal(replay.maxAttempts, 2);
    assert.equal(queue.failed().length, 0);
    assert.equal(queue.enqueue('ignored', null, { idempotencyKey: 'key' }).id, original.id);
    assert.equal(queue.retryFailed(original.id), null);
    assert.equal(queue.claim(), null);
    now += 5;
    const next = queue.claim();
    assert.notEqual(next.leaseToken, claimed.leaseToken);
    assert.throws(() => queue.ack(original.id, claimed.leaseToken), LeaseLostError);
    assert.equal(queue.ack(next.id, next.leaseToken).status, 'completed');
  } finally { queue.close(); }
});

test('backoff caps growth and jitter and rejects invalid random results', () => {
  const low = exponentialBackoff({ baseMs: 100, maxMs: 500, jitter: 0.2, random: () => 0 });
  const high = exponentialBackoff({ baseMs: 100, maxMs: 500, jitter: 0.2, random: () => 1 });
  assert.equal(low(1), 80);
  assert.equal(high(1), 120);
  assert.equal(high(50), 500);
  assert.equal(low(50), 400);
  assert.equal(exponentialBackoff({ baseMs: 0 })(Number.MAX_SAFE_INTEGER), 0);
  assert.throws(() => high(0), RangeError);
  assert.throws(() => exponentialBackoff({ random: () => NaN })(1), RangeError);
});

test('concurrency is bounded and graceful shutdown never claims the remaining job', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const jobs = Array.from({ length: 3 }, () => f.queue.enqueue('work', {}));
  const gate = deferred();
  let started = 0;
  const running = f.worker({ work: async () => { started++; await gate.promise; } }, { concurrency: 2 })
    .run({ signal: f.stop.signal });
  await eventually(() => started === 2);
  assert.equal(f.queue.get(jobs[2].id).status, 'pending');
  f.stop.abort();
  gate.resolve();
  assert.equal((await running).completed, 2);
  assert.equal(started, 2);
  assert.equal(f.queue.get(jobs[2].id).status, 'pending');
  assert.equal(f.timers.size, 0);
});

test('heartbeats continue during graceful draining', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {});
  const gate = deferred();
  let entered = false;
  const running = f.worker({ work: async () => { entered = true; await gate.promise; } })
    .run({ signal: f.stop.signal });
  await eventually(() => entered);
  await f.advance(20);
  assert.equal(f.queue.get(job.id).leaseExpiresAt, 1120);
  f.stop.abort();
  await f.advance(20);
  assert.equal(f.queue.get(job.id).leaseExpiresAt, 1140);
  assert.equal(f.queue.claim(), null);
  gate.resolve();
  assert.equal((await running).completed, 1);
  assert.equal(f.timers.size, 0);
});

test('a late heartbeat aborts stale handlers without mutating a replacement lease', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {});
  const gate = deferred();
  let handlerSignal;
  const running = f.worker({ work: async (_, { signal }) => { handlerSignal = signal; await gate.promise; } })
    .run({ signal: f.stop.signal });
  await eventually(() => handlerSignal);
  f.stop.abort();
  await f.advance(100);
  await eventually(() => handlerSignal.aborted);
  assert.ok(handlerSignal.reason instanceof LeaseLostError);
  const replacement = f.queue.claim();
  assert.equal(replacement.id, job.id);
  f.stop.abort();
  assert.equal((await running).leaseLost, 1);
  assert.equal(f.queue.get(job.id).leaseToken, replacement.leaseToken);
  gate.resolve();
  await setImmediate();
  assert.equal(f.queue.get(job.id).status, 'active');
});

test('shutdown deadline abandons uncooperative work without acknowledging it', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {});
  const gate = deferred();
  let handlerSignal;
  const running = f.worker({ work: async (_, { signal }) => { handlerSignal = signal; await gate.promise; } }, {
    shutdownTimeoutMs: 10,
  }).run({ signal: f.stop.signal });
  await eventually(() => handlerSignal);
  f.stop.abort();
  await f.advance(10);
  assert.equal((await running).abandoned, 1);
  assert.ok(handlerSignal.reason instanceof ShutdownTimeoutError);
  assert.equal(f.queue.get(job.id).status, 'active');
  assert.equal(f.timers.size, 0);
  gate.resolve();
  await setImmediate();
  assert.equal(f.queue.get(job.id).status, 'active');
  await f.advance(90);
  assert.equal(f.queue.claim().attempts, 2);
});

test('handler failures use delayed retries before completing', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {}, { maxAttempts: 2 });
  let calls = 0;
  const running = f.worker({
    work: () => { if (++calls === 1) throw new Error('offline'); f.stop.abort(); },
  }, { retryDelay: exponentialBackoff({ baseMs: 10, jitter: 0 }) }).run({ signal: f.stop.signal });
  await eventually(() => f.queue.get(job.id).status === 'pending');
  assert.equal(f.queue.get(job.id).availableAt, 1010);
  await f.advance(10);
  const stats = await running;
  assert.equal(calls, 2);
  assert.equal(stats.retried, 1);
  assert.equal(stats.completed, 1);
});

test('storage failure during a heartbeat stops all worker lanes', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  f.queue.enqueue('work', {});
  f.queue.enqueue('work', {});
  const gate = deferred();
  const signals = [];
  const result = f.worker({
    work: async (_, { signal }) => { signals.push(signal); await gate.promise; },
  }, { concurrency: 2 }).run({ signal: f.stop.signal }).then(() => null, (error) => error);
  await eventually(() => signals.length === 2);
  f.queue.renew = () => { throw new Error('storage offline'); };
  await f.advance(20);
  assert.match((await result).message, /storage offline/);
  assert.ok(signals.every((signal) => signal.aborted));
  gate.resolve();
  assert.equal(f.timers.size, 0);
});

test('acknowledgement storage failure never becomes a handler retry', { timeout: 5000 }, async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {});
  f.queue.ack = () => { throw new Error('ack storage offline'); };
  await assert.rejects(f.worker({ work: () => {} }).run({ signal: f.stop.signal }), /ack storage offline/);
  assert.equal(f.queue.get(job.id).status, 'active');
  assert.equal(f.queue.get(job.id).attempts, 1);
  assert.equal(f.timers.size, 0);
});

test('an already-aborted run claims nothing and workers are single use', async (t) => {
  const f = fixture(t);
  const job = f.queue.enqueue('work', {});
  f.stop.abort();
  const worker = f.worker({ work: () => assert.fail('Should not run') });
  assert.equal((await worker.run({ signal: f.stop.signal })).completed, 0);
  assert.equal(f.queue.get(job.id).status, 'pending');
  assert.equal(f.timers.size, 0);
  await assert.rejects(worker.run(), /only run once/);
});
