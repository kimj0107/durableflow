import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { Queue, LeaseLostError } from '../src/index.js';

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'durableflow-'));
  const path = join(directory, 'queue.db');
  let now = 1000;
  const connections = [];
  const children = [];
  t.after(async () => {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = once(child, 'exit');
        child.kill('SIGKILL');
        await exited;
      }
    }
    for (const queue of connections.reverse()) queue.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    path,
    children,
    open() {
      const queue = new Queue(path, { clock: () => now });
      connections.push(queue);
      return queue;
    },
    advance(ms) { now += ms; },
  };
}

function message(child, kind) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      child.off('message', onMessage);
      child.off('exit', onExit);
      child.off('error', onError);
    }
    function onMessage(value) {
      if (value.kind === kind) {
        cleanup();
        resolve(value);
      }
    }
    function onExit(code, signal) {
      cleanup();
      reject(new Error(`Worker exited before ${kind}: ${code ?? signal}`));
    }
    function onError(error) {
      cleanup();
      reject(error);
    }
    child.on('message', onMessage);
    child.on('exit', onExit);
    child.on('error', onError);
  });
}

async function worker(fixture) {
  const child = fork(new URL('./fixtures/worker.js', import.meta.url), [fixture.path], {
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  fixture.children.push(child);
  await message(child, 'ready');
  return child;
}

test('keyed enqueue survives reopen and returns the original job unchanged', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const key = "receipt'); DROP TABLE jobs; --";
  const first = queue.enqueue('receipt', { order: 42 }, { idempotencyKey: key });
  const reopened = f.open();
  assert.deepEqual(reopened.get(first.id), first);
  assert.deepEqual(reopened.enqueue('different', { order: 99 }, {
    idempotencyKey: key, maxAttempts: 1, delayMs: 500,
  }), first);
  const claimed = queue.claim();
  assert.equal(claimed.id, first.id);
  assert.equal(reopened.claim(), null);
  queue.ack(claimed.id, claimed.leaseToken);
  assert.equal(reopened.enqueue('receipt', null, { idempotencyKey: key }).status, 'completed');
  assert.notEqual(queue.enqueue('receipt', null).id, queue.enqueue('receipt', null).id);
});

test('claims due work in order and acknowledges only the matching active lease', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const delayed = queue.enqueue('delayed', [], { delayMs: 100 });
  const first = queue.enqueue('first', null);
  const second = queue.enqueue('second', false);
  const claim = queue.claim({ leaseMs: 50 });
  assert.equal(claim.id, first.id);
  assert.equal(claim.attempts, 1);
  assert.equal(claim.leaseExpiresAt, 1050);
  assert.throws(() => queue.ack(claim.id, 'wrong-token'), LeaseLostError);
  assert.equal(queue.get(claim.id).status, 'active');
  const done = queue.ack(claim.id, claim.leaseToken);
  assert.equal(done.status, 'completed');
  assert.equal(done.leaseToken, null);
  assert.throws(() => queue.ack(claim.id, claim.leaseToken), LeaseLostError);
  assert.equal(queue.claim().id, second.id);
  assert.equal(queue.claim(), null);
  f.advance(100);
  assert.equal(queue.claim().id, delayed.id);
});

test('expiry boundary invalidates old workers before recovery and fences a new lease', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const submitted = queue.enqueue('work', {});
  const first = queue.claim({ leaseMs: 10 });
  f.advance(10);
  assert.throws(() => queue.ack(first.id, first.leaseToken), LeaseLostError);
  assert.throws(() => queue.fail(first.id, first.leaseToken, 'late failure'), LeaseLostError);
  assert.equal(queue.recoverExpired(), 1);
  assert.equal(queue.recoverExpired(), 0);
  assert.equal(queue.get(submitted.id).lastError, 'Lease expired');
  const second = queue.claim();
  assert.notEqual(second.leaseToken, first.leaseToken);
  assert.equal(second.attempts, 2);
  assert.throws(() => queue.ack(first.id, first.leaseToken), LeaseLostError);
  assert.throws(() => queue.fail(first.id, first.leaseToken, 'stale'), LeaseLostError);
  assert.equal(queue.ack(second.id, second.leaseToken).status, 'completed');
});

test('explicit failures retry after a delay and stop at the attempt budget', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const original = queue.enqueue('flaky', { value: 1 }, { maxAttempts: 2 });
  const first = queue.claim();
  const pending = queue.fail(first.id, first.leaseToken, new Error('offline'), {
    retryDelayMs: 100,
  });
  assert.equal(pending.status, 'pending');
  assert.equal(pending.lastError, 'offline');
  assert.equal(pending.attempts, 1);
  assert.equal(queue.claim(), null);
  f.advance(99);
  assert.equal(queue.claim(), null);
  f.advance(1);
  const second = queue.claim();
  assert.equal(second.id, original.id);
  assert.equal(second.attempts, 2);
  const failed = queue.fail(second.id, second.leaseToken, 'still offline');
  assert.equal(failed.status, 'failed');
  assert.equal(failed.lastError, 'still offline');
  f.advance(100_000);
  assert.equal(queue.claim(), null);
  assert.equal(f.open().get(original.id).status, 'failed');
});

test('claim automatically recovers leases and final expiry exhausts retries', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const submitted = queue.enqueue('crashing', 0, { maxAttempts: 2 });
  queue.claim({ leaseMs: 1 });
  f.advance(1);
  const retried = queue.claim({ leaseMs: 1 });
  assert.equal(retried.id, submitted.id);
  assert.equal(retried.attempts, 2);
  f.advance(1);
  assert.equal(queue.claim(), null);
  const failed = queue.get(submitted.id);
  assert.equal(failed.status, 'failed');
  assert.equal(failed.lastError, 'Lease expired');
  assert.equal(failed.leaseToken, null);
});

test('cancellation affects pending work only and preserves deduplication keys', (t) => {
  const f = fixture(t);
  const queue = f.open();
  const pending = queue.enqueue('delayed', {}, { delayMs: 10, idempotencyKey: 'cancel-me' });
  assert.equal(queue.cancel(pending.id), true);
  assert.equal(queue.cancel(pending.id), false);
  assert.equal(queue.cancel('missing'), false);
  assert.equal(queue.enqueue('delayed', {}, { idempotencyKey: 'cancel-me' }).status, 'cancelled');
  const active = queue.enqueue('active', {});
  const claim = queue.claim();
  assert.equal(claim.id, active.id);
  assert.equal(queue.cancel(active.id), false);
  queue.fail(claim.id, claim.leaseToken, 'try later', { retryDelayMs: 50 });
  assert.equal(queue.cancel(active.id), true);
  f.advance(100);
  assert.equal(queue.claim(), null);
  assert.equal(f.open().get(pending.id).status, 'cancelled');
});

test('invalid inputs throw without losing pending work or leaving a transaction open', (t) => {
  const f = fixture(t);
  const queue = f.open();
  for (const options of [{ maxAttempts: 0 }, { delayMs: -1 }, { idempotencyKey: '' }]) {
    assert.throws(() => queue.enqueue('job', {}, options), TypeError);
  }
  for (const payload of [undefined, 1n, () => {}]) {
    assert.throws(() => queue.enqueue('job', payload), TypeError);
  }
  const circular = {};
  circular.self = circular;
  assert.throws(() => queue.enqueue('job', circular), TypeError);
  assert.throws(() => queue.enqueue('', {}), TypeError);
  assert.throws(() => queue.enqueue('job', {}, { delayMs: Number.MAX_SAFE_INTEGER }), TypeError);
  const submitted = queue.enqueue('job', {});
  assert.throws(() => queue.claim({ leaseMs: 0 }), TypeError);
  assert.throws(() => queue.claim({ leaseMs: Number.MAX_SAFE_INTEGER }), TypeError);
  const claimed = queue.claim();
  assert.equal(claimed.id, submitted.id);
  assert.throws(() => queue.fail(claimed.id, claimed.leaseToken, 'failure', {
    retryDelayMs: Number.MAX_SAFE_INTEGER,
  }), TypeError);
  assert.equal(queue.ack(claimed.id, claimed.leaseToken).status, 'completed');
  assert.equal(queue.get('missing'), null);
});

test('independent processes competing for a job get only one unexpired claim', {
  timeout: 15_000,
}, async (t) => {
  const f = fixture(t);
  const queue = f.open();
  const submitted = queue.enqueue('one', {});
  const [a, b] = await Promise.all([worker(f), worker(f)]);
  const results = [message(a, 'claimed'), message(b, 'claimed')];
  a.send('claim');
  b.send('claim');
  const claims = (await Promise.all(results)).map((result) => result.job);
  assert.equal(claims.filter(Boolean).length, 1);
  assert.equal(claims.find(Boolean).id, submitted.id);
  assert.equal(queue.get(submitted.id).attempts, 1);
});

test('simultaneous keyed enqueues from independent processes return one persisted job', {
  timeout: 15_000,
}, async (t) => {
  const f = fixture(t);
  const queue = f.open();
  const [a, b] = await Promise.all([worker(f), worker(f)]);
  const results = [message(a, 'enqueued'), message(b, 'enqueued')];
  a.send('enqueue');
  b.send('enqueue');
  const [first, second] = (await Promise.all(results)).map((result) => result.job);
  assert.deepEqual(first, second);
  assert.deepEqual(queue.get(first.id), first);
  assert.equal(queue.claim().id, first.id);
  assert.equal(queue.claim(), null);
});

test('a killed worker leaves a committed lease that recovers after reopen', {
  timeout: 15_000,
}, async (t) => {
  const f = fixture(t);
  const { path } = f;
  const original = new Queue(path, { clock: () => 1000 });
  const submitted = original.enqueue('survive', { persisted: true }, { idempotencyKey: 'crash' });
  original.close();
  const child = await worker(f);
  const result = message(child, 'claimed');
  child.send('claim');
  const { job: abandoned } = await result;
  assert.equal(abandoned.id, submitted.id);
  const exited = once(child, 'exit');
  child.kill('SIGKILL');
  await exited;
  const reopened = new Queue(path, { clock: () => 1100 });
  try {
    assert.equal(reopened.get(submitted.id).status, 'active');
    assert.deepEqual(reopened.get(submitted.id).payload, { persisted: true });
    assert.equal(reopened.recoverExpired(), 1);
    const recovered = reopened.claim();
    assert.equal(recovered.id, submitted.id);
    assert.equal(recovered.attempts, 2);
    assert.notEqual(recovered.leaseToken, abandoned.leaseToken);
    assert.throws(() => reopened.ack(abandoned.id, abandoned.leaseToken), LeaseLostError);
    reopened.ack(recovered.id, recovered.leaseToken);
  } finally {
    reopened.close();
  }
  const final = new Queue(path);
  try {
    assert.equal(final.enqueue('survive', {}, { idempotencyKey: 'crash' }).status, 'completed');
    assert.equal(final.claim(), null);
  } finally {
    final.close();
  }
});
