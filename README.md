# DurableFlow

An original, dependency-free Node.js job queue using built-in `node:sqlite`.
The **persistent-queue** and **worker-lifecycle** milestones are implemented: persisted JSON jobs,
idempotent enqueue, competing claims, fenced leases, acknowledgement, expiry
recovery, bounded retries, cancellation of pending work, concurrent workers,
lease heartbeats, graceful shutdown, and explicit dead-letter replay.

**Delivery is at least once, not exactly once.** A process can perform an
external effect and crash before acknowledgement. Its job can then run again.
Handlers must make their own effects idempotent, for example using the job ID
as a downstream deduplication key. Enqueue deduplication does not deduplicate
external effects. Retry limits can exhaust; successful processing is not
guaranteed.

## Run

Requires Node.js 24+ and npm. No external packages or services are required.
`node:sqlite` may emit an experimental-feature warning on some Node versions.

```sh
npm test
npm run check
npm run example
npm run example:worker
```

The example creates `durableflow-example.db` in the current directory and
persists its idempotency key. Running it again does not submit a second job.
`npm run check` syntax-checks every source, example, test, and check script;
it is not a TypeScript type checker.

## Use

From this checkout:

```js
import { Queue, LeaseLostError } from './src/index.js';

const queue = new Queue('jobs.db');
try {
  const submitted = queue.enqueue('send-receipt', { orderId: 'order-123' }, {
    idempotencyKey: 'receipt:order-123',
    maxAttempts: 3,
  });
  const work = queue.claim({ leaseMs: 30_000 });
  if (work) {
    // Replace with an idempotent handler. Do not rely on console output deduplication.
    console.log(work.type, work.payload, 'effect key:', work.id);
    queue.ack(work.id, work.leaseToken);
  }
  console.log(queue.get(submitted.id));
} finally {
  queue.close();
}
```

The npm package entry point exports `Queue` and `LeaseLostError`; after local
installation, use `import { Queue } from 'durableflow'`. This project has not
been published as part of this milestone.

## API

All methods are synchronous; SQLite work briefly blocks the calling thread.
Separate processes or threads can open the same local database file.

| Method | Behavior |
| --- | --- |
| `new Queue(path, options?)` | Opens/creates a queue. Options: `busyTimeoutMs` (default 5000), `clock` (default `Date.now`). Use one dedicated database per queue. |
| `enqueue(type, payload, options?)` | Returns the persisted job. Options: `idempotencyKey` (default `null`), `maxAttempts` (default 3), `delayMs` (default 0). Payload uses JSON serialization; unsupported top-level values, cycles, and BigInt throw. |
| `claim({ leaseMs = 30000 }?)` | Recovers expired leases, then atomically claims one due job or returns `null`. Increments attempts and issues a fresh random lease token. |
| `ack(id, leaseToken)` | Completes work only with the current unexpired token; otherwise throws `LeaseLostError`. Returns the updated job. |
| `renew(id, leaseToken, { leaseMs = 30000 }?)` | Extends an unexpired matching lease without shortening it. Cannot resurrect an expired lease. |
| `fail(id, leaseToken, error, { retryDelayMs = 0 }?)` | Accepts an Error or nonempty string. Requeues work if attempts remain; otherwise marks it failed. Requires the current unexpired token. Returns the updated job. |
| `recoverExpired()` | Requeues expired active jobs with attempts remaining; marks exhausted jobs failed. Returns the number changed. |
| `cancel(id)` | Cancels only pending work, including delayed retries. Returns `false` for missing, active, or terminal jobs; never interrupts a running handler. |
| `failed({ limit = 100 }?)` | Lists up to 1000 failed jobs in update order for dead-letter inspection. |
| `retryFailed(id, { maxAttempts, delayMs = 0 }?)` | Explicitly requeues a failed job with a fresh attempt budget, preserving its ID and enqueue key. Returns `null` if it is not failed. Clears the previous error; this is not an audit log. |
| `get(id)` | Returns a job snapshot or `null`. Does not automatically recover expired work. |
| `close()` | Closes this connection; call once when finished. Does not release active leases. |

Jobs expose `id`, `type`, `payload`, `idempotencyKey`, `status`, `attempts`,
`maxAttempts`, `availableAt`, `leaseToken`, `leaseExpiresAt`, `lastError`,
`createdAt`, and `updatedAt`. Times are integer Unix milliseconds. Durations
are nonnegative safe integers; lease duration and attempt limit must be
positive. Use trustworthy, synchronized clocks across workers. The optional
clock function is useful for deterministic tests, not distributed clock repair.

Idempotency keys are global to a database (not scoped by type). The first
committed enqueue wins: duplicates return that job unchanged, even if payload
or options differ and even after completion, cancellation, or failure.
Jobs without keys are independent. Terminal jobs and keys remain stored;
there is no pruning API. Types and keys must be nonempty strings. JSON follows
standard `JSON.stringify` semantics, including nested value omission and
non-finite numbers becoming `null`.

## Failure and concurrency guarantees

- Mutations use `BEGIN IMMEDIATE` transactions and parameterized data values.
  Concurrent claimers cannot claim the same unexpired job. SQLite serializes
  writers; this is not parallel database writing.
- Each claim consumes one attempt, including a claim whose worker crashes.
  Due work is ordered by availability, creation time, then insertion order;
  execution/completion order across workers is not guaranteed.
- At `leaseExpiresAt <= now`, the token is invalid even before recovery runs.
  Recovery clears it, records `Lease expired`, and either requeues immediately
  or marks the last attempt failed. Old workers cannot ack or fail a new lease.
  Fencing applies only to queue state, not to external effects.
- On a worker crash, committed jobs and claims survive reopening. Call
  `recoverExpired()` or continue calling `claim()` after expiry. Opening the
  database alone does not run recovery; there is no background scheduler.
- WAL mode and `synchronous=FULL` request SQLite's local durable commits.
  Host filesystem/hardware durability still matters. These tests cover process
  termination, not power-loss or storage-corruption scenarios. Use a local
  filesystem, not a network share. `:memory:` is supported but not persistent.
- Invalid inputs, serialization failures, SQLite failures (including lock
  timeout), and stale lease operations throw. Failed mutations roll back.
  A crash around commit can leave a caller uncertain whether it succeeded:
  retry enqueue with the same key and inspect persisted state.

## Worker lifecycle

```js
import { Queue, Worker } from './src/index.js';

const queue = new Queue('jobs.db');
const stop = new AbortController();
process.once('SIGINT', () => stop.abort());
const worker = new Worker(queue, {
  receipt: async (job, { signal }) => {
    signal.throwIfAborted();
    await sendReceiptIdempotently(job.payload, job.id, { signal });
  },
}, { concurrency: 4, leaseMs: 30000, heartbeatMs: 10000 });
try {
  console.log(await worker.run({ signal: stop.signal }));
} finally {
  queue.close();
}
```

`sendReceiptIdempotently` above is your application handler, not a supplied
function. `npm run example:worker` is the complete offline runnable example.

Workers claim no more than `concurrency` jobs at a time (default 4, maximum
1000). This is async I/O concurrency, not parallel CPU execution. Handlers are
registered by job type; an unknown type fails using the same retry budget.
Each worker instance runs once and does not own or close the queue.

Aborting `run`'s signal stops new claims while existing handlers drain.
Heartbeats continue during draining. After `shutdownTimeoutMs` (default 30000),
unfinished handler signals are aborted and the worker stops waiting for them.
Their leases are left to expire rather than falsely acknowledged or failed.
Handler cancellation is cooperative: ignored signals cannot stop external
effects or prevent duplicate execution. Resolve/close external resources in
handlers; use a separate process for untrusted or CPU-bound work.

A missed/expired heartbeat aborts the affected handler and fences all stale
queue mutations. Other storage errors stop the worker and reject `run`;
inspect persisted state before restarting. A blocked event loop or SQLite
operation can delay both heartbeats and shutdown timers, so this is not a
hard real-time deadline.

Default retry delays grow exponentially from 1000 ms, with +/-20% jitter and
a 60000 ms cap. `exponentialBackoff({ baseMs, maxMs, jitter, random })` produces
an injectable policy. The cap applies after jitter; attempt 1 is the first
failure. `retryDelay(attempt)` may be supplied instead. Queue clocks and the
worker's abort-aware `wait(ms, signal)` can be injected for deterministic tests.
`run` returns counts for completed, retried, failed, lease-lost, and abandoned
jobs; these are per invocation, not persistent metrics.

## Remaining scope

Not implemented: job pruning, metrics, distributed storage, or a dashboard.
This is a small queue engine, not a production-readiness or throughput claim.

Tests use temporary databases, independent worker processes for competing
claims, forced worker termination and reopening, and controlled clocks for
lease/retry boundaries. There are no fabricated benchmarks or usage claims.

This original implementation, tests, and documentation were developed with
GitHub Copilot AI assistance. AI assistance is not evidence of independent
author expertise; anyone presenting this project should review and understand
its implementation and failure semantics. Licensed under MIT.
