import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { LeaseLostError } from './errors.js';

export { LeaseLostError, ShutdownTimeoutError } from './errors.js';
export { Worker, exponentialBackoff } from './worker.js';

function integer(value, name, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new TypeError(`${name} must be a safe integer >= ${minimum}`);
  }
  return value;
}

function text(value, name) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a nonempty string`);
  }
  return value;
}

function job(row) {
  if (!row) return null;
  return {
    id: row.id,
    type: row.type,
    payload: JSON.parse(row.payload),
    idempotencyKey: row.idempotency_key,
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    availableAt: row.available_at,
    leaseToken: row.lease_token,
    leaseExpiresAt: row.lease_expires_at,
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export class Queue {
  #db;
  #clock;

  constructor(path, { busyTimeoutMs = 5000, clock = Date.now } = {}) {
    text(path, 'path');
    integer(busyTimeoutMs, 'busyTimeoutMs');
    if (typeof clock !== 'function') throw new TypeError('clock must be a function');
    this.#clock = clock;
    this.#db = new DatabaseSync(path, { timeout: busyTimeoutMs });
    try {
      this.#db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;');
      this.#transaction(() => {
        const { user_version: version } = this.#db.prepare('PRAGMA user_version').get();
        if (version !== 0 && version !== 1) {
          throw new Error(`Unsupported DurableFlow schema version: ${version}`);
        }
        if (version === 0) {
          this.#db.exec(`
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              type TEXT NOT NULL,
              payload TEXT NOT NULL,
              idempotency_key TEXT UNIQUE,
              status TEXT NOT NULL CHECK (
                status IN ('pending', 'active', 'completed', 'failed', 'cancelled')
              ),
              attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
              max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
              available_at INTEGER NOT NULL,
              lease_token TEXT,
              lease_expires_at INTEGER,
              last_error TEXT,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              CHECK (attempts <= max_attempts),
              CHECK (
                (status = 'active' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
                OR (status != 'active' AND lease_token IS NULL AND lease_expires_at IS NULL)
              )
            );
            CREATE INDEX jobs_pending ON jobs (available_at, created_at)
              WHERE status = 'pending';
            CREATE INDEX jobs_expiring ON jobs (lease_expires_at)
              WHERE status = 'active';
            PRAGMA user_version = 1;
          `);
        }
      });
    } catch (error) {
      this.#db.close();
      throw error;
    }
  }

  #now() {
    return integer(this.#clock(), 'clock result');
  }

  #transaction(operation) {
    this.#db.exec('BEGIN IMMEDIATE');
    try {
      const result = operation();
      this.#db.exec('COMMIT');
      return result;
    } catch (error) {
      this.#db.exec('ROLLBACK');
      throw error;
    }
  }

  #read(id) {
    return job(this.#db.prepare('SELECT * FROM jobs WHERE id = ?').get(id));
  }

  enqueue(type, payload, {
    idempotencyKey = null,
    maxAttempts = 3,
    delayMs = 0,
  } = {}) {
    text(type, 'type');
    if (idempotencyKey !== null) text(idempotencyKey, 'idempotencyKey');
    integer(maxAttempts, 'maxAttempts', 1);
    integer(delayMs, 'delayMs');
    const serialized = JSON.stringify(payload);
    if (serialized === undefined) throw new TypeError('payload must be JSON serializable');
    return this.#transaction(() => {
      const now = this.#now();
      const availableAt = integer(now + delayMs, 'availableAt');
      const id = randomUUID();
      this.#db.prepare(`
        INSERT INTO jobs (
          id, type, payload, idempotency_key, status, max_attempts,
          available_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)
        ON CONFLICT (idempotency_key) DO NOTHING
      `).run(id, type, serialized, idempotencyKey, maxAttempts, availableAt, now, now);
      return idempotencyKey === null
        ? this.#read(id)
        : job(this.#db.prepare('SELECT * FROM jobs WHERE idempotency_key = ?').get(idempotencyKey));
    });
  }

  #recover(now) {
    return this.#db.prepare(`
      UPDATE jobs SET
        status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
        available_at = ?, lease_token = NULL, lease_expires_at = NULL,
        last_error = 'Lease expired', updated_at = ?
      WHERE status = 'active' AND lease_expires_at <= ?
    `).run(now, now, now).changes;
  }

  recoverExpired() {
    return this.#transaction(() => this.#recover(this.#now()));
  }

  claim({ leaseMs = 30_000 } = {}) {
    integer(leaseMs, 'leaseMs', 1);
    return this.#transaction(() => {
      const now = this.#now();
      const expiresAt = integer(now + leaseMs, 'leaseExpiresAt');
      this.#recover(now);
      const row = this.#db.prepare(`
        SELECT id FROM jobs
        WHERE status = 'pending' AND available_at <= ? AND attempts < max_attempts
        ORDER BY available_at, created_at, rowid LIMIT 1
      `).get(now);
      if (!row) return null;
      this.#db.prepare(`
        UPDATE jobs SET status = 'active', attempts = attempts + 1,
          lease_token = ?, lease_expires_at = ?, updated_at = ?
        WHERE id = ?
      `).run(randomUUID(), expiresAt, now, row.id);
      return this.#read(row.id);
    });
  }

  ack(id, leaseToken) {
    text(id, 'id');
    text(leaseToken, 'leaseToken');
    return this.#transaction(() => {
      const now = this.#now();
      const result = this.#db.prepare(`
        UPDATE jobs SET status = 'completed', lease_token = NULL,
          lease_expires_at = NULL, updated_at = ?
        WHERE id = ? AND status = 'active' AND lease_token = ? AND lease_expires_at > ?
      `).run(now, id, leaseToken, now);
      if (result.changes !== 1) throw new LeaseLostError(id);
      return this.#read(id);
    });
  }

  renew(id, leaseToken, { leaseMs = 30_000 } = {}) {
    text(id, 'id');
    text(leaseToken, 'leaseToken');
    integer(leaseMs, 'leaseMs', 1);
    return this.#transaction(() => {
      const now = this.#now();
      const expiresAt = integer(now + leaseMs, 'leaseExpiresAt');
      const result = this.#db.prepare(`
        UPDATE jobs SET lease_expires_at = MAX(lease_expires_at, ?), updated_at = ?
        WHERE id = ? AND status = 'active' AND lease_token = ? AND lease_expires_at > ?
      `).run(expiresAt, now, id, leaseToken, now);
      if (result.changes !== 1) throw new LeaseLostError(id);
      return this.#read(id);
    });
  }

  fail(id, leaseToken, error, { retryDelayMs = 0 } = {}) {
    text(id, 'id');
    text(leaseToken, 'leaseToken');
    integer(retryDelayMs, 'retryDelayMs');
    const message = error instanceof Error ? error.message : text(error, 'error');
    return this.#transaction(() => {
      const now = this.#now();
      const availableAt = integer(now + retryDelayMs, 'availableAt');
      const result = this.#db.prepare(`
        UPDATE jobs SET
          status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END,
          available_at = ?, lease_token = NULL, lease_expires_at = NULL,
          last_error = ?, updated_at = ?
        WHERE id = ? AND status = 'active' AND lease_token = ? AND lease_expires_at > ?
      `).run(availableAt, message, now, id, leaseToken, now);
      if (result.changes !== 1) throw new LeaseLostError(id);
      return this.#read(id);
    });
  }

  cancel(id) {
    text(id, 'id');
    return this.#transaction(() => this.#db.prepare(`
      UPDATE jobs SET status = 'cancelled', updated_at = ?
      WHERE id = ? AND status = 'pending'
    `).run(this.#now(), id).changes === 1);
  }

  failed({ limit = 100 } = {}) {
    integer(limit, 'limit', 1);
    if (limit > 1000) throw new RangeError('limit must be <= 1000');
    return this.#db.prepare(`
      SELECT * FROM jobs WHERE status = 'failed'
      ORDER BY updated_at, rowid LIMIT ?
    `).all(limit).map(job);
  }

  retryFailed(id, { maxAttempts, delayMs = 0 } = {}) {
    text(id, 'id');
    if (maxAttempts !== undefined) integer(maxAttempts, 'maxAttempts', 1);
    integer(delayMs, 'delayMs');
    return this.#transaction(() => {
      const now = this.#now();
      const availableAt = integer(now + delayMs, 'availableAt');
      const result = this.#db.prepare(`
        UPDATE jobs SET status = 'pending', attempts = 0,
          max_attempts = COALESCE(?, max_attempts), available_at = ?,
          last_error = NULL, updated_at = ?
        WHERE id = ? AND status = 'failed'
      `).run(maxAttempts ?? null, availableAt, now, id);
      return result.changes === 1 ? this.#read(id) : null;
    });
  }

  get(id) {
    return this.#read(text(id, 'id'));
  }

  close() {
    this.#db.close();
  }
}
