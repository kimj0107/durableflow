export class LeaseLostError extends Error {
  constructor(jobId) {
    super(`Job ${jobId} has no matching, unexpired lease`);
    this.name = 'LeaseLostError';
    this.jobId = jobId;
  }
}

export class ShutdownTimeoutError extends Error {
  constructor() {
    super('Worker shutdown deadline exceeded; unfinished leases will expire');
    this.name = 'ShutdownTimeoutError';
  }
}
