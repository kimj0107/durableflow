import { Queue } from '../src/index.js';

const queue = new Queue(process.argv[2] ?? 'durableflow-example.db');
try {
  const submitted = queue.enqueue('greet', { name: 'Ada' }, {
    idempotencyKey: 'example:greet:ada',
  });
  console.log('Submitted:', submitted.id, submitted.status);
  const claimed = queue.claim({ leaseMs: 10_000 });
  if (claimed) {
    try {
      console.log(`Hello, ${claimed.payload.name}!`);
    } catch (error) {
      queue.fail(claimed.id, claimed.leaseToken, error, { retryDelayMs: 1000 });
      throw error;
    }
    queue.ack(claimed.id, claimed.leaseToken);
    console.log('Acknowledged:', claimed.id);
  } else {
    console.log('No available work (a repeated example run is deduplicated).');
  }
} finally {
  queue.close();
}
