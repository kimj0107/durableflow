import { Queue } from '../../src/index.js';

const queue = new Queue(process.argv[2], { clock: () => 1000 });
process.on('message', (message) => {
  if (message === 'claim') {
    process.send({ kind: 'claimed', job: queue.claim({ leaseMs: 100 }) });
  } else if (message === 'enqueue') {
    process.send({
      kind: 'enqueued',
      job: queue.enqueue('competing', { worker: process.pid }, { idempotencyKey: 'shared' }),
    });
  }
});
process.send({ kind: 'ready' });
