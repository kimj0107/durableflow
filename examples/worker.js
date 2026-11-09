import { Queue, Worker } from '../src/index.js';

const queue = new Queue(':memory:');
const stop = new AbortController();
for (let index = 0; index < 6; index++) {
  queue.enqueue('receipt', { orderId: `demo-${index}` }, { idempotencyKey: `receipt-${index}` });
}
let handled = 0;
const worker = new Worker(queue, {
  receipt: async (job, { signal }) => {
    signal.throwIfAborted();
    console.log(`Handle ${job.payload.orderId}; downstream idempotency key = ${job.id}`);
    if (++handled === 6) stop.abort();
  },
}, { concurrency: 2 });
try {
  console.log(await worker.run({ signal: stop.signal }));
} finally {
  queue.close();
}
