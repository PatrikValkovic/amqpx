# Work Queue

Multiple consumers compete for messages from a single queue. Each message is processed by exactly one worker. Use `prefetch: 1` to ensure work is distributed fairly — a worker only receives a new message after it finishes the current one.

## Code

```typescript
import { connect, Connection, AssertionMode, ConsumptionFailureStrategy, Queue } from 'amqpx'

type Job = { jobId: string; payload: string }

async function startWorker(queue: Queue, workerId: number) {
  const consumer = await queue.createConsumer<Job>({
    prefetch: 1,  // one job at a time per worker
    failureStrategy: ConsumptionFailureStrategy.Requeue,
  })

  await consumer.listen(async ({ message }) => {
    console.log(`[worker-${workerId}] processing job ${message.jobId}`)
    await doWork(message)
    console.log(`[worker-${workerId}] done`)
  })

  return consumer
}

async function doWork(job: Job): Promise<void> {
  // simulate work
  await new Promise(resolve => setTimeout(resolve, 100))
}

// ---- Topology ----

async function topology(connection: Connection) {
  const channel = await connection.createChannel()
  const queue = channel.createQueue('jobs', { durable: true })
  return { queue }
}

// ---- Setup ----

const connection = await connect({
  hostname: 'localhost',
  username: 'guest',
  password: 'guest',
})

const { queue } = await topology(connection)

const producer = await queue.createProducer<Job>()

// Start three competing workers
const workers = await Promise.all([
  startWorker(queue, 1),
  startWorker(queue, 2),
  startWorker(queue, 3),
])

// Submit some jobs
for (let i = 1; i <= 9; i++) {
  await producer.publish({ jobId: `job-${i}`, payload: `data-${i}` })
}

// Each worker processes roughly 3 jobs
```

## Key points

- `prefetch: 1` ensures each worker processes one message at a time. Without it, the broker could overwhelm a single worker.
- `failureStrategy: Requeue` returns a failed job to the queue so another worker can try. Be careful of infinite retry loops — consider using a dead-letter exchange for persistent failures.
- `options: { persistent: true }` tells RabbitMQ to persist messages to disk so they survive a broker restart.
- Each worker has its own dedicated channel. Sharing a channel between workers is not recommended because `prefetch` is channel-scoped.
