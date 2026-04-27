# Batch Consuming

A `BatchConsumer<T>` delivers messages in groups rather than one at a time. This is useful when your processing is more efficient in bulk — for example, writing to a database with a single `INSERT` instead of many individual ones.

## When to use batch consuming

- Bulk writes to a database or external service
- Reducing per-message overhead when throughput is more important than individual latency
- Coalescing acknowledgements to reduce broker round-trips

## Creating a batch consumer

This example assumes `defineTopology` sets up the connection and returns the exchange and queues — see [Topology](/guide/topology):

```typescript
import { ConnectionImplementation } from 'amqpx'

const connection = new ConnectionImplementation({ hostname: 'localhost', username: 'guest', password: 'guest' })

const { queue } = await defineTopology(connection)

type Event = { id: string; payload: unknown }

const consumer = await queue.createBatchConsumer<Event>({ batchSize: 50, prefetch: 50 })

await consumer.listen(async ({ messages }) => {
  const rows = messages.map(({ message }) => message)
  await db.bulkInsert(rows)
  // all messages in the batch are automatically acknowledged after the handler returns
})
```

The handler receives a `messages` array. Each entry has:
- `message` — the parsed payload typed as `T`
- `rabbitMessage` — the raw `amqp.Message`

## Batch size

`batchSize` controls the maximum number of messages per batch. If not set, it falls back to `prefetch`; if neither is set, it defaults to **20**.

Set `prefetch` to at least `batchSize` so the broker can pre-deliver a full batch:

```typescript
const consumer = await queue.createBatchConsumer<Event>({
  batchSize: 30,
  prefetch: 120,
})
```

## Partial batches

If not enough messages arrive within `maxWaitTimeForBatch` (default **100 ms**), the consumer flushes whatever it has. This prevents messages from sitting idle in the internal buffer when traffic is low:

```typescript
const consumer = await queue.createBatchConsumer<Event>({
  batchSize: 100,
  maxWaitTimeForBatch: 500, // wait up to 500ms for a full batch
})
```

Tune `maxWaitTimeForBatch` based on your latency tolerance: a longer value maximizes batch fill rate at the cost of tail latency.

## Acknowledgement coalescing

After a batch is processed, amqpx acknowledges all its messages with a single broker call (using `ack(message, allUpTo: true)`). When multiple batches are in flight in parallel, `maxWaitTimeForAck` (default **0 ms**) lets amqpx wait briefly for older batches to finish so it can coalesce even more acknowledgements:

```typescript
const consumer = await queue.createBatchConsumer<Event>({
  batchSize: 50,
  prefetch: 200,       // 4 batches in flight at once
  maxWaitTimeForAck: 50,
})
```

If the older batch does not finish within the timeout, each message must be acknowledged individually.

It is recommended to set `maxWaitTimeForAck` if your handler processing time varies, as it can help reduce the number of individual acknowledgements. But as a side effect, single message may be processed multiple times if the broker restarts or the connection drops before the acknowledgment is sent.

Note that as default value is 0 ms, the implementation will acknowledge all messages immediately after processing.

## Failure strategies

### Batch-level: `batchFailureStrategy`

When the handler throws, `batchFailureStrategy` determines what happens to the whole batch:

| Strategy | Behaviour |
|---|---|
| `Fail` _(default)_ | Reject the entire batch; each message is handled according to `failureStrategy` |
| `Split` | Re-run the handler for each message separately; failures then fall through to `failureStrategy` |

`Split` is useful when a single bad message should not block the rest of the batch. The `batchSize` must be greater than 1.

### Message-level: `failureStrategy`

After a batch fails (or after split), each individual message is handled by `failureStrategy` — the same `Drop / Requeue / Reject` options as in [regular consuming](/guide/consuming#failure-strategies).

```typescript
import { BatchFailureStrategy, ConsumptionFailureStrategy } from 'amqpx'

const consumer = await queue.createBatchConsumer<Event>({
  batchSize: 50,
  batchFailureStrategy: BatchFailureStrategy.Split,
  failureStrategy: ConsumptionFailureStrategy.Reject,
})
```

## Other creation paths

Batch consumers can also be created from an exchange directly, or via connection-level shortcuts that handle channel creation automatically:

```typescript
// from exchange directly — amqpx creates a temporary exclusive queue internally
const consumer = await exchange.createBatchConsumer<Event>({ pattern: 'events.*', batchSize: 20 })

// connection-level shortcut — creates a dedicated channel automatically
const consumer = await connection.createBatchConsumerForQueue(queue, { batchSize: 50 })
const consumer = await connection.createBatchConsumerForExchange(exchange, { pattern: 'events.#', batchSize: 50 })
```

## `BatchConsumerOptions` reference

Inherits all options from [`ConsumerOptions`](/guide/consuming#consumeroptions-reference) plus:

| Option | Type | Default | Description |
|---|---|---|---|
| `batchSize` | `number` | `prefetch` or `20` | Max messages per batch |
| `maxWaitTimeForBatch` | `number` | `100` | ms to wait for a full batch before flushing |
| `maxWaitTimeForAck` | `number` | `0` | ms to wait for older batches before acking individually |
| `batchFailureStrategy` | `BatchFailureStrategy` | `Fail` | What to do when the handler throws |
