# Consuming

A `Consumer<T>` receives messages of type `T` from a queue. You register a handler with `listen()` and the consumer calls it for each incoming message.

## Creating a consumer

Start from a queue. This example assumes `defineTopology` sets up the connection and returns the exchange and queues — see [Topology](/guide/topology):

```typescript
import { connect, ConsumptionFailureStrategy } from 'amqpx'

const connection = await connect({ hostname: 'localhost', username: 'guest', password: 'guest' })

const { queue } = await defineTopology(connection)

type Order = { orderId: string; total: number }

const consumer = await queue.createConsumer<Order>({ prefetch: 10 })

await consumer.listen(async ({ message, channel, rabbitMessage }) => {
  console.log('received order', message.orderId)
  // message is typed as Order
  // rabbitMessage is the raw amqplib Message if you need headers, routing key, etc.
})
```

The handler receives:
- `message` — the parsed payload typed as `T`
- `channel` — the amqpx channel (useful for manual ack/nack if needed)
- `rabbitMessage` — the raw `amqp.Message` with headers, `routingKey`, `fields`, etc.

## Failure strategies

When your handler throws, amqpx applies the `failureStrategy`:

| Strategy | What happens | Use when |
|---|---|---|
| `Reject` _(default)_ | Message is nacked; moves to dead-letter queue if configured | You want failed messages in a DLQ |
| `Drop` | Message is acked (discarded) | Errors are expected and ignorable |
| `Requeue` | Message is nacked with `requeue: true`; returned to the queue | Transient failures, but beware of infinite retry loops |

```typescript
import { ConsumptionFailureStrategy } from 'amqpx'

const consumer = await queue.createConsumer<Order>({
  failureStrategy: ConsumptionFailureStrategy.Requeue,
})
```

## Prefetch

`prefetch` controls how many unacknowledged messages the broker delivers at once. It is the primary way to limit consumer concurrency:

```typescript
const consumer = await queue.createConsumer<Order>({ prefetch: 5 })
```

> **Warning:** prefetch is set at the channel level. Use a dedicated channel per consumer (which is the default) so different consumers do not share the same prefetch counter or influence each other.

## Custom deserialization

Messages are parsed with `JSON.parse` by default. Override it to handle other formats:

```typescript
import { decode } from 'some-proto-library'

const consumer = await queue.createConsumer<Order>({
  parseMessageFn: (buffer) => decode(buffer),
})
```

## amqplib consume options

Pass options directly to `amqplib.channel.consume` (except `noAck`, which is managed by the `failureStrategy`):

```typescript
const consumer = await queue.createConsumer<Order>({
  consumeOptions: { exclusive: true, priority: 10 },
})
```

## Consumer events

The consumer emits three events for error handling and lifecycle observation:

```typescript
consumer.on('handlingFailed', (error) => {
  // handler threw — error is whatever was thrown
})
consumer.on('reconnectError', (error) => {
  // an error occurred while reconnecting to the broker
})
consumer.on('close', () => {
  // consumer is fully closed and all in-flight messages have been processed
})
```

## Closing a consumer

`consumer.close(timeout?)` stops the consumer from receiving new messages and waits for in-flight handlers to finish. Defaults to 30 000 ms:

```typescript
await consumer.close()          // wait up to 30s
await consumer.close(5_000)     // wait up to 5s
```

## Consuming from an exchange

When you attach a consumer directly to an exchange, amqpx creates an exclusive temporary queue internally and binds it with the pattern you specify:

```typescript
const consumer = await exchange.createConsumer<Order>(
  { pattern: 'orders.#', prefetch: 5 },
)
```

You can control the temporary queue's properties with a second argument:

```typescript
const consumer = await exchange.createConsumer<Order>(
  { pattern: 'orders.*' },
  { arguments: { 'x-message-ttl': 60_000 } },
)
```

## Other creation paths

The connection exposes shortcut methods that create a dedicated channel automatically:

```typescript
const consumer = await connection.createConsumerForQueue(queue, { prefetch: 5 })
const consumer = await connection.createConsumerForExchange(exchange, { pattern: 'orders.#' })
```

## `ConsumerOptions` reference

| Option | Type | Default | Description |
|---|---|---|---|
| `failureStrategy` | `ConsumptionFailureStrategy` | `Reject` | What to do when the handler throws |
| `parseMessageFn` | `(buf: Buffer) => T \| Promise<T>` | `JSON.parse` | Deserialize the message |
| `consumeOptions` | `Omit<amqp.Options.Consume, 'noAck'>` | `{}` | amqplib consume options |
| `prefetch` | `number` | `0` (unlimited) | Max unacked messages from broker |
| `channel` | `Channel \| null` | `null` (new channel) | Existing channel to reuse |
