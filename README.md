# amqpx

Type-safe RabbitMQ messaging for TypeScript, built on top of [amqplib](https://www.npmjs.com/package/amqplib).

- **Generic producers and consumers** — define message shapes once, TypeScript enforces them everywhere
- **Automatic reconnect** — pluggable retry strategies handle broker failures transparently
- **Composable topology** — Connection → Channel → Exchange / Queue → Producer / Consumer
- **Failure strategies** — drop, requeue, or reject on handler errors, per consumer
- **Batch consuming** — built-in batching with configurable size, timeouts, and ack coalescing
- **Extensions** — optional Zod validation and in-memory test doubles for Vitest/Jest

## Installation

```bash
npm install amqpx
```

| Entry point | Contents |
|---|---|
| `amqpx` | Core entities |
| `amqpx/zod` | `ZodValidatedConsumer`, `ZodValidatedBatchConsumer` |
| `amqpx/jest` | Test doubles for Jest |
| `amqpx/vitest` | Test doubles for Vitest |

## Quick example

```typescript
import { ConnectionImplementation, AssertionMode } from 'amqpx'

type Order = { orderId: string; total: number }

const connection = new ConnectionImplementation({
  hostname: 'localhost',
  username: 'guest',
  password: 'guest',
})
await connection.connect()

// Topology
const producerChannel = await connection.createChannel()
const queue = producerChannel.createQueue('orders', { durable: true })
await queue.assert()

// Produce
const producer = await queue.createProducer<Order>()
await producer.publish({ orderId: 'abc-123', total: 49.99 })

// Consume
const consumerChannel = await connection.createChannel()
const consumerQueue = consumerChannel.createQueue('orders', {
  assertionMode: AssertionMode.Check,
})
const consumer = await consumerQueue.createConsumer<Order>({ prefetch: 10 })

await consumer.listen(async ({ message }) => {
  console.log('order', message.orderId, 'total', message.total)
})
```

## Documentation

Full documentation at **https://patrikvalkovic.github.io/amqpx/**

- [Guide](https://patrikvalkovic.github.io/amqpx/guide/what-is-amqpx)
- [Examples](https://patrikvalkovic.github.io/amqpx/examples/)
- [API Reference](https://patrikvalkovic.github.io/amqpx/api/)

## License

MIT
