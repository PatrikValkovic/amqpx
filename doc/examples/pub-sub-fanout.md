# Pub/Sub (Fanout)

Every subscriber receives every message, regardless of routing key. Uses RabbitMQ's built-in fanout exchange.

## Code

```typescript
import { connect, Connection, predefined } from 'amqpx'

type Notification = { event: string; payload: unknown }

// ---- Topology ----

async function topology(connection: Connection) {
  const channel = await connection.createChannel()
  // No assert() needed — the predefined exchange already exists in RabbitMQ
  return { fanout: predefined.fanoutExchange(channel) }
}

// ---- Setup ----

const connection = await connect({
  hostname: 'localhost',
  port: 5672,
  username: 'guest',
  password: 'guest',
})

const { fanout } = await topology(connection)

// ---- Publisher ----

const producer = await fanout.createProducer<Notification>()

// ---- Subscribers ----

// Each subscriber gets its own channel and an exclusive queue that is
// automatically bound to the fanout exchange by amqpx.
const consumer1 = await fanout.createConsumer<Notification>({
  pattern: '',  // fanout ignores the pattern, but it is required by the API
})
const consumer2 = await fanout.createConsumer<Notification>({
  pattern: '',
})

await consumer1.listen(async ({ message }) => {
  console.log('[subscriber-1] received', message.event)
})

await consumer2.listen(async ({ message }) => {
  console.log('[subscriber-2] received', message.event)
})

// ---- Publish ----

await producer.publish({ event: 'user.signed_up', payload: { userId: 42 } })
// Both subscribers receive the message.
```

## Key points

- `predefined.fanoutExchange()` references the built-in `amq.fanout` exchange. No assertion is needed.
- Each `exchange.createConsumer()` call creates a dedicated exclusive queue internally. When the consumer closes, the queue is deleted automatically.
- All subscribers receive all published messages — there is no routing.
