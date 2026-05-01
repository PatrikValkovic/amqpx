# Topic Routing

Route messages to different consumers based on dot-separated routing key patterns. `*` matches exactly one word; `#` matches zero or more words.

## Code

```typescript
import { connect, Connection } from 'amqpx'

type OrderEvent = {
  orderId: string
  region: 'eu' | 'us'
  status: 'created' | 'paid' | 'shipped' | 'cancelled'
}

// ---- Topology ----

async function topology(connection: Connection) {
  const channel = await connection.createChannel()
  const exchange = channel.createExchange('order-events', 'topic', { durable: true })
  return { exchange }
}

// ---- Setup ----

const connection = await connect({
  hostname: 'localhost',
  username: 'guest',
  password: 'guest',
})

const { exchange } = await topology(connection)

// ---- Producer ----
// Route key: orders.<region>.<status>

const producer = await exchange.createProducer<OrderEvent>({
  routingKey: (event) => `orders.${event.region}.${event.status}`,
})

// ---- Consumers ----

// Consumer 1: EU orders only
// Pattern `orders.eu.*` matches exactly one word after "orders.eu."
const euOrdersConsumer = await exchange.createConsumer<OrderEvent>({
  pattern: 'orders.eu.*',
  prefetch: 10,
  channel: await connection.createChannel(),
})

// Consumer 2: all orders from all regions (full audit log)
// Pattern `orders.#` matches any number of words after "orders."
const allOrdersConsumer = await exchange.createConsumer<OrderEvent>({
  pattern: 'orders.#',
  prefetch: 10,
  channel: await connection.createChannel(),
})

await euOrdersConsumer.listen(async ({ message, rabbitMessage }) => {
  console.log(
    `[eu-orders]   ${rabbitMessage.fields.routingKey}: order ${message.orderId} is ${message.status}`
  )
})

await allOrdersConsumer.listen(async ({ message, rabbitMessage }) => {
  console.log(
    `[all-orders]  ${rabbitMessage.fields.routingKey}: order ${message.orderId} is ${message.status}`
  )
})

// ---- Publish ----

await producer.publish({ orderId: 'O-1', region: 'eu', status: 'created' })
// → both consumers receive (routing key: orders.eu.created)

await producer.publish({ orderId: 'O-1', region: 'eu', status: 'paid' })
// → both consumers receive (routing key: orders.eu.paid)

await producer.publish({ orderId: 'O-2', region: 'us', status: 'created' })
// → only allOrdersConsumer receives (routing key: orders.us.created)

await producer.publish({ orderId: 'O-2', region: 'us', status: 'shipped' })
// → only allOrdersConsumer receives (routing key: orders.us.shipped)
```

## Pattern reference

| Pattern | Matches | Does not match |
|---|---|---|
| `orders.eu.*` | `orders.eu.created`, `orders.eu.paid` | `orders.us.created`, `orders.eu.next.day` |
| `orders.#` | `orders.eu.created`, `orders.us.shipped`, `orders` | — |
| `#` | everything | — |
| `orders.*.created` | `orders.eu.created`, `orders.us.created` | `orders.eu.paid`, `orders.eu.next.day` |

## Key points

- `exchange.createConsumer()` creates a temporary exclusive queue and binds it with the given pattern. The queue is cleaned up when the consumer closes.
- The raw routing key is available on `rabbitMessage.fields.routingKey`.
- Multiple patterns can be consumed by creating multiple consumers or by binding additional queues with `exchange.bindQueue(queue, pattern)`.
