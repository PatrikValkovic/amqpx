# Publishing

A `Producer<T>` sends messages of type `T` to a and exchange or queue (through default exchange). The generic type is enforced by TypeScript — passing the wrong payload shape is a compile error.

## Creating a producer

The typical starting point is creating a producer from an exchange. This example assumes `defineTopology` sets up the connection and returns the exchange and queues — see [Topology](/guide/topology):

```typescript
import { ConnectionImplementation } from 'amqpx'

const connection = new ConnectionImplementation({ hostname: 'localhost', username: 'guest', password: 'guest' })
await connection.connect()

const { exchange } = await defineTopology(connection)

type Order = { orderId: string; total: number }

const producer = await exchange.createProducer<Order>({ routingKey: 'orders.new' })
await producer.publish({ orderId: 'abc-123', total: 49.99 })
```

## Routing keys

By default, the routing key is an empty string. You can set a static key or derive it from the message at the producer level, or override it per individual publish call:

```typescript
// static key at producer level
const producer = await exchange.createProducer<Order>({ routingKey: 'orders.new' })

// derived from message at producer level
const producer = await exchange.createProducer<Order>({
  routingKey: (order) => `orders.${order.status}`,
})

// override per publish call — accepts a static string or a function
await producer.publish(order, 'orders.priority')
await producer.publish(order, (o) => `orders.${o.region}`)
```

A per-call routing key takes precedence over the producer-level `routingKey` option.

## Serialization

Messages must be serialized to `Buffer` before being sent. The default is JSON. Provide your own function to use a different format:

```typescript
import { encode } from 'some-proto-library'

const producer = await exchange.createProducer<Order>({
  stringifyMessage: (order) => encode(order),
})
```

> **Note:** when using a custom serializer, the consumer must use a matching deserializer — see [Custom deserialization](/guide/consuming#custom-deserialization).

## amqplib publish options

Pass amqplib options at the producer level to apply them to every message:

```typescript
const producer = await exchange.createProducer<Order>({
  options: {
    persistent: true,
    contentType: 'application/json',
    headers: { source: 'checkout-service' },
  },
})
```

Per-publish options are **merged** with the producer-level options — only the fields you provide are overridden, the rest carry over:

```typescript
// persistent and contentType still apply; only headers is overridden
await producer.publish(
  order,
  'order.priority',
  { 
    headers: {
      source: 'priority-service'
    }
  }
)
```

## Standard channel vs publisher confirms

On a **standard channel**, `publish()` is fire-and-forget — it returns as soon as the message is handed to the TCP buffer, without waiting for broker acknowledgement. There is no delivery guarantee.

On a **confirm channel**, `publish()` waits for the broker to ACK or NACK the message. On NACK (meaning one or more queues are full), amqpx retries automatically using `retryStrategy`. This gives you exactly-once delivery semantics at the cost of latency.

## Publisher confirms

Pass `isConfirmed: true` as the second argument to `createProducer`, or create the producer from a confirmed channel:

```typescript
// pass isConfirmed flag
const producer = await exchange.createProducer<Order>(
  { retryStrategy: { maxRetries: 5 } },
  true, // isConfirmed
)

// or create from a confirmed channel
const confirmedChannel = await connection.createChannel(true)
const producer = await confirmedChannel.createProducerForExchange(exchange, {
  routingKey: 'orders.new',
})
```

`retryStrategy` controls how NACKed publishes are retried. It can be overridden per-call:

```typescript
await producer.publish(order, undefined, {
  retryStrategy: { maxRetries: 3, reconnectionTimeoutMs: 500 },
})
```

### Error window

On a **standard channel**, the broker never tells the producer whether a message was delivered. The `errorWindow` option (default 5000 ms) compensates: if the channel errors within that window after a publish, amqpx treats messages sent in that window as potentially lost and republishes them. This provides at-least-once delivery within the error window.

Set to `0` to opt out (effectivelly turning publisher into at-most-once delivery semantic):

```typescript
const producer = await exchange.createProducer<Order>({ errorWindow: 0 })
```

Note that `errorWindow` does not apply to confirm-channel producers — delivery is already tracked explicitly via broker ACK/NACK.

## Backpressure

amqplib signals backpressure by returning `false` from `channel.publish()` when its write buffer is full. amqpx handles this automatically — it waits for a `drain` event before continuing. You can tune how long to wait for the vent with `drainTimeout` (default 30 000 ms):

```typescript
const producer = await exchange.createProducer<Order>({ drainTimeout: 10_000 })
```

If the drain does not arrive within the timeout, a `DrainError` is thrown and the producer is closed.

## Producer events

The producer emits events you can hook into for logging, metrics, or error handling:

```typescript
producer.on('beforeSend', (message, routingKey) => {
  // called before each publish
})
producer.on('afterSend', (message, routingKey) => {
  // called after successful publish
})
producer.on('republishFailed', (message, error) => {
  // called when retry is exhausted for a confirmed publish
})
```

## Closing a producer

`producer.close()` waits for all in-flight publishes to complete before returning:

```typescript
await producer.close()
```

## Other creation paths

Beyond `exchange.createProducer()`, producers can also be created directly from a queue (useful when publishing via the default exchange) or from the connection:

```typescript
// publish directly to a queue via the default exchange
const producer = await queue.createProducer<Order>()

// connection-level shortcut — creates a dedicated channel automatically
const producer = await connection.createProducerForQueue(queue)
const producer = await connection.createProducerForExchange(exchange)
```

## `ProducerOptions` reference

| Option | Type | Default | Description |
|---|---|---|---|
| `stringifyMessage` | `(msg: T) => Buffer \| Promise<Buffer>` | `JSON.stringify` | Serialize the message |
| `routingKey` | `string \| (msg: T) => string` | `''` | Routing key |
| `options` | `amqp.Options.Publish` | `{}` | amqplib publish options |
| `channel` | `Channel \| null` | `null` | Existing channel to reuse |
| `drainTimeout` | `number` | `30000` | ms to wait for drain before throwing |
| `errorWindow` | `number` | `5000` | ms window for at-least-once retries |
| `retryStrategy` | `RetryStrategy` | exponential, 10 retries | Retry for confirmed publishes |
