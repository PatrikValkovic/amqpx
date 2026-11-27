# Topology

Topology refers to the shape of your RabbitMQ setup: exchanges, queues, and the bindings between them. amqpx models these as a hierarchy of objects, each exposing factory methods for the layer below.

```
Connection
  ├── Exchange
  │     ├── Producer
  │     └── Consumer (creates a temporary exclusive queue internally)
  └── Queue
        ├── Producer
        └── Consumer
```

Every producer and consumer gets its own dedicated channel automatically, so you never have to manage channels in the typical workflow. For the rare cases where you need one, see [Direct channel access](#direct-channel-access).

## Connection

Use `connect` to create a connection and establish it in one step:

```typescript
import { connect } from 'amqpx'

const connection = await connect({
  hostname: 'localhost',
  port: 5672,
  username: 'guest',
  password: 'guest',
  vhost: '/',
})
```

`connect` accepts:
1. `options` — amqplib `Options.Connect` (hostname, port, vhost, credentials, etc.)
2. `retryStrategy` _(optional)_ — how to handle reconnections (see below)
3. `socketOptions` _(optional)_ — passed through to amqplib's socket

Call `connection.close()` when you are done. For graceful shutdown across multiple connections and consumers see the [Graceful Shutdown example](/examples/graceful-shutdown).

### Retry strategy

By default, amqpx retries up to **10 times** with exponential backoff starting at 100 ms and jitter. You can override this.

For example to has exponential delay starting at 200 ms with maximum 10 000 ms delay use `cappedExponentialBackoff`.

```typescript
import { connect, retryStrategies } from 'amqpx'

const connection = await connect(
  { hostname: 'localhost', username: 'guest', password: 'guest' },
  {
    reconnectionTimeoutMs: retryStrategies.cappedExponentialBackoff(200, 10_000),
    maxRetries: 20,
  },
)
```

Available strategies (all from `retryStrategies.*`):

| Strategy | Formula | Grows |
|---|---|---|
| `linearBackoff(delay)` | constant `delay` | never |
| `exponentialBackoff(multiplier, base?, jitter?)` | `multiplier × base^(step−1)` | doubles by default |
| `cappedExponentialBackoff(multiplier, maxDelay, base?, jitter?)` | exponential, capped at `maxDelay` | up to cap |
| `polynomialBackoff(multiplier, exponent?, jitter?)` | `multiplier × step^exponent` | quadratic by default |
| `fibonacciBackoff(multiplier, jitter?)` | Fibonacci sequence × multiplier | slower than exponential |

All strategies accept an optional `jitter` parameter (default `0.25`) that randomizes delays within ±25% to prevent thundering herd when many clients reconnect simultaneously. Set `jitter: 0` to disable it.

`reconnectionTimeoutMs` can also be a plain number (treated as a constant delay in ms).

When `maxRetries` is exhausted, amqpx emits `connectionRetryExhausted` and throws a `TooManyRetriesError`. After that, you may call `connect()` again to start a fresh sequence.

### Connection events

The connection emits events covering the full reconnection lifecycle:

```typescript
connection.on('connected', () => console.log('connected'))
connection.on('reconnecting', () => console.log('reconnecting...'))
connection.on('connectionError', (err) => console.error('attempt failed', err))
connection.on('connectionRetryExhausted', () => console.error('gave up'))
connection.on('close', () => console.log('connection closed'))
connection.on('error', (err) => console.error('connection error', err))
```

## Exchanges

Create and assert an exchange from the connection:

```typescript
import { AssertionMode } from 'amqpx'

const exchange = await connection.createExchange('orders', 'topic', {
  assertionMode: AssertionMode.Assert,  // default
  durable: true,
})
await exchange.assert()
```

You can pass any [amqplib](https://amqp-node.github.io/amqplib/channel_api.html#channel_assertExchange) options to the exchange constructor.

`assert()` makes the actual broker call. You don't necessary need to call it, as it will be called by default by entities up the chain (e.g. producer or during  binding). But it is good practice if you want to have stable topology or you code may not attach the consumer right away.

### AssertionMode

| Mode | Behaviour |
|---|---|
| `Assert` _(default)_ | Creates the entity if absent; throws if it exists with incompatible options |
| `Check` | Verifies the entity exists; throws if absent, does not create |
| `Passive` | Skips all broker interaction; no network call is made |

### Predefined exchanges

Amqpx ships factory functions for RabbitMQ's built-in exchange types. They use `AssertionMode.Passive` so no assertion is attempted — they reference the exchange that RabbitMQ has running by default. Pass the connection and await the result:

```typescript
import { predefined } from 'amqpx'

const direct  = await predefined.directExchange(connection)   // amq.direct
const fanout  = await predefined.fanoutExchange(connection)   // amq.fanout
const topic   = await predefined.topicExchange(connection)    // amq.topic
const headers = await predefined.headersExchange(connection)  // amq.headers
const match   = await predefined.matchExchange(connection)    // amq.match
const def     = await predefined.defaultExchange(connection)  // '' (default exchange)
```

## Queues

```typescript
const queue = await connection.createQueue('order-events', {
  assertionMode: AssertionMode.Assert,
  durable: true,
})
await queue.assert()
```

Same `AssertionMode` semantics as exchanges.

To inspect a queue without creating it, use `connection.checkQueue`. It resolves with the queue metadata (`queue`, `messageCount`, `consumerCount`) and rejects if the queue does not exist:

```typescript
const { messageCount, consumerCount } = await connection.checkQueue('order-events')
```

## Bindings

Bind a queue to an exchange with a routing key:

```typescript
await exchange.bindQueue(queue, 'orders.#')
// or from the queue side:
await queue.bindExchange(exchange, 'orders.#')
```

Both forms do the same thing: the queue performs the binding and remembers it. Whenever the queue is re-declared (for example after a reconnect, when a consumer starts listening again), it re-asserts the exchange if needed and restores the binding. Re-asserting only the exchange does not restore queue bindings.

Or exchange-to-exchange binding:

```typescript
await sourceExchange.bindExchange(destinationExchange, 'routing.key')
```

For headers exchanges, pass binding arguments as a third parameter:

```typescript
await exchange.bindQueue(queue, '', { 'x-match': 'all', region: 'eu' })
```

## Topology definition

Typically, you want to have function in your code that will define the whole topology in one place and then attach your producers and consumers.

```typescript
async function topology(connection: Connection) {
    const exchange = await connection.createExchange('orders', 'topic', { durable: true });

    const priorityDlq = await connection.createQueue('priority-orders-dlq', { durable: true });
    const priorityOrders = await connection.createQueue('priority-orders', {
        durable: true,
        arguments: {
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': await priorityDlq.name(),
        },
    });

    const ordersDlq = await connection.createQueue('orders-dlq', { durable: true });
    const orders = await connection.createQueue('orders', {
        durable: true,
        arguments: {
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': await ordersDlq.name(),
        },
    });

    await Promise.all([
        priorityOrders.bindExchange(exchange, 'priority.#'),
        orders.bindExchange(exchange, 'normal.#'),
    ]);

    // No need to assert, bindings asserted entities for you 

    return {
        exchange,
        priorityOrders,
        orders,
    };
}
```

## Escape hatch — `.native()`

If you need amqplib features not exposed by amqpx, the connection gives you the underlying amqplib object, already initialized:

```typescript
const rawConnection = await connection.native() // amqp.ChannelModel
```

Channels expose the same method; see [Direct channel access](#direct-channel-access).

## Direct channel access

You do not need channels in the typical workflow: exchanges and queues are created from the connection, and every producer and consumer opens its own dedicated channel. Create a channel yourself only when you need direct control over it, for example to:

- open a confirm channel by hand instead of passing `isConfirmed: true` to a producer,
- share one channel between several producers or consumers through their `channel` option,
- call amqplib directly with `channel.native()`.

Create a channel from the connection; pass `true` for a confirm channel:

```typescript
const channel = connection.createChannel()                // regular channel
const confirmedChannel = connection.createChannel(true)  // confirm channel
```

A channel offers the same factory methods as the connection (`createExchange`, `createQueue`, `checkQueue`, `createProducerForQueue`, `createConsumerForQueue`, …), but producers and consumers created from it share that channel instead of getting their own. Keep in mind that prefetch is configured per channel, so consumers sharing a channel also share their prefetch limit.

The predefined exchanges accept a channel too; in that case they return the exchange synchronously:

```typescript
const fanout = predefined.fanoutExchange(channel)
```

To pass an existing channel to a single producer or consumer, use the `channel` option:

```typescript
const consumer = await queue.createConsumer<Order>({ channel })
```

`channel.native()` returns the underlying amqplib channel, already open:

```typescript
const rawChannel = await channel.native() // amqp.Channel | amqp.ConfirmChannel
```
