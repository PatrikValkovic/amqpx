# Topology

Topology refers to the shape of your RabbitMQ setup: exchanges, queues, and the bindings between them. amqpx models these as a hierarchy of objects, each exposing factory methods for the layer below.

```
Connection
  └── Channel
        ├── Exchange
        │     ├── Producer
        │     └── Consumer (creates a temporary exclusive queue internally)
        └── Queue
              ├── Producer
              └── Consumer
```

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

## Channel

Each producer or consumer should have a dedicated channel. Create one from a connection:

```typescript
const channel = await connection.createChannel()          // regular channel
const confirmedChannel = await connection.createChannel(true)  // confirm channel
```

Use a **confirm channel** when you need delivery guarantees (publisher confirms). See [Publishing](/guide/publishing) for details.

## Exchanges

Create and assert an exchange from a channel:

```typescript
import { AssertionMode } from 'amqpx'

const exchange = channel.createExchange('orders', 'topic', {
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

Amqpx ships factory functions for RabbitMQ's built-in exchange types. They use `AssertionMode.Passive` so no assertion is attempted — they reference the exchange that RabbitMQ has running by default:

```typescript
import { predefined } from 'amqpx'

const direct  = predefined.directExchange(channel)   // amq.direct
const fanout  = predefined.fanoutExchange(channel)   // amq.fanout
const topic   = predefined.topicExchange(channel)    // amq.topic
const headers = predefined.headersExchange(channel)  // amq.headers
const match   = predefined.matchExchange(channel)    // amq.match
const def     = predefined.defaultExchange(channel)  // '' (default exchange)
```

## Queues

```typescript
const queue = channel.createQueue('order-events', {
  assertionMode: AssertionMode.Assert,
  durable: true,
})
await queue.assert()
```

Same `AssertionMode` semantics as exchanges.

## Bindings

Bind a queue to an exchange with a routing key:

```typescript
await exchange.bindQueue(queue, 'orders.#')
// or from the queue side:
await queue.bindExchange(exchange, 'orders.#')
```

Or exchange-to-exchange binding:

```typescript
await sourceExchange.bindExchange(destinationExchange, 'routing.key')
```

For headers exchanges, pass binding arguments as a third parameter:

```typescript
await exchange.bindQueue(queue, '', { 'x-match': 'all', region: 'eu' })
```

## Escape hatch — `.native()`

If you need amqplib features not exposed by amqpx, every entity gives you direct access:

```typescript
const rawChannel    = await channel.native()    // amqp.Channel | amqp.ConfirmChannel
const rawConnection = await connection.native() // amqp.ChannelModel
const rawQueue      = await queue.native()      // amqp.Replies.AssertQueue
```

The returned values are already initialized and asserted.

## Topology definition

Typically, you want to have function in your code that will define the whole topology in one place and then attach your producers and consumers.

```typescript
async function topology(connection: Connection) {
    const channel = await connection.createChannel();

    const exchange = await channel.createExchange('orders', 'topic', { durable: true });

    const priorityDlq = await channel.createQueue('priority-orders-dlq', { durable: true });
    const priorityOrders = await channel.createQueue('priority-orders', {
        durable: true,
        arguments: {
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': await priorityDlq.name(),
        },
    });

    const ordersDlq = await channel.createQueue('orders-dlq', { durable: true });
    const orders = await channel.createQueue('orders', {
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