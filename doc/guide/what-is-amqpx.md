# What is amqpx?

amqpx is a TypeScript library that wraps [amqplib](https://www.npmjs.com/package/amqplib) with a structured, composable API. It adds:

- **Generic message types** — producers and consumers are typed so TypeScript catches mismatches at compile time.
- **Automatic reconnection** — when the broker drops the connection, amqpx reconnects transparently using a configurable retry strategy.
- **Topology helpers** — declare exchanges, queues, and bindings through a layered object model rather than raw channel calls. Ensure the topology survives broker restarts and recreate it if broker burns down.
- **Failure strategies** — choose how a consumer handles errors (drop, requeue, reject) without any implementation on your side or touching transport plumbing.
- **Batch consuming** — built-in batching with configurable size, timeouts, and acknowledgement coalescing.
- **Extensions** — optional Zod validation and test doubles for Vitest/Jest ship as separate entry points.

## Relation to amqplib

amqpx does not replace amqplib — it layers on top of it. Every entity in amqpx wraps the corresponding amqplib primitive:

| amqpx | amqplib equivalent |
|---|---|
| `ConnectionImplementation` | `amqp.connect()` result (`ChannelModel`) |
| `Channel` | `connection.createChannel()` result |
| `Exchange.assert()` | `channel.assertExchange()` |
| `Queue.assert()` | `channel.assertQueue()` |
| `Producer.publish()` | `channel.publish()` / `channel.sendToQueue()` |
| `Consumer.listen()` | `channel.consume()` |

If you ever need to drop down to the amqplib level — for features not exposed by amqpx — every entity exposes a `.native()` method that returns the underlying amqplib object already initialized:

```typescript
const rawChannel = await channel.native() // amqp.Channel | amqp.ConfirmChannel
const rawConnection = await connection.native() // amqp.ChannelModel
```

## Installation

```bash
npm install amqpx
```

amqpx ships four entry points:

| Import path | Contents |
|---|---|
| `amqpx` | Core entities (Connection, Channel, Exchange, Queue, Producer, Consumer, BatchConsumer) |
| `amqpx/zod` | `ZodValidatedConsumer` and `ZodValidatedBatchConsumer` |
| `amqpx/jest` | Test doubles for Jest |
| `amqpx/vitest` | Test doubles for Vitest |

The extension entry points require the corresponding peer dependency:

```bash
npm install zod          # for amqpx/zod
npm install jest         # for amqpx/jest
npm install vitest       # for amqpx/vitest
```

### TypeScript

Amqpx is written in TypeScript and provides TypeScript declarations, bringing full type safety out of the box. All entities are properly typed, and you can leverage TypeScript's type system to catch errors at compile time.

## Connection states

A `Connection` moves through a defined set of states over its lifetime:

```
                                ┌──────────────────────────────────┐
                                🡫                                  |
preconnect ──connect()──► connecting ──success──► connected──► server drop
                                🡫                     🡫
                         failure (retry)           close()
                                🡫                     │
                        connectionError               🡫
                                🡫                  closing
                       maxRetries exhausted           │
                                🡫                     🡫
                       connectionRetryExhausted ──► closed
```

You can read the current state with `connection.state()` and listen to transitions via events:

| Event | Fired when |
|---|---|
| `connected` | Connection established or re-established |
| `reconnecting` | Server dropped the connection; client is retrying |
| `connectionError` | A single connection attempt failed (fires once per retry) |
| `connectionRetryExhausted` | All retries exhausted; connection moves to `closed` |
| `close` | `connection.close()` was called and completed |
| `error` | The underlying amqplib connection emitted an error |
