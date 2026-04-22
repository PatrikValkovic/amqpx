# Getting Started

## Installation

```bash
npm install amqpx
```

Peer dependencies are optional — install only what you need:

```bash
# Zod validation consumer
npm install zod

# Test utilities for Vitest
npm install --save-dev vitest

# Test utilities for Jest
npm install --save-dev jest
```

## Entry points

| Import path    | Contents                                                                |
|----------------|-------------------------------------------------------------------------|
| `amqpx`        | Core library — Connection, Channel, Exchange, Queue, Producer, Consumer |
| `amqpx/vitest` | In-memory mock implementations for Vitest                               |
| `amqpx/jest`   | In-memory mock implementations for Jest                                 |
| `amqpx/zod`    | ZodValidatedConsumer decorator                                          |

## Quick example

The following sets up a connection, declares a queue, publishes a message, and consumes it.

```typescript
import { Connection } from 'amqpx'

const connection = Connection.create({ url: 'amqp://localhost' })
await connection.connect()

const channel = await connection.channel()
const queue = await channel.queue('my-queue')
await queue.assert()

// Publish
const producer = await queue.producer<{ text: string }>()
await producer.send({ text: 'hello' })

// Consume
const consumer = await queue.consumer<{ text: string }>({
  handler: async (message) => {
    console.log(message.text)
  },
})
await consumer.start()
```

## Automatic reconnect

Pass a `RetryStrategy` when creating the connection to enable automatic reconnection on broker failure:

```typescript
import { Connection, ExponentialRetryStrategy } from 'amqpx'

const connection = Connection.create({
  url: 'amqp://localhost',
  retry: new ExponentialRetryStrategy({ base: 500, max: 30_000 }),
})
```

## Using with Zod

```typescript
import { ZodValidatedConsumer } from 'amqpx/zod'
import { z } from 'zod'

const schema = z.object({ text: z.string() })

const consumer = new ZodValidatedConsumer(baseConsumer, schema, {
  handler: async (message) => {
    // message is typed as { text: string }
    console.log(message.text)
  },
})
```

## Testing

Use the in-memory mocks to unit-test code that depends on amqpx without a running broker:

```typescript
import { TestConnection } from 'amqpx/vitest'

const connection = new TestConnection()
const channel = await connection.channel()
const queue = await channel.queue('my-queue')

// Inject a message directly
await queue.inject({ text: 'hello' })
```
