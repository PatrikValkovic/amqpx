# Validating with Zod

amqpx ships an optional `ZodValidatedConsumer` that validates incoming messages against a Zod schema before passing them to your handler. This keeps transport concerns separate from domain validation.

## Setup

Install the peer dependency if you haven't:

```bash
npm install zod
```

Import from the dedicated entry point:

```typescript
import { ZodValidatedConsumer, ZodValidatedBatchConsumer } from 'amqpx/zod'
import { z } from 'zod'
```

## Basic usage

Wrap any existing consumer with a schema. The output type is inferred from the schema. This example assumes `defineTopology` sets up the connection and returns the exchange and queues — see [Topology](/guide/topology):

```typescript
import { connect } from 'amqpx'
import { ZodValidatedConsumer } from 'amqpx/zod'
import { z } from 'zod'

const connection = await connect({ hostname: 'localhost', username: 'guest', password: 'guest' })

const { queue } = await defineTopology(connection)

const schema = z.object({
  orderId: z.string(),
  total: z.number().positive(),
})

// The base consumer uses `unknown` — it will accept anything from the wire
const base = await queue.createConsumer<unknown>()
const consumer = new ZodValidatedConsumer(base, schema)

await consumer.listen(async ({ message }) => {
  // message is typed as { orderId: string; total: number }
  console.log('order', message.orderId, 'total', message.total)
})
```

## Handling validation errors

If a message fails validation, the consumer emits `handlingFailed` with a `ZodError`:

```typescript
import { ZodError } from 'zod'

consumer.on('handlingFailed', (error) => {
  if (error instanceof ZodError) {
    console.error('invalid message', error.issues)
  } else {
    console.error('handler error', error)
  }
})
```

The message is then handled according to the consumer's `failureStrategy`.

## Type transformation

Zod's transform and coerce features let you reshape the incoming data. The output type reflects the transformed type:

```typescript
const schema = z.object({
  orderId: z.string(),
  createdAt: z.string().transform((s) => new Date(s)),  // string on wire, Date in handler
  total: z.coerce.number(),
})

const consumer = new ZodValidatedConsumer(base, schema)

await consumer.listen(async ({ message }) => {
  // message.createdAt is a Date
  console.log(message.createdAt.toISOString())
})
```

## Batch consumer

`ZodValidatedBatchConsumer` works the same way for batch consumers. Each message in the batch is validated individually — if any one fails, `handlingFailed` is emitted for that message:

```typescript
import { ZodValidatedBatchConsumer } from 'amqpx/zod'

const base = await queue.createBatchConsumer<unknown>({ batchSize: 50 })
const consumer = new ZodValidatedBatchConsumer(base, schema)

await consumer.listen(async ({ messages }) => {
  for (const { message } of messages) {
    await db.insert(message)
  }
})
```

## Composing wrappers

`ZodValidatedConsumer` wraps any `Consumer<InputMessage>`, including other wrappers, so you can stack them:

```typescript
const base = await queue.createConsumer<unknown>()
const validated = new ZodValidatedConsumer(base, schema)
// wrap further if needed...
await validated.listen(handler)
```
