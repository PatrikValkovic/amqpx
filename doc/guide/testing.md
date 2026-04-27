# Testing

amqpx ships in-memory test doubles for all core entities. They do not connect to a broker — everything runs in process. All methods are standard mocks (`jest.fn()` / `vitest.fn()`) so you can use the full assertion API of your test framework.

## Entry points

| Entry point | Mock implementation |
|---|---|
| `amqpx/vitest` | Uses `vitest.fn()` |
| `amqpx/jest` | Uses `jest.fn()` |

The API is identical between the two. Pick the one that matches your test runner.

## TestConsumer

`TestConsumer<T>` is a mock `Consumer<T>`. Its `listen` mock records the registered callback. Use `deliverMessage()` to simulate a message arriving:

```typescript
import { TestConsumer } from 'amqpx/vitest'

type Order = { orderId: string; total: number }

const consumer = new TestConsumer<Order>()
const handler = vitest.fn()

await consumer.listen(handler)

// simulate a message arriving from RabbitMQ
await consumer.deliverMessage({ orderId: 'abc', total: 49.99 })

expect(handler).toHaveBeenCalledOnce()
expect(handler).toHaveBeenCalledWith(
  expect.objectContaining({ message: { orderId: 'abc', total: 49.99 } })
)
```

`deliverMessage` accepts an optional second argument to override serialization or amqplib message fields (routing key, headers, etc.):

```typescript
await consumer.deliverMessage(order, {
  fields: { routingKey: 'orders.priority', redelivered: false },
  properties: { headers: { source: 'checkout' } },
})
```

## TestBatchConsumer

`TestBatchConsumer<T>` mirrors `TestConsumer` but delivers a batch:

```typescript
import { TestBatchConsumer } from 'amqpx/vitest'

const consumer = new TestBatchConsumer<Order>()
const handler = vitest.fn()
await consumer.listen(handler)

await consumer.deliverMessages([
  { orderId: '1', total: 10 },
  { orderId: '2', total: 20 },
])

expect(handler).toHaveBeenCalledOnce()
const { messages } = handler.mock.calls[0][0]
expect(messages).toHaveLength(2)
```

## TestProducer

`TestProducer<T>` is a mock `Producer<T>`. Use `getPublishedMessages()` to inspect what was published:

```typescript
import { TestProducer } from 'amqpx/vitest'

const producer = new TestProducer<Order>()

await producer.publish({ orderId: 'abc', total: 49.99 })
await producer.publish({ orderId: 'xyz', total: 9.99 })

expect(producer.getPublishedMessages()).toEqual([
  { orderId: 'abc', total: 49.99 },
  { orderId: 'xyz', total: 9.99 },
])
```

`getPublishedMessages()` extracts the first argument from each `publish.mock.calls` — equivalent to `producer.publish.mock.calls.map(([msg]) => msg)`.

## TestConnection, TestChannel, TestQueue, TestExchange

These are full mock implementations of the corresponding interfaces. Factory methods return the appropriate test doubles:

```typescript
import { TestConnection, TestChannel, TestQueue } from 'amqpx/vitest'

const connection = new TestConnection()
// connection.state() returns ConnectionState.connected immediately

const channel = connection.createChannel()
// connection.createChannel() returns a TestChannel

const queue = channel.createQueue('orders')
// queue is a TestQueue
```

## Testing code that uses ZodValidatedConsumer

Combine `TestConsumer` with `ZodValidatedConsumer` to test validation error paths without a broker:

```typescript
import { TestConsumer } from 'amqpx/vitest'
import { ZodValidatedConsumer } from 'amqpx/zod'
import { z, ZodError } from 'zod'

const schema = z.object({ orderId: z.string(), total: z.number() })
const base = new TestConsumer<unknown>()
const consumer = new ZodValidatedConsumer(base, schema)

const failHandler = vitest.fn()
consumer.on('handlingFailed', failHandler)

await consumer.listen(vitest.fn())

// deliver a message that fails validation
await base.deliverMessage({ orderId: 123, total: 'wrong' })  // wrong types

expect(failHandler).toHaveBeenCalledOnce()
expect(failHandler.mock.calls[0][0]).toBeInstanceOf(ZodError)
```

## Simulating disconnection

`TestConnection` has a `simulateDisconnect()` method that emits the `reconnecting` event, letting you test how your code handles reconnection:

```typescript
const connection = new TestConnection()
const reconnectHandler = vitest.fn()
connection.on('reconnecting', reconnectHandler)

connection.simulateDisconnect()

expect(reconnectHandler).toHaveBeenCalledOnce()
```
