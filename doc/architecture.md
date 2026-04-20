# Architecture

## Layer hierarchy

amqpx models the AMQP topology as a dependency hierarchy. Each layer is responsible for one concern and exposes factory methods for the layer directly below it.

```
Connection
  └── Channel
        ├── Exchange
        │     └── (bind queues / exchanges)
        └── Queue
              ├── Producer
              └── Consumer
```

A `Connection` creates `Channel`s. A `Channel` creates `Exchange`s and `Queue`s. A `Queue` creates `Producer`s and `Consumer`s.

## Connection

`Connection` manages the lifecycle of the underlying TCP connection to the broker. It runs as a state machine with three states:

- **preconnect** — initial state before `connect()` is called
- **connected** — live connection, channels can be created
- **closed** — permanently shut down

When a `RetryStrategy` is configured, transient failures move the connection back to *preconnect* and trigger a reconnect attempt according to the chosen time strategy.

## Channel

`Channel` wraps a single amqplib channel. It is the unit of isolation for AMQP operations — prefetch settings, publisher confirms, and unacknowledged message counts are all per-channel.

## Exchange

`Exchange` represents an AMQP exchange. Calling `assert()` performs the actual `assertExchange` call against the broker; the object can be created before the connection is ready, but must be asserted before use.

Five predefined exchanges matching RabbitMQ defaults are available in `predefined.ts`.

## Queue

`Queue` represents an AMQP queue. Like `Exchange`, it must be asserted before messages can be routed to it. A queue can bind to one or more exchanges via `bind()`.

## Producer

`Producer<T>` publishes messages of type `T`. Serialization defaults to JSON but can be overridden. A routing key function and `beforeSend`/`afterSend` hooks allow per-message customization. Backpressure from a full broker write buffer is handled via drain events.

## Consumer

`Consumer<T>` subscribes to a queue and invokes a handler for each message of type `T`. Three failure strategies control what happens when the handler throws:

| Strategy | Behavior |
|---|---|
| `Drop` | Acknowledge the message (discard it) |
| `Requeue` | Nack with `requeue: true` |
| `Reject` | Nack with `requeue: false` (dead-letter if configured) |

## Retry strategies

`RetryStrategy` is used by `Connection` to schedule reconnect attempts. Two built-in time strategies are provided:

- **Linear** — fixed delay between attempts
- **Exponential** — delay doubles up to a configured maximum

Both are composable: you can implement `TimeStrategy` to supply custom backoff logic.

## Extensions

| Path | Description |
|---|---|
| `amqpx/zod` | `ZodValidatedConsumer` — decorator that validates messages against a Zod schema |
| `amqpx/vitest` | `TestConnection`, `TestChannel`, `TestQueue`, `TestExchange`, `TestProducer`, `TestConsumer` |
| `amqpx/jest` | Same mock implementations, compatible with Jest's fake timer API |
