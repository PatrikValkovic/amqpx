# NEXTSTEPS.md

Aggregate findings from five independent analysis agents: code review, bug hunting, devil's advocate critique, and feature gap analysis.

---

## 1. BUGS (Correctness Issues)

Ordered roughly by severity.

### Critical — Data Loss / Deadlock

**B2. Lost message acknowledgements during channel close**
`src/consumer/consumer-implementation.ts:92–142` — The `stillConnected` guard at line 117 only protects the ACK path. For `Requeue` and `Reject` strategies, `nack()` is still called (lines 129/132) regardless of connection state. A channel close mid-processing causes a nack on a closed channel, silently discarding the message.

**B16. `removeProcessedBatches` range compaction silently skips middle indexes**
`src/consumer/batch-consumer-implementation.ts:311–319` — The `forEach` that compresses sorted indexes into splice ranges calls `return` when a consecutive index is found but never updates `currentEnd`. For a run `[0, 1, 2]`: index `1` is consecutive so the loop returns with `currentEnd` still `0`; index `2` then fails the `currentEnd + 1` test, pushes range `[0, 0]`, and starts a new range `[2, 2]`. After `splice(0, 1)` the array shifts by one, making the second `splice(2, 1)` a no-op. The batch at original index `1` is never removed, its messages leak, `currentlyProcessingMessages` is never decremented, the broker holds unacked messages, and `close()` hangs indefinitely. Triggered whenever three or more consecutive batches are acknowledged at once (e.g., with `maxWaitTimeForAck > 0`).

### High — Memory Leaks

**B5. Consumer `'close'` listener never removed**
`src/consumer/consumer-implementation.ts:32`, `src/consumer/batch-consumer-implementation.ts:46` — The `channelCloseCallback` registered in the constructor is never cleaned up: not in `close()`, not in any error path. Each consumer instance that is created and destroyed leaves a permanent listener on the channel.

**B6. `channelCloseCallback` not removed on `close()` timeout**
`src/consumer/consumer-implementation.ts:45–61`, `src/consumer/batch-consumer-implementation.ts:49–65` — When the timeout branch triggers, the promise rejects but the channel `'close'` listener remains attached, leaking forever.

**B17. `parseMessageFn` failure permanently inflates `currentlyProcessingMessages`**
`src/consumer/batch-consumer-implementation.ts:132–134` — `currentlyProcessingMessages` is incremented before `await parseMessageFn(content)`. If parsing throws, the message is never added to any batch and `removeProcessedBatches` is never called, so the counter is never decremented. `close()` waits for the counter to reach zero and hangs indefinitely. The broker also holds the message unacked until the channel closes and redelivers it.

### High — Race Conditions / State Corruption

**B10. Consumer auto-reconnect spawns unbounded concurrent `listen()` calls**
`src/consumer/consumer-implementation.ts:26–31`, `src/consumer/batch-consumer-implementation.ts:40–45` — Each `'close'` event fires `setTimeout(() => this.listen(...), 100)` with no guard. Rapid successive close events queue up many concurrent reconnect attempts. No `isClosed` flag prevents reconnection after explicit `close()`.

**B11. Consumer uses stale channel reference for ack/nack**
`src/consumer/consumer-implementation.ts:92–96`, `src/consumer/batch-consumer-implementation.ts:69–81` — `originalChannel` is captured at `listen()` time. If the channel is internally recreated, all ack/nack calls after reconnection target the old, closed channel. In `BatchConsumerImplementation` the stale capture is compounded: the timer callback additionally closes over `originalChannel` from whichever `messageReceiver` coroutine started the timer, which may be from a previous channel generation.

**B18. `stillConnected` guard is stale for timer-triggered batch processing**
`src/consumer/batch-consumer-implementation.ts:125–177` — Each `messageReceiver` coroutine registers a `'close'` handler (line 129) that sets `stillConnected.value = false`. For the batch-full path the handler is still live while `handleBatch` awaits, so a channel close is detected correctly. For the timer path `messageReceiver` returns after setting the timer and the `finally` block (line 176) removes the handler immediately — before the timer fires. If the channel closes during the timer delay `stillConnected.value` is never set to `false`, and `handleBatch` proceeds to ACK messages on the closed channel.

### Medium — Unhandled Rejections / Swallowed Errors

**B15. Consumer auto-reconnect swallows all errors**
`src/consumer/consumer-implementation.ts:27–30`, `src/consumer/batch-consumer-implementation.ts:41–44` — If `listen()` fails after channel reconnect (e.g., queue deleted), the consumer silently enters a dead state with no event emitted and no way for the caller to detect failure.


---

## 2. CODE QUALITY / DESIGN IMPROVEMENTS

### Type Safety

- **`any[]` in event handler callbacks** — `connection-implementation.ts:33–34`, `consumer-implementation.ts:35–36`, `producer-implementation.ts:77–78`. Replace with typed overload signatures per event name.
- **`ZodValidatedConsumer` type parameter mismatch** — `extensions/zod/zod-validated-consumer.ts:33`. Second generic param of `z.ZodType` should be `z.ZodTypeDef`, not the input message type.
- **`deepMerge` uses multiple `as any` casts** — `utils.ts:25–29`. Strengthening types here would catch silent option-merging bugs.
- **`externallyResolvedPromise` requires `// @ts-expect-error`** — `utils.ts:1–8`. The resolve variable assignment is not type-safe; use a safer factory pattern.

### Unbounded Recursion in `publish()` on Repeated Backpressure

`src/channel/channel-implementation.ts:151–152` — After a drain event resolves, the method retries via `await this.publish(...)` (tail recursion). If backpressure recurs on the retry, all concurrent callers recurse again. Each drain cycle adds a stack frame for every waiting publisher. Under sustained backpressure with concurrent producers this will eventually overflow the call stack. Replace the tail recursion with a `while (true)` loop.

### API Consistency

- **`createConsumerFor*` methods lack `isConfirmed` parameter** — `connection-implementation.ts:105–107`. The `createProducerFor*` counterparts have it; consumers should too for symmetry.
- **Boilerplate repeated in Exchange and Queue** — Assertion caching + channel-close reset is identical in both. Extract into a shared `AssertableResource` base or composition class.

### Channel Isolation Warning

The library docs and channel interface comments warn that "each producer/consumer should have separate channels" but the default factory path (`queue.createConsumer()`) reuses the queue's channel. This is a footgun by default. Consider making the safe path the easy path: have `queue.createConsumer()` create a new channel automatically, or at minimum throw if no separate channel is provided.

---

## 3. DEVIL'S ADVOCATE CONCERNS

The following are architectural-level concerns that cannot be fixed with small patches. They are recorded here so design decisions are made consciously.

### Abstraction Value vs. Complexity Cost

Every interface still exposes raw amqplib types (`amqp.Options.Publish`, `amqp.ConsumeMessage`, `amqp.Channel`, etc.). Users must understand amqplib to use this library effectively. The 4–5 layer hierarchy (Connection → Channel → Exchange/Queue → Producer/Consumer) multiplies cognitive load without fully hiding the underlying model.

**Implication**: Consider whether the interfaces should aim for full abstraction (no amqplib leakage) or explicitly embrace being a thin typed wrapper. Sitting between the two satisfies neither goal well.

### Silent Failure by Design

Auto-reconnection loops, consumer reconnects, and publish errors all default to silent swallowing. In production:
- There is no structured logging hook
- There is no way to detect that a consumer is in a dead state short of watching for `handlingFailed` events
- There is no circuit breaker

**Implication**: The library is operationally opaque. Add at minimum: a logger interface injection point, and expose a "is this consumer/connection healthy" property.

### Exactly-Once Is Impossible Without Deduplication

The 5-second error window in the producer (`producer-implementation.ts:43–62`) can cause message duplication on network errors. Combined with the `Drop` default in consumers, this means at-least-once delivery for the producer and at-most-once for the consumer. The combination means messages can be both duplicated AND dropped depending on timing.

**Implication**: Document this explicitly, or add application-level deduplication hooks and change the consumer default to `Reject`.

### Test Mocks Do Not Exercise Real Failure Modes

`TestChannel`, `TestProducer`, `TestConsumer` all succeed instantly and never simulate:
- Channel close during processing
- Drain backpressure
- Malformed message parsing
- Reconnection

Tests written against these mocks will pass while the real system fails silently in any of these scenarios.

**Implication**: Either extend the mocks to support failure simulation, or document prominently that integration tests with a real broker are required.

### Binding Loss on Instance Replacement

Exchange bindings are stored on the instance (`this.bindings`). If an Exchange instance is replaced (e.g., after channel failure), the new instance starts with an empty binding list and silently stops routing messages. There is no way to recover this without re-declaring bindings from application code.

**Implication**: Either bind-list should be externalizable/injectable, or the library should warn loudly when an Exchange instance is discarded.

---

## 4. MISSING FEATURES & EXTENSIONS

### Tier 2 — Observability (Required for Production Operations)

| Feature | Why Needed |
|---------|-----------|
| **Logger interface injection** | No way to centralize library log output; errors surface only via EventEmitter. |
| **Metrics hooks (Prometheus/StatsD compatible)** | No counters for publishes, consumes, retries, failures, or latency. |
| **OpenTelemetry distributed tracing extension** | No trace context propagation across message boundaries. |
| **Health check / readiness probe abstraction** | Kubernetes/orchestration systems need liveness/readiness signals. |

### Tier 3 — Messaging Patterns

| Feature | Why Needed |
|---------|-----------|
| **RPC / request-reply pattern** | Very common; requires manual `reply_to`/`correlationId` wiring today. |
| **Consumer middleware pipeline** | No preprocessing chain (decompression, decryption, validation) without modifying handler code. |
| **Producer middleware pipeline** | `beforeSend`/`afterSend` hooks exist but are not composable as a chain. |

### Tier 5 — Testing Improvements

- `testConnection.simulateDisconnect()` — simulate network failure
- `testChannel.simulateClose()` — trigger channel close event
- `testProducer.getPublishedMessages()` — assert on published payloads
- `testConsumer.deliverMessage(msg)` — push a test message into the handler
- Simulated drain backpressure in `TestChannel`

### Tier 6 — Framework Integration

- **NestJS module** — Decorators (`@RabbitProducer`, `@RabbitConsumer`), DI integration, health check provider
- **Graceful shutdown improvements** — Parallel consumer shutdown, force-close after timeout with proper NACK of in-flight messages
