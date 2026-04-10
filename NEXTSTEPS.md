# NEXTSTEPS.md

Aggregate findings from five independent analysis agents: code review, bug hunting, devil's advocate critique, and feature gap analysis.

---

## 1. BUGS (Correctness Issues)

Ordered roughly by severity.

### Critical — Data Loss / Deadlock

**B2. Lost message acknowledgements during channel close**
`src/consumer/consumer-implementation.ts:92–142` — The `stillConnected` guard at line 117 only protects the ACK path. For `Requeue` and `Reject` strategies, `nack()` is still called (lines 129/132) regardless of connection state. A channel close mid-processing causes a nack on a closed channel, silently discarding the message.

### High — Memory Leaks

**B5. Consumer `'close'` listener never removed**
`src/consumer/consumer-implementation.ts:32` — The `channelCloseCallback` registered in the constructor is never cleaned up: not in `close()`, not in any error path. Each consumer instance that is created and destroyed leaves a permanent listener on the channel.

**B6. `channelCloseCallback` not removed on `close()` timeout**
`src/consumer/consumer-implementation.ts:45–61` — When the timeout branch triggers (line 50), the promise rejects but the channel `'close'` listener remains attached, leaking forever.

**B7. Exchange and Queue `'close'` listeners accumulate**
`src/exchange/exchange-implementation.ts:26–28` and `src/queue/queue-implementation.ts:19–21` — Both register a channel `'close'` listener in the constructor and never remove it.

**B8. Producer error-handler timeout accumulates on concurrent publishes**
`src/producer/producer-implementation.ts:47–62` — Each `publish()` schedules a 5-second `setTimeout` plus attaches a channel `'error'` listener. Concurrent publishes accumulate unbounded timers and listeners.

### High — Race Conditions / State Corruption

**B10. Consumer auto-reconnect spawns unbounded concurrent `listen()` calls**
`src/consumer/consumer-implementation.ts:26–31` — Each `'close'` event fires `setTimeout(() => this.listen(...), 100)` with no guard. Rapid successive close events queue up many concurrent reconnect attempts. No `isClosed` flag prevents reconnection after explicit `close()`.

**B11. Consumer uses stale channel reference for ack/nack**
`src/consumer/consumer-implementation.ts:92–96` — `originalChannel` is captured at `listen()` time. If the channel is internally recreated, all ack/nack calls after reconnection target the old, closed channel.

**B12. Binding deduplication calls amqplib redundantly**
`src/exchange/exchange-implementation.ts:65, 90` — The `every()` guard prevents adding to the local `bindings` array but does NOT prevent calling `channel.bindQueue/bindExchange` on RabbitMQ when a duplicate bind is attempted. Local state diverges from broker state.

**B13. `rebind()` uses `Promise.all()` — partial rebind on failure**
`src/exchange/exchange-implementation.ts:104–114` — A single binding failure aborts all remaining rebinds, leaving the exchange in a partially-bound state after reconnection. Use `Promise.allSettled()` with individual error handling.

### Medium — Unhandled Rejections / Swallowed Errors

**B15. Consumer auto-reconnect swallows all errors**
`src/consumer/consumer-implementation.ts:27–30` — Same pattern. If `listen()` fails after channel reconnect (e.g., queue deleted), the consumer silently enters a dead state.


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
- **`Promise.resolve(new ...)` vs `async/await` inconsistency** — `queue-implementation.ts:34, 42, 48–50`. Pick one style for factory methods.
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
