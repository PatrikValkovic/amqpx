# NEXTSTEPS.md

Aggregate findings from five independent analysis agents: code review, bug hunting, devil's advocate critique, and feature gap analysis.

---

## 1. BUGS (Correctness Issues)

Ordered roughly by severity.

**B3. `clearTimeout` cannot cancel a timer callback that has already been queued**
`src/consumer/batch-consumer-implementation.ts:98–121` — When the newest message fills the batch to `effectiveBatchSize`, `messageReceiver` calls `clearTimeout(this.batchFillTimer)` and then `handleBatch(lastBatch)`. If the timer already fired in the same event-loop tick (easy when `maxWaitTimeForBatch` is 0, or when async work under the receiver stalled past the delay), `clearTimeout` is a no-op. The queued timer callback and the size-path receiver both invoke `handleBatch` on the same batch, re-running the user callback and corrupting `batch.state`. The timer callback then triggers B2 in its `finally`.

**B4. `batchFillTimer` closure captures a `lastBatch` that may be retired before the timer fires**
`src/consumer/batch-consumer-implementation.ts:92,112–121` — The timer's arrow function closes over the `lastBatch` local of the receiver invocation that installed it. Later receivers may push to that same batch, hit the size threshold, move it through `Processing → Processed → Acknowledged`, and splice it out of `this.batches` — all while `clearTimeout` fails to cancel the already-queued timer (B3). The timer then runs `handleBatch` on a retired batch with no state guard, re-invoking the user callback and manipulating a batch that no longer belongs to the consumer.

**B5. `splitBatch` runs sub-batches in parallel, multiplying the ack race**
`src/consumer/batch-consumer-implementation.ts:257–275` — `Promise.all(splitBatches.map(batch => this.handleBatch(...)))` deliberately runs every sub-batch's `handleBatch` concurrently. Each sub-batch's `finally` calls `planMessageAcknowledgment`, so a single failed batch of N messages produces up to N overlapping invocations that all race per B2. If some sub-batches succeed and others fail, the failing sub-batches' `handleBatchError → nackMessages` path interleaves with the successful sub-batches' `planMessageAcknowledgment → ack` path, and the resulting ack/nack sequence depends purely on scheduler ordering.

---

## 2. CODE QUALITY / DESIGN IMPROVEMENTS

### Type Safety

- **`any[]` in event handler callbacks** — `connection-implementation.ts:33–34`, `consumer-implementation.ts:35–36`, `producer-implementation.ts:77–78`. Replace with typed overload signatures per event name.
- **`ZodValidatedConsumer` type parameter mismatch** — `extensions/zod/zod-validated-consumer.ts:33`. Second generic param of `z.ZodType` should be `z.ZodTypeDef`, not the input message type.
- **`deepMerge` uses multiple `as any` casts** — `utils.ts:25–29`. Strengthening types here would catch silent option-merging bugs.

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


| Feature                         | Why Needed                                                                     |
|---------------------------------|--------------------------------------------------------------------------------|
| **Logger interface injection**  | No way to centralize library log output; errors surface only via EventEmitter. |
| **RPC / request-reply pattern** | Very common; requires manual `reply_to`/`correlationId` wiring today.          |

### Framework Integration

- **Graceful shutdown improvements** — Parallel consumer shutdown, force-close after timeout with proper NACK of in-flight messages
