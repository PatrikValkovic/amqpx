# Issues & Bugs

This document is the result of an in-depth audit of the `amqpx` codebase, with a
specific focus on concurrency hazards and integration issues across the
`Connection → Channel → Queue/Exchange → Producer/Consumer` layers.

Issues are graded:

- **CRITICAL** — wrong behavior, data loss, lifecycle corruption
- **HIGH** — likely to bite users in production
- **MEDIUM** — correctness issues with limited blast radius
- **LOW** — code-quality / robustness nits

---

## 1. Consumer / BaseConsumer

### 1.1 [CRITICAL] `BaseConsumer` re-creates a consumer after `close()` — "zombie" consumer

`src/consumer/base-consumer.ts:39-56` registers `channelCloseCallback` on the
channel using `on('close', …)`. The callback uses `setTimeout(…, RECONNECT_TIMEOUT)`
to schedule a reconnect.

There is no synchronization between `close()` and the pending `setTimeout`:

1. The channel emits `close` → 100 ms timer is armed.
2. User calls `consumer.close()`. `close()` cancels the AMQP consumer, waits for
   in-flight messages, sets `this.consumer = null`, emits `'close'`.
3. The 100 ms timer fires. `this.consumer` is `null`, so the `if (this.consumer)`
   guard at line 45 does **not** fire — but the danger comes earlier: the timer
   awaits `this.consumer` and then calls `this.listen(callback)`, which
   re-installs the AMQP consumer on the channel. This effectively
   **resurrects the consumer after `close()`**.

The race window is `RECONNECT_TIMEOUT` ms (default 100 ms) but the bug also
applies to any future channel-close event because the listener is never removed
(see 1.2).

**Fix:** in `close()`, also clear/cancel the pending reconnect (track the
timeout handle), and detach `channelCloseCallback` from the channel.

### 1.2 [HIGH] `channelCloseCallback` is never removed from the channel — leaks listener after close

`base-consumer.ts:39` uses `this.channel.on('close', this.channelCloseCallback)`.
Nothing in `close()` removes it. A long-lived channel that hosts many
short-lived consumers accumulates listeners on every consumer construction (see
also 9.1: `Channel`/`EventEmitter` default listener cap of 10 will fire warnings
and ultimately keep dead handlers alive).

A second knock-on effect: every closed-and-re-created channel triggers reconnect
attempts even for consumers that have been explicitly closed.

### 1.3 [HIGH] `ConsumerImplementation.listen` leaves `this.consumer` set after a failed listen

`src/consumer/consumer-implementation.ts:30-66` assigns `this.consumer` to the
in-flight async-IIFE *immediately* (line 30). If the IIFE rejects (e.g.
`channel.consume` throws), `this.consumer` keeps the **rejected** promise.

Subsequent `listen()` calls hit the `if (this.consumer) throw new Error('Listener is already attached')`
guard at line 27, so the consumer is permanently wedged. The same pattern
appears in `BatchConsumerImplementation.listen` (`batch-consumer-implementation.ts:41-69`).

**Fix:** wrap the IIFE in `try { … } catch { this.consumer = null; throw; }`.

### 1.4 [HIGH] `ConsumerImplementation.messageReceiver`: ack/nack happens inside the try block — failure cascade

`consumer-implementation.ts:84-122`:
- `originalChannel.ack(msg)` is **inside** the try block (line 96).
- If the ack fails (channel closed mid-flight, etc.) the catch runs and tries
  `nack()`, which will also fail.
- The `handlingFailed` event fires for an "ack" failure that has nothing to do
  with the user's handler, which is misleading.

**Fix:** move the success-path ack outside the try (after `await callback(...)`)
or wrap ack/nack calls in their own `try/catch` (`swallowError`) like the
channel code does for `close()`/`waitForConfirms()`.

### 1.5 [HIGH] `messageReceiver` ack uses stale `channelStatus` after fast reconnect

`channelStatus` is created freshly each `listen()` invocation
(`consumer-implementation.ts:41-47`). When the channel closes and the consumer
reconnects via 1.1, a *new* `channelStatus` is created for the new consumer
loop. However, in-flight handlers from the previous loop still hold a reference
to the **old** object — which was set to `isConnected = false` by the
`once('close')` listener. That's actually fine.

But if the *new* channel emits `close` quickly and the **new** `channelStatus`
flips to `false`, the **old** in-flight handler still believes its channel is
alive and calls `originalChannel.ack(msg)` on a closed channel → throw, see 1.4.

**Fix:** the `originalChannel` reference itself is a stable amqplib channel —
check the channel state directly instead of a separate flag, or wrap ack in
swallowError.

### 1.6 [MEDIUM] `BaseConsumer.close` race: `notifyMessageProcessed` set after `cancel()`

`base-consumer.ts:60-91`:

```ts
await channel.cancel(consumer.amqpConsumer.consumerTag);
const waitForAllMessagesPromise = new Promise<void>(resolve => {
    this.notifyMessageProcessed = () => {
        if (this.currentlyProcessingMessages === 0) resolve();
    };
    this.notifyMessageProcessed();   // immediate self-call
});
```

The `messageReceiver` decrements the counter and then calls
`this.notifyMessageProcessed?.()` in `finally`. If the **last** in-flight
message finishes between the `cancel()` returning and the new
`notifyMessageProcessed` being assigned, the resolver is never invoked and
`close()` blocks until the timeout. The immediate self-call does not save us
because the counter may still be `> 0` at that moment.

**Fix:** read `currentlyProcessingMessages` *before* assigning
`notifyMessageProcessed` and short-circuit if already 0; or use a more
robust wait primitive such as a counted promise.

### 1.7 [MEDIUM] `BaseConsumer.close` does not unregister `notifyMessageProcessed` after resolving

After `close()` returns, `notifyMessageProcessed` keeps a reference to
the resolver. Subsequent in-flight messages (if any survive the race in 1.1)
still call it, which is harmless but wasteful. Also `this.consumer = null` is
set in `finally`, so future `close()` calls are no-ops — but the listener leaks
remain.

### 1.8 [MEDIUM] `setTimeout` in `BaseConsumer.channelCloseCallback` has no `unref()`

A pending reconnect timer keeps the Node process alive even when nothing else
is going on. If a consumer is closed but the channel just emitted `close` 50 ms
ago, the process won't exit cleanly.

### 1.9 [LOW] `BaseConsumer.channelCloseCallback` ignores the result of `this.consumer` rejection

If awaiting `this.consumer` itself throws (because the previous listen failed —
see 1.3), the unhandled promise rejection escapes into the `setTimeout`
callback as an unhandled error (no try/catch around the await).

---

## 2. BatchConsumer

### 2.1 [CRITICAL] `shouldAutoAck` ignores `prefetch`, breaking flow control

`src/consumer/batch-consumer-implementation.ts:145-147`:

```ts
private get shouldAutoAck() {
    return this.options.failureStrategy === ConsumptionFailureStrategy.Drop;
}
```

`ConsumerImplementation.shouldAcknowledge()` (`consumer-implementation.ts:124-135`)
explicitly handles the case where `prefetch > 0` — auto-ack must be **off**, or
the broker ignores prefetch and floods the consumer.

`BatchConsumerImplementation` does not make that distinction. Configuring
`{ failureStrategy: Drop, prefetch: 50 }` on a `BatchConsumer` will set
`noAck: true` on `channel.consume`, and prefetch will silently have no effect.

**Fix:** mirror the regular consumer's logic.

### 2.2 [HIGH] Stale `lastBatch` captured by `batchFillTimer`

`batch-consumer-implementation.ts:122-134`:

```ts
if (!this.batchFillTimer) {
    this.batchFillTimer = setTimeout(async () => {
        ...
        await this.handleBatch(callback, originalChannel, lastBatch, ...);
    }, this.maxWaitTimeForBatch);
}
```

`lastBatch` is captured from the surrounding closure — the value of `last(this.batches)`
**at the moment the timer was set**. If the batch fills before the timer
fires (handled correctly), or new batches are pushed while this batch is
still `WaitingForData`, the timer still runs against the original reference.
That happens to be the correct semantic *only* because the timer is gated by
`if (!this.batchFillTimer)` and cleared when a full batch is processed.
However, if `splitBatch` runs (line 294-315), it `splice`s the batch out of
`this.batches`, but the timer captured a reference to the (now removed) batch.

When the timer fires, `handleBatch` is called against a batch that no longer
appears in `this.batches`. `handleBatch` will execute the user callback for the
already-split content, *and* then `removeProcessedBatches` will look for it via
`indexOf` and not find it — but state mutations (`batch.state = ...`) don't
crash. Net effect: the user's callback is invoked an **extra time** for the
same messages.

**Fix:** in `splitBatch`, clear `batchFillTimer` if the split batch was the one
the timer was tracking; or have the timer re-resolve `last(this.batches)` and
verify state at fire time.

### 2.3 [HIGH] `handleBatch` does not clear `batchFillTimer` when it processes the timer's batch

When the timer fires, `handleBatch` runs. Inside `handleBatch`, on the success
path, the timer is *not* cleared (it has already fired, so technically OK), but
the `this.batchFillTimer` field is set to `undefined` at line 125 *before*
awaiting `handleBatch`. If `handleBatch`'s callback rethrows synchronously
before the timer field is cleared (e.g., `processError` thrown synchronously),
the next `messageReceiver` call sees `this.batchFillTimer === undefined` and
sets a new one, which is correct.

Conversely, when the *full-batch* path runs (line 109-111), the timer is
cleared with `clearTimeout(this.batchFillTimer)` — but it was set against
**lastBatch**. If a previous batch was still waiting on a timer (impossible
since only one timer is allowed at a time), this would be a problem. The
single-timer invariant assumed by the code is intact, but the design is fragile
and depends on that invariant — it should be made explicit.

### 2.4 [HIGH] `splitBatch` runs all sub-batches concurrently and they all enter `planMessageAcknowledgment` independently

`batch-consumer-implementation.ts:294-315`:

```ts
await Promise.all(splitBatches.map(batch => this.handleBatch(...)));
```

Each child `handleBatch` invocation:

1. Awaits the user callback in parallel (intended).
2. Calls `planMessageAcknowledgment(...)` in `finally`.
3. Calls `removeProcessedBatches(...)`.

Multiple concurrent `planMessageAcknowledgment` runs all iterate
`this.batches`, all compute `processedToIndex`, all decide whether to bulk-ack.
They **could** issue overlapping bulk-ack calls, all acking up to the same
delivery tag. The CLAUDE.md note ("already-acked messages are no-op for a
subsequent bulk ack") covers this for the broker, but the function also
mutates batch state from `Processed → Acknowledging → Acknowledged` and starts
`confirmTimer`s. Two parallel runs can both flip the same `Processed` batch to
`Acknowledging` and both await ack — minor wasted work, but it can also leave
two `confirmTimer`s armed if state transitions interleave between the
`!batch.confirmTimer` check (line 400) and assignment (line 403).

**Fix:** serialize access to the `batches` array (a simple async lock or a
shared "ack-in-progress" flag), or detect the situation when entering and
short-circuit.

### 2.5 [HIGH] `nackMessages` partial-batch path uses `multiple = true` even when earlier batches still hold older delivery tags

`batch-consumer-implementation.ts:269-292`:

```ts
} else if (indexOfBatch === 0) {
    const lastMessage = last(batch.messages);
    ...
    await originalChannel.nack(lastMessage.rabbitMessage, true, requeue);
}
```

`nack(msg, true, requeue)` nacks the message **and all messages with smaller
delivery tags** that are still un-acked. The code only takes this branch when
`indexOfBatch === 0`, i.e. this batch is the head — so by construction there
should be nothing older still un-acked. **However**: the head being
"this batch" is true within `this.batches` (which only tracks our own batches),
but other consumers on the same channel could have older un-acked messages.
Per AMQP, prefetch is per-channel/per-consumer; these will be separate
channels in well-behaved usage, but the recommendation in the docs is just
"each consumer should have its own dedicated channel" — it's not enforced.
Sharing a channel between consumers and using bulk nack with `multiple=true`
will silently nack the other consumer's messages.

**Fix:** document the constraint loudly, or always nack individually.

### 2.6 [MEDIUM] `removeProcessedBatches` does not always notify after counter changes

`batch-consumer-implementation.ts:317-350`:

```ts
if (indicesOfBatchesToRemove.length === 0)
    return;
```

If the function is called and no batches were eligible for removal, but
some other code path decremented `currentlyProcessingMessages`, the
`notifyMessageProcessed?.()` is not invoked. There is no other decrement path
in this file so it is currently safe — but it leaves the close() resolver
asleep if a future change adds another decrement site.

### 2.7 [MEDIUM] `confirmTimer` callback can ack after `close()` finished

The `confirmTimer` set inside `planMessageAcknowledgment` (line 403) carries
references to `originalChannel`, `stillConnected`, and `batch`. Nothing in
`close()` clears those timers. After close, the timer fires, calls
`originalChannel.ack(...)` on a closed channel → throws, becomes an unhandled
rejection (the timer callback is `async` but no one awaits it).

`removeProcessedBatches` does clear timers when removing acknowledged batches,
but `close()` itself does not iterate the batches array to cancel pending
`confirmTimer`s.

### 2.8 [MEDIUM] `batchFillTimer` callback also leaks past `close()`

Same family as 2.7 — `close()` does not `clearTimeout(this.batchFillTimer)`.

### 2.9 [LOW] `processError` emits `'error'` but the function returns the error — caller behavior is inconsistent

`processError` is sometimes used as `throw this.processError(...)` and sometimes
the returned error is discarded. When thrown, listeners receive the `'error'`
event and the throw bubbles up — that's two different reactions for the same
condition.

### 2.10 [LOW] Diagnostic noise

The TS diagnostics surfaced today flag two `await` calls as having no effect on
the type (lines reported as 251, 346). These are likely artifacts of the
function being typed `void` somewhere up the chain (recursion in
`splitBatch → handleBatch`). Worth fixing the inferred type to remove the
warning.

---

## 3. Channel

### 3.1 [HIGH] `publish()` may deadlock awaiting a `drainPromise` belonging to a closed/replaced channel

`src/channel/channel-implementation.ts:120-175`. The `while (true)` loop:

1. `const native = await this.native();` — may reconnect channel.
2. `if (this.drainPromise) await this.drainPromise;`

`this.drainPromise` is created the first time backpressure is observed
(line 156-172). It resolves on `'drain'` from `this` (re-emitted from the
underlying amqplib channel's `'drain'` event). If the underlying channel was
closed *between* `publish` returning `false` and the drain firing, the new
channel does not know about the old `drainPromise`. The drain handler is
attached to **this** ChannelImplementation EventEmitter, not to the underlying
channel — but the underlying channel forwards drain only while alive. After
close, no more drain events from the closed channel, and the new channel will
not emit drain unless **it** also fills its buffer.

The `setTimeout(drainTimeout)` will eventually fire and reject the
drainPromise, which closes the *current* native channel. Net effect: any
publish in flight after a close-during-backpressure waits for `drainTimeout`
ms (default 30 000) and then forces the *current* (potentially healthy) channel
closed.

**Fix:** invalidate `drainPromise` when the channel emits `'close'`, e.g. by
rejecting it with an internal "channel-closed" error and clearing the field.

### 3.2 [HIGH] Confirm-channel publish: `drain` registration races against the in-flight retry

For confirmed channels, `publishResult = await retryLoop(...)` (line 135-146).
While this awaits, the underlying channel can emit `drain`. Only **after** the
await do we register the `once('drain')` listener (line 156-172). If drain
already fired, we will wait for the *next* drain — which may never come unless
the buffer refills.

The visible symptom: a confirmed publisher can stall under heavy load until
`drainTimeout` then close the channel.

**Fix:** track drain state via a flag set on the underlying `'drain'` event so
the publish loop can detect "already drained" and skip the wait.

### 3.3 [HIGH] `channel.on('error')` and `'close'` set `wrapper.channel = null` — but the in-flight `connect()` IIFE still resolves the original channel

`channel-implementation.ts:42-72`. The async IIFE attaches `error`/`close`
handlers on the channel and then returns the channel. If the channel emits
`error` *before* the IIFE resolves (or during resolution), `wrapper.channel`
is set to `null` inside the handler — but the IIFE then assigns a fresh
promise that resolves to the now-broken channel.

Subsequent `await this.wrapper.channel` calls are guarded by the field-null
checks inside `native()`/`close()`, but a caller that already cached the
returned promise will see the broken channel.

### 3.4 [MEDIUM] `close()` sets `wrapper.channel = null` in `finally` only after awaiting `waitForConfirms` and `close()`

If a publish is in flight while `close()` is running, it goes through
`native()` → `await this.connect()` → since `wrapper.channel` is still set
(close hasn't reached its finally yet), it returns the same channel. That
channel is then `close()`-ed under the publisher's feet.

This is mostly accepted behavior (close means close), but it's worth noting
that there is no "draining" period for in-flight publishers.

### 3.5 [MEDIUM] `publish()` confirm-channel callback referencing `status` is order-of-operations brittle

```ts
const status = channel.publish(exchange, routingKey, content, options, err => {
    err ? reject(err) : resolve(status);
});
```

The callback references `status` declared in the same statement. JS `let`
hoisting + the fact that confirm callbacks are always asynchronous (network
round-trip) saves this from being a TDZ bug, but a future refactor that
makes the callback fire synchronously (e.g. a mock) will break it. Capture
the boolean via a separate mutable variable to make the intent obvious.

### 3.6 [MEDIUM] `publish()` `setTimeout` rejector closes the channel inside the rejection's microtask

```ts
timeoutHandler = setTimeout(async () => {
    this.removeListener('drain', drainHandler);
    this.drainPromise = null;
    reject(new DrainError('Rabbit drain timeout'));
    await native.close();
}, drainTimeout);
```

`native.close()` is awaited inside the timer callback; if it throws, the
rejection is unhandled (timer callback is `async` but called from a `setTimeout`
that does not consume the promise). Also, the `native` reference may already be
a stale channel by the time the timer fires (3.1).

### 3.7 [LOW] `EventEmitter` default `maxListeners` is 10

`ChannelImplementation` (and several other classes) inherit from
`EventEmitter` but never call `setMaxListeners(0)` (the test classes do). With
many short-lived consumers/producers attaching `'error'`/`'close'` listeners,
Node will print "MaxListenersExceededWarning" in production.

---

## 4. Producer

### 4.1 [HIGH] `setInterval` keeps the Node process alive

`src/producer/producer-implementation.ts:34-46`:

```ts
this.interval = setInterval(() => { ... }, Math.max(100, this.options.errorWindow));
```

No `.unref()`. A user who creates a producer but forgets to `close()` it cannot
exit the process gracefully. Apply `.unref()` to the timer.

### 4.2 [HIGH] `inFlight` window: messages added *after* the publish resolves can be missed

`producer-implementation.ts:73-117`. The flow:

1. User calls `publish()`.
2. `await this.channel.publish(...)` (network round-trip).
3. **Then** the entry is added to `this.inFlight` (line 107).

If the channel emits `error` *during* step 2, the in-flight message is **not**
in `inFlight` yet, so `handleChannelError` will not republish it. Meanwhile,
`channel.publish` itself may throw (the await rejects), so the user's `publish`
call rejects — at which point the user thinks the message was not delivered.
But the broker may actually have received and persisted it. Now we have
**both** "user re-publishes manually" and the channel reconnects; result is
duplicate delivery without the documented `errorWindow` semantics.

Also, the entry is added even on success — but is removed by the periodic
sweeper. If the channel errors *after* the publish but before the next sweep
tick, the message will be republished even though it was successfully sent.
This is documented (`Note: this may result in more-than-once delivery`), so
is acceptable for that case.

**Fix for the missed-during-publish case:** add the entry to `inFlight`
*before* the publish, then remove on failure (and decrement on the duplicate
send if needed).

### 4.3 [MEDIUM] `close()` does not await in-flight publishes

`close()` sets `this.closed = true`, clears the interval, removes the channel
error handler, and returns. Any publish promises currently awaiting
`this.channel.publish(...)` continue. If the channel is then closed by
`RabbitCloser`, those publishes throw mid-call. The producer never reports
which messages succeeded or failed.

### 4.4 [LOW] `handleChannelError` republishes via `this.publish(entry.message, entry.routingKey, entry.options)`

`entry.routingKey` may be a function. `publish()` re-evaluates it each call,
which is fine — but if the function is non-deterministic, the message ends up
on a different routing key than originally targeted. Worth mentioning in docs.

### 4.5 [LOW] `inFlight` is `Set<InFlightEntry>` — uniqueness by reference only

A retry inserts a *new* entry for the same message. There is no de-duplication.
Combined with the missed-window of 4.2, the same logical message can have
0, 1, or 2 entries in `inFlight` at once.

---

## 5. Connection

### 5.1 [MEDIUM] `close()` blocks until the in-flight `retryLoop` finishes

`connection-implementation.ts:99-122`. `close()` awaits `this.connection`,
which is the long-running retry-loop promise. The retry loop honors state
inside its callback — so the *next* attempt sees `closing` and returns `null`
— but the `sleepPromise(delay)` between attempts is not interrupted. With
exponential backoff defaults, `close()` may block for many seconds before the
loop checks state again.

**Fix:** make the retry loop racable against an abort signal/promise; or stop
sleeping between attempts when the connection state is terminal.

### 5.2 [MEDIUM] `nativeConnection.on('close')` always sets `this.connection = null` even on a graceful close

```ts
nativeConnection.on('close', () => {
    this.connection = null;
    if ([ConnectionState.closed, ConnectionState.closing].includes(this.connectionState))
        return;
    ...
});
```

`close()` in its `finally` *also* sets `this.connection = null`. Order
matters: if amqplib synchronously triggers `close` from inside its own
`.close()`, the listener runs first and then the `finally` runs. Both writes
agree, so no logical bug — but the duplication is a footgun for future
refactors.

### 5.3 [MEDIUM] Reconnect logic re-emits handlers on the same `EventEmitter` indefinitely

Every successful `connect()` registers a fresh pair of `'close'`/`'error'`
listeners on `nativeConnection`. Because the *native* connection is fresh on
each reconnect, that's actually fine — but if amqplib ever decides to reuse
the same instance after recovery, listeners would accumulate.

### 5.4 [LOW] `native()` may throw `Internal error: Connection is null after connect`

`connection-implementation.ts:128-136`. After `connect()` returns successfully,
`this.connection` is awaited again. In a tight reconnect loop where the
connection drops between the first await and the second, `this.connection` may
have been nulled by the close listener and the second `await this.connection`
resolves to `undefined`. The subsequent throw is a generic "internal error"
that is hard to debug.

---

## 6. Queue / Exchange

### 6.1 [HIGH] `assertPromise` retains rejections forever — assert can never recover without channel-close

`src/queue/queue-implementation.ts:34-63` and
`src/exchange/exchange-implementation.ts:35-65`. After the first `assert()`:

```ts
if (this.assertPromise) {
    await this.assertPromise;
    return this;
}
```

A rejected promise is non-null; subsequent calls re-await and re-throw.
`this.assertPromise` is only nulled on the channel `'close'` event. If the
broker rejects an assert (e.g. type mismatch), every subsequent operation on
this Queue/Exchange throws the same error, even after the user fixes the
config in code. The only way to recover is to drop and recreate the Queue
instance or close the channel.

**Fix:** clear `assertPromise` in a `catch` so the next call can retry.

### 6.2 [MEDIUM] `Queue.bindExchange` does not record the binding locally; relies on `Exchange.bindings`

`queue-implementation.ts:69-73` calls `exchange.bindQueue(...)`. The exchange
records the binding and rebinds on its own channel-close → reassert. If the
**queue's** channel closes/reasserts but the **exchange's** does not (different
channels), the broker has lost its end of the binding (queue might have been
deleted-and-recreated) but the exchange isn't told to rebind. Net effect:
silently lost subscription.

**Fix:** also track bindings on the queue and rebind on its own reassert; or
unify the rebinding under a queue/exchange-pair owner.

### 6.3 [MEDIUM] `assertPromise` is reset on `'close'` but the in-flight assert still resolves

When the channel closes mid-assert, the original `assertPromise` is set to
`null`, but the underlying `channel.assertQueue` is still in flight against the
now-dead native channel. It will reject. Concurrent `assert()` callers who
captured the old promise reference will observe the rejection.

Meanwhile, a new caller creates a new `assertPromise` against a new channel.
This is mostly correct, but the rejection of the old promise is unhandled by
the second caller (only the first caller awaited it).

### 6.4 [MEDIUM] `Exchange.rebind` re-binds in parallel without ordering guarantees

`exchange-implementation.ts:125-141` uses `Promise.allSettled` and rethrows the
first failure. If two bindings both fail, only the first failure is surfaced;
the second is silently swallowed. With many bindings, partial-failure
scenarios are hard to debug.

### 6.5 [LOW] `Exchange/Queue` register an unconditional `channel.on('close', …)` listener

Same listener-leak family as 1.2 — every Queue/Exchange instance attaches a
permanent listener; they accumulate on long-lived channels. There's no
`detach()` / unbind on object disposal.

### 6.6 [LOW] `Exchange.createConsumer` ignores `queueOptions.exclusive` / `durable` overrides

`exchange-implementation.ts:143-150` hard-codes `durable: false, exclusive: true`
*after* spreading `queueOptions`. The type already excludes these keys so the
override is fine, but a user passing the same object to other places could
expect them honored. Minor.

---

## 7. Cross-cutting / integration

### 7.1 [HIGH] `Producer` and `Consumer` constructors register listeners synchronously, so `Promise.resolve(new …)` factories never give the caller a chance to clean up

`Queue.createProducer` and `Queue.createConsumer` both do
`Promise.resolve(new XImplementation(...))`. The constructor of
`ProducerImplementation` (`producer-implementation.ts:21-46`) starts an
interval and registers a `channel.on('error')` listener. There is no async
work between the `new` and the `Promise.resolve(...)`, so the only way to free
those resources is to call `.close()` — which is fine, *but* if the
constructor throws (e.g. `deepMerge` fails), the listener and interval are
never unregistered.

### 7.2 [MEDIUM] `RabbitCloser` closes producer's *and* consumer's channels in two passes

If a producer and a consumer share a channel (allowed but discouraged),
`RabbitCloser` will call `channel.close()` twice. The second call returns
early (`if (!this.wrapper.channel) return;`), so it is a no-op — but it does
not re-await the first close, so a race is possible if the first close has not
finished by the time the second producer/consumer tries to close.

Worse: `producer.getChannel().close()` runs concurrently with
`consumer.channel.close()` in the same `Promise.all`, so the same channel can
have two concurrent `close()` invocations whose second observes
`wrapper.channel != null` and triggers a duplicate
`waitForConfirms()` + `channel.close()`.

### 7.3 [MEDIUM] Reconnect-after-close ordering across layers

Layered reconnect:

- `Connection` reconnects on its own (5.x).
- `Channel.connect` is **not** invoked automatically on connection
  reconnection. The channel's underlying amqplib channel will be invalid until
  the next `native()` call detects `wrapper.channel === null` (set by the
  channel `'close'` listener).
- Consumers re-`listen()` via `BaseConsumer.channelCloseCallback` (1.1).
- Producers do **not** re-do anything; `handleChannelError` republishes
  in-flight messages but does not re-establish the channel; the next
  `publish()` will re-`native()` which will reconnect.

Net effect: a Producer that does *not* publish during the reconnect window
holds a stale channel reference. Fine in normal operation, but if the user
also called `close()` during that window, the close path runs against a
half-dead channel. This is fragile.

### 7.4 [MEDIUM] No back-pressure between `BatchConsumer` callback execution and arriving messages

`BatchConsumerImplementation` does not pause/resume amqplib delivery; it
relies entirely on prefetch + manual ack timing. With `Drop` strategy + a
large prefetch (broken by 2.1, but assume fixed), the broker can dump
`prefetch` messages into Node's heap; if the user callback is slow the heap
grows.

### 7.5 [LOW] `EventEmitter` listeners on Connection accumulate across reconnects

Same family as 5.3.

---

## 8. Retry / utilities

### 8.1 [MEDIUM] `retryLoop` cannot be canceled

`retry-implementation.ts`. Once entered, the loop runs to completion or
`TooManyRetriesError`. There is no abort-signal hook. Combined with 5.1, this
is the root cause of `Connection.close()` blocking on retry.

### 8.2 [LOW] `sleepPromise` does not support cancellation

Same family as 8.1.

### 8.3 [LOW] `swallowError` returns `null` instead of preserving the error type

Acceptable for the purpose, but combined with 1.4 — wrapping ack/nack in
`swallowError` would lose the error context. Consider variant that logs.

### 8.4 [LOW] `deepMerge` mutates the `target` argument

`utils.ts:8-32`. Not flagged as a bug because it's used with `{}` as the
target. But anyone passing a real object (e.g. a captured options object)
will see it mutated.

---

## 9. Miscellaneous

### 9.1 [MEDIUM] No `setMaxListeners(0)` on real implementations

The Test* classes call `setMaxListeners(0)`; the production
implementations do not. This combined with the listener leaks in 1.2, 6.5
will produce warnings in production.

### 9.2 [LOW] Unused `debug` imports

The new diagnostics surfaced today flag unused `debug` declarations in
`base-consumer.ts:9`, `consumer-implementation.ts:10`, and
`batch-consumer-implementation.ts:18`. Either remove or actually use them.

### 9.3 [LOW] `batch-consumer-implementation.spec.ts` is missing test-runner imports

The diagnostics show many `Cannot find name 'describe' / 'beforeEach' / 'expect' / 'vi'`
errors. The file should `import { describe, beforeEach, afterEach, expect, test, vi } from 'vitest'`.

### 9.4 [LOW] `retry-implementation.ts` line 55 diagnostic — `Expected 2 arguments, but got 1`

The current file has only 53 lines, so this likely refers to a pending edit
in `retry-implementation.ts` that supplies one argument to a 2-arg call. Worth
confirming.

### 9.5 [LOW] `Connection.connect` re-uses `this.connection` for both "in-flight" and "done" — no third state

A caller cannot distinguish "still trying" from "successfully connected" at
the field level. Operations that want fast-fail behavior have to use
`state()`, which has its own race surface.

---

## Suggested priorities

1. Fix the consumer-resurrection race (1.1, 1.2). Add a teardown that cancels
   the pending reconnect and removes the channel listener.
2. Fix `BatchConsumerImplementation.shouldAutoAck` to honor `prefetch` (2.1).
3. Fix `assertPromise` retaining rejections (6.1).
4. Make `setInterval` and `setTimeout` callers `.unref()` (4.1, 1.8).
5. Fix the `drainPromise` lifetime across channel reconnects (3.1, 3.2).
6. Fix listener leaks across the codebase (1.2, 4.x, 6.5, 7.1, 9.1).
7. Then tackle the more theoretical concurrency races (2.4, 2.5, 7.2).
