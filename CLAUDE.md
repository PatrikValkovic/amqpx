Here's the compressed version:

---

# CLAUDE.md

File give guidance to Claude Code (claude.ai/code) for this repo.

## Commands

```bash
npm run build          # Compile TypeScript → dist/ (CommonJS)
npm run lint           # ESLint
npm run typecheck      # tsc --noEmit (includes test files via tsconfig.test.json)
npm run test           # Vitest (all *.spec.ts files)
npm run test:coverage  # Vitest with v8 coverage
```

Run single test file:
```bash
npx vitest run src/path/to/file.spec.ts
```

## Architecture

OOP wrap over `amqplib` for RabbitMQ. Core entities: interface + impl pair.

**Dependency hierarchy (top → bottom):**
```
Connection → Channel → Exchange / Queue → Producer / Consumer
```

Each layer expose factory methods for layer below (e.g. `Channel.queue()` returns `Queue`, `Queue.consumer()` returns `Consumer`).

### Core layers (`src/`)

| Layer | Dir | Purpose |
|---|---|---|
| Connection | `/connection` | Lifecycle state machine (preconnect→connected→closed), reconnect via `RetryStrategy` |
| Channel | `/channel` | Wrap amqplib channel; create exchanges, queues, producers, consumers |
| Exchange | `/exchange` | assert, bind queues/exchanges; `predefined.ts` has 5 default RabbitMQ exchanges |
| Queue | `/queue` | assert, bind to exchange, create consumer/producer |
| Producer | `/producer` | Generic `<T>`, serialization, routing key fn, `beforeSend`/`afterSend` hooks, drain backpressure |
| Consumer | `/consumer` | Generic `<Message>`, failure strategies (Drop/Requeue/Reject), prefetch, auto-reconnect |
| Retry | `/retry` | `RetryStrategy` with pluggable `TimeStrategy` (linear, exponential); used by Connection |

### Extensions (`src/extensions/`)

- **`/zod`** — `ZodValidatedConsumer`: decorator validate messages against Zod schema before handler
- **`/vitest`** and **`/jest`** — Mock impls (`TestConnection`, `TestChannel`, `TestQueue`, `TestExchange`, `TestProducer`, `TestConsumer`) for unit tests

### Package exports

| Export path | Content |
|---|---|
| `.` (main) | All core entities |
| `./vitest` | Vitest mock implementations |
| `./jest` | Jest mock implementations |
| `./zod` | Zod validation consumer |

## Key conventions

- Core interfaces exported from dir `index.ts`; impls not exported directly.
- `assert()` on Exchange/Queue lazy — do actual amqplib assertion, must await before use.
- Message serialization default JSON; override via `serialize`/`parse` on Producer/Consumer.
- `ConsumptionFailedStrategy` on Consumer control handler errors: Drop (ack), Requeue, Reject (nack).
- Retry strategies use `externallyResolvedPromise` + `sleepPromise` from `src/utils.ts`.
- AMQP ack/nack tracked per-message on broker: `ack(tag, multiple=true)` only affects still-unacknowledged messages; already-nacked messages are unaffected by a subsequent bulk ack covering their delivery tag.