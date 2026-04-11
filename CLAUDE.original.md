# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

OOP wrapper over `amqplib` for RabbitMQ. All core entities have an interface + implementation pair.

**Dependency hierarchy (top → bottom):**
```
Connection → Channel → Exchange / Queue → Producer / Consumer
```

Each layer exposes factory methods to create the layer below (e.g. `Channel.queue()` returns a `Queue`, `Queue.consumer()` returns a `Consumer`).

### Core layers (`src/`)

| Layer | Dir | Purpose |
|---|---|---|
| Connection | `/connection` | Lifecycle state machine (preconnect→connected→closed), reconnection via `RetryStrategy` |
| Channel | `/channel` | Wraps amqplib channel; creates exchanges, queues, producers, consumers |
| Exchange | `/exchange` | assert, bind queues/exchanges; `predefined.ts` has the 5 default RabbitMQ exchanges |
| Queue | `/queue` | assert, bind to exchange, create consumer/producer |
| Producer | `/producer` | Generic `<T>`, serialization, routing key fn, `beforeSend`/`afterSend` hooks, drain backpressure |
| Consumer | `/consumer` | Generic `<Message>`, failure strategies (Drop/Requeue/Reject), prefetch, auto-reconnect |
| Retry | `/retry` | `RetryStrategy` with pluggable `TimeStrategy` (linear, exponential); used by Connection |

### Extensions (`src/extensions/`)

- **`/zod`** — `ZodValidatedConsumer`: decorator that validates incoming messages against a Zod schema before passing to the handler
- **`/vitest`** and **`/jest`** — Mock implementations (`TestConnection`, `TestChannel`, `TestQueue`, `TestExchange`, `TestProducer`, `TestConsumer`) for use in unit tests

### Package exports

| Export path | Content |
|---|---|
| `.` (main) | All core entities |
| `./vitest` | Vitest mock implementations |
| `./jest` | Jest mock implementations |
| `./zod` | Zod validation consumer |

## Key conventions

- All core interfaces are exported from their directory's `index.ts`; implementations are not exported directly.
- `assert()` methods on Exchange/Queue are lazy — they perform the actual amqplib assertion and should be awaited before use.
- Message serialization defaults to JSON; override via `serialize`/`parse` options on Producer/Consumer.
- `ConsumptionFailedStrategy` on Consumer controls what happens on handler errors: Drop (ack), Requeue, or Reject (nack).
- Retry strategies use `externallyResolvedPromise` + `sleepPromise` utilities from `src/utils.ts`.
