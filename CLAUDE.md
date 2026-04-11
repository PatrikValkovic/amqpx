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

OOP wrapper over `amqplib` for RabbitMQ. All core entities have interface + implementation pair.

**Dependency hierarchy (top → bottom):**
```
Connection → Channel → Exchange / Queue → Producer / Consumer
```

Each layer expose factory methods to create layer below (e.g. `Channel.queue()` returns `Queue`, `Queue.consumer()` returns `Consumer`).

### Core layers (`src/`)

| Layer | Dir | Purpose |
|---|---|---|
| Connection | `/connection` | Lifecycle state machine (preconnect→connected→closed), reconnection via `RetryStrategy` |
| Channel | `/channel` | Wraps amqplib channel; creates exchanges, queues, producers, consumers |
| Exchange | `/exchange` | assert, bind queues/exchanges; `predefined.ts` has 5 default RabbitMQ exchanges |
| Queue | `/queue` | assert, bind to exchange, create consumer/producer |
| Producer | `/producer` | Generic `<T>`, serialization, routing key fn, `beforeSend`/`afterSend` hooks, drain backpressure |
| Consumer | `/consumer` | Generic `<Message>`, failure strategies (Drop/Requeue/Reject), prefetch, auto-reconnect |
| Retry | `/retry` | `RetryStrategy` with pluggable `TimeStrategy` (linear, exponential); used by Connection |

### Extensions (`src/extensions/`)

- **`/zod`** — `ZodValidatedConsumer`: decorator validate incoming messages against Zod schema before handler
- **`/vitest`** and **`/jest`** — Mock implementations (`TestConnection`, `TestChannel`, `TestQueue`, `TestExchange`, `TestProducer`, `TestConsumer`) for unit tests

### Package exports

| Export path | Content |
|---|---|
| `.` (main) | All core entities |
| `./vitest` | Vitest mock implementations |
| `./jest` | Jest mock implementations |
| `./zod` | Zod validation consumer |

## Key conventions

- All core interfaces exported from directory `index.ts`; implementations not exported directly.
- `assert()` on Exchange/Queue lazy — perform actual amqplib assertion, must await before use.
- Message serialization default JSON; override via `serialize`/`parse` options on Producer/Consumer.
- `ConsumptionFailedStrategy` on Consumer control handler errors: Drop (ack), Requeue, or Reject (nack).
- Retry strategies use `externallyResolvedPromise` + `sleepPromise` utilities from `src/utils.ts`.