# CLAUDE.md

File give guidance to Claude Code (claude.ai/code) for this repo.

## Commands

```bash
npm run build              # Compile TypeScript → dist/cjs/ (CommonJS) + dist/esm/ (ESM)
npm run lint               # ESLint
npm run typecheck          # tsc --noEmit (includes test files via tsconfig.test.json)
npm run test               # Vitest (all *.spec.ts files in src/)
npm run test:coverage      # Vitest with v8 coverage
npm run test:integration   # Vitest integration tests (requires Docker)
npm run doc:dev            # VitePress dev server for docs
npm run doc:build          # Build full docs (TypeDoc + VitePress)
```

Run single test file:
```bash
npx vitest run src/path/to/file.spec.ts
```

## Architecture

Wrapper over `amqplib` for RabbitMQ. All core entities have interface + implementation pair.

**Dependency hierarchy (top → bottom):**
```
Connection → Channel → Exchange / Queue → Producer / Consumer
```

Each layer expose factory methods to create layer below (e.g. `Channel.queue()` returns `Queue`, `Queue.consumer()` returns `Consumer`).

### Core layers (`src/`)

| Layer | Dir | Purpose |
|---|---|---|
| Connection | `/connection` | Lifecycle state machine (preconnect→connected→closed), reconnection via `RetryStrategy`; exports `connect` factory fn |
| Channel | `/channel` | Wraps amqplib channel; creates exchanges, queues, producers, consumers |
| Exchange | `/exchange` | assert, bind queues/exchanges; `DefaultExchange` wraps broker default exchange |
| Queue | `/queue` | assert, bind to exchange, create consumer/producer |
| Producer | `/producer` | Generic `<T>`, serialization, routing key fn, `beforeSend`/`afterSend` hooks, drain backpressure |
| Consumer | `/consumer` | `Consumer<Message>` + `BatchConsumer<Message>`; failure strategies (Drop/Requeue/Reject), prefetch, auto-reconnect; batch configurable by size/timeout |
| Retry | `/retry` | `RetryStrategy` with pluggable `TimeStrategy`; used by Connection |
| GracefulShutdown | `/graceful-shutdown` | `RabbitCloser`: orderly shutdown — producers → consumers → channels → connections, with optional timeout budget |

**Root-level source files:**
- `src/predefined.ts` — 6 factory fns for broker-default exchanges (`directExchange`, `fanoutExchange`, `headersExchange`, `matchExchange`, `topicExchange`, `defaultExchange`); exported as `predefined.*` namespace
- `src/errors.ts` — `TooManyRetriesError`, `DrainError`
- `src/types.ts` — Shared types

### Extensions (`src/extensions/`)

- **`/zod`** — `ZodValidatedConsumer` and `ZodValidatedBatchConsumer`: validate incoming messages against a Zod schema before handler; throws `ZodError` on validation failure
- **`/vitest`** and **`/jest`** — Mock implementations (`TestConnection`, `TestChannel`, `TestQueue`, `TestExchange`, `TestProducer`, `TestConsumer`, `TestBatchConsumer`) for unit tests

### Package exports

| Export path | Content |
|---|---|
| `.` (main) | All core entities (CJS + ESM) |
| `./vitest` | Vitest mock implementations |
| `./jest` | Jest mock implementations |
| `./zod` | Zod validation consumers |

## Key conventions

- All core interfaces exported from directory `index.ts`; implementations not exported directly, always use barrel exports.
- `assert()` on Exchange/Queue lazy — perform actual amqplib assertion, must await before use. Is not necessary when operation down the stream uses the entity.
- Message serialization default JSON; override via `stringifyMessage`/`parseMessageFn` options on Producer/Consumer.
- `ConsumptionFailureStrategy` on Consumer controls handler errors: Drop (ack), Requeue (nack with requeue), or Reject (nack). `BatchFailureStrategy` for batch consumers (Fail or split the batch in individual messages).
- Retry time strategies (`retryStrategies.*`): `linearBackoff`, `exponentialBackoff`, `cappedExponentialBackoff`, `polynomialBackoff`, `fibonacciBackoff` — all support a jitter param.
- Retry strategies use `externallyResolvedPromise` + `sleepPromise` utilities from `src/utils.ts`.
- Integration tests live in `tests/` (not `src/`) and require a running Docker stack; run with `npm run test:integration`. See `tests/CLAUDE.md` for conventions.
- AMQP ack/nack is tracked per-message on the broker: calling `ack(tag, multiple=true)` only affects messages that are still unacknowledged. A message that was already nacked is unaffected by a subsequent bulk ack covering its delivery tag — the broker will not re-ack it.

## Rules to follow

- Run typecheck, lint, and tests after changes to code and ensure everything pass.
- Never run integration tests, let user run them on its own.
- If public interface and locations changes, verify changes in CLAUDE.md files, README.md, and doc directory. If there is discrepancy, update these files as well.
