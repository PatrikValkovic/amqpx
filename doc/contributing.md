# Contributing

## Prerequisites

- Node.js (LTS)
- npm
- Docker (for integration tests — starts a RabbitMQ container automatically)

## Setup

```bash
git clone https://github.com/PatrikValkovic/amqpx.git
cd amqpx
npm install
```

## Running unit tests

Unit tests use Vitest and run without a broker:

```bash
npm test
```

To run a single test file:

```bash
npx vitest run src/path/to/file.spec.ts
```

Coverage report:

```bash
npm run test:coverage
```

## Running integration tests

Integration tests require Docker. The test setup in `tests/setup/` starts and stops a RabbitMQ container automatically using `testcontainers`. [Toxiproxy](https://github.com/Shopify/toxiproxy) is used for network fault injection (e.g. simulating dropped connections mid-stream).

```bash
npm run test:integration
```

## Building the library

The library is published as both CommonJS and ESM:

```bash
npm run build          # cleans dist/ and builds both formats
npm run build:cjs      # CommonJS only (dist/cjs/)
npm run build:esm      # ESM only (dist/esm/)
```

Output goes to `dist/`. The `dist/` directory is what ships in the npm package (see `files` in `package.json`).

## Running the documentation site

Generate the API reference first, then start the dev server:

```bash
npm run doc:api        # generates doc/api/ from TypeDoc
npm run doc:dev        # starts VitePress at http://localhost:5173/amqpx/
```

Build the static site:

```bash
npm run doc:build      # output goes to doc_build/
```

## Code style

ESLint is configured with `@patrikvalkovic/eslint-config`. Run it before committing:

```bash
npm run lint
```

## TypeScript

The project uses strict TypeScript. There are several tsconfig files:

| File | Purpose |
|---|---|
| `tsconfig.json` | Base config (strict mode, source only) |
| `tsconfig.cjs.json` | Extends base, targets CommonJS output |
| `tsconfig.esm.json` | Extends base, targets ESM output |
| `tsconfig.test.json` | Extends base, adds test files |

Type check everything (source + tests):

```bash
npm run typecheck
```

## Project structure

```
src/
  connection/        Connection interface + ConnectionImplementation
  channel/           Channel interface + ChannelImplementation
  exchange/          Exchange interface + ExchangeImplementation + predefined exchanges
  queue/             Queue interface + QueueImplementation
  producer/          Producer interface + ProducerImplementation
  consumer/          Consumer + BatchConsumer interfaces + implementations
  retry/             RetryStrategy + TimeStrategy implementations
  graceful-shutdown/ RabbitCloser
  extensions/
    zod/             ZodValidatedConsumer + ZodValidatedBatchConsumer
    jest/            Test doubles (Jest)
    vitest/          Test doubles (Vitest)
tests/               Integration tests (require Docker)
doc/                 VitePress documentation source
```

## Adding a feature

A typical feature spans several layers. Suggested checklist:

1. Define or extend the public interface in `src/<layer>/interface.ts` (or the relevant `*.ts` file)
2. Implement it in the implementation class
3. Write a unit spec (`*.spec.ts`) alongside the implementation
4. Add an integration test in `tests/` if it involves broker interaction
5. Ensure your changes are covered by tests
6. Export from `src/index.ts` (or the appropriate extension index)
7. Update the documentation

## Commit conventions

- Use convention commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, etc.
- Use imperative mood: "feat: add batch timeout" not "feat: added batch timeout" or "feat: adding batch timeout"
- Keep commits focused — one logical change per commit
- Reference issues or PRs in the commit body when relevant

## Pull requests

Before opening a PR, ensure all of the following pass locally:

```bash
npm run lint
npm run typecheck
npm test
npm test:integration
```

Integration tests (`npm run test:integration`) run separately because they require Docker.
