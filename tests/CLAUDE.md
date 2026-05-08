# Integration Tests

All tests in this directory are integration tests that run against live Docker containers.

## Running tests

Always run integration tests after any change to files under `tests/`:

```bash
npm run test:integration
```

## Directory layout

```
tests/
  helpers/          # Shared utilities (Docker control, Toxiproxy, broker URLs, name generation)
  setup/            # Vitest lifecycle hooks
    global-setup.ts # Starts docker-compose stack; tears it down after the suite
    before-each.ts  # Ensures containers are running and Toxiproxy is reset before each test
  connection/       # Tests per entity — one spec file per class
```

## Test structure conventions

- **One top-level `describe` per file**, named after the class under test (e.g. `'Connection integration'`).
- **Nested `describe` per method** (e.g. `describe('connect', ...)`, `describe('close', ...)`).

## Event assertions

Use `vi.fn()` callbacks registered with `.once()` / `.on()`, then assert with `vi.waitFor()` or `handler.toHaveBeenCalledOnce`:

```ts
// correct
const reconnectingMock = vi.fn();
conn.once('reconnecting', reconnectingMock);
await stopContainer(RABBIT_CONTAINER);
await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1));

// incorrect — do not use the Promise wrapper pattern
const p = new Promise<void>(resolve => conn.once('reconnecting', resolve));
```

## Spying on amqplib

`amqplib` is an ESM module — `vi.spyOn` cannot intercept its exports. Use `vi.mock` with
`importOriginal` at the top of the file instead, storing the real implementation for restoration
in `afterEach`:

```ts
// vi.mock is hoisted — use vi.hoisted() to share state with the factory
const connectRef = vi.hoisted(() => ({
    fn: null as ((...args: Parameters<typeof import('amqplib').connect>) => ReturnType<typeof import('amqplib').connect>) | null,
}));

vi.mock('amqplib', async importOriginal => {
    const actual = await importOriginal<typeof import('amqplib')>();
    connectRef.fn = (...args) => actual.connect(...args);
    return {
        ...actual,
        connect: vi.fn((...args: Parameters<typeof actual.connect>) => connectRef.fn!(...args)),
    };
});

// in afterEach — restore call-through so one test's mockRejectedValue doesn't leak:
vi.mocked(amqp.connect).mockImplementation((...args) => connectRef.fn!(...args));
```

## Simulating events

Use real Docker container lifecycle (stop/start) and docker exec rather than artificially emitting events on
the native connection object:

```ts
// correct
await stopContainer(RABBIT_CONTAINER);
await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1));

// incorrect — do not fabricate events
const native = await conn.native();
native.emit('close');
```

Different between case:
- `startContainer`, `stopContainer`, `restartContainer` will keep defined topology.
- use `destroyContainer` if test requires recreation of topology

Always use `waitForHealthy` before performing any more actions.

## Error events

If the code emits "error" event, warn user about it. Error event in JavaScript behave
a bit differently than rest of the events and will throw exception if
no handler is attached to it. Never defensivelly add dummy listeners to the event.

## Rules to follow

- Never mock internal functionality of the library or 3rd parties
- Use `sleepPromise` for waiting, never use fake timers
- Wait 10s before asserting broker state (e.g. `queueDetail`)
