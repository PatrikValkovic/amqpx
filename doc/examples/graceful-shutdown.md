# Graceful Shutdown

`RabbitCloser` orchestrates an orderly shutdown: it stops producers first (waiting for in-flight publishes), then consumers (waiting for in-flight handlers), then closes channels, and finally closes connections.

## Code

```typescript
import { RabbitCloser } from 'amqpx'

// ... set up connection, topology, producers, consumers ...

const closer = new RabbitCloser(
  [connection],
  [consumer],
  [producer],
)

async function shutdown() {
  console.log('shutting down...')
  await closer.close(30_000)  // 30 s total budget across all stages
  console.log('shutdown complete')
  process.exit(0)
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
```

## Key points

- Pass all producers, consumers, and connections to a single `RabbitCloser`. You do not need to close channels manually — `RabbitCloser` handles them.
- The timeout budget shrinks across stages: producers get the full budget, consumers get whatever remains after producers finish, channels and connections get even less. If a stage exceeds the remaining budget it throws.
