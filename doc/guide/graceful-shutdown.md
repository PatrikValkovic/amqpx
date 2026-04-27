# Graceful Shutdown

When your process exits, you need to stop producers and consumers in the right order to avoid losing in-flight messages or leaving broker resources open. amqpx provides `RabbitCloser` for this.

## RabbitCloser

`RabbitCloser` closes provided entities in a fixed sequence: **producers → consumers → channels → connections**. This order ensures all pending publishes are flushed before consumers stop, and all consumers have finished their handlers before the connection is torn down.

```typescript
import { RabbitCloser } from 'amqpx'

const closer = new RabbitCloser(
  [connection],          // connections — closed last
  [orderConsumer],       // consumers
  [orderProducer],       // producers — closed first
)
```

`RabbitCloser` does not hook into any process signals (`SIGTERM`, `SIGINT`, etc.) — you are responsible for calling `closer.close()` at the right moment. A typical setup:

```typescript
process.on('SIGTERM', async () => {
  await closer.close()
  process.exit(0)
})
```
