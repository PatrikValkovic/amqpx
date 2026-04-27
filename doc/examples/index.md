# Examples

Complete, copy-pasteable examples for common RabbitMQ patterns.

## [Pub/Sub (Fanout)](/examples/pub-sub-fanout)

One publisher, multiple independent subscribers. Every subscriber receives every message. Uses `predefined.fanoutExchange`.

## [Work Queue](/examples/work-queue)

One queue, multiple competing consumers. Each message is processed by exactly one worker. Demonstrates fair dispatch with `prefetch: 1` and `ConsumptionFailureStrategy.Requeue`.

## [Topic Routing](/examples/topic-routing)

Messages routed to subscribers based on routing key patterns. Demonstrates `orders.eu.*` vs `orders.#` with a topic exchange.

## [Graceful Shutdown](/examples/graceful-shutdown)

How to stop producers, consumers, channels, and connections cleanly on `SIGTERM` or `SIGINT` using `RabbitCloser`.
