---
layout: home

hero:
  name: amqpx
  text: RabbitMQ for TypeScript
  tagline: Type-safe messages, automatic reconnect, and a clean composable API built on top of amqplib.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/what-is-amqpx
    - theme: alt
      text: API Reference
      link: /api/

features:
  - title: Type-safe messaging
    details: Generic producers and consumers let you define message shapes once and have TypeScript enforce them everywhere.
  - title: Automatic reconnect
    details: Pluggable retry strategies handle transient broker failures so your service recovers without manual intervention.
  - title: Composable layers
    details: Connection → Channel → Exchange / Queue → Producer / Consumer. Each layer exposes factory methods for the one below.
  - title: Failure strategies
    details: Per-consumer error handling — drop, requeue, or reject — without tangling application logic with transport concerns.
  - title: Batch consuming
    details: Built-in batch consumer groups messages for bulk processing with configurable batch sizes, timeouts, and ack coalescing.
  - title: Test utilities included
    details: In-memory mock implementations for Vitest and Jest ship as dedicated entry points — no broker needed in unit tests.
---
