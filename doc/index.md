---
layout: home

hero:
  name: amqpx
  text: RabbitMQ for TypeScript
  tagline: An object-oriented wrapper over amqplib — clean API, automatic reconnect, type-safe messages.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Architecture
      link: /architecture

features:
  - title: Type-safe messaging
    details: Generic producers and consumers let you define message shapes once and have TypeScript enforce them everywhere.
  - title: Automatic reconnect
    details: A pluggable RetryStrategy handles transient broker failures so your service recovers without manual intervention.
  - title: Composable layers
    details: Connection → Channel → Exchange / Queue → Producer / Consumer. Each layer exposes factory methods for the one below it.
  - title: Failure strategies
    details: Per-consumer error handling — drop, requeue, or reject — without tangling application logic with transport concerns.
  - title: Test utilities included
    details: In-memory mock implementations (TestConnection, TestChannel, …) for Vitest and Jest ship as separate entry points.
  - title: Zod validation
    details: Optional ZodValidatedConsumer decorator validates incoming messages against a schema before they reach your handler.
---
