import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'amqpx',
  description: 'Wrapper around amqplib with retries, topology, batching, and validation',
  base: '/amqpx/',
  outDir: '../doc_build',

  rewrites: {
    'api-overview.md': 'api/index.md',
  },

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/what-is-amqpx' },
      { text: 'Examples', link: '/examples/' },
      { text: 'API Reference', link: '/api/' },
      { text: 'Contributing', link: '/contributing' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'What is amqpx?', link: '/guide/what-is-amqpx' },
          ],
        },
        {
          text: 'Typical Workflow',
          items: [
            { text: 'Topology', link: '/guide/topology' },
            { text: 'Publishing', link: '/guide/publishing' },
            { text: 'Consuming', link: '/guide/consuming' },
            { text: 'Batch Consuming', link: '/guide/batch-consuming' },
            { text: 'Validation', link: '/guide/validation' },
            { text: 'Graceful Shutdown', link: '/guide/graceful-shutdown' },
            { text: 'Testing', link: '/guide/testing' },
          ],
        },
      ],
      '/examples/': [
        {
          text: 'Examples',
          items: [
            { text: 'Overview', link: '/examples/' },
            { text: 'Pub/Sub (Fanout)', link: '/examples/pub-sub-fanout' },
            { text: 'Work Queue', link: '/examples/work-queue' },
            { text: 'Topic Routing', link: '/examples/topic-routing' },
            { text: 'Graceful Shutdown', link: '/examples/graceful-shutdown' },
          ],
        },
      ],
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'Overview', link: '/api/' },
          ],
        },
        {
          text: 'Interfaces',
          collapsed: false,
          items: [
            { text: 'Connection', link: '/api/index/interfaces/Connection' },
            { text: 'Channel', link: '/api/index/interfaces/Channel' },
            { text: 'Exchange', link: '/api/index/interfaces/Exchange' },
            { text: 'Queue', link: '/api/index/interfaces/Queue' },
            { text: 'Producer', link: '/api/index/interfaces/Producer' },
            { text: 'Consumer', link: '/api/index/interfaces/Consumer' },
            { text: 'BatchConsumer', link: '/api/index/interfaces/BatchConsumer' },
            { text: 'RetryStrategy', link: '/api/index/interfaces/RetryStrategy' },
          ],
        },
        {
          text: 'Classes',
          collapsed: true,
          items: [
            { text: 'ConnectionImplementation', link: '/api/index/classes/ConnectionImplementation' },
            { text: 'ChannelImplementation', link: '/api/index/classes/ChannelImplementation' },
            { text: 'ExchangeImplementation', link: '/api/index/classes/ExchangeImplementation' },
            { text: 'QueueImplementation', link: '/api/index/classes/QueueImplementation' },
            { text: 'ProducerImplementation', link: '/api/index/classes/ProducerImplementation' },
            { text: 'ConsumerImplementation', link: '/api/index/classes/ConsumerImplementation' },
            { text: 'BatchConsumerImplementation', link: '/api/index/classes/BatchConsumerImplementation' },
            { text: 'RabbitCloser', link: '/api/index/classes/RabbitCloser' },
            { text: 'TooManyRetriesError', link: '/api/index/classes/TooManyRetriesError' },
            { text: 'DrainError', link: '/api/index/classes/DrainError' },
          ],
        },
        {
          text: 'Enumerations',
          collapsed: true,
          items: [
            { text: 'AssertionMode', link: '/api/index/enumerations/AssertionMode' },
            { text: 'ConnectionState', link: '/api/index/enumerations/ConnectionState' },
            { text: 'ConsumptionFailureStrategy', link: '/api/index/enumerations/ConsumptionFailureStrategy' },
            { text: 'BatchFailureStrategy', link: '/api/index/enumerations/BatchFailureStrategy' },
          ],
        },
        {
          text: 'Options & Types',
          collapsed: true,
          items: [
            { text: 'ProducerOptions', link: '/api/index/type-aliases/ProducerOptions' },
            { text: 'ProducerPublishOptions', link: '/api/index/type-aliases/ProducerPublishOptions' },
            { text: 'ConsumerOptions', link: '/api/index/type-aliases/ConsumerOptions' },
            { text: 'ConsumerCallbackFn', link: '/api/index/type-aliases/ConsumerCallbackFn' },
            { text: 'BatchConsumerOptions', link: '/api/index/type-aliases/BatchConsumerOptions' },
            { text: 'BatchConsumerCallbackFn', link: '/api/index/type-aliases/BatchConsumerCallbackFn' },
            { text: 'BatchConsumerCallbackArgs', link: '/api/index/type-aliases/BatchConsumerCallbackArgs' },
            { text: 'ExchangeOptions', link: '/api/index/type-aliases/ExchangeOptions' },
            { text: 'ExchangeTypes', link: '/api/index/type-aliases/ExchangeTypes' },
            { text: 'ExchangeConsumerOptions', link: '/api/index/type-aliases/ExchangeConsumerOptions' },
            { text: 'ExchangeConsumerQueueOptions', link: '/api/index/type-aliases/ExchangeConsumerQueueOptions' },
            { text: 'ExchangeBatchConsumerOptions', link: '/api/index/type-aliases/ExchangeBatchConsumerOptions' },
            { text: 'QueueOptions', link: '/api/index/type-aliases/QueueOptions' },
            { text: 'MaybePromise', link: '/api/index/type-aliases/MaybePromise' },
          ],
        },
        {
          text: 'predefined',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/api/index/namespaces/predefined/README' },
            { text: 'defaultExchange()', link: '/api/index/namespaces/predefined/functions/defaultExchange' },
            { text: 'directExchange()', link: '/api/index/namespaces/predefined/functions/directExchange' },
            { text: 'fanoutExchange()', link: '/api/index/namespaces/predefined/functions/fanoutExchange' },
            { text: 'headersExchange()', link: '/api/index/namespaces/predefined/functions/headersExchange' },
            { text: 'matchExchange()', link: '/api/index/namespaces/predefined/functions/matchExchange' },
            { text: 'topicExchange()', link: '/api/index/namespaces/predefined/functions/topicExchange' },
          ],
        },
        {
          text: 'retryStrategies',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/api/index/namespaces/retryStrategies/README' },
            { text: 'linearBackoff()', link: '/api/index/namespaces/retryStrategies/functions/linearBackoff' },
            { text: 'exponentialBackoff()', link: '/api/index/namespaces/retryStrategies/functions/exponentialBackoff' },
            { text: 'cappedExponentialBackoff()', link: '/api/index/namespaces/retryStrategies/functions/cappedExponentialBackoff' },
            { text: 'fibonacciBackoff()', link: '/api/index/namespaces/retryStrategies/functions/fibonacciBackoff' },
            { text: 'polynomialBackoff()', link: '/api/index/namespaces/retryStrategies/functions/polynomialBackoff' },
            { text: 'TimeStrategy', link: '/api/index/namespaces/retryStrategies/type-aliases/TimeStrategy' },
          ],
        },
        {
          text: 'amqpx/zod',
          collapsed: true,
          items: [
            { text: 'ZodValidatedConsumer', link: '/api/extensions/zod/classes/ZodValidatedConsumer' },
            { text: 'ZodValidatedBatchConsumer', link: '/api/extensions/zod/classes/ZodValidatedBatchConsumer' },
          ],
        },
        {
          text: 'amqpx/jest',
          collapsed: true,
          items: [
            { text: 'TestConnection', link: '/api/extensions/jest/classes/TestConnection' },
            { text: 'TestChannel', link: '/api/extensions/jest/classes/TestChannel' },
            { text: 'TestExchange', link: '/api/extensions/jest/classes/TestExchange' },
            { text: 'TestQueue', link: '/api/extensions/jest/classes/TestQueue' },
            { text: 'TestProducer', link: '/api/extensions/jest/classes/TestProducer' },
            { text: 'TestConsumer', link: '/api/extensions/jest/classes/TestConsumer' },
            { text: 'TestBatchConsumer', link: '/api/extensions/jest/classes/TestBatchConsumer' },
          ],
        },
        {
          text: 'amqpx/vitest',
          collapsed: true,
          items: [
            { text: 'TestConnection', link: '/api/extensions/vitest/classes/TestConnection' },
            { text: 'TestChannel', link: '/api/extensions/vitest/classes/TestChannel' },
            { text: 'TestExchange', link: '/api/extensions/vitest/classes/TestExchange' },
            { text: 'TestQueue', link: '/api/extensions/vitest/classes/TestQueue' },
            { text: 'TestProducer', link: '/api/extensions/vitest/classes/TestProducer' },
            { text: 'TestConsumer', link: '/api/extensions/vitest/classes/TestConsumer' },
            { text: 'TestBatchConsumer', link: '/api/extensions/vitest/classes/TestBatchConsumer' },
          ],
        },
      ],
    },

    editLink: {
      pattern: 'https://github.com/PatrikValkovic/amqpx/edit/master/doc/:path',
      text: 'Edit this page on GitHub',
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © Patrik Valkovic',
    },

    outline: { level: [2, 3] },

    search: {
      provider: 'local',
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/PatrikValkovic/amqpx' },
    ],
  },
})
