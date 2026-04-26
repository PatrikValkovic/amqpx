import { ExchangeImplementation, Exchange } from './exchange';
import { Channel } from './channel';
import { DefaultExchange } from './exchange/default-exchange';

/**
 * This will create a direct exchange that is created by default by RabbitMQ container.
 *
 * The direct exchange has name *amqp.direct*, is durable, and has type *direct*.
 *
 * @param channel The channel that should assert the exchange
 * @returns The exchange that was created
 */
export const directExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.direct',
    'direct',
    {
        durable: true,
    },
);

/**
 * This will create a fanout exchange that is created by default by RabbitMQ container.
 *
 * The fanout exchange has name *amqp.fanout*, is durable, and has type *fanout*.
 *
 * @param channel The channel that should assert the exchange
 * @returns The exchange that was created
 */
export const fanoutExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.fanout',
    'fanout',
    {
        durable: true,
    },
);

/**
 * This will create a headers exchange that is created by default by RabbitMQ container.
 *
 * The headers exchange has name *amqp.headers*, is durable, and has type *headers*.
 *
 * @param channel The channel that should assert the exchange
 * @returns The exchange that was created
 */
export const headersExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.headers',
    'headers',
    {
        durable: true,
    },
);

/**
 * This will create a match exchange that is created by default by RabbitMQ container.
 * Behaves the same as headers exchange, but has a different name.
 *
 * The match exchange has name *amqp.match*, is durable, and has type *match*.
 *
 * @param channel The channel that should assert the exchange
 * @returns The exchange that was created
 */
export const matchExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.match',
    'match',
    {
        durable: true,
    },
);

/**
 * This will create a topic exchange that is created by default by RabbitMQ container.
 *
 * The topic exchange has name *amqp.topic*, is durable, and has type *topic*.
 *
 * @param channel The channel that should assert the exchange
 * @returns The exchange that was created
 */
export const topicExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.topic',
    'topic',
    {
        durable: true,
    },
);

/**
 * This will create a default exchange that is created by default by the broker.
 * Behaves like direct exchange, but has special property that every queue created within the
 * cluster is automatically bound to this exchange with routing key equal to queue name.
 *
 * The default exchange has empty string as a name ("").
 *
 * This is the only way to create a default exchange in the application, because it can't be asserted and
 * thus the standard exchange implementation can't be used.
 *
 * @param channel The channel that should check the exchange.
 * @returns The exchange that was created
 */
export const defaultExchange = (channel: Channel): Exchange => new DefaultExchange(channel);
