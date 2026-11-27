import { ExchangeImplementation, Exchange } from './exchange';
import { Channel } from './channel';
import { DefaultExchange } from './exchange/default-exchange';
import { AssertionMode } from './types';

/**
 * Returns the direct exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.direct*, is durable, and has type *direct*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export const directExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amq.direct',
    'direct',
    {
        durable: true,
        assertionMode: AssertionMode.Passive,
    },
);

/**
 * Returns the fanout exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.fanout*, is durable, and has type *fanout*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export const fanoutExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amq.fanout',
    'fanout',
    {
        durable: true,
        assertionMode: AssertionMode.Passive,
    },
);

/**
 * Returns the headers exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.headers*, is durable, and has type *headers*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export const headersExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amq.headers',
    'headers',
    {
        durable: true,
        assertionMode: AssertionMode.Passive,
    },
);

/**
 * Returns the match exchange that RabbitMQ creates by default in every virtual host.
 * Behaves the same as {@link headersExchange}, but has a different name.
 *
 * The exchange has name *amq.match*, is durable, and has type *headers*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export const matchExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amq.match',
    'headers',
    {
        durable: true,
        assertionMode: AssertionMode.Passive,
    },
);

/**
 * Returns the topic exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.topic*, is durable, and has type *topic*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export const topicExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amq.topic',
    'topic',
    {
        durable: true,
        assertionMode: AssertionMode.Passive,
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
