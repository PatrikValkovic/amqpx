import { ExchangeImplementation, Exchange, ExchangeTypes } from './exchange';
import { Channel } from './channel';
import { Connection } from './connection';
import { DefaultExchange } from './exchange/default-exchange';
import { AssertionMode } from './types';

const PREDEFINED_EXCHANGES = {
    direct: { name: 'amq.direct', type: 'direct' },
    fanout: { name: 'amq.fanout', type: 'fanout' },
    headers: { name: 'amq.headers', type: 'headers' },
    match: { name: 'amq.match', type: 'headers' },
    topic: { name: 'amq.topic', type: 'topic' },
    default: { name: '', type: 'direct' },
} as const satisfies Record<string, { name: string; type: ExchangeTypes }>;

type PredefinedExchange = keyof typeof PREDEFINED_EXCHANGES;

const PREDEFINED_EXCHANGE_OPTIONS = {
    durable: true,
    assertionMode: AssertionMode.Passive,
} as const;

const isConnection = (target: Channel | Connection): target is Connection =>
    'createChannel' in target;

/** Builds the exchange on a channel synchronously, or through `connection.createExchange`. */
function predefinedExchange(exchange: PredefinedExchange, target: Channel | Connection): Exchange | Promise<Exchange> {
    const { name, type } = PREDEFINED_EXCHANGES[exchange];
    if (isConnection(target))
        return target.createExchange(name, type, { ...PREDEFINED_EXCHANGE_OPTIONS });
    if (exchange === 'default')
        return new DefaultExchange(target);
    return new ExchangeImplementation(target, name, type, { ...PREDEFINED_EXCHANGE_OPTIONS });
}

/**
 * Returns the direct exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.direct*, is durable, and has type *direct*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the predefined exchange
 */
export function directExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the direct exchange *amq.direct* bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export function directExchange(channel: Channel): Exchange;
export function directExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('direct', target);
}

/**
 * Returns the fanout exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.fanout*, is durable, and has type *fanout*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the predefined exchange
 */
export function fanoutExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the fanout exchange *amq.fanout* bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export function fanoutExchange(channel: Channel): Exchange;
export function fanoutExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('fanout', target);
}

/**
 * Returns the headers exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.headers*, is durable, and has type *headers*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the predefined exchange
 */
export function headersExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the headers exchange *amq.headers* bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export function headersExchange(channel: Channel): Exchange;
export function headersExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('headers', target);
}

/**
 * Returns the match exchange that RabbitMQ creates by default in every virtual host.
 * Behaves the same as {@link headersExchange}, but has a different name.
 *
 * The exchange has name *amq.match*, is durable, and has type *headers*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the predefined exchange
 */
export function matchExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the match exchange *amq.match* bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export function matchExchange(channel: Channel): Exchange;
export function matchExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('match', target);
}

/**
 * Returns the topic exchange that RabbitMQ creates by default in every virtual host.
 *
 * The exchange has name *amq.topic*, is durable, and has type *topic*.
 * The exchange always exists on the broker, so it is never declared.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the predefined exchange
 */
export function topicExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the topic exchange *amq.topic* bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The predefined exchange
 */
export function topicExchange(channel: Channel): Exchange;
export function topicExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('topic', target);
}

/**
 * Returns the default exchange that RabbitMQ creates in every virtual host.
 * Behaves like a direct exchange, but every queue is automatically bound to it
 * with routing key equal to the queue name.
 *
 * The default exchange has empty string as a name (""). It can't be declared,
 * so use this function whenever you need it in the application.
 *
 * @param connection The connection to use the exchange with
 * @returns Promise resolving to the default exchange
 */
export function defaultExchange(connection: Connection): Promise<Exchange>;
/**
 * Returns the default exchange (empty name) bound to the given channel.
 *
 * @param channel The channel the exchange belongs to
 * @returns The default exchange
 */
export function defaultExchange(channel: Channel): Exchange;
export function defaultExchange(target: Channel | Connection): Exchange | Promise<Exchange> {
    return predefinedExchange('default', target);
}
