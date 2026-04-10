import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Exchange } from './exchange';

/**
 * ExchangeTypes
 *
 * @description
 * The type of exchange to create. Can be one of: 'direct', 'fanout', 'topic', 'headers', 'match'.
 *
 * This is the type that is passed as second parameter to `channel.assertExchange` method of amqplib library.
 *
 * @see * https://amqp-node.github.io/amqplib/channel_api.html
 */
export type ExchangeTypes = Parameters<amqp.Channel['assertExchange']>[1];

export type ExchangeConsumerQueueOptions = Omit<amqp.Options.AssertQueue, 'durable' | 'exclusive'>;

export type BindingArgs = Record<string, string | number | boolean>;

export enum BindingType {
    queue= 'queue',
    exchange = 'exchange',
}

type Pattern = string;
export type Binding = {
    pattern: Pattern;
    args?: BindingArgs;
} & ({
    type: BindingType.queue;
    queue: Queue;
} | {
    type: BindingType.exchange;
    exchange: Exchange;
});

/**
 * Controls how an exchange is verified against the broker when `assert()` is called.
 *
 * - `Assert` (default) — declare the exchange, creating it if absent and failing on config mismatch.
 * - `Check` — verify the exchange exists without creating it; fails if absent.
 * - `Passive` — skip all broker interaction; no network call is made.
 */
export enum ExchangeAssertionMode {
    /**
     * Assert the exchange against the broker.
     * Creates the exchange if it does not exist; throws if it exists with incompatible options.
     * This is the default mode.
     */
    Assert = 'Assert',
    /**
     * Only check that the exchange exists on the broker.
     * Does not create the exchange; throws if it is absent.
     */
    Check = 'Check',
    /**
     * Skip all broker-side verification.
     * No network call is made when `assert()` is invoked.
     */
    Passive = 'Passive',
}

export type ExchangeOptions = amqp.Options.AssertExchange & {
    /**
     * Controls how the exchange is verified against the broker when `assert()` is called.
     * Defaults to `ExchangeAssertionMode.Assert`.
     *
     * @see ExchangeAssertionMode
     */
    assertionMode?: ExchangeAssertionMode;
};
