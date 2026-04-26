import * as amqp from 'amqplib';
import { AssertionMode } from '../types';
import { ConsumerOptions, BatchConsumerOptions } from '../consumer';
import { Queue } from '../queue';
import { Exchange } from './exchange';

/**
 * The type of exchange to create. Accepted values: `'direct'`, `'fanout'`, `'topic'`, `'headers'`, `'match'`.
 *
 * Passed as the second argument to `channel.assertExchange` in the amqplib API.
 *
 * @see https://amqp-node.github.io/amqplib/channel_api.html
 */
export type ExchangeTypes = Parameters<amqp.Channel['assertExchange']>[1];

/**
 * Options for the auto-created exclusive queue when consuming from an exchange.
 * The `durable` and `exclusive` fields are omitted because they are managed internally.
 */
export type ExchangeConsumerQueueOptions = Omit<amqp.Options.AssertQueue, 'durable' | 'exclusive'>;

export type ExchangeConsumerBindingOptions = {
    pattern: string;
    bindingArgs?: BindingArgs;
};
/**
 * Options for consuming messages from an exchange.
 * Combines binding options (`pattern`, optional `bindingArgs`) with standard {@link ConsumerOptions}.
 */
export type ExchangeConsumerOptions<T> = ExchangeConsumerBindingOptions & ConsumerOptions<T>;
/**
 * Options for batch-consuming messages from an exchange.
 * Combines binding options (`pattern`, optional `bindingArgs`) with standard {@link BatchConsumerOptions}.
 */
export type ExchangeBatchConsumerOptions<T> = ExchangeConsumerBindingOptions & BatchConsumerOptions<T>;

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

export type ExchangeOptions = amqp.Options.AssertExchange & {
    /**
     * Controls how the exchange is verified against the broker when `assert()` is called.
     * Defaults to `AssertionMode.Assert`.
     *
     * @see AssertionMode
     */
    assertionMode?: AssertionMode;
};
