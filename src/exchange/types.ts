import * as amqp from 'amqplib';
import { AssertionMode } from '../types';
import { ConsumerOptions, BatchConsumerOptions } from '../consumer';
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

export type ExchangeConsumerBindingOptions = {
    pattern: string;
    bindingArgs?: BindingArgs;
};
export type ExchangeConsumerOptions<T> = ExchangeConsumerBindingOptions & ConsumerOptions<T>;
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
