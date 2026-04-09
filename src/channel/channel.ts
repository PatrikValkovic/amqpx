import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Replies } from 'amqplib';
import { Queue, QueueOptions } from '../queue';
import { ExchangeTypes, Exchange } from '../exchange';
import { Producer } from '../producer';
import { ProducerOptions } from '../producer/types';
import { ConsumerOptions, Consumer } from '../consumer';
import { ExchangeConsumerQueueOptions, ExchangeOptions } from '../exchange/types';
import { ChannelPublishOptions } from './types';

/**
 * Events emitted by a {@link Channel}.
 */
export interface ChannelEventMap {
    /**
     * Emitted when the underlying amqplib channel encounters an error.
     */
    error: [err: unknown];
    /**
     * Emitted when the underlying amqplib channel is closed.
     */
    close: [];
    /**
     * Emitted when the internal write buffer drains after backpressure.
     */
    drain: [];
}

export interface Channel extends EventEmitter<ChannelEventMap> {
    connect(): Promise<Channel>;
    close(): Promise<void>;

    createExchange(name: string, type: ExchangeTypes, options?: ExchangeOptions): Promise<Exchange>;
    createQueue(name: string, options?: QueueOptions): Promise<Queue>;
    /**
     * Create producer for queue using current channel.
     * Each producer/consumer should have separate channel, which this call will not ensure.
     * Consider using `connection.createProducerForQueue` method.
     */
    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>): Promise<Producer<T>>;
    /**
     * Create producer for exchange using current channel.
     * Each producer/consumer should have separate channel, which this call will not ensure.
     * Consider using `connection.createProducerForExchange` method.
     */
    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>): Promise<Producer<T>>;
    /**
     * Create consumer of queue using current channel.
     * Each producer/consumer should have separate channel, which this call will not ensure.
     * Consider using `connection.createConsumerForQueue` method.
     */
    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>>;
    /**
     * Create consumer for exchange using current channel.
     * It will create exclusive queue and bind it to the exchange.
     * Each producer/consumer should have separate channel, which this call will not ensure.
     * Consider using `connection.createConsumerForExchange` method.
     */
    createConsumerForExchange<T>(exchange: Exchange, options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    /**
     * Get native channel of amqplib.
     * This is intended for internal use.
     */
    native(): Promise<amqp.Channel | amqp.ConfirmChannel>;

    /**
     * Publish message directly using amqplib publish function.
     * This is intended for internal use.
     * Use `createProducer*` functions instead.
     */
    publish(exchange: string, routingKey: string, content: Buffer, options: ChannelPublishOptions): Promise<void>;

    checkQueue(name: string): Promise<Replies.AssertQueue>;
}
