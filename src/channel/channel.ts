import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Replies } from 'amqplib';
import { Queue, QueueOptions } from '../queue';
import { ExchangeTypes, Exchange } from '../exchange';
import { Producer, ProducerOptions } from '../producer';
import { ConsumerOptions, Consumer, BatchConsumerOptions, BatchConsumer } from '../consumer';
import { ExchangeConsumerQueueOptions, ExchangeConsumerOptions, ExchangeBatchConsumerOptions, ExchangeOptions } from '../exchange/types';
import { Connection } from '../connection';
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

    /**
     * The {@link Connection} that owns this channel.
     */
    get connection(): Connection;

    /**
     * Open the underlying amqplib channel.
     * Must be called before any other operation.
     * Subsequent calls are no-ops if the channel is already open.
     */
    connect(): Promise<Channel>;

    /**
     * Close the underlying amqplib channel.
     * For confirm channels, waits for all pending confirmations before closing.
     */
    close(): Promise<void>;

    /**
     * Assert an exchange on the broker and return an {@link Exchange} handle.
     *
     * @param name - Exchange name.
     * @param type - Exchange type (direct, fanout, topic, headers, …).
     * @param options - Optional exchange assertion options.
     */
    createExchange(name: string, type: ExchangeTypes, options?: ExchangeOptions): Promise<Exchange>;

    /**
     * Assert a queue on the broker and return a {@link Queue} handle.
     *
     * @param name - Queue name.
     * @param options - Optional queue assertion options.
     */
    createQueue(name: string, options?: QueueOptions): Promise<Queue>;

    /**
     * Check whether a queue exists on the broker without asserting it.
     * Resolves with queue metadata; rejects if the queue does not exist.
     *
     * @param name - Queue name to inspect.
     */
    checkQueue(name: string): Promise<Replies.AssertQueue>;

    /**
     * Get native channel of amqplib. Will call connect if the channel is not yet created.
     * This is intended for internal use.
     */
    native(): Promise<amqp.Channel | amqp.ConfirmChannel>;

    /**
     * Publish message directly using amqplib publish function.
     * Handles backpressure (drain) and confirm-channel retries automatically.
     * This is intended for internal use.
     * Use `createProducer*` functions instead.
     *
     * @param exchange - Exchange name to publish to.
     * @param routingKey - Routing key for the message.
     * @param content - Serialized message body.
     * @param options - Publish options including drain timeout and retry strategy.
     * @returns `true` if the broker confirmed receipt (confirm channel), `false` if fire-and-forget (non-confirm channel).
     */
    publish(exchange: string, routingKey: string, content: Buffer, options: ChannelPublishOptions): Promise<boolean>;

    /**
     * Create a producer that publishes directly to a queue using this channel.
     * Each producer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createProducerForQueue` or pass channel
     * in `options` param.
     *
     * @param queue - Target queue.
     * @param options - Optional producer options (serialization, routing key, hooks, …).
     */
    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>): Promise<Producer<T>>;

    /**
     * Create a producer that publishes to an exchange using this channel.
     * Each producer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createProducerForExchange` or pass channel
     * in `options` param.
     *
     * @param exchange - Target exchange.
     * @param options - Optional producer options (serialization, routing key, hooks, …).
     */
    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>): Promise<Producer<T>>;

    /**
     * Create a consumer that reads from a queue using this channel.
     * Each consumer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createConsumerForQueue` or pass channel
     * in `options` param.
     *
     * @param queue - Source queue.
     * @param options - Optional consumer options (failure strategy, prefetch, …).
     */
    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>>;

    /**
     * Create a consumer that reads from an exchange using this channel.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * Each consumer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createConsumerForExchange` or pass channel
     * in `options` param.
     *
     * @param exchange - Source exchange.
     * @param options - Optional consumer options (failure strategy, prefetch, …).
     * @param queueOptions - Options for the auto-created exclusive queue binding.
     */
    createConsumerForExchange<T>(exchange: Exchange, options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    /**
     * Create a batch consumer that reads from a queue using this channel.
     * Each consumer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createBatchConsumerForQueue` or pass channel
     * in `options` param.
     *
     * @param queue - Source queue.
     * @param options - Optional batch consumer options (batch size, failure strategy, prefetch, …).
     */
    createBatchConsumerForQueue<T>(queue: Queue, options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>>;

    /**
     * Create a batch consumer that reads from an exchange using this channel.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * Each consumer should have its own dedicated channel; this method use current
     * channel. Consider using `connection.createBatchConsumerForExchange` or pass channel
     * in `options` param.
     *
     * @param exchange - Source exchange.
     * @param options - Optional batch consumer options (batch size, failure strategy, prefetch, …).
     * @param queueOptions - Options for the auto-created exclusive queue binding.
     */
    createBatchConsumerForExchange<T>(exchange: Exchange, options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>>;
}
