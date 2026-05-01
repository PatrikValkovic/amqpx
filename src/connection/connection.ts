import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { Queue } from '../queue';
import { ProducerOptions, Producer } from '../producer';
import { Exchange, ExchangeConsumerQueueOptions, ExchangeConsumerOptions, ExchangeBatchConsumerOptions } from '../exchange';
import { ConsumerOptions, Consumer, BatchConsumerOptions, BatchConsumer } from '../consumer';

/**
 * State in which the connection can be.
 */
export enum ConnectionState {
    /**
     * Connection is not created yet and there hasn't been attempt to establish it.
     * This is the beginning state.
     */
    preconnect = 'preconnect',
    /**
     * The initial connection is being established.
     */
    connecting = 'connecting',
    /**
     * The connection is established and ready to be used.
     */
    connected = 'connected',
    /**
     * The connection is being closed.
     */
    closing = 'closing',
    /**
     * The connection is fully closed.
     */
    closed = 'closed',
}

/**
 * Events emitted by a {@link Connection}.
 */
export interface ConnectionEventMap {
    /**
     * Emitted when the connection retry attempts have been exhausted.
     */
    connectionRetryExhausted: [];
    /**
     * Emitted when the connection is explicitly closed by calling {@link Connection.close}.
     * This event fires after the underlying amqplib connection is fully shut down.
     * It is NOT emitted when the server drops the connection — in that case,
     * the `reconnecting` event is emitted instead and the client attempts to reconnect.
     * Not emitted if the connection was never established (`preconnect` state) or already closed.
     */
    close: [];
    /**
     * Emitted once when the server drops the connection and the client begins attempting to reconnect.
     * Followed by one or more `error` events (one per failed attempt), and then either `connected` on success
     * or `connectionRetryExhausted` if all retries are exhausted.
     */
    reconnecting: [];
    /**
     * Emitted when a connection attempt fails.
     * The error originates from `amqplib` and is forwarded directly.
     * This may fire multiple times during reconnection retries before the connection succeeds or
     * `connectionRetryExhausted` is emitted.
     */
    connectionError: [err: unknown];
    /**
     * Emitted when the connection is successfully established (or re-established after reconnection).
     */
    connected: [connection: Connection];
    /**
     * Emitted when the underlying amqplib connection encounters an error.
     */
    error: [err: unknown];
}

export interface Connection extends EventEmitter<ConnectionEventMap> {
    /**
     * Establishes the connection. Safe to call concurrently — multiple callers will share the same attempt.
     * After retries are exhausted ({@link ConnectionEventMap.connectionRetryExhausted}), the connection moves to the
     * `closed` state, but `connect()` may be called again to start a fresh connection attempt.
     */
    connect(): Promise<Connection>;

    /**
     * Returns the current state of the connection.
     */
    state(): ConnectionState;

    /**
     * Gracefully closes the connection.
     * Emits the `close` event after the underlying amqplib connection is fully shut down.
     * Has no effect if the connection is in `preconnect` or `closed` state.
     * @param timeout - Maximum milliseconds to wait for the underlying connection to close.
     *   Throws `'Connection close timed out'` if exceeded. Waits 30s when omitted.
     */
    close(timeout?: number): Promise<void>;

    /**
     * Creates a new channel on this connection.
     * @param isConfirmed - If true, creates a confirm channel (publisher confirms enabled).
     */
    createChannel(isConfirmed?: boolean): Channel;

    /**
     * Creates a producer for a queue and opens a dedicated channel for it.
     * @param queue - Target queue.
     * @param options - Optional producer options (serialization, routing key, hooks, …).
     * @param isConfirmed - If true, the dedicated channel uses publisher confirms.
     */
    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>>;
    /**
     * Creates a producer for an exchange and opens a dedicated channel for it.
     * @param exchange - Target exchange.
     * @param options - Optional producer options (serialization, routing key, hooks, …).
     * @param isConfirmed - If true, the dedicated channel uses publisher confirms.
     */
    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>>;
    /**
     * Creates a consumer for a queue and opens a dedicated channel for it.
     * @param queue - Source queue.
     * @param options - Optional consumer options (failure strategy, prefetch, …).
     */
    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>>;
    /**
     * Creates a consumer for an exchange and opens a dedicated channel for it.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * @param exchange - Source exchange.
     * @param options - Consumer options, including the binding pattern.
     * @param queueOptions - Options for the auto-created exclusive queue.
     */
    createConsumerForExchange<T>(exchange: Exchange, options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;
    /**
     * Creates a batch consumer for a queue and opens a dedicated channel for it.
     * @param queue - Source queue.
     * @param options - Optional batch consumer options (batch size, failure strategy, prefetch, …).
     */
    createBatchConsumerForQueue<T>(queue: Queue, options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>>;
    /**
     * Creates a batch consumer for an exchange and opens a dedicated channel for it.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * @param exchange - Source exchange.
     * @param options - Batch consumer options, including the binding pattern.
     * @param queueOptions - Options for the auto-created exclusive queue.
     */
    createBatchConsumerForExchange<T>(exchange: Exchange, options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>>;

    /**
     * Returns the underlying amqplib connection. Resolves once the connection is established.
     * This is intended for internal use.
     */
    native(): Promise<amqp.ChannelModel>;
}
