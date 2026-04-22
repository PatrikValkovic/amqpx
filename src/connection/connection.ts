import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { Queue } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer, BatchConsumerOptions, BatchConsumer } from '../consumer';
import { ExchangeConsumerQueueOptions } from '../exchange/types';

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
     * The connection is being closed.
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
     */
    close(): Promise<void>;

    /**
     * Creates a new channel on this connection.
     * @param isConfirmed - If true, creates a confirm channel (publisher confirms enabled).
     */
    createChannel(isConfirmed?: boolean): Channel;

    /**
     * Create producer for queue and create new channel for it.
     */
    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>>;
    /**
     * Create producer for exchange and create new channel for it.
     */
    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>>;
    /**
     * Create consumer of queue and create new channel for it.
     */
    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>>;
    /**
     * Create consumer for exchange and create new channel for it.
     * It will create exclusive queue and bind it to the exchange.
     */
    createConsumerForExchange<T>(exchange: Exchange, options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;
    /**
     * Create batch consumer of queue and create new channel for it.
     */
    createBatchConsumerForQueue<T>(queue: Queue, options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>>;
    /**
     * Create batch consumer for exchange and create new channel for it.
     * It will create exclusive queue and bind it to the exchange.
     */
    createBatchConsumerForExchange<T>(exchange: Exchange, options?: BatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>>;

    /**
     * Returns the underlying amqplib connection. Resolves once the connection is established.
     * This is intended for internal use.
     */
    native(): Promise<amqp.ChannelModel>;
}
