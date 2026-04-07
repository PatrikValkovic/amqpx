import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { Queue } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer } from '../consumer';
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
    // /**
    //  * The connection has been lost and attempt to reconnect is in progress.
    //  */
    // reconnection = 'reconnection', // TODO not implemented yet
}

export interface Connection {
    connect(): Promise<Connection>;
    state(): ConnectionState;
    close(): Promise<void>;

    createChannel(isConfirmed?: boolean): Channel;

    /**
     * Create consumer for queue and create new channel for it.
     */
    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>): Promise<Producer<T>>;
    /**
     * Create producer for exchange and create new channel for it.
     */
    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>>;
    /**
     * Create consumer of queue and create new channel for it.
     */
    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>, isConfirmed?: boolean): Promise<Consumer<T>>;
    /**
     * Create consumer for exchange and create new channel for it.
     * It will create exclusive queue and bind it to the exchange.
     */
    createConsumerForExchange<T>(exchange: Exchange, options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    native(): Promise<amqp.Connection>;

    /**
     * Some other description
     * @param eventName
     * @param callback
     */
    on(eventName: 'connectionRetryExhausted', callback: () => void): Connection;

    /**
     * Some description
     * @param eventName
     * @param callback
     */
    // eslint-disable-next-line @typescript-eslint/unified-signatures
    on(eventName: 'close', callback: () => void): Connection;
    on(eventName: 'error', callback: (err: unknown) => void): Connection;
    on(eventName: 'connected', callback: (connection: Connection) => void): Connection;
}
