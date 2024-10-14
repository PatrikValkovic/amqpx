import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { Queue } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer } from '../consumer';
import { ExchangeConsumerQueueOptions } from '../exchange/types';

export enum ConnectionState {
    preconnect = 'preconnect',
    connecting = 'connecting',
    connected = 'connected',
    closed = 'closed',
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

    on(eventName: 'connectionRetryExhausted' | 'close', callback: () => void): Connection;
    on(eventName: 'error', callback: (err: unknown) => void): Connection;
    on(eventName: 'connected', callback: (connection: Connection) => void): Connection;
}
