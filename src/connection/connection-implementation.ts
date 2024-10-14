import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { DEFAULT_RETRY_STRATEGY, normalizeRetryStrategy, RetryStrategy } from '../retry';
import { ITimeStrategy } from '../retry/time-strategies';
import { Channel, ChannelImplementation } from '../channel';
import { Exchange } from '../exchange';
import { Consumer, ConsumerOptions } from '../consumer';
import { ExchangeConsumerQueueOptions } from '../exchange/types';
import { Queue } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { sleepPromise } from '../utils';
import { Connection, ConnectionState } from './connection';


export class ConnectionImplementation implements Connection {
    private readonly eventEmitter = new EventEmitter();
    private readonly retryStrategy: Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: ITimeStrategy };
    private connection: amqp.Connection | null = null;
    private connectionAttempts = -1;
    private connectionState: ConnectionState = ConnectionState.preconnect;

    constructor(
        private readonly options: amqp.Options.Connect,
        retryStrategy: RetryStrategy = {},
    ) {
        this.retryStrategy = normalizeRetryStrategy({
            ...DEFAULT_RETRY_STRATEGY,
            ...retryStrategy,
        });
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    on(eventName: string, callback: (...args: any[]) => void) {
        this.eventEmitter.on(eventName, callback);
        return this;
    }

    async connect() {
        while (this.connectionState === ConnectionState.connecting)

            await sleepPromise(this.retryStrategy.waitTimeoutMs);

        if (this.connectionState === ConnectionState.connected)
            return this;

        for (;;) {
            try {
                this.connectionState = ConnectionState.connecting;
                this.connectionAttempts++;
                this.connection = await amqp.connect(this.options);
                this.connectionAttempts = 0;
                this.connectionState = ConnectionState.connected;
                this.eventEmitter.emit('connected', this);
                this.connection.on('close', () => {
                    this.connection = null;
                    this.connectionState = ConnectionState.closed;
                    this.connect().catch(() => { /* ignore */ });
                });
                return this;
            } catch (error) {
                this.eventEmitter.emit('error', error);
                if (this.connectionAttempts >= this.retryStrategy.maxRetries) {
                    this.connectionState = ConnectionState.closed;
                    this.eventEmitter.emit('connectionRetryExhausted');
                    throw new Error(`Connection could not be established`);
                }
                await sleepPromise(this.retryStrategy.reconnectionTimeoutMs(this.connectionAttempts));
            }
        }
    }

    async close(): Promise<void> {
        if ([ConnectionState.closed, ConnectionState.preconnect].includes(this.connectionState))
            return Promise.resolve();

        while (this.connectionState === ConnectionState.connecting)
            await sleepPromise(this.retryStrategy.waitTimeoutMs);

        if ([ConnectionState.connected].includes(this.connectionState))
            await this?.connection?.close();

        this.eventEmitter.emit('close');
        return Promise.resolve();
    }

    state() {
        return this.connectionState;
    }

    async native(): Promise<amqp.Connection> {
        while (!this.connection || this.connectionState !== ConnectionState.connected)
            await this.connect();
        return this.connection;
    }

    createChannel(isConfirmed = false): Channel {
        return new ChannelImplementation(this, isConfirmed);
    }

    createConsumerForExchange<T>(exchange: Exchange, options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>> {
        return this.createChannel().createConsumerForExchange(exchange, options, queueOptions);
    }

    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>> {
        return this.createChannel().createConsumerForQueue(queue, options);
    }

    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>> {
        return this.createChannel(isConfirmed).createProducerForExchange(exchange, options);
    }

    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>> {
        return this.createChannel(isConfirmed).createProducerForQueue(queue, options);
    }
}
