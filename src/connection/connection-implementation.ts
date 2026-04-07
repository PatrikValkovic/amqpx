import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { DEFAULT_RETRY_STRATEGY, normalizeRetryStrategy, retryLoop, RetryStrategy } from '../retry';
import { ITimeStrategy } from '../retry/time-strategies';
import { Channel, ChannelImplementation } from '../channel';
import { Exchange } from '../exchange';
import { Consumer, ConsumerOptions } from '../consumer';
import { ExchangeConsumerQueueOptions } from '../exchange/types';
import { Queue } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { TooManyRetriesError } from '../utils';
import { Connection, ConnectionState } from './connection';


export class ConnectionImplementation implements Connection {
    private readonly eventEmitter = new EventEmitter();
    private readonly retryStrategy: Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: ITimeStrategy };
    private connection: Promise<amqp.ChannelModel | null> | null = null;
    private closingHandler: Promise<void> | null = null;
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
        if (this.connection) {
            await this.connection;
            return this;
        }

        this.connection = (async () => {
            const terminationStates = [ConnectionState.closed, ConnectionState.closing];
            if (terminationStates.includes(this.connectionState)) {
                this.connection = null;
                return null;
            }
            this.connectionState = ConnectionState.connecting;
            try {
                const nativeConnection = await retryLoop(
                    this.retryStrategy,
                    () => {
                        if (terminationStates.includes(this.connectionState))
                            return null;
                        return amqp.connect(this.options);
                    },
                    error => {
                        this.eventEmitter.emit('error', error);
                        return true;
                    },
                );
                if (!nativeConnection)
                    return null;
                this.connectionState = ConnectionState.connected;
                this.eventEmitter.emit('connected', this);
                nativeConnection.on('close', () => {
                    this.connection = null;
                    if ([ConnectionState.closed, ConnectionState.closing].includes(this.connectionState))
                        return;
                    this.eventEmitter.emit('reconnecting');
                    this.connect().catch(() => { /* ignore */
                    });
                });
                return nativeConnection;
            } catch (err) {
                this.connection = null;
                this.connectionState = ConnectionState.closed;
                if (err instanceof TooManyRetriesError)
                    this.eventEmitter.emit('connectionRetryExhausted');
                throw err;
            }
        })();

        await this.connection;
        return this;
    }

    async close(): Promise<void> {
        if ([ConnectionState.closed, ConnectionState.preconnect].includes(this.connectionState))
            return;

        if (this.closingHandler)
            return this.closingHandler;

        this.closingHandler = (async () => {
            this.connectionState = ConnectionState.closing;
            const connection = await this.connection?.catch(() => { /* ignore */ });
            try {
                await connection?.close();
            } finally {
                // even when close fails clean up references
                this.closingHandler = null;
                this.connection = null;
                this.connectionState = ConnectionState.closed;
                this.eventEmitter.emit('close');
            }
        })();
        return this.closingHandler;
    }

    state() {
        return this.connectionState;
    }

    async native(): Promise<amqp.ChannelModel> {
        const connection = await this.connection;
        if (!connection || this.connectionState !== ConnectionState.connected)
            await this.connect();
        const establishedConnection = await this.connection;
        if (!establishedConnection)
            throw new Error('Connection is null after connect, this should never happen. If you see this error, report it as a bug please.');
        return establishedConnection;
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
