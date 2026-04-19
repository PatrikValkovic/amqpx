import { debuglog } from 'util';
import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { DEFAULT_RETRY_STRATEGY, normalizeRetryStrategy, retryLoop, RetryStrategy } from '../retry';
import { TimeStrategy } from '../retry/time-strategies';
import { Channel, ChannelImplementation } from '../channel';
import { Exchange } from '../exchange';
import { Consumer, ConsumerOptions } from '../consumer';
import { ExchangeConsumerQueueOptions } from '../exchange/types';
import { Queue } from '../queue';
import { ProducerOptions, Producer } from '../producer';
import { TooManyRetriesError } from '../errors';
import { errToMessage, LIB_NAME, maskAmqpUrl } from '../utils';
import { Connection, ConnectionEventMap, ConnectionState } from './connection';

const debug = debuglog(`${LIB_NAME}:connection`);


export class ConnectionImplementation extends EventEmitter<ConnectionEventMap> implements Connection {
    private readonly retryStrategy: Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: TimeStrategy };
    private connection: Promise<amqp.ChannelModel | null> | null = null;
    private closingHandler: Promise<void> | null = null;
    private connectionState: ConnectionState = ConnectionState.preconnect;

    constructor(
        private readonly options: amqp.Options.Connect,
        retryStrategy: RetryStrategy = {},
        private readonly socketOptions?: unknown,
    ) {
        super();
        this.retryStrategy = normalizeRetryStrategy({
            ...DEFAULT_RETRY_STRATEGY,
            ...retryStrategy,
        });
    }

    async connect() {
        if (this.connection) {
            await this.connection;
            return this;
        }

        this.connection = (async () => {
            this.connectionState = ConnectionState.connecting;
            debug('connecting url=%s', maskAmqpUrl(this.options));
            try {
                const nativeConnection = await retryLoop(
                    this.retryStrategy,
                    () => {
                        const terminationStates = [ConnectionState.closed, ConnectionState.closing];
                        if (terminationStates.includes(this.connectionState))
                            return null;
                        return amqp.connect(this.options, this.socketOptions);
                    },
                    error => {
                        this.emit('connectionError', error);
                        debug('retry-connection error=%s', errToMessage(error));
                        return true;
                    },
                );
                if (!nativeConnection)
                    return null;

                this.connectionState = ConnectionState.connected;
                debug('connected url=%s', maskAmqpUrl(this.options));
                this.emit('connected', this);

                nativeConnection.on('close', () => {
                    this.connection = null;
                    if ([ConnectionState.closed, ConnectionState.closing].includes(this.connectionState))
                        return;
                    debug('reconnecting url=%s', maskAmqpUrl(this.options));
                    this.emit('reconnecting');
                    this.connect().catch(() => { /* ignore */ });
                });
                nativeConnection.on('error', err => {
                    debug('native-error error=%s', errToMessage(err));
                    this.emit('error', err);
                });

                return nativeConnection;
            } catch (err) {
                this.connection = null;
                this.connectionState = ConnectionState.closed;
                if (err instanceof TooManyRetriesError) {
                    this.emit('connectionRetryExhausted');
                    debug('retry-exhausted last-error=%s', err.message);
                } else {
                    debug('connecting-error error=%s', (err as Error)?.message);
                }
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
            debug('closing url=%s', maskAmqpUrl(this.options));
            this.connectionState = ConnectionState.closing;
            const connection = await this.connection?.catch(() => { /* ignore */ });
            try {
                await connection?.close();
            } finally {
                // even when close fails clean up references
                this.closingHandler = null;
                this.connection = null;
                this.connectionState = ConnectionState.closed;
                debug('closed url=%s', maskAmqpUrl(this.options));
                this.emit('close');
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
            throw new Error('Internal error: Connection is null after connect');
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
