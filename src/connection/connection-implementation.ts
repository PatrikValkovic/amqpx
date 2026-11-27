import { debuglog } from 'util';
import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Replies } from 'amqplib';
import { DEFAULT_RETRY_STRATEGY, normalizeRetryStrategy, retryLoop, RetryStrategy } from '../retry';
import { TimeStrategy } from '../retry/time-strategies';
import { Channel, ChannelImplementation } from '../channel';
import {
    Exchange,
    ExchangeConsumerQueueOptions,
    ExchangeConsumerOptions,
    ExchangeBatchConsumerOptions,
    ExchangeOptions,
    ExchangeTypes,
} from '../exchange';
import { Consumer, ConsumerOptions, BatchConsumer, BatchConsumerOptions } from '../consumer';
import { Queue, QueueOptions } from '../queue';
import { ProducerOptions, Producer } from '../producer';
import { TooManyRetriesError } from '../errors';
import { errToMessage, LIB_NAME, maskAmqpUrl, sleepPromise } from '../utils';
import { Connection, ConnectionEventMap, ConnectionState } from './connection';

const debug = debuglog(`${LIB_NAME}:connection`);


export class ConnectionImplementation extends EventEmitter<ConnectionEventMap> implements Connection {
    private readonly retryStrategy: Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: TimeStrategy };
    private connection: Promise<amqp.ChannelModel | null> | null = null;
    private closingHandler: Promise<void> | null = null;
    private connectionState: ConnectionState = ConnectionState.preconnect;
    /** Channel for declaring exchanges, queues and bindings; opened on first use. */
    private readonly topologyChannel: ChannelImplementation;

    constructor(
        private readonly options: amqp.Options.Connect,
        retryStrategy: RetryStrategy = {},
        private readonly socketOptions?: amqp.SocketOptions,
    ) {
        super();
        this.retryStrategy = normalizeRetryStrategy({
            ...DEFAULT_RETRY_STRATEGY,
            ...retryStrategy,
        });
        this.topologyChannel = new ChannelImplementation(this, false);
        // every exchange and queue declared through it listens for its `close` event
        this.topologyChannel.setMaxListeners(0);
        this.topologyChannel.on('error', err => {
            debug('topology-channel-error error=%s', errToMessage(err));
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

    async close(timeout = 30_000): Promise<void> {
        if ([ConnectionState.closed, ConnectionState.preconnect].includes(this.connectionState))
            return;

        if (this.closingHandler)
            return this.closingHandler;

        this.closingHandler = (async () => {
            debug('closing url=%s', maskAmqpUrl(this.options));
            this.connectionState = ConnectionState.closing;
            const connection = await this.connection?.catch(() => { /* ignore */ });
            try {
                const start = performance.now();
                // a failing channel must not keep the connection open
                await this.topologyChannel.close(timeout).catch(err => {
                    debug('topology-channel-close-error error=%s', errToMessage(err));
                });
                const remaining = Math.max(0, timeout - (performance.now() - start));
                const abort = new AbortController();
                await Promise.race([
                    Promise.resolve(connection?.close()).then(() => abort.abort()),
                    sleepPromise(remaining).then(() => {
                        if (!abort.signal.aborted)
                            throw new Error('Connection close timed out');
                    }),
                ]);
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

    createExchange(name: string, type: ExchangeTypes, options?: ExchangeOptions): Promise<Exchange> {
        return this.topologyChannel.createExchange(name, type, options);
    }

    createQueue(name: string, options?: QueueOptions): Promise<Queue> {
        return this.topologyChannel.createQueue(name, options);
    }

    checkQueue(name: string): Promise<Replies.AssertQueue> {
        return this.topologyChannel.checkQueue(name);
    }

    createConsumerForExchange<T>(exchange: Exchange, options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>> {
        return this.createChannel().createConsumerForExchange(exchange, options, queueOptions);
    }

    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>> {
        return this.createChannel().createConsumerForQueue(queue, options);
    }

    createBatchConsumerForQueue<T>(queue: Queue, options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>> {
        return this.createChannel().createBatchConsumerForQueue(queue, options);
    }

    createBatchConsumerForExchange<T>(exchange: Exchange, options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>> {
        return this.createChannel().createBatchConsumerForExchange(exchange, options, queueOptions);
    }

    createProducerForExchange<T>(exchange: Exchange, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>> {
        return this.createChannel(isConfirmed).createProducerForExchange(exchange, options);
    }

    createProducerForQueue<T>(queue: Queue, options?: ProducerOptions<T>, isConfirmed?: boolean): Promise<Producer<T>> {
        return this.createChannel(isConfirmed).createProducerForQueue(queue, options);
    }
}

/**
 * Creates a {@link ConnectionImplementation} and establishes the connection in one step.
 * Equivalent to `new ConnectionImplementation(...args).connect()`.
 * @param args - Same arguments as the {@link ConnectionImplementation} constructor.
 */
export async function connect(
    ...args: ConstructorParameters<typeof ConnectionImplementation>
): Promise<Connection> {
    return new ConnectionImplementation(...args).connect();
}
