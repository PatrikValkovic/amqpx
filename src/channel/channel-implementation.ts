import { debuglog } from 'util';
import { EventEmitter } from 'events';
import { performance } from 'perf_hooks';
import * as amqp from 'amqplib';
import { ConfirmChannel, Replies } from 'amqplib';
import { Connection } from '../connection';
import { Exchange, ExchangeImplementation, ExchangeTypes } from '../exchange';
import { Queue, QueueImplementation, QueueOptions } from '../queue';
import { ProducerOptions, Producer } from '../producer';
import { ExchangeConsumerQueueOptions, ExchangeConsumerOptions, ExchangeBatchConsumerOptions, ExchangeOptions } from '../exchange/types';
import { Consumer, ConsumerOptions, BatchConsumer, BatchConsumerOptions } from '../consumer';
import { DEFAULT_RETRY_STRATEGY, retryLoop } from '../retry';
import { DrainError } from '../errors';
import { errToMessage, LIB_NAME, swallowError } from '../utils';
import { Channel, ChannelEventMap } from './channel';
import { ChannelWrapper, ChannelPublishOptions } from './types';

const debug = debuglog(`${LIB_NAME}:channel`);

export class ChannelImplementation extends EventEmitter<ChannelEventMap> implements Channel {
    private readonly wrapper: ChannelWrapper;
    private closeHandler: Promise<void> | null = null;
    private drainPromise: Promise<void> | null = null;

    constructor(
        public readonly connection: Connection,
        publishConfirm: boolean,
    ) {
        super();
        this.wrapper = {
            channel: null,
            isConfirmed: publishConfirm,
        };
    }

    async connect(): Promise<ChannelImplementation> {
        if (this.wrapper.channel) {
            await this.wrapper.channel;
            return this;
        }

        this.wrapper.channel = (async () => {
            try {
                debug('connecting confirmed=%s', this.wrapper.isConfirmed);
                const connection = await this.connection.native();
                const channel = await (this.wrapper.isConfirmed ? connection.createConfirmChannel() : connection.createChannel());
                debug('connected confirmed=%s', this.wrapper.isConfirmed);

                channel.on('error', error => {
                    this.wrapper.channel = null;
                    debug('native-error error=%s', error?.message);
                    this.emit('error', error);
                });
                channel.on(`close`, async () => {
                    this.wrapper.channel = null;
                    debug('native-close');
                    this.emit('close');
                });
                channel.on('drain', () => {
                    debug('native-drain');
                    this.emit('drain');
                });
                return channel;
            } catch (err) {
                this.wrapper.channel = null;
                throw err;
            }
        })();

        await this.wrapper.channel;
        return this;
    }

    async close(): Promise<void> {
        if (!this.wrapper.channel)
            return;

        this.closeHandler ??= (async () => {
            try {
                debug('closing');
                const wrapper = {
                    isConfirmed: this.wrapper.isConfirmed,
                    channel: await this.wrapper.channel,
                };
                if (!wrapper.channel)
                    throw new Error('Internal error: channel is empty but app attempted to close it');
                if (wrapper.isConfirmed)
                    await swallowError((wrapper.channel as ConfirmChannel).waitForConfirms());
                await swallowError(wrapper.channel.close?.());
                debug('closed');
            } finally {
                this.closeHandler = null;
                this.wrapper.channel = null;
            }
        })();

        return this.closeHandler;
    }

    createExchange(name: string, type: ExchangeTypes, options?: ExchangeOptions): Promise<Exchange> {
        return new ExchangeImplementation(this, name, type, options).assert();
    }

    createQueue(name: string, options?: QueueOptions): Promise<Queue> {
        return new QueueImplementation(this, name, options).assert();
    }

    async checkQueue(name: string): Promise<Replies.AssertQueue> {
        const nativeChannel = await this.native();
        return nativeChannel.checkQueue(name);
    }

    async native() {
        await this.connect();
        if (this.wrapper.channel)
            return this.wrapper.channel;
        throw new Error(`Channel not created`);
    }

    async publish(exchange: string, routingKey: string, content: Buffer, optionsArgs: ChannelPublishOptions): Promise<boolean> {
        debug('publish exchange=%s routing-key=%s confirmed=%s', exchange, routingKey, this.wrapper.isConfirmed);
        const { drainTimeout, retryStrategy = DEFAULT_RETRY_STRATEGY, ...options } = optionsArgs;

        while (true) {
            const native = await this.native();
            if (this.drainPromise)
                await this.drainPromise;

            let publishResult: boolean;
            if (!this.wrapper.isConfirmed) {
                const channel = native as amqp.Channel;
                publishResult = channel.publish(exchange, routingKey, content, options);
            } else {
                const channel = native as amqp.ConfirmChannel;
                publishResult = await retryLoop(
                    retryStrategy,
                    () => new Promise<boolean>((resolve, reject) => {
                        const status = channel.publish(exchange, routingKey, content, options, err => {
                            err ? reject(err) : resolve(status);
                        });
                    }),
                    err => {
                        debug('publish-nacked error=%s', errToMessage(err));
                        return errToMessage(err) === 'message nacked';
                    },
                );
            }
            if (publishResult) {
                debug('publish-success exchange=%s routing-key=%s', exchange, routingKey);
                return this.wrapper.isConfirmed;
            }

            if (!this.drainPromise) {
                const drainStart = performance.now();
                debug('backpressure-detected exchange=%s routing-key=%s', exchange, routingKey);
                this.drainPromise = new Promise<void>((resolve, reject) => {
                    let timeoutHandler: NodeJS.Timeout;
                    const drainHandler = () => {
                        clearTimeout(timeoutHandler);
                        debug('drain-cleared after=%dms', performance.now() - drainStart);
                        this.drainPromise = null;
                        resolve();
                    };
                    this.once('drain', drainHandler);
                    this.once('close', drainHandler);
                    timeoutHandler = setTimeout(async () => {
                        this.removeListener('drain', drainHandler);
                        this.drainPromise = null;
                        debug('drain-timeout exchange=%s routing-key=%s timeout=%dms', exchange, routingKey, drainTimeout);
                        reject(new DrainError('Rabbit drain timeout'));
                        void native.close();
                    }, drainTimeout);
                });
            }
        }
    }

    createProducerForQueue<T>(queue: Queue, options: ProducerOptions<T> = {}): Promise<Producer<T>> {
        return queue.createProducer({
            channel: this,
            ...options,
        });
    }

    createProducerForExchange<T>(exchange: Exchange, options: ProducerOptions<T> = {}): Promise<Producer<T>> {
        return exchange.createProducer({
            channel: this,
            ...options,
        });
    }

    createConsumerForQueue<T>(queue: Queue, options?: ConsumerOptions<T>): Promise<Consumer<T>> {
        return queue.createConsumer({
            channel: this,
            ...options,
        });
    }

    createConsumerForExchange<T>(exchange: Exchange, options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>> {
        return exchange.createConsumer({
            channel: this,
            ...options,
        }, queueOptions);
    }

    createBatchConsumerForQueue<T>(queue: Queue, options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>> {
        return queue.createBatchConsumer({
            channel: this,
            ...options,
        });
    }

    createBatchConsumerForExchange<T>(exchange: Exchange, options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>> {
        return exchange.createBatchConsumer({
            channel: this,
            ...options,
        }, queueOptions);
    }
}
