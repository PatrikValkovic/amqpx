import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Replies } from 'amqplib';
import { Connection } from '../connection';
import { Exchange, ExchangeImplementation, ExchangeTypes } from '../exchange';
import { Queue, QueueImplementation, QueueOptions } from '../queue';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { ExchangeConsumerQueueOptions, ExchangeOptions } from '../exchange/types';
import { Consumer, ConsumerOptions } from '../consumer';
import { DEFAULT_RETRY_STRATEGY, retryLoop } from '../retry';
import { externallyResolvedPromise } from '../utils';
import { Channel } from './channel';
import { ChannelWrapper, ChannelPublishOptions } from './types';

export class ChannelImplementation implements Channel {
    private readonly eventEmitter = new EventEmitter();
    private readonly wrapper: ChannelWrapper;
    private isDraining = false;

    constructor(
        private readonly connection: Connection,
        publishConfirm: boolean,
    ) {
        this.wrapper = {
            channel: null,
            isConfirmed: publishConfirm,
        };
    }

    async connect(): Promise<ChannelImplementation> {
        if (this.wrapper.channel)
            return this;
        if (this.wrapper.awaiting) {
            await this.wrapper.awaiting;
            return this.connect();
        }

        const [promise, resolvePromise] = externallyResolvedPromise();
        this.wrapper.awaiting = promise;
        const connection = await this.connection.native();
        this.wrapper.channel = await (this.wrapper.isConfirmed ? connection.createConfirmChannel() : connection.createChannel());
        const { channel } = this.wrapper;
        channel.on('error', error => {
            this.wrapper.channel = null;
            this.eventEmitter.emit('error', error);
        });
        channel.on(`close`, () => {
            this.wrapper.channel = null;
            this.eventEmitter.emit('close');
        });
        channel.on('drain', () => {
            this.eventEmitter.emit('drain');
        });
        resolvePromise();
        this.wrapper.awaiting = undefined;
        return this;
    }

    async close(): Promise<void> {
        if (this.wrapper.isConfirmed)
            await this.wrapper.channel?.waitForConfirms();
        return this.wrapper.channel?.close?.();
    }

    async publish(exchange: string, routingKey: string, content: Buffer, optionsArgs: ChannelPublishOptions): Promise<void> {
        const { drainTimeout, isRecursion = false, retryStrategy = DEFAULT_RETRY_STRATEGY, ...options } = optionsArgs;
        const native = await this.native();

        if (this.isDraining) {
            // internal buffer is full, we need to wait
            if (isRecursion)
                throw new Error(`Rabbit drain recursion timeout`);

            let timeoutHandler: NodeJS.Timeout;
            await new Promise((resolve, reject) => {
                const drainHandler = () => {
                    this.isDraining = false;
                    if (timeoutHandler)
                        clearTimeout(timeoutHandler);
                    this.publish(exchange, routingKey, content, {
                        ...optionsArgs,
                        isRecursion: true,
                    }).then(resolve).catch(reject);
                };
                this.eventEmitter.once('drain', drainHandler);
                timeoutHandler = setTimeout(async () => {
                    this.eventEmitter.removeListener('drain', drainHandler);
                    reject(new Error('Rabbit drain timeout'));
                    await native.close();
                }, drainTimeout);
            });
        }

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
                err => err.message === 'message nacked',
            );
        }

        if (publishResult) {
            this.isDraining = false;
            return;
        }

        this.isDraining = true;
        await this.publish(exchange, routingKey, content, optionsArgs);
    }

    async native() {
        await this.connect();
        if (this.wrapper.channel)
            return this.wrapper.channel;
        throw new Error(`Channel not created`);
    }

    createQueue(name: string, options?: QueueOptions): Promise<Queue> {
        return new QueueImplementation(this, name, options).assert();
    }

    createExchange(name: string, type: ExchangeTypes, options?: ExchangeOptions): Promise<Exchange> {
        return new ExchangeImplementation(this, name, type, options).assert();
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

    createConsumerForExchange<T>(exchange: Exchange, options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>> {
        return exchange.createConsumer({
            channel: this,
            ...(options ?? {}),
        }, queueOptions);
    }

    off(event: string, callback: (...args: unknown[]) => void): void {
        this.eventEmitter.off(event, callback);
    }

    on(event: string, callback: (...args: unknown[]) => void): void {
        this.eventEmitter.on(event, callback);
    }

    once(event: string, callback: (...args: unknown[]) => void): void {
        this.eventEmitter.once(event, callback);
    }

    internalEmitter(): EventEmitter {
        return this.eventEmitter;
    }

    async checkQueue(name: string): Promise<Replies.AssertQueue> {
        const nativeChannel = await this.native();
        return nativeChannel.checkQueue(name);
    }
}
