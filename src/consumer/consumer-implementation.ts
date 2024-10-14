import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { deepMerge } from '../utils';
import { Consumer } from './consumer';
import { ConsumerCallbackFn, ConsumerOptions, ConsumptionFailedStrategy, DEFAULT_CONSUMER_OPTIONS } from './types';

export class ConsumerImplementation<Message> implements Consumer<Message> {
    private readonly eventEmitter = new EventEmitter();
    private readonly options: Required<ConsumerOptions<Message>>;
    private consumer: amqp.Replies.Consume | null = null;
    private callback: ConsumerCallbackFn<Message> | null = null;
    private currentlyProcessingMessages = 0;
    private notifyMessageProcessed: (() => void) | undefined = undefined;
    private readonly channelCloseCallback: (() => void);

    constructor(
        private readonly channel: Channel,
        private readonly queue: Queue,
        options: ConsumerOptions<Message>,
    ) {
        this.options = deepMerge({}, DEFAULT_CONSUMER_OPTIONS, options) as typeof DEFAULT_CONSUMER_OPTIONS;
        // This magic will make sure the implementation tries to reconnect to the channel with
        // some backoff when the connection is lost.
        this.channelCloseCallback = () => {
            setTimeout(() => {
                if (this.callback)
                    this.listen(this.callback).catch(() => { /* ignore */ });
            }, 100);
        };
        this.channel.on('close', this.channelCloseCallback);
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    on(eventName: string, callback: (...args: any[]) => void): Consumer<Message> {
        this.eventEmitter.on(eventName, callback);
        return this;
    }

    async setPrefetch(prefetch: number): Promise<void> {
        (await this.channel.native()).prefetch(prefetch);
    }

    async close(timeout = 30000): Promise<void> {
        const channel = await this.channel.native();
        this.consumer?.consumerTag && await channel.cancel(this.consumer.consumerTag);
        return new Promise((resolve, reject) => {
            const timeoutHandler = setTimeout(() => {
                reject(new Error('Consumer close timeout'));
            }, timeout);
            this.notifyMessageProcessed = () => {
                if (this.currentlyProcessingMessages === 0) {
                    clearTimeout(timeoutHandler);
                    this.channel.off('close', this.channelCloseCallback);
                    resolve();
                }
            };
            this.notifyMessageProcessed();
        });
    }

    async listen(callback: ConsumerCallbackFn<Message>) {
        this.callback = callback;
        const [channel, queueName] = await Promise.all([
            this.channel.native(),
            this.queue.name(),
        ]);
        await channel.prefetch(this.options.prefetch);
        const shouldAck =
            this.options.prefetch > 0 ||
            this.options.failureStrategy !== ConsumptionFailedStrategy.Drop;
        this.consumer = await channel.consume(
            queueName,
            this.messageReceiver.bind(this, callback, shouldAck, channel),
            {
                ...this.options.consumeOptions,
                noAck: !shouldAck,
            },
        );
        return this;
    }

    getQueue(): Queue {
        return this.queue;
    }

    getChannel(): Channel {
        return this.channel;
    }

    private async messageReceiver(
        callback: ConsumerCallbackFn<Message>,
        shouldAck: boolean,
        originalChannel: amqp.Channel,
        msg: amqp.ConsumeMessage | null,
    ) {
        // empty message means consumer is cancelled
        if (!msg) {
            await this.channel.close();
            return;
        }
        let stillConnected = true;
        const handler = () => {
            stillConnected = false;
        };
        this.channel.once('close', handler);
        try {
            this.currentlyProcessingMessages++;
            const { content } = msg;
            const parsed = await this.options.parseMessageFn(content);
            await callback({
                message: parsed,
                rabbitMessage: msg,
                channel: this.channel,
            });
            if (stillConnected && shouldAck)
                originalChannel.ack(msg);
        } catch (error) {
            this.eventEmitter.emit('handlingFailed', error);
            if (!stillConnected)
                return;
            switch (this.options.failureStrategy) {
            case ConsumptionFailedStrategy.Drop:
                if (shouldAck)
                    await originalChannel.ack(msg);
                return;
            case ConsumptionFailedStrategy.Reject:
                originalChannel.nack(msg, false, false);
                return;
            case ConsumptionFailedStrategy.Requeue:
                originalChannel.nack(msg, false, true);
                return;
            default:
                throw new Error(`Not supported failure strategy: ${this.options.failureStrategy}`);
            }
        } finally {
            this.currentlyProcessingMessages--;
            this.notifyMessageProcessed?.();
            this.channel.off('close', handler);
        }
    }
}
