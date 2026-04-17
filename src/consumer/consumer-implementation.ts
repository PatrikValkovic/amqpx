import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { deepMerge, sleepPromise } from '../utils';
import { Consumer, ConsumerEventMap, RECONNECT_TIMEOUT } from './consumer';
import { ConsumerCallbackFn, ConsumerOptions, ConsumptionFailureStrategy, ConsumerWrapper, DEFAULT_CONSUMER_OPTIONS } from './types';

export class ConsumerImplementation<Message> extends EventEmitter<ConsumerEventMap> implements Consumer<Message> {
    private readonly options: Required<ConsumerOptions<Message>>;
    private consumer: ConsumerWrapper<ConsumerCallbackFn<Message>> | null = null;
    private currentlyProcessingMessages = 0;
    private notifyMessageProcessed: (() => void) | undefined = undefined;

    constructor(
        private readonly channel: Channel,
        private readonly queue: Queue,
        options: ConsumerOptions<Message>,
    ) {
        super();
        this.options = deepMerge({}, DEFAULT_CONSUMER_OPTIONS, options) as typeof DEFAULT_CONSUMER_OPTIONS;
        // This make sure the implementation tries to reconnect to the channel
        this.channel.on('close', this.channelCloseCallback);
    }

    // Must be kept as attribute instead of method, so I don't need to deal with .bind
    // This magic will make sure the implementation tries to reconnect to the channel with
    // some backoff when the connection is lost.
    private readonly channelCloseCallback = () => {
        setTimeout(() => {
            if (this.consumer) {
                const { callback } = this.consumer;
                this.consumer = null;
                this.listen(callback).catch(() => {
                    void this.close();
                });
            }
        }, RECONNECT_TIMEOUT);
    };

    async close(timeout = 30000): Promise<void> {
        if (this.consumer) {
            const channel = await this.channel.native();

            const { consumer } = this;
            await channel.cancel(consumer.amqpConsumer.consumerTag);

            const waitForAllMessagesPromise = new Promise<void>((resolve => {
                this.notifyMessageProcessed = () => {
                    if (this.currentlyProcessingMessages === 0)
                        resolve();
                };
                this.notifyMessageProcessed();
            }));

            try {
                await Promise.race([
                    waitForAllMessagesPromise,
                    sleepPromise(timeout).then(() => {
                        throw new Error('Consumer close timeout');
                    }),
                ]);
            } finally {
                this.consumer = null;
            }
        }
    }

    async listen(callback: ConsumerCallbackFn<Message>) {
        if (this.consumer)
            throw new Error('Listener is already attached');

        const [channel, queueName] = await Promise.all([
            this.channel.native(),
            this.queue.name(),
        ]);

        await channel.prefetch(this.options.prefetch);
        const shouldAck =
            // if prefetch is greater than zero, then in order to consumer not process more messages
            // than specified, acknowledgement must be enabled, otherwise (if value is false) auto
            // acknowledgement is enabled and broker automatically knowledge any sent message resulting
            // in multiple messages on consumer than specified
            this.options.prefetch > 0 ||
            // acknowledgement must be also enabled on any non-drop failure strategy
            this.options.failureStrategy !== ConsumptionFailureStrategy.Drop;
        // effectively enabling auto-acknowledgement only if there is no limit and drop failure strategy

        const channelStatus = {
            isConnected: true,
        };
        this.channel.once('close', () => {
            channelStatus.isConnected = false;
        });

        this.currentlyProcessingMessages = 0;
        const amqpConsumer = await channel.consume(
            queueName,
            this.messageReceiver.bind(this, callback, shouldAck, channel, channelStatus),
            {
                ...this.options.consumeOptions,
                noAck: !shouldAck,
            },
        );

        this.consumer = {
            amqpConsumer,
            callback,
        };

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
        channelStatus: { isConnected: boolean },
        msg: amqp.ConsumeMessage | null,
    ) {
        // empty message means consumer is canceled
        if (!msg) {
            await this.channel.close();
            return;
        }

        try {
            this.currentlyProcessingMessages++;
            const { content } = msg;
            const parsed = await this.options.parseMessageFn(content);
            await callback({
                message: parsed,
                rabbitMessage: msg,
                channel: this.channel,
            });
            if (channelStatus.isConnected && shouldAck)
                // keep await there in case API change in the future
                await originalChannel.ack(msg);
        } catch (error) {
            this.emit('handlingFailed', error);
            if (!channelStatus.isConnected)
                return;
            switch (this.options.failureStrategy) {
            case ConsumptionFailureStrategy.Drop:
                if (shouldAck)
                    // keep await there in case API change in the future
                    await originalChannel.ack(msg);
                return;
            case ConsumptionFailureStrategy.Reject:
                originalChannel.nack(msg, false, false);
                return;
            case ConsumptionFailureStrategy.Requeue:
                originalChannel.nack(msg, false, true);
                return;
            default:
                throw new Error(`Not supported failure strategy: ${this.options.failureStrategy}`);
            }
        } finally {
            this.currentlyProcessingMessages--;
            this.notifyMessageProcessed?.();
        }
    }
}
