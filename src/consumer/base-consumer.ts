import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { sleepPromise } from '../utils';
import { RECONNECT_TIMEOUT } from './consumer';
import { ConsumerWrapper } from './types';

/**
 * Events emitted by a {@link BaseConsumer}.
 */
export type BaseConsumerEventMap = {
    /**
     * Emitted when an error occurs during reconnection to RabbitMQ.
     */
    reconnectError: [error: unknown];
    /**
     * Emitted when the consumer is closed and all in-flight messages have been processed.
     */
    close: [];
    [event: string]: unknown[];
};

export abstract class BaseConsumer<
    CallbackFn,
    EventMap extends BaseConsumerEventMap,
> extends EventEmitter<EventMap> {
    protected consumer: Promise<ConsumerWrapper<CallbackFn>> | null = null;
    protected currentlyProcessingMessages = 0;
    protected notifyMessageProcessed: (() => void) | undefined = undefined;

    constructor(
        public readonly channel: Channel,
        public readonly queue: Queue,
    ) {
        super();
        this.channel.on('close', this.channelCloseCallback);
    }

    private readonly channelCloseCallback = () => {
        setTimeout(async () => {
            if (this.consumer) {
                const { callback } = await this.consumer;
                this.consumer = null;
                this.listen(callback).catch(err => {
                    (this as EventEmitter<BaseConsumerEventMap>).emit('reconnectError', err);
                    void this.close();
                });
            }
        }, RECONNECT_TIMEOUT);
    };

    abstract listen(callback: CallbackFn): Promise<this>;

    async close(timeout = 30000): Promise<void> {
        if (this.consumer) {
            const consumer = await this.consumer;
            const channel = await this.channel.native();

            await channel.cancel(consumer.amqpConsumer.consumerTag);

            const waitForAllMessagesPromise = new Promise<void>(resolve => {
                this.notifyMessageProcessed = () => {
                    if (this.currentlyProcessingMessages === 0)
                        resolve();
                };
                this.notifyMessageProcessed();
            });

            try {
                await Promise.race([
                    waitForAllMessagesPromise,
                    sleepPromise(timeout).then(() => {
                        throw new Error('Consumer close timeout');
                    }),
                ]);
            } finally {
                (this as EventEmitter<BaseConsumerEventMap>).emit('close');
                this.consumer = null;
            }
        }
    }
}
