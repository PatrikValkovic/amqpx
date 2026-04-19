import { debuglog } from 'util';
import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { errToMessage, LIB_NAME, sleepPromise } from '../utils';
import { RECONNECT_TIMEOUT } from './consumer';
import { ConsumerWrapper } from './types';

const debug = debuglog(`${LIB_NAME}:consume`);

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
    private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    protected constructor(
        public readonly channel: Channel,
        public readonly queue: Queue,
    ) {
        super();
    }

    protected readonly channelCloseCallback = () => {
        debug(`native-close reconnect-in=%dms`, RECONNECT_TIMEOUT);
        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = undefined;
            if (this.consumer) {
                debug(`reconnecting`);
                try {
                    const { callback } = await this.consumer;
                    this.consumer = null;
                    await this.listen(callback);
                } catch (err) {
                    debug(`reconnect-error error=%s`, errToMessage(err));
                    (this as EventEmitter<BaseConsumerEventMap>).emit('reconnectError', err);
                    void this.close();
                }
            }
        }, RECONNECT_TIMEOUT);
    };

    abstract listen(callback: CallbackFn): Promise<this>;

    async close(timeout = 30000): Promise<void> {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
        this.channel.off('close', this.channelCloseCallback);

        if (this.consumer) {
            debug(`closing timeout=%dms`, timeout);
            const consumer = await this.consumer;
            const channel = await this.channel.native();

            debug('closing tag=%s in-flight=%d', consumer.amqpConsumer.consumerTag, this.currentlyProcessingMessages);
            await channel.cancel(consumer.amqpConsumer.consumerTag);

            const waitForAllMessagesPromise = new Promise<void>(resolve => {
                this.notifyMessageProcessed = () => {
                    if (this.currentlyProcessingMessages === 0)
                        resolve();
                };
                this.notifyMessageProcessed();
            });


            try {
                const abort = new AbortController();
                await Promise.race([
                    waitForAllMessagesPromise.then(() => abort.abort()),
                    sleepPromise(timeout).then(() => {
                        if (!abort.signal.aborted)
                            throw new Error('Consumer close timeout');
                    }),
                ]);
            } finally {
                debug('closed');
                (this as EventEmitter<BaseConsumerEventMap>).emit('close');
                this.consumer = null;
                this.notifyMessageProcessed = undefined;
            }
        }
    }
}
