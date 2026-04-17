import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { sleepPromise } from '../utils';
import { RECONNECT_TIMEOUT } from './consumer';
import { ConsumerWrapper } from './types';

export abstract class BaseConsumer<
    CallbackFn,
    EventMap extends Record<string | symbol, unknown[]>,
> extends EventEmitter<EventMap> {
    protected consumer: ConsumerWrapper<CallbackFn> | null = null;
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

    abstract listen(callback: CallbackFn): Promise<this>;

    async close(timeout = 30000): Promise<void> {
        if (this.consumer) {
            const channel = await this.channel.native();

            const { consumer } = this;
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
                this.consumer = null;
            }
        }
    }
}
