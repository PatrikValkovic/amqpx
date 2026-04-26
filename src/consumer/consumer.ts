import { EventEmitter } from 'events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { ConsumerCallbackFn } from './types';
import { BaseConsumerEventMap } from './base-consumer';

export const RECONNECT_TIMEOUT = 100;

/**
 * Events emitted by a {@link Consumer}.
 */
export type ConsumerEventMap = BaseConsumerEventMap & {
    /**
     * Emitted when the message handler throws an error.
     */
    handlingFailed: [error: unknown];
};

/**
 * Represents a service consuming messages from RabbitMQ.
 * The generic type `Message` is the type of the parsed message payload,
 * and `AdditionalProperties` are extra fields that a consumer implementation can add to the callback arguments.
 */
export interface Consumer<Message, AdditionalProperties = Record<string, unknown>> extends EventEmitter<ConsumerEventMap> {
    /**
     * Stops consuming new messages and waits for all in-flight message handlers to finish.
     * @param timeout - Maximum time in milliseconds to wait for in-flight messages to complete. Defaults to 30000.
     */
    close(timeout?: number): Promise<void>;

    /**
     * Registers the message handler and starts consuming from the queue.
     * @param callback - Handler invoked for each received message.
     */
    listen(callback: ConsumerCallbackFn<Message, AdditionalProperties>): Promise<Consumer<Message, AdditionalProperties>>;

    /**
     * The queue this consumer reads from.
     */
    get queue(): Queue;

    /**
     * The channel used by this consumer.
     */
    get channel(): Channel;
}
