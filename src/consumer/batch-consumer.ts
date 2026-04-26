import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { BatchConsumerCallbackFn } from './types';
import { BaseConsumerEventMap } from './base-consumer';

/**
 * Events emitted by a {@link BatchConsumer}.
 */
export type BatchConsumerEventMap = BaseConsumerEventMap & {
    /**
     * Emitted when an internal processing error occurs.
     */
    error: [error: Error];
    /**
     * Emitted when the message handler or message parsing throws an error.
     */
    handlingFailed: [error: unknown];
};

/**
 * Represents a service consuming messages from RabbitMQ in batches.
 * The generic type `Message` is the type of each parsed message payload,
 * and `AdditionalProperties` are extra fields that a consumer implementation can add to each message entry in the callback.
 */
export interface BatchConsumer<
    Message,
    AdditionalProperties = Record<string, unknown>,
> extends EventEmitter<BatchConsumerEventMap> {
    /**
     * Stops consuming new messages and waits for all in-flight batches to finish processing.
     * @param timeout - Maximum time in milliseconds to wait for in-flight messages to complete. Defaults to 30000.
     */
    close(timeout?: number): Promise<void>;

    /**
     * Registers the batch handler and starts consuming from the queue.
     * @param callback - Handler invoked with a batch of received messages.
     */
    listen(callback: BatchConsumerCallbackFn<Message, AdditionalProperties>): Promise<BatchConsumer<Message, AdditionalProperties>>;

    /**
     * The queue this consumer reads from.
     */
    get queue(): Queue;

    /**
     * The channel used by this consumer.
     */
    get channel(): Channel;
}
