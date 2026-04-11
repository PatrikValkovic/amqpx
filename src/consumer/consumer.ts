import { EventEmitter } from 'events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { ConsumerCallbackFn } from './types';

/**
 * Events emitted by a {@link Consumer}.
 */
export interface ConsumerEventMap {
    /**
     * Emitted when the message handler throws an error.
     */
    handlingFailed: [error: unknown];
}

/**
 * Represents service consuming messages from RabbitMQ.
 * The generic properties are `Message` (the message received from rabbit as a whole)
 * and `AdditionalProperties` (properties that can implementation of consumer add to the callback).
 */
export interface Consumer<Message, AdditionalProperties = Record<string, unknown>> extends EventEmitter<ConsumerEventMap> {
    close(timeout?: number): Promise<void>;

    listen(callback: ConsumerCallbackFn<Message, AdditionalProperties>): Promise<Consumer<Message, AdditionalProperties>>;

    getQueue(): Queue;

    getChannel(): Channel;
}
