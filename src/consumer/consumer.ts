import { Queue } from '../queue/queue';
import { Channel } from '../channel';
import { ConsumerCallbackFn } from './types';

/**
 * Represents service consuming messages from RabbitMQ.
 * The generic properties are `Message` (the message received from rabbit as a whole)
 * and `AdditionalProperties` (properties that can implementation of consumer add to the callback).
 */
export interface Consumer<Message, AdditionalProperties = Record<string, unknown>> {
    close(timeout?: number): Promise<void>;

    listen(callback: ConsumerCallbackFn<Message, AdditionalProperties>): Promise<Consumer<Message, AdditionalProperties>>;

    on(eventName: 'handlingFailed', callback: (error: unknown) => void): Consumer<Message, AdditionalProperties>;

    getQueue(): Queue;

    getChannel(): Channel;
}
