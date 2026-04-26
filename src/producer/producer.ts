import { EventEmitter } from 'events';
import { Channel } from '../channel';
import { ProducerPublishOptions, RoutingKeyGenerator } from './types';

/**
 * Events emitted by a {@link Producer}.
 */
export interface ProducerEventMap<WholeMessage> {
    /**
     * Emitted after a message has been serialized, before it is sent to RabbitMQ.
     */
    beforeSend: [message: WholeMessage, buffer: Buffer];
    /**
     * Emitted after a message has been successfully sent to RabbitMQ.
     */
    afterSend: [message: WholeMessage, buffer: Buffer];
    /**
     * Emitted when an automatic republish attempt (triggered by a channel error) fails.
     */
    republishFailed: [message: WholeMessage, error: unknown];
}

/**
 * Represents service sending messages into the RabbitMQ.
 * The generic properties are `Message` (the content published by the client), and `WholeMessage` that can contain
 * additional properties that can be added by the producer implementation.
 */
export interface Producer<T, WholeMessage = T> extends EventEmitter<ProducerEventMap<WholeMessage>> {

    /**
     * Stops accepting new publishes and waits for all pending publish operations to settle.
     */
    close(): Promise<void>;

    /**
     * Publishes a message to RabbitMQ.
     * @param message - Message to publish. The producer implementation may augment it with additional properties.
     * @param routingKey - Either a static routing key string, or a generator function that derives the key from the message.
     * @param options - Per-publish options such as a retry strategy override.
     */
    publish(message: T, routingKey?: RoutingKeyGenerator<T>, options?: ProducerPublishOptions): Promise<WholeMessage>;

    /**
     * The channel used by this producer for publishing.
     */
    get channel(): Channel;
}
