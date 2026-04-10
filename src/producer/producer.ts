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
     * Publish message into the RabbitMQ.
     * @param message Message to publish. Producer implementation may add more properties into the message.
     * @param routingKey Either string of generator that accepts the message and returns its routing key.
     * @param options Publish options.
     */
    close(): Promise<void>;

    publish(message: T, routingKey?: RoutingKeyGenerator<T>, options?: ProducerPublishOptions): Promise<WholeMessage>;

    getChannel(): Channel;
}
