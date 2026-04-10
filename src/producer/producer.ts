import { EventEmitter } from 'events';
import { Channel } from '../channel';
import { ProducerPublishOptions, RoutingKeyGenerator } from './types';

/**
 * Events emitted by a {@link Producer}.
 */
export interface ProducerEventMap<WholeMessage> {
    /**
     * Emitted before a message is serialized and sent to RabbitMQ.
     */
    beforeSend: [message: WholeMessage, buffer: Buffer];
    /**
     * Emitted after a message has been successfully sent to RabbitMQ.
     */
    afterSend: [message: WholeMessage, buffer: Buffer];
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
    publish(message: T, routingKey?: RoutingKeyGenerator<T>, options?: ProducerPublishOptions): Promise<WholeMessage>;

    getChannel(): Channel;
}
