import { Channel } from '../channel';
import { ProducerEvents, ProducerPublishOptions, RoutingKeyGenerator } from './types';

/**
 * Represents service sending messages into the RabbitMQ.
 * The generic properties are `Message` (the content published by the client), and `WholeMessage` that can contain
 * additional properties that can be added by the producer implementation.
 */
export interface Producer<T, WholeMessage = T> {
    /**
     * Publish message into the RabbitMQ.
     * @param message Message to publish. Producer implementation may add more properties into the message.
     * @param routingKey Either string of generator that accepts the message and returns its routing key.
     * @param options Publish options.
     */
    publish(message: T, routingKey?: RoutingKeyGenerator<T>, options?: ProducerPublishOptions): Promise<WholeMessage>;

    on(eventName: keyof typeof ProducerEvents, callback: (msg: WholeMessage, buffer: Buffer) => void): Producer<T, WholeMessage>;

    getChannel(): Channel;
}
