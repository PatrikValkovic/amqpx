import type * as amqp from 'amqplib';

/**
 * Options of the `deliverMessage` and `deliverMessages` methods of the test consumers.
 * Everything you leave out is filled with a default.
 */
export type DeliverMessageOptions<T> = {
    /**
     * Turns the payload into the message body. Defaults to `JSON.stringify`.
     */
    serialize?(message: T): Buffer;
    /**
     * Overrides of the amqplib message fields (routing key, delivery tag, …).
     */
    fields?: Partial<amqp.ConsumeMessageFields>;
    /**
     * Overrides of the amqplib message properties (headers, content type, …).
     */
    properties?: Partial<amqp.MessageProperties>;
};
