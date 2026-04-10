import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { DEFAULT_RETRY_STRATEGY, RetryStrategy } from '../retry';
import { MaybePromise } from '../types';

export type RoutingKeyGenerator<T> = ((param: T) => MaybePromise<string>) | string;

export type ProducerOptions<T> = {
    /**
     * Serialize the message into a Buffer before sending to RabbitMQ.
     * Defaults to JSON serialization.
     */
    stringifyMessage?(message: T): MaybePromise<Buffer>;
    /**
     * Routing key to use when publishing messages.
     * Can be a static string or a function that derives the key from the message.
     * Defaults to an empty string.
     */
    routingKey?: RoutingKeyGenerator<T>;
    /**
     * Additional amqplib publish options passed directly to the underlying channel publish call.
     */
    options?: amqp.Options.Publish;
    /**
     * Channel to use for publishing. If `null`, the producer will use the channel
     * provided by the exchange it is attached to.
     */
    channel?: Channel | null;
    /**
     * For how long to wait for a drain event when amqplib has a full buffer.
     * Specified in milliseconds.
     */
    drainTimeout?: number;
    /**
     * During publishing, the channel may fail but the publisher won't receive info about it until later (e.g. by closing the channel).
     * This variable specifies a window in milliseconds. If the publisher receives an error from RabbitMQ, it considers
     * all messages sent within that window as not delivered and will try to deliver them again.
     * Note: this may result in more-than-once delivery. Set to `0` to disable this behavior entirely.
     * Defaults to 5000ms.
     */
    errorWindow?: number;
    /**
     * For confirmed publish only, how to retry publishing of the message if the message is rejected by the receiving queue.
     * Can be overridden per individual {@link publish} call via {@link ProducerPublishOptions.retryStrategy}.
     * By default, uses exponential backoff with 10 retries and an initial wait time of 100ms.
     */
    retryStrategy?: RetryStrategy;
};

export const DEFAULT_PRODUCER_OPTIONS = {
    stringifyMessage: (message: unknown) => Buffer.from(JSON.stringify(message)),
    routingKey: '',
    options: {},
    channel: null,
    drainTimeout: 30000,
    errorWindow: 5000,
    retryStrategy: DEFAULT_RETRY_STRATEGY,
} as const satisfies Required<ProducerOptions<unknown>>;

export const ProducerEvents = {
    /**
     * Emitted after a message has been serialized, before it is sent to RabbitMQ.
     */
    beforeSend: 'beforeSend',
    /**
     * Emitted after a message has been successfully sent to RabbitMQ.
     */
    afterSend: 'afterSend',
    /**
     * Emitted when an automatic republish attempt (triggered by a channel error) fails.
     */
    republishFailed: 'republishFailed',
} as const;

export type ProducerPublishOptions = amqp.Options.Publish & {
    /**
     * For confirmed publish only, how to retry publishing of the message if the message is rejected by the receiving queue.
     * Overrides the {@link ProducerOptions.retryStrategy} set on the producer for this individual publish call.
     */
    retryStrategy?: RetryStrategy;
};

export type InFlightEntry<T> = {
    message: T;
    routingKey?: RoutingKeyGenerator<T>;
    options?: ProducerPublishOptions;
    expiresAt: number;
};
