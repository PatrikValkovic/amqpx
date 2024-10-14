import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { RetryStrategy } from '../retry';
import { MaybePromise } from '../types';

export type RoutingKeyGenerator<T> = ((param: T) => MaybePromise<string>) | string;

export type ProducerOptions<T> = {
    stringifyMessage?(message: T): MaybePromise<Buffer>;
    routingKey?: RoutingKeyGenerator<T>;
    options?: amqp.Options.Publish;
    channel?: Channel | null;
    /**
     * For how long to wait for drain event when amqplib has full buffer.
     * Specified in milliseconds.
     */
    drainTimeout?: number;
    /**
     * During publishing, channel may fail but the publisher won't receive info about it until later.
     * This variable specifies window in milliseconds. If publisher receives error from rabbit, it considers
     * all messages send within that window as not delivered and will try to deliver it again.
     */
    errorWindow?: number;
};

export const DEFAULT_PRODUCER_OPTIONS = {
    stringifyMessage: (message: unknown) => Buffer.from(JSON.stringify(message)),
    routingKey: '',
    options: {},
    channel: null,
    drainTimeout: 30000,
    errorWindow: 5000,
} as const satisfies Required<ProducerOptions<unknown>>;

export const ProducerEvents = {
    beforeSend: 'beforeSend',
    afterSend: 'afterSend',
} as const;

export type ProducerPublishOptions = amqp.Options.Publish & {
    /**
     * For confirmed publish only, how to retry publishing of the message if the mesages is rejected by the receiving queue.
     * By default, will use exponential backoff with 10 retries and initial wait time of 100ms.
     */
    retryStrategy?: RetryStrategy;
};
