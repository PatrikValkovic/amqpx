import { EventEmitter } from 'events';
import type * as amqp from 'amqplib';
import { BatchConsumer } from '../../index';
import { BatchConsumerCallbackFn } from '../../consumer';
import { TestQueue } from './test-queue';
import { TestChannel } from './test-channel';

/**
 * Test implementation of BatchConsumer using jest mocks.
 *
 * `listen` method instantly returns current instance wrapped in Promise.
 * You can assert the parameters with which was this method called to assert
 * `listen` method was called.
 *
 * `queue` and `channel` properties return new instances of TestQueue and
 * TestChannel respectively. `close` method is a jest mock returning void.
 *
 * Use `deliverMessages` to simulate a batch of messages arriving and invoke
 * the registered listener.
 *
 * @example
 * ```ts
 * import { TestBatchConsumer } from 'amqpx/jest';
 *
 * const consumer = new TestBatchConsumer<{ id: number }>();
 *
 * const handler = jest.fn();
 * await consumer.listen(handler);
 *
 * await consumer.deliverMessages([{ id: 1 }, { id: 2 }]);
 *
 * expect(handler).toHaveBeenCalledTimes(1);
 * ```
 */
export class TestBatchConsumer<T> extends EventEmitter implements BatchConsumer<T> {

    constructor() {
        super();
        this.setMaxListeners(0);
    }

    close = jest.fn().mockImplementation(() => Promise.resolve());

    listen = jest.fn().mockImplementation(() => Promise.resolve(this));

    queue = new TestQueue();

    channel = new TestChannel();

    /**
     * Simulates a batch of messages arriving from RabbitMQ by invoking the most recently registered listener.
     * Throws if `listen` has not been called yet.
     * @param messages - Array of parsed message payloads to deliver as a single batch.
     * @param options - Optional serialization override and raw amqplib message field overrides applied to each message.
     */
    async deliverMessages(
        messages: T[],
        options: { serialize?(msg: T): Buffer } & Omit<Partial<amqp.ConsumeMessage>, 'content'> = {},
    ): Promise<void> {
        const { calls } = this.listen.mock;
        const lastCall = calls.length > 0 ? calls[calls.length - 1] : [];
        const callback = lastCall[0] as BatchConsumerCallbackFn<T> | undefined;
        if (!callback)
            throw new Error('No listener registered. Call listen() before deliverMessages().');

        const {
            serialize = (msg: T) => Buffer.from(JSON.stringify(msg)),
            ...rest
        } = options;

        const parsedMessages = messages.map(message => {
            const raw: amqp.ConsumeMessage = {
                content: serialize(message),
                fields: {
                    deliveryTag: 1,
                    redelivered: false,
                    exchange: '',
                    routingKey: '',
                    consumerTag: '',
                    ...rest.fields,
                },
                properties: {
                    headers: {},
                    ...rest.properties,
                } as amqp.MessageProperties,
            };
            return { message, rabbitMessage: raw };
        });

        await callback({ channel: this.channel, messages: parsedMessages });
    }
}
