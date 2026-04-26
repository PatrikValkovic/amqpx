import { EventEmitter } from 'events';
import type * as amqp from 'amqplib';
import { Consumer, ConsumerCallbackFn } from '../../index';
import { TestQueue } from './test-queue';
import { TestChannel } from './test-channel';

/**
 * Test implementation of Consumer using jest mocks.
 *
 * `listen` method instantly returns current instance wrapped in Promise.
 * You can assert the parameters with which was this method called to assert
 * `listen` method was called.
 *
 * `queue` and `channel` properties are new instances of TestQueue and
 * TestChannel respectively. All other methods are jest mocks returning
 * void or the current instance, depending on the method semantic.
 *
 * @example
 * ```ts
 * import { TestConsumer } from 'amqpx/jest';
 *
 * const consumer = new TestConsumer();
 *
 * const channel = consumer.getChannel();
 *
 * expect(channel).toBeInstanceOf(TestChannel);
 *
 * const listener = () => { / * empty * / };
 * await consumer.listen(listener);
 *
 * expect(consumer.listen).toBeCalledWith(listener);
 * ```
 */
export class TestConsumer<T> extends EventEmitter implements Consumer<T> {

    constructor() {
        super();
        this.setMaxListeners(0);
    }

    close = jest.fn().mockImplementation(() => Promise.resolve());

    listen = jest.fn().mockImplementation(() => Promise.resolve(this));

    setPrefetch = jest.fn().mockImplementation(() => Promise.resolve());

    queue = new TestQueue();

    channel = new TestChannel();

    /**
     * Simulates a message arriving from RabbitMQ by invoking the most recently registered listener.
     * Throws if `listen` has not been called yet.
     * @param message - The parsed message payload to deliver.
     * @param options - Optional serialization override and raw amqplib message field overrides.
     */
    async deliverMessage(
        message: T,
        options: { serialize?(msg: T): Buffer } & Omit<Partial<amqp.ConsumeMessage>, 'content'> = {},
    ): Promise<void> {
        const { calls } = this.listen.mock;
        const lastCall = calls.length > 0 ? calls[calls.length - 1] : [];
        const callback = lastCall[0] as ConsumerCallbackFn<T> | undefined;
        if (!callback)
            throw new Error('No listener registered. Call listen() before deliverMessage().');

        const {
            serialize = (msg: T) => Buffer.from(JSON.stringify(msg)),
            ...rest
        } = options;

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
        await callback({ channel: this.channel, message, rabbitMessage: raw });
    }
}
