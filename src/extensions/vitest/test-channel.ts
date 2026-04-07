import { Channel } from '../../index';
import { TestConsumer, TestExchange, TestProducer, TestQueue } from '.';
/**
 * Mock implementation of Channel using vitest mocks.
 *
 * The channel is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods returns default
 * mock implementations returning undefined (or the same instance if semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestChannel } from 'amqp-oop/vitest';
 *
 * const connection = new TestConnection();
 *
 * const channel = connection.createChannel();
 * // or new TestConnection();
 *
 * const exchange = channel.createExchange();
 *
 * expect(exchange).toBeInstanceOf(TestExchange);
 * ```
 */
export class TestChannel implements Channel {

    connect = vitest.fn().mockImplementation(() => Promise.resolve(this));

    close = vitest.fn().mockImplementation(() => Promise.resolve());

    createExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestExchange(),
    ));

    createQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestQueue(),
    ));

    createProducerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createConsumerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    native = vitest.fn().mockImplementation(() => Promise.resolve(undefined));

    publish = vitest.fn().mockImplementation(() => Promise.resolve());

    on = vitest.fn().mockImplementation(() => undefined);

    off = vitest.fn().mockImplementation(() => undefined);

    once = vitest.fn().mockImplementation(() => undefined);

    internalEmitter = vitest.fn().mockImplementation(
        () => {
            throw new Error('Not implemented for tests');
        },
    );

    checkQueue = vitest.fn().mockImplementation((queue: string) => Promise.resolve({
        queue,
        messageCount: 0,
        consumerCount: 0,
    }));
}
