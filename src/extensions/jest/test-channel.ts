import { Channel } from '../../index';
import { TestConsumer, TestExchange, TestProducer, TestQueue } from '.';
/**
 * Mock implementation of Channel using jest mocks.
 *
 * The channel is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods returns default
 * mock implementations returning undefined (or the same instance if semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestChannel } from 'amqp-oop/jest';
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

    consumeResponse = {
        consumerTag: crypto.randomUUID(),
    };

    nativeChannel = {
        cancel: jest.fn().mockImplementation(() => Promise.resolve()),
        prefetch: jest.fn().mockImplementation(() => Promise.resolve()),
        consume: jest.fn().mockImplementation(() => Promise.resolve(this.consumeResponse)),
        ack: jest.fn(),
        nack: jest.fn(),
    };

    connect = jest.fn().mockImplementation(() => Promise.resolve(this));

    close = jest.fn().mockImplementation(() => Promise.resolve());

    createExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestExchange(),
    ));

    createQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestQueue(),
    ));

    createProducerForQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createConsumerForQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    native = jest.fn().mockImplementation(() => Promise.resolve(this.nativeChannel));

    publish = jest.fn().mockImplementation(() => Promise.resolve());

    on = jest.fn().mockImplementation(() => undefined);

    off = jest.fn().mockImplementation(() => undefined);

    once = jest.fn().mockImplementation(() => undefined);

    internalEmitter = jest.fn().mockImplementation(
        () => {
            throw new Error('Not implemented for tests');
        },
    );

    checkQueue = jest.fn().mockImplementation((queue: string) => Promise.resolve({
        queue,
        messageCount: 0,
        consumerCount: 0,
    }));
}
