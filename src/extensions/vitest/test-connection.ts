import { EventEmitter } from 'events';
import { Connection, ConnectionState } from '../../index';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';

/**
 * Mock implementation of Connection using vitest mocks.
 *
 * The connection is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods returns default
 * mock implementations returning undefined (or the same instance if semantic
 * of the method requires so.
 *
 * @example
 * ```ts
 * import { TestConnection } from 'amqpx/vitest';
 *
 * const connection = new TestConnection();
 *
 * const channel = connection.createChannel();
 *
 * expect(channel).toBeInstanceOf(TestChannel);
 * ```
 */
export class TestConnection extends EventEmitter implements Connection {
    close = vitest.fn().mockImplementation(() => Promise.resolve());

    connect = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel = vitest.fn().mockImplementation(() => new TestChannel());

    createConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    native = vitest.fn().mockResolvedValue(undefined);

    state = vitest.fn().mockReturnValue(ConnectionState.connected);
}
