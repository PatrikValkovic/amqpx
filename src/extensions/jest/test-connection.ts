import { EventEmitter } from 'events';
import { Connection, ConnectionState } from '../../index';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';
import { TestBatchConsumer } from './test-batch-consumer';

/**
 * Mock implementation of Connection using jest mocks.
 *
 * The connection is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods returns default
 * mock implementations returning undefined (or the same instance if semantic
 * of the method requires so.
 *
 * @example
 * ```ts
 * import { TestConnection } from 'amqpx/jest';
 *
 * const connection = new TestConnection();
 *
 * const channel = connection.createChannel();
 *
 * expect(channel).toBeInstanceOf(TestChannel);
 * ```
 */
export class TestConnection extends EventEmitter implements Connection {
    close = jest.fn().mockImplementation(() => Promise.resolve());

    connect = jest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel = jest.fn().mockImplementation(() => new TestChannel());

    createConsumerForExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createBatchConsumerForQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createBatchConsumerForExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createProducerForExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    native = jest.fn().mockResolvedValue(undefined);

    state = jest.fn().mockReturnValue(ConnectionState.connected);

    simulateDisconnect(): void {
        this.emit('reconnecting');
    }
}
