import { EventEmitter } from 'events';
import { Connection, ConnectionState } from '../../index';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';
import { TestBatchConsumer } from './test-batch-consumer';
import { TestExchange } from './test-exchange';
import { TestQueue } from './test-queue';

/**
 * Mock implementation of Connection using jest mocks.
 *
 * The connection is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods return default
 * mock implementations returning undefined (or the same instance if the semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestQueue } from 'amqpx/jest';
 *
 * const connection = new TestConnection();
 *
 * const queue = await connection.createQueue('orders');
 *
 * expect(queue).toBeInstanceOf(TestQueue);
 * expect(connection.createQueue).toHaveBeenCalledWith('orders');
 * ```
 */
export class TestConnection extends EventEmitter implements Connection {
    close = jest.fn().mockImplementation(() => Promise.resolve());

    connect = jest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel = jest.fn().mockImplementation(() => new TestChannel(this));

    createExchange = jest.fn().mockImplementation(() => Promise.resolve(
        new TestExchange(),
    ));

    createQueue = jest.fn().mockImplementation(() => Promise.resolve(
        new TestQueue(),
    ));

    checkQueue = jest.fn().mockImplementation((queue: string) => Promise.resolve({
        queue,
        messageCount: 0,
        consumerCount: 0,
    }));

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

    /**
     * Simulates a server-side disconnect by emitting the `reconnecting` event.
     */
    simulateDisconnect(): void {
        this.emit('reconnecting');
    }
}
