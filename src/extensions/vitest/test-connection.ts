import { EventEmitter } from 'events';
import type { Mock } from 'vitest';
import { Connection, ConnectionState } from '../../index';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';
import { TestBatchConsumer } from './test-batch-consumer';
import { TestExchange } from './test-exchange';
import { TestQueue } from './test-queue';

/**
 * Mock implementation of Connection using vitest mocks.
 *
 * The connection is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods return default
 * mock implementations returning undefined (or the same instance if the semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestQueue } from 'amqpx/vitest';
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
    close: Mock = vitest.fn().mockImplementation(() => Promise.resolve());

    connect: Mock = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel: Mock = vitest.fn().mockImplementation(() => new TestChannel(this));

    createExchange: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestExchange(),
    ));

    createQueue: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestQueue(),
    ));

    checkQueue: Mock = vitest.fn().mockImplementation((queue: string) => Promise.resolve({
        queue,
        messageCount: 0,
        consumerCount: 0,
    }));

    createConsumerForExchange: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForQueue: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createBatchConsumerForQueue: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createBatchConsumerForExchange: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createProducerForExchange: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForQueue: Mock = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    native: Mock = vitest.fn().mockResolvedValue(undefined);

    state: Mock = vitest.fn().mockReturnValue(ConnectionState.connected);

    /**
     * Simulates a server-side disconnect by emitting the `reconnecting` event.
     */
    simulateDisconnect(): void {
        this.emit('reconnecting');
    }
}
