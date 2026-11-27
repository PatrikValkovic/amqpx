import { EventEmitter } from 'events';
import type { Mock } from 'vitest';
import { Connection, ConnectionState } from '../../index';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';
import { TestBatchConsumer } from './test-batch-consumer';

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
    close: Mock = vitest.fn().mockImplementation(() => Promise.resolve());

    connect: Mock = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel: Mock = vitest.fn().mockImplementation(() => new TestChannel(this));

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
