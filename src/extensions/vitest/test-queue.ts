import { Queue } from '../../index';
import { TestConsumer } from './test-consumer';
import { TestProducer } from './test-producer';

/**
 * Mock implementation of Queue using vitest mocks.
 *
 * All `create*` methods return test instance of classes from this package.
 * `assert` and `bindExchange` methods are vitest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestQueue } from 'amqpx/vitest';
 *
 * const queue = new TestQueue();
 * // or create from TestChannel, TestConnection, or TestExchange
 *
 * expect(queue.bindExchange).toBeCalledTimes(0);
 * ```
 */
export class TestQueue implements Queue {
    assert = vitest.fn().mockImplementation(() => Promise.resolve(this));

    name = vitest.fn().mockImplementation(() => Promise.resolve('test-queue'));

    bindExchange = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
