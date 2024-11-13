import { Queue } from '../../index';
import { TestConsumer } from './test-consumer';
import { TestProducer } from './test-producer';

/**
 * Mock implementation of Queue using jest mocks.
 *
 * All `create*` methods return test instance of classes from this package.
 * `assert` and `bind` methods are jest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestQueue } from 'amqp-oop/jest';
 *
 * const queue = new TestQueue();
 * // or create from TestChannel, TestConnection, or TestExchange
 *
 * expect(queue.bind).toBeCalledTimes(0);
 * ```
 */
export class TestQueue implements Queue {
    assert = jest.fn().mockImplementation(() => Promise.resolve(this));

    name = jest.fn().mockImplementation(() => Promise.resolve('test-queue'));

    bind = jest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
