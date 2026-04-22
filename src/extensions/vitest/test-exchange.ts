import { Exchange } from '../../index';
import { TestConsumer, TestBatchConsumer, TestProducer } from '.';

/**
 * Mock implementation of Exchange using vitest mocks.
 *
 * All `create*` methods return test classes from this package.
 * `assert`, `bindQueue`, and `bindExchange` methods are vitest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestExchange } from 'amqpx/vitest';
 *
 * const exchange = new TestExchange();
 * // or create from TestChannel or TestConnection
 *
 * expect(exchange.bindQueue).toBeCalledTimes(0);
 * ```
 */
export class TestExchange implements Exchange {
    assert = vitest.fn().mockImplementation(() => Promise.resolve(this));

    name = vitest.fn().mockImplementation(() => Promise.resolve('test-exchange'));

    bindQueue = vitest.fn().mockImplementation(() => Promise.resolve(this));

    bindExchange = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createBatchConsumer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createProducer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
