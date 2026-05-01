import { Exchange } from '../../index';
import { TestConsumer, TestBatchConsumer, TestProducer } from '.';

/**
 * Mock implementation of Exchange using jest mocks.
 *
 * All `create*` methods return test classes from this package.
 * `assert`, `bindQueue`, and `bindExchange` methods are jest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestExchange } from 'amqpx/jest';
 *
 * const exchange = new TestExchange();
 * // or create from TestChannel or TestConnection
 *
 * expect(exchange.bindQueue).toBeCalledTimes(0);
 * ```
 */
export class TestExchange implements Exchange {
    assert = jest.fn().mockImplementation(() => Promise.resolve(this));

    name = jest.fn().mockImplementation(() => Promise.resolve('test-exchange'));

    bindQueue = jest.fn().mockImplementation(() => Promise.resolve(this));

    bindExchange = jest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createBatchConsumer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createProducer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
