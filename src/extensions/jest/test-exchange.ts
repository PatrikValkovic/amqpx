import { Exchange } from '../../index';
import { TestConsumer, TestProducer } from '.';

/**
 * Mock implementation of Exchange using jest mocks.
 *
 * All `create*` methods return test classes from this package.
 * `assert` and `bind` methods are jest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestExchange } from 'amqp-oop/jest';
 *
 * const exchange = new TestExchange();
 * // or create from TestChannel or TestConnection
 *
 * expect(exchange.bind).toBeCalledTimes(0);
 * ```
 */
export class TestExchange implements Exchange {
    assert = jest.fn().mockImplementation(() => Promise.resolve(this));

    name = jest.fn().mockImplementation(() => Promise.resolve('test-exchange'));

    bind = jest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducer = jest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
