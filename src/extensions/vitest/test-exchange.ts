import { Exchange } from '../../index';
import { TestConsumer, TestProducer } from '.';

/**
 * Mock implementation of Exchange using vitest mocks.
 *
 * All `create*` methods return test classes from this package.
 * `assert` and `bind` methods are vitest mocks returning current instance.
 *
 * @example
 * ```ts
 * import { TestExchange } from 'amqp-oop/vitest';
 *
 * const exchange = new TestExchange();
 * // or create from TestChannel or TestConnection
 *
 * expect(exchange.bind).toBeCalledTimes(0);
 * ```
 */
export class TestExchange implements Exchange {
    assert = vitest.fn().mockImplementation(() => Promise.resolve(this));

    name = vitest.fn().mockImplementation(() => Promise.resolve('test-exchange'));

    bind = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
