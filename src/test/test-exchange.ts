import { Exchange } from '../exchange';
import { TestConsumer, TestProducer } from '.';

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
