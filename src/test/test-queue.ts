import { Queue } from '../lib/queue';
import { TestConsumer } from './test-consumer';
import { TestProducer } from './test-producer';

export class TestQueue implements Queue {
    assert = vitest.fn().mockImplementation(() => Promise.resolve(this));

    name = vitest.fn().mockImplementation(() => Promise.resolve('test-queue'));

    bind = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createConsumer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducer = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));
}
