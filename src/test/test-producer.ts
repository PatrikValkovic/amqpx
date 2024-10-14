import { Producer } from '../lib/producer';
import { TestChannel } from './test-channel';

export class TestProducer<T> implements Producer<T> {
    on = vitest.fn().mockImplementation(() => this);

    publish = vitest.fn().mockImplementation(msg => Promise.resolve(msg));

    getChannel = vitest.fn().mockImplementation(() => new TestChannel());
}
