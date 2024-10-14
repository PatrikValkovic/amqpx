import { Channel } from '../lib/channel';
import { TestExchange, TestProducer, TestQueue } from '.';

export class TestChannel implements Channel {

    connect = vitest.fn().mockImplementation(() => Promise.resolve(this));

    close = vitest.fn().mockImplementation(() => Promise.resolve());

    createExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestExchange(),
    ));

    createQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestQueue(),
    ));

    createProducerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createConsumerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    native = vitest.fn().mockImplementation(() => Promise.resolve(undefined));

    publish = vitest.fn().mockImplementation(() => Promise.resolve());

    on = vitest.fn().mockImplementation(() => undefined);

    off = vitest.fn().mockImplementation(() => undefined);

    once = vitest.fn().mockImplementation(() => undefined);

    internalEmitter = vitest.fn().mockImplementation(
        () => {
            throw new Error('Not implemented for tests');
        },
    );

    checkQueue = vitest.fn().mockImplementation(() => Promise.resolve({
        queue: 'test-queue',
        messageCount: 0,
        consumerCount: 0,
    }));
}
