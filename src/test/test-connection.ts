import { Connection, ConnectionState } from '../lib/connection';
import { TestProducer } from './test-producer';
import { TestChannel } from './test-channel';
import { TestConsumer } from './test-consumer';

export class TestConnection implements Connection {
    close = vitest.fn().mockImplementation(() => Promise.resolve());

    connect = vitest.fn().mockImplementation(() => Promise.resolve(this));

    createChannel = vitest.fn().mockImplementation(() => new TestChannel());

    createConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createConsumerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    createProducerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    createProducerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestProducer(),
    ));

    native = vitest.fn().mockResolvedValue(undefined);

    on = vitest.fn().mockReturnValue(this);

    state = vitest.fn().mockReturnValue(ConnectionState.connected);
}
