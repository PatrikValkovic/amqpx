import { Message } from 'amqplib';
import { Channel } from '../channel';
import { Consumer } from '../consumer';
import { TestQueue } from './test-queue';
import { TestChannel } from './test-channel';

export const createTestConsumerCallbackArgs = <T>() => ({
    message: null as unknown as T,
    rabbitMessage: null as unknown as Message,
    channel: null as unknown as Channel,
});

export class TestConsumer<T> implements Consumer<T> {
    close = vitest.fn().mockImplementation(() => Promise.resolve());

    listen = vitest.fn().mockImplementation(() => Promise.resolve(this));

    on = vitest.fn().mockImplementation(() => this);

    setPrefetch = vitest.fn().mockImplementation(() => Promise.resolve());

    getQueue = vitest.fn().mockImplementation(() => new TestQueue());

    getChannel = vitest.fn().mockImplementation(() => new TestChannel());
}
