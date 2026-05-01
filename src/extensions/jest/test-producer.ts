import { EventEmitter } from 'events';
import { Producer } from '../../index';
import { TestChannel } from './test-channel';

/**
 * Test implementation of Producer using jest mocks.
 *
 * `publish` method instantly returns the message argument wrapped in a Promise.
 * `channel` property is a new TestChannel instance.
 *
 * @example
 * ```ts
 * import { TestProducer } from 'amqpx/jest';
 *
 * const producer = new TestProducer();
 *
 * const channel = producer.getChannel();
 *
 * expect(channel).toBeInstanceOf(TestChannel);
 *
 * const msg = await producer.publish('test');
 *
 * expect(msg).toBe('test');
 * ```
 */
export class TestProducer<T> extends EventEmitter implements Producer<T> {
    close = jest.fn().mockResolvedValue(undefined);

    publish = jest.fn().mockImplementation(msg => Promise.resolve(msg));

    channel = new TestChannel();

    /**
     * Returns the first argument (the message payload) from every `publish` call recorded so far.
     */
    getPublishedMessages(): T[] {
        return this.publish.mock.calls.map(([msg]) => msg as T);
    }
}
