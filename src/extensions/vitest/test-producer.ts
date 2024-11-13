import { Producer } from '../../index';
import { TestChannel } from './test-channel';

/**
 * Test implementation of Producer using vitest mocks.
 *
 * `publish` method instantly returns message from parameter and wrap it
 * into Promise. `getChannel` method returns new instance of TestChannel.
 *
 * @example
 * ```ts
 * import { TestProducer } from 'amqp-oop/vitest';
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
export class TestProducer<T> implements Producer<T> {
    on = vitest.fn().mockImplementation(() => this);

    publish = vitest.fn().mockImplementation(msg => Promise.resolve(msg));

    getChannel = vitest.fn().mockImplementation(() => new TestChannel());
}
