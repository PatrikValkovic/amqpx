import { EventEmitter } from 'events';
import { Producer } from '../../index';
import { TestChannel } from './test-channel';

/**
 * Test implementation of Producer using jest mocks.
 *
 * `publish` method instantly returns message from parameter and wrap it
 * into Promise. `getChannel` method returns new instance of TestChannel.
 *
 * @example
 * ```ts
 * import { TestProducer } from 'amqp-oop/jest';
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
    publish = jest.fn().mockImplementation(msg => Promise.resolve(msg));

    getChannel = jest.fn().mockImplementation(() => new TestChannel());
}
