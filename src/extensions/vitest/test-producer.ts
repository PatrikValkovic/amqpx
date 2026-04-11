import { EventEmitter } from 'events';
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
 * import { TestProducer } from 'amqpx/vitest';
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
    constructor() {
        super();
        this.setMaxListeners(0);
    }

    close = vitest.fn().mockResolvedValue(undefined);

    publish = vitest.fn().mockImplementation(msg => Promise.resolve(msg));

    getChannel = vitest.fn().mockImplementation(() => new TestChannel());
}
