import { EventEmitter } from 'events';
import { Consumer } from '../../index';
import { TestQueue } from './test-queue';
import { TestChannel } from './test-channel';

/**
 * Test implementation of Consumer using vitest mocks.
 *
 * `listen` method instantly returns current instance wrapped in Promise.
 * You can assert the parameters with which was this method called to assert
 * `listen` method was called.
 *
 * `getQueue` and `getChannel` methods return new instances of TestQueue and
 * TestChannel respectively. All other methods are vitest mocks returning
 * void or current instance, depending on the method semantic.
 *
 * @example
 * ```ts
 * import { TestConsumer } from 'amqp-oop/vitest';
 *
 * const consumer = new TestConsumer();
 *
 * const channel = consumer.getChannel();
 *
 * expect(channel).toBeInstanceOf(TestChannel);
 *
 * const listener = () => { / * empty * / };
 * await consumer.listen(listener);
 *
 * expect(consumer.listen).toBeCalledWith(listener);
 * ```
 */
export class TestConsumer<T> extends EventEmitter implements Consumer<T> {

    constructor() {
        super();
        this.setMaxListeners(0);
    }

    close = vitest.fn().mockImplementation(() => Promise.resolve());

    listen = vitest.fn().mockImplementation(() => Promise.resolve(this));

    setPrefetch = vitest.fn().mockImplementation(() => Promise.resolve());

    getQueue = vitest.fn().mockImplementation(() => new TestQueue());

    getChannel = vitest.fn().mockImplementation(() => new TestChannel());
}
