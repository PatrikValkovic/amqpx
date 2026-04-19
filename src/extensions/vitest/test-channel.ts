import { EventEmitter } from 'events';
import { Channel } from '../../index';
import { TestConsumer, TestExchange, TestProducer, TestQueue } from '.';
/**
 * Mock implementation of Channel using vitest mocks.
 *
 * The channel is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods returns default
 * mock implementations returning undefined (or the same instance if semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestChannel } from 'amqpx/vitest';
 *
 * const connection = new TestConnection();
 *
 * const channel = connection.createChannel();
 * // or new TestConnection();
 *
 * const exchange = channel.createExchange();
 *
 * expect(exchange).toBeInstanceOf(TestExchange);
 * ```
 */
export class TestChannel extends EventEmitter implements Channel {

    constructor() {
        super();
        this.setMaxListeners(0);
    }

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
        new TestConsumer(),
    ));

    createConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestConsumer(),
    ));

    native = vitest.fn().mockImplementation(() => Promise.resolve(this.nativeChannel));

    private _drainPending = false;
    private _drainResolvers: Array<() => void> = [];

    publish = vitest.fn().mockImplementation((): Promise<boolean> => {
        if (this._drainPending) {
            return new Promise<boolean>(resolve => {
                this._drainResolvers.push(() => resolve(false));
            });
        }
        return Promise.resolve(false);
    });

    checkQueue = vitest.fn().mockImplementation((queue: string) => Promise.resolve({
        queue,
        messageCount: 0,
        consumerCount: 0,
    }));

    simulateClose(): void {
        this.emit('close');
    }

    simulateDrainBackpressure(): void {
        this._drainPending = true;
    }

    releaseDrain(): void {
        this._drainPending = false;
        const resolvers = this._drainResolvers.splice(0);
        this.emit('drain');
        for (const resolve of resolvers)
            resolve();
    }

    consumeResponse = {
        consumerTag: crypto.randomUUID(),
    };

    nativeChannel = {
        cancel: vitest.fn().mockImplementation(() => Promise.resolve()),
        prefetch: vitest.fn().mockImplementation(() => Promise.resolve()),
        consume: vitest.fn().mockImplementation(() => Promise.resolve(this.consumeResponse)),
        ack: vitest.fn(),
        nack: vitest.fn(),
    };
}
