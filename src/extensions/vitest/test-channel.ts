import { EventEmitter } from 'events';
import { Channel, Connection } from '../../index';
import { TestConsumer, TestBatchConsumer, TestExchange, TestProducer, TestQueue, TestConnection } from '.';
/**
 * Mock implementation of Channel using vitest mocks.
 *
 * The channel is instantly connected and all `create*` methods return
 * test classes from this package. Non-creation methods return default
 * mock implementations returning undefined (or the same instance if the semantic
 * of the method requires so).
 *
 * @example
 * ```ts
 * import { TestConnection, TestChannel } from 'amqpx/vitest';
 *
 * const connection = new TestConnection();
 *
 * const channel = connection.createChannel();
 * // or new TestChannel();
 *
 * const exchange = channel.createExchange();
 *
 * expect(exchange).toBeInstanceOf(TestExchange);
 * ```
 */
export class TestChannel extends EventEmitter implements Channel {

    constructor(public readonly connection: Connection = new TestConnection()) {
        super();
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

    createBatchConsumerForQueue = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
    ));

    createBatchConsumerForExchange = vitest.fn().mockImplementation(() => Promise.resolve(
        new TestBatchConsumer(),
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

    /**
     * Simulates a channel close by emitting the `close` event.
     */
    simulateClose(): void {
        this.emit('close');
    }

    /**
     * Simulates write-buffer backpressure. Subsequent `publish` calls will not resolve until {@link releaseDrain} is called.
     */
    simulateDrainBackpressure(): void {
        this._drainPending = true;
    }

    /**
     * Releases simulated backpressure, resolves all pending `publish` calls, and emits the `drain` event.
     */
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
        assertQueue: vitest.fn().mockImplementation((queue: string) => Promise.resolve({
            queue,
            messageCount: 0,
            consumerCount: 0,
        })),
        checkQueue: vitest.fn().mockImplementation((queue: string) => Promise.resolve({
            queue,
            messageCount: 0,
            consumerCount: 0,
        })),
        assertExchange: vitest.fn().mockImplementation((exchange: string) => Promise.resolve({
            exchange,
        })),
        checkExchange: vitest.fn().mockResolvedValue(undefined),
        bindQueue: vitest.fn(),
        bindExchange: vitest.fn(),
    };
}
