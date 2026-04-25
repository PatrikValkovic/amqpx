import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { ConnectionImplementation } from './connection-implementation';
import { ConnectionState } from './connection';
import { ChannelImplementation } from '../channel/channel-implementation';
import { ConsumerImplementation } from '../consumer';
import { BatchConsumerImplementation } from '../consumer';
import { ProducerImplementation } from '../producer/producer-implementation';
import { TestQueue, TestExchange } from '../extensions/vitest';

vi.mock('amqplib');

describe('ConnectionImplementation', () => {
    let mockNativeConnection: EventEmitter & {
        close: ReturnType<typeof vi.fn>;
        createChannel: ReturnType<typeof vi.fn>;
    };
    let connection: ConnectionImplementation;

    beforeEach(() => {
        vitest.useRealTimers();
        vi.clearAllMocks();
        mockNativeConnection = Object.assign(new EventEmitter(), {
            close: vi.fn().mockResolvedValue(undefined),
            createChannel: vi.fn().mockResolvedValue({}),
        });
        vi.mocked(amqp.connect).mockResolvedValue(mockNativeConnection as any);
        connection = new ConnectionImplementation({ hostname: 'localhost' });
    });

    afterEach(async () => {
        await connection.close().catch(() => undefined);
    });

    describe('connect', () => {
        it('should call amqp.connect with the provided options', async () => {
            await connection.connect();

            expect(amqp.connect).toHaveBeenCalledTimes(1);
            expect(amqp.connect).toHaveBeenCalledWith({ hostname: 'localhost' }, undefined);
        });

        it('should return the ConnectionImplementation itself for chaining', async () => {
            const result = await connection.connect();

            expect(result).toBe(connection);
        });

        it('should only call amqp.connect once when connect() is called concurrently', async () => {
            await Promise.all([connection.connect(), connection.connect(), connection.connect()]);

            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });

        it('should transition state from preconnect to connected', async () => {
            expect(connection.state()).toBe(ConnectionState.preconnect);

            await connection.connect();

            expect(connection.state()).toBe(ConnectionState.connected);
        });

        it('should emit "connected" event after successful connect', async () => {
            const connectedListener = vi.fn();
            connection.on('connected', connectedListener);

            await connection.connect();

            expect(connectedListener).toHaveBeenCalledTimes(1);
            expect(connectedListener).toHaveBeenCalledWith(connection);
        });

        it('should emit "connectionError" for each failed connect attempt before eventual success', async () => {
            vitest.useFakeTimers();
            const errorListener = vi.fn();
            connection.on('connectionError', errorListener);

            vi.mocked(amqp.connect)
                .mockRejectedValueOnce(new Error('first failure'))
                .mockResolvedValue(mockNativeConnection as any);

            const connectPromise = connection.connect();
            await vitest.advanceTimersByTimeAsync(200);
            await connectPromise;

            expect(errorListener).toHaveBeenCalledTimes(1);
            expect(amqp.connect).toHaveBeenCalledTimes(2);
        });

        it('should emit "connectionRetryExhausted" and throw when all retries are exhausted', async () => {
            vitest.useFakeTimers();
            const exhaustedListener = vi.fn();
            const fastFailConnection = new ConnectionImplementation(
                { hostname: 'localhost' },
                { maxRetries: 2, reconnectionTimeoutMs: 0 },
            );
            fastFailConnection.on('connectionRetryExhausted', exhaustedListener);
            vi.mocked(amqp.connect).mockRejectedValue(new Error('always fails'));

            const connectPromise = fastFailConnection.connect();
            // Attach handler before advancing to avoid unhandled rejection window
            const rejectExpectation = expect(connectPromise).rejects.toThrow();
            await vitest.advanceTimersByTimeAsync(2000);
            await rejectExpectation;

            expect(exhaustedListener).toHaveBeenCalledTimes(1);
        });

        it('should automatically reconnect when native connection emits "close" and not closing', async () => {
            vitest.useFakeTimers();
            await connection.connect();

            const secondConnection = Object.assign(new EventEmitter(), {
                close: vi.fn().mockResolvedValue(undefined),
                createChannel: vi.fn().mockResolvedValue({}),
            });
            vi.mocked(amqp.connect).mockResolvedValueOnce(secondConnection as any);

            mockNativeConnection.emit('close');

            await vitest.advanceTimersByTimeAsync(1000);

            expect(amqp.connect).toHaveBeenCalledTimes(2);
        });

        it('should NOT reconnect when native "close" fires after explicit close()', async () => {
            await connection.connect();
            await connection.close();

            vi.mocked(amqp.connect).mockClear();
            mockNativeConnection.emit('close');

            // Allow any synchronous reconnect logic to execute
            await Promise.resolve();
            await Promise.resolve();

            expect(amqp.connect).not.toHaveBeenCalled();
        });

        it('should emit "reconnecting" when native connection drops unexpectedly', async () => {
            vitest.useFakeTimers();
            const reconnectingListener = vi.fn();
            connection.on('reconnecting', reconnectingListener);

            await connection.connect();
            mockNativeConnection.emit('close');

            await vitest.advanceTimersByTimeAsync(0);

            expect(reconnectingListener).toHaveBeenCalledTimes(1);
        });

        it('should emit "error" when native connection emits error', async () => {
            const errorListener = vi.fn();
            connection.on('error', errorListener);

            await connection.connect();
            mockNativeConnection.emit('error', new Error('native error'));

            expect(errorListener).toHaveBeenCalledTimes(1);
            expect(errorListener).toHaveBeenCalledWith(new Error('native error'));
        });
    });

    describe('close', () => {
        it('should close the native connection', async () => {
            await connection.connect();
            await connection.close();

            expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
        });

        it('should emit "close" event after closing', async () => {
            const closeListener = vi.fn();
            connection.on('close', closeListener);

            await connection.connect();
            await connection.close();

            expect(closeListener).toHaveBeenCalledTimes(1);
        });

        it('should transition state to closed', async () => {
            await connection.connect();
            await connection.close();

            expect(connection.state()).toBe(ConnectionState.closed);
        });

        it('should no-op (resolve immediately) when in preconnect state', async () => {
            await expect(connection.close()).resolves.toBeUndefined();
            expect(mockNativeConnection.close).not.toHaveBeenCalled();
        });

        it('should be idempotent — two concurrent close() calls share the same promise', async () => {
            await connection.connect();

            const close1 = connection.close();
            const close2 = connection.close();

            await Promise.all([close1, close2]);

            expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
        });

        it('should emit "close" even when native connection.close() throws', async () => {
            const closeListener = vi.fn();
            connection.on('close', closeListener);

            mockNativeConnection.close = vi.fn().mockRejectedValue(new Error('close failed'));

            await connection.connect();
            // close() propagates the native close error but still emits 'close' via finally
            await connection.close().catch(() => {});

            expect(closeListener).toHaveBeenCalledTimes(1);
        });
    });

    describe('state', () => {
        it('should return ConnectionState.preconnect initially', () => {
            expect(connection.state()).toBe(ConnectionState.preconnect);
        });

        it('should return ConnectionState.connected after successful connect', async () => {
            await connection.connect();

            expect(connection.state()).toBe(ConnectionState.connected);
        });

        it('should return ConnectionState.closed after close()', async () => {
            await connection.connect();
            await connection.close();

            expect(connection.state()).toBe(ConnectionState.closed);
        });
    });

    describe('native', () => {
        it('should resolve to the native amqplib connection after connect()', async () => {
            await connection.connect();

            const native = await connection.native();

            expect(native).toBe(mockNativeConnection);
        });

        it('should auto-trigger connect() when not yet connected', async () => {
            const native = await connection.native();

            expect(amqp.connect).toHaveBeenCalledTimes(1);
            expect(native).toBe(mockNativeConnection);
        });
    });

    describe('createChannel', () => {
        it('should return an instance of ChannelImplementation', () => {
            const channel = connection.createChannel();

            expect(channel).toBeInstanceOf(ChannelImplementation);
        });

        it('should return an unconfirmed channel by default', () => {
            const channel = connection.createChannel() as ChannelImplementation;

            // @ts-expect-error wrapper is private
            expect(channel.wrapper.isConfirmed).toBe(false);
        });

        it('should return a confirmed channel when isConfirmed=true', () => {
            const channel = connection.createChannel(true) as ChannelImplementation;

            // @ts-expect-error wrapper is private
            expect(channel.wrapper.isConfirmed).toBe(true);
        });

        it('should set the channel connection to this ConnectionImplementation', () => {
            const channel = connection.createChannel();

            expect(channel.connection).toBe(connection);
        });
    });

    describe('factory methods', () => {
        // ConnectionImplementation factory methods delegate to createChannel().createXxx(queue/exchange, options).
        // Since TestQueue/TestExchange return their own mocks (TestConsumer, TestProducer, etc.),
        // we verify delegation by checking that the queue/exchange mock was called correctly.

        it('createConsumerForQueue calls queue.createConsumer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createConsumerForQueue(queue);

            expect(queue.createConsumer).toHaveBeenCalledTimes(1);
            expect(queue.createConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel: expect.any(ChannelImplementation) }),
            );
        });

        it('createBatchConsumerForQueue calls queue.createBatchConsumer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createBatchConsumerForQueue(queue);

            expect(queue.createBatchConsumer).toHaveBeenCalledTimes(1);
            expect(queue.createBatchConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel: expect.any(ChannelImplementation) }),
            );
        });

        it('createProducerForExchange calls exchange.createProducer with a ChannelImplementation', async () => {
            const exchange = new TestExchange();
            await connection.createProducerForExchange(exchange);

            expect(exchange.createProducer).toHaveBeenCalledTimes(1);
            expect(exchange.createProducer).toHaveBeenCalledWith(
                expect.objectContaining({ channel: expect.any(ChannelImplementation) }),
            );
        });

        it('createProducerForQueue calls queue.createProducer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createProducerForQueue(queue);

            expect(queue.createProducer).toHaveBeenCalledTimes(1);
            expect(queue.createProducer).toHaveBeenCalledWith(
                expect.objectContaining({ channel: expect.any(ChannelImplementation) }),
            );
        });

        it('createConsumerForExchange calls exchange.createConsumer with channel and pattern merged', async () => {
            const exchange = new TestExchange();
            await connection.createConsumerForExchange(exchange, { pattern: 'test.#' });

            expect(exchange.createConsumer).toHaveBeenCalledTimes(1);
            const [firstArg] = exchange.createConsumer.mock.calls[0]!;
            expect((firstArg as Record<string, unknown>)['pattern']).toBe('test.#');
            expect((firstArg as Record<string, unknown>)['channel']).toBeInstanceOf(ChannelImplementation);
        });

        it('createBatchConsumerForExchange calls exchange.createBatchConsumer with channel and pattern merged', async () => {
            const exchange = new TestExchange();
            await connection.createBatchConsumerForExchange(exchange, { pattern: 'test.#' });

            expect(exchange.createBatchConsumer).toHaveBeenCalledTimes(1);
            const [firstArg] = exchange.createBatchConsumer.mock.calls[0]!;
            expect((firstArg as Record<string, unknown>)['pattern']).toBe('test.#');
            expect((firstArg as Record<string, unknown>)['channel']).toBeInstanceOf(ChannelImplementation);
        });

        it('createProducerForExchange uses a confirmed channel when isConfirmed=true', async () => {
            const exchange = new TestExchange();
            await connection.createProducerForExchange(exchange, {}, true);

            const [calledWith] = exchange.createProducer.mock.calls[0]!;
            const channelArg = (calledWith as { channel: ChannelImplementation }).channel;
            // @ts-expect-error wrapper is private
            expect(channelArg.wrapper.isConfirmed).toBe(true);
        });
    });
});
