import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { ChannelImplementation } from '../channel';
import { TestExchange, TestQueue } from '../extensions/vitest';
import { TooManyRetriesError } from '../errors';
import { ConnectionImplementation, connect } from './connection-implementation';
import { ConnectionState } from './connection';

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
        // typing doesn't match
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        vi.mocked(amqp.connect).mockResolvedValue(mockNativeConnection as any);
        connection = new ConnectionImplementation({ hostname: 'localhost' });
    });

    afterEach(async () => {
        await connection.close();
    });

    describe('connect', () => {
        it('should call amqp.connect with the provided options', async () => {
            const result = await connection.connect();

            expect(amqp.connect).toHaveBeenCalledTimes(1);
            expect(amqp.connect).toHaveBeenCalledWith({ hostname: 'localhost' }, undefined);

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

            const err = new Error('first failure');
            vi.mocked(amqp.connect)
                .mockRejectedValueOnce(err)
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                .mockResolvedValue(mockNativeConnection as any);

            const connectPromise = connection.connect();
            await vitest.advanceTimersByTimeAsync(200);
            await connectPromise;

            expect(errorListener).toHaveBeenCalledTimes(1);
            expect(errorListener).toHaveBeenCalledWith(err);
            expect(amqp.connect).toHaveBeenCalledTimes(2);
        });

        it('should emit "connectionRetryExhausted" and throw when all retries are exhausted', async () => {
            vitest.useFakeTimers();
            const fastFailConnection = new ConnectionImplementation(
                { hostname: 'localhost' },
                { maxRetries: 2, reconnectionTimeoutMs: 0 },
            );

            const exhaustedListener = vi.fn();
            fastFailConnection.on('connectionRetryExhausted', exhaustedListener);

            vi.mocked(amqp.connect).mockRejectedValue(new Error('always fails'));

            const connectPromise = fastFailConnection.connect();
            // Attach handler before advancing to avoid unhandled rejection window
            const rejectExpectation = expect(connectPromise).rejects.toThrow(TooManyRetriesError);
            await vitest.advanceTimersByTimeAsync(2000);
            await rejectExpectation;

            expect(exhaustedListener).toHaveBeenCalledTimes(1);
        });

        it('should emit "error" when native connection emits error', async () => {
            const errorListener = vi.fn();
            connection.on('error', errorListener);

            await connection.connect();

            const err = new Error('native error');
            mockNativeConnection.emit('error', err);

            expect(errorListener).toHaveBeenCalledTimes(1);
            expect(errorListener).toHaveBeenCalledWith(err);
        });

        it('should abort connecting and resolve when close() is called during retry delay', async () => {
            vitest.useFakeTimers();

            const slowConnection = new ConnectionImplementation(
                { hostname: 'localhost' },
                { maxRetries: 5, reconnectionTimeoutMs: 100 },
            );

            vi.mocked(amqp.connect).mockRejectedValue(new Error('refused'));

            const connectPromise = slowConnection.connect();
            // Let the first amqp.connect fail and enter the 100ms retry delay
            await vitest.advanceTimersByTimeAsync(50);

            const closePromise = slowConnection.close();

            // Advance past the 100ms delay so the retry callback runs with closing state
            await vitest.advanceTimersByTimeAsync(200);

            await Promise.all([connectPromise, closePromise]);

            expect(slowConnection.state()).toBe(ConnectionState.closed);
            // Second attempt never reached amqp.connect — aborted by state check
            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });
    });

    describe('reconnect', () => {
        it('should automatically reconnect when native connection emits "close" and not closing', async () => {
            vitest.useFakeTimers();

            const connectHandler = vitest.fn();
            connection.on('connected', connectHandler);
            const reconnectingHandler = vitest.fn();
            connection.on('reconnecting', reconnectingHandler);

            await connection.connect();

            mockNativeConnection.emit('close');

            await vitest.advanceTimersByTimeAsync(1000);

            expect(amqp.connect).toHaveBeenCalledTimes(2);
            expect(reconnectingHandler).toHaveBeenCalledTimes(1);
            expect(connectHandler).toHaveBeenCalledTimes(2);
        });

        it('should NOT reconnect when native "close" fires after explicit close()', async () => {
            vitest.useFakeTimers();
            await connection.connect();
            expect(amqp.connect).toHaveBeenCalledTimes(1);

            await connection.close();

            mockNativeConnection.emit('close');

            // Allow any synchronous reconnect logic to execute
            await vitest.advanceTimersByTimeAsync(1000);

            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });
    });

    describe('close', () => {
        it('should close the native connection', async () => {
            await connection.connect();

            const closeListener = vi.fn();
            connection.on('close', closeListener);

            const closePromise = connection.close();
            expect(connection.state()).toEqual(ConnectionState.closing);
            await closePromise;

            expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
            expect(closeListener).toHaveBeenCalledTimes(1);
            expect(connection.state()).toBe(ConnectionState.closed);
        });

        it('should no-op (resolve immediately) when in preconnect state', async () => {
            await expect(connection.close()).resolves.toBeUndefined();
            expect(mockNativeConnection.close).not.toHaveBeenCalled();
        });

        it('should be idempotent — multiple concurrent close() calls share the same promise', async () => {
            await connection.connect();

            const close1 = connection.close();
            const close2 = connection.close();
            const close3 = connection.close();
            const close4 = connection.close();

            await Promise.all([close1, close2, close3, close4]);

            expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
        });

        it('should no-op when already closed', async () => {
            await connection.connect();
            await connection.close();

            await expect(connection.close()).resolves.toBeUndefined();
            expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
        });

        it('should emit "close" even when native connection.close() throws', async () => {
            const closeListener = vi.fn();
            connection.on('close', closeListener);

            const err = new Error('close failed');
            mockNativeConnection.close = vi.fn().mockRejectedValue(err);

            await connection.connect();
            // close() propagates the native close error but still emits 'close' via finally
            await expect(connection.close()).rejects.toThrow(err);

            expect(closeListener).toHaveBeenCalledTimes(1);
            expect(connection.state()).toEqual(ConnectionState.closed);
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

        it('should ignore error during reconnect and left connection in closed state', async () => {
            vitest.useFakeTimers();

            connection = new ConnectionImplementation(
                { hostname: 'localhost' },
                { maxRetries: 1, reconnectionTimeoutMs: 100 },
            );

            vi.mocked(amqp.connect)
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                .mockResolvedValueOnce(mockNativeConnection as any)
                .mockRejectedValue(new Error('connection-error'));

            await connection.connect();
            await vitest.advanceTimersByTimeAsync(10_000);

            mockNativeConnection.emit('close');
            expect(connection.state()).toEqual(ConnectionState.connecting);

            await vitest.advanceTimersByTimeAsync(10_000);
            expect(connection.state()).toEqual(ConnectionState.closed);
        });
    });

    describe('createChannel', () => {
        it('should return an instance of ChannelImplementation', () => {
            const channel = connection.createChannel();

            expect(channel).toBeInstanceOf(ChannelImplementation);
            // @ts-expect-error wrapper is private
            expect(channel.wrapper.isConfirmed).toBe(false);
        });

        it('should return a confirmed channel when isConfirmed=true', () => {
            const channel = connection.createChannel(true);

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
        // These are already tested, so no extra care needs to be done here

        it('createConsumerForQueue calls queue.createConsumer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createConsumerForQueue(queue);

            expect(queue.createConsumer).toHaveBeenCalledTimes(1);
        });

        it('createBatchConsumerForQueue calls queue.createBatchConsumer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createBatchConsumerForQueue(queue);

            expect(queue.createBatchConsumer).toHaveBeenCalledTimes(1);
        });

        it('createProducerForExchange calls exchange.createProducer with a ChannelImplementation', async () => {
            const exchange = new TestExchange();
            await connection.createProducerForExchange(exchange);

            expect(exchange.createProducer).toHaveBeenCalledTimes(1);
        });

        it('createProducerForQueue calls queue.createProducer with a ChannelImplementation', async () => {
            const queue = new TestQueue();
            await connection.createProducerForQueue(queue);

            expect(queue.createProducer).toHaveBeenCalledTimes(1);
        });

        it('createConsumerForExchange calls exchange.createConsumer with channel and pattern merged', async () => {
            const exchange = new TestExchange();
            await connection.createConsumerForExchange(exchange, { pattern: 'test.#' });

            expect(exchange.createConsumer).toHaveBeenCalledTimes(1);
            const [firstArg] = exchange.createConsumer.mock.calls[0]!;
            expect(firstArg.pattern).toBe('test.#');
            expect(firstArg.channel).toBeInstanceOf(ChannelImplementation);
        });

        it('createBatchConsumerForExchange calls exchange.createBatchConsumer with channel and pattern merged', async () => {
            const exchange = new TestExchange();
            await connection.createBatchConsumerForExchange(exchange, { pattern: 'test.#' });

            expect(exchange.createBatchConsumer).toHaveBeenCalledTimes(1);
            const [firstArg] = exchange.createBatchConsumer.mock.calls[0]!;
            expect(firstArg.pattern).toBe('test.#');
            expect(firstArg.channel).toBeInstanceOf(ChannelImplementation);
        });

        it('createProducerForExchange uses a confirmed channel when isConfirmed=true', async () => {
            const exchange = new TestExchange();
            await connection.createProducerForExchange(exchange, {}, true);

            const [calledWith] = exchange.createProducer.mock.calls[0]!;
            expect(calledWith.channel.wrapper.isConfirmed).toBe(true);
        });
    });
});

describe('connect', () => {
    let mockNativeConnection: EventEmitter & {
        close: ReturnType<typeof vi.fn>;
        createChannel: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockNativeConnection = Object.assign(new EventEmitter(), {
            close: vi.fn().mockResolvedValue(undefined),
            createChannel: vi.fn().mockResolvedValue({}),
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        vi.mocked(amqp.connect).mockResolvedValue(mockNativeConnection as any);
    });

    it('should call amqp.connect with provided options and return a connected Connection', async () => {
        const connection = await connect({ hostname: 'localhost' });

        expect(amqp.connect).toHaveBeenCalledWith({ hostname: 'localhost' }, undefined);
        expect(connection.state()).toBe(ConnectionState.connected);
        await connection.close();
    });

    it('should forward socketOptions to amqp.connect', async () => {
        const socketOptions = { timeout: 5000 };
        const connection = await connect({ hostname: 'localhost' }, undefined, socketOptions);

        expect(amqp.connect).toHaveBeenCalledWith({ hostname: 'localhost' }, socketOptions);
        await connection.close();
    });
});
