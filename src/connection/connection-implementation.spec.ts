import { EventEmitter } from 'events';
import * as amqp from 'amqplib';
import { ChannelImplementation } from '../channel';
import { ExchangeImplementation } from '../exchange';
import { QueueImplementation } from '../queue';
import { TestExchange, TestQueue } from '../extensions/vitest';
import { AssertionMode } from '../types';
import { TooManyRetriesError } from '../errors';
import { ConnectionImplementation, connect } from './connection-implementation';
import { ConnectionState } from './connection';

vi.mock('amqplib');

type NativeChannelMock = EventEmitter & Record<
    'close' | 'assertExchange' | 'checkExchange' | 'assertQueue' | 'checkQueue' | 'bindQueue' | 'bindExchange',
    ReturnType<typeof vi.fn>
>;

const createNativeChannel = (): NativeChannelMock => Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
    assertExchange: vi.fn().mockImplementation((exchange: string) => Promise.resolve({ exchange })),
    checkExchange: vi.fn().mockResolvedValue({}),
    assertQueue: vi.fn().mockImplementation((queue: string) => Promise.resolve({ queue, messageCount: 0, consumerCount: 0 })),
    checkQueue: vi.fn().mockImplementation((queue: string) => Promise.resolve({ queue, messageCount: 3, consumerCount: 1 })),
    bindQueue: vi.fn().mockResolvedValue({}),
    bindExchange: vi.fn().mockResolvedValue({}),
});

describe('ConnectionImplementation', () => {
    let mockNativeConnection: EventEmitter & {
        close: ReturnType<typeof vi.fn>;
        createChannel: ReturnType<typeof vi.fn>;
    };
    let nativeChannels: NativeChannelMock[];
    let connection: ConnectionImplementation;

    beforeEach(() => {
        vitest.useRealTimers();
        vi.clearAllMocks();
        nativeChannels = [];
        mockNativeConnection = Object.assign(new EventEmitter(), {
            close: vi.fn().mockResolvedValue(undefined),
            createChannel: vi.fn().mockImplementation(() => {
                const nativeChannel = createNativeChannel();
                nativeChannels.push(nativeChannel);
                return Promise.resolve(nativeChannel);
            }),
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

        describe('topology channel', () => {
            const openedChannel = () => {
                const [channel] = nativeChannels;
                if (!channel)
                    throw new Error('the topology channel was not opened');
                return channel;
            };

            it('should close the topology channel before the native connection when the channel was opened', async () => {
                await connection.createQueue('orders');
                const topologyChannel = openedChannel();

                await connection.close();

                expect(nativeChannels).toHaveLength(1);
                expect(topologyChannel.close).toHaveBeenCalledTimes(1);
                expect(topologyChannel.close.mock.invocationCallOrder[0])
                    .toBeLessThan(mockNativeConnection.close.mock.invocationCallOrder[0] ?? 0);
            });

            it('should not open any channel when closing a connection that never declared anything', async () => {
                await connection.connect();

                await connection.close();

                expect(mockNativeConnection.createChannel).not.toHaveBeenCalled();
                expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
            });

            it('should still close the native connection when closing the topology channel times out', async () => {
                await connection.createQueue('orders');
                openedChannel().close.mockImplementation(() => new Promise(() => { /* never settles */ }));
                const closeListener = vi.fn();
                connection.on('close', closeListener);

                await expect(connection.close(50)).resolves.toBeUndefined();

                expect(mockNativeConnection.close).toHaveBeenCalledTimes(1);
                expect(closeListener).toHaveBeenCalledTimes(1);
                expect(connection.state()).toBe(ConnectionState.closed);
            });

            it('should leave the native connection only the time the topology channel did not use when closing', async () => {
                await connection.createQueue('orders');
                openedChannel().close.mockImplementation(() => new Promise(resolve => setTimeout(resolve, 150)));
                mockNativeConnection.close.mockImplementation(() => new Promise(() => { /* never settles */ }));

                const start = performance.now();
                await expect(connection.close(300)).rejects.toThrow('Connection close timed out');
                const elapsed = performance.now() - start;

                // the budget is shared: ~300 ms in total, not 150 + 300
                expect(elapsed).toBeGreaterThanOrEqual(280);
                expect(elapsed).toBeLessThan(400);
            });
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

    describe('topology', () => {
        const topologyChannel = () => (connection as unknown as { topologyChannel: ChannelImplementation }).topologyChannel;

        describe('createExchange', () => {
            it('should assert the exchange and return an exchange handle when called', async () => {
                const exchange = await connection.createExchange('orders', 'topic', { durable: true });

                expect(exchange).toBeInstanceOf(ExchangeImplementation);
                expect(await exchange.name()).toBe('orders');
                expect(nativeChannels).toHaveLength(1);
                expect(nativeChannels[0]!.assertExchange).toHaveBeenCalledWith('orders', 'topic', expect.objectContaining({ durable: true }));
            });

            it('should only check the exchange when assertionMode is Check', async () => {
                await connection.createExchange('orders', 'topic', { assertionMode: AssertionMode.Check });

                expect(nativeChannels[0]!.checkExchange).toHaveBeenCalledWith('orders');
                expect(nativeChannels[0]!.assertExchange).not.toHaveBeenCalled();
            });

            it('should not touch the broker when assertionMode is Passive', async () => {
                await connection.createExchange('orders', 'topic', { assertionMode: AssertionMode.Passive });

                expect(nativeChannels[0]!.checkExchange).not.toHaveBeenCalled();
                expect(nativeChannels[0]!.assertExchange).not.toHaveBeenCalled();
            });
        });

        describe('createQueue', () => {
            it('should assert the queue and return a queue handle when called', async () => {
                const queue = await connection.createQueue('jobs', { durable: true });

                expect(queue).toBeInstanceOf(QueueImplementation);
                expect(await queue.name()).toBe('jobs');
                expect(nativeChannels).toHaveLength(1);
                expect(nativeChannels[0]!.assertQueue).toHaveBeenCalledWith('jobs', expect.objectContaining({ durable: true }));
            });

            it('should only check the queue when assertionMode is Check', async () => {
                await connection.createQueue('jobs', { assertionMode: AssertionMode.Check });

                expect(nativeChannels[0]!.checkQueue).toHaveBeenCalledWith('jobs');
                expect(nativeChannels[0]!.assertQueue).not.toHaveBeenCalled();
            });

            it('should not touch the broker when assertionMode is Passive', async () => {
                await connection.createQueue('jobs', { assertionMode: AssertionMode.Passive });

                expect(nativeChannels[0]!.checkQueue).not.toHaveBeenCalled();
                expect(nativeChannels[0]!.assertQueue).not.toHaveBeenCalled();
            });
        });

        describe('checkQueue', () => {
            it('should resolve the queue metadata when the queue exists', async () => {
                const reply = await connection.checkQueue('jobs');

                expect(reply).toEqual({ queue: 'jobs', messageCount: 3, consumerCount: 1 });
                expect(nativeChannels[0]!.checkQueue).toHaveBeenCalledWith('jobs');
            });

            it('should reject when the broker rejects the check', async () => {
                const error = new Error('NOT_FOUND');
                await connection.createQueue('first');
                nativeChannels[0]!.checkQueue.mockRejectedValueOnce(error);

                await expect(connection.checkQueue('missing')).rejects.toBe(error);
            });
        });

        describe('channel usage', () => {
            it('should share one native channel when exchanges and queues are declared and checked', async () => {
                await connection.createExchange('orders', 'topic');
                await connection.createQueue('jobs');
                await connection.checkQueue('jobs');

                expect(mockNativeConnection.createChannel).toHaveBeenCalledTimes(1);
                expect(nativeChannels[0]!.assertExchange).toHaveBeenCalledTimes(1);
                expect(nativeChannels[0]!.assertQueue).toHaveBeenCalledTimes(1);
                expect(nativeChannels[0]!.checkQueue).toHaveBeenCalledTimes(1);
            });

            it('should not open the native channel when nothing is declared', async () => {
                await connection.connect();

                expect(mockNativeConnection.createChannel).not.toHaveBeenCalled();
            });

            it('should not share the native channel with createChannel when both are used', async () => {
                await connection.createQueue('jobs');
                const channel = connection.createChannel();

                expect(channel).not.toBe(topologyChannel());
                expect(await channel.native()).not.toBe(await topologyChannel().native());
            });

            it('should give each producer and consumer its own channel when created from the connection, a queue or an exchange', async () => {
                const queue = await connection.createQueue('jobs');
                const exchange = await connection.createExchange('orders', 'topic');

                const producers = [
                    await queue.createProducer(),
                    await exchange.createProducer(),
                    await connection.createProducerForQueue(queue),
                    await connection.createProducerForExchange(exchange),
                ];
                const consumers = [
                    await queue.createConsumer(),
                    await queue.createBatchConsumer(),
                    await exchange.createConsumer({ pattern: '#' }),
                    await exchange.createBatchConsumer({ pattern: '#' }),
                    await connection.createConsumerForQueue(queue),
                    await connection.createBatchConsumerForQueue(queue),
                    await connection.createConsumerForExchange(exchange, { pattern: '#' }),
                    await connection.createBatchConsumerForExchange(exchange, { pattern: '#' }),
                ];
                const channels = [...producers, ...consumers].map(entity => entity.channel);

                expect(new Set(channels).size).toBe(channels.length);
                expect(channels).not.toContain(topologyChannel());
                const nativeTopology = await topologyChannel().native();
                expect(await producers[0]!.channel.native()).not.toBe(nativeTopology);
                expect(await consumers[0]!.channel.native()).not.toBe(nativeTopology);

                await Promise.all(producers.map(producer => producer.close()));
            });
        });

        describe('channel failures', () => {
            it('should not throw nor emit connection error when the native channel emits error', async () => {
                const connectionErrorListener = vi.fn();
                connection.on('error', connectionErrorListener);
                await connection.createQueue('jobs');

                expect(() => nativeChannels[0]!.emit('error', new Error('PRECONDITION_FAILED'))).not.toThrow();
                expect(connectionErrorListener).not.toHaveBeenCalled();
            });

            it('should open a new native channel on the next call when the previous one errored', async () => {
                await connection.createQueue('jobs');
                nativeChannels[0]!.emit('error', new Error('PRECONDITION_FAILED'));
                nativeChannels[0]!.emit('close');

                const reply = await connection.checkQueue('jobs');

                expect(reply.queue).toBe('jobs');
                expect(nativeChannels).toHaveLength(2);
                expect(nativeChannels[1]!.checkQueue).toHaveBeenCalledWith('jobs');
            });

            it('should re-assert declared entities on the new native channel when the previous one closed', async () => {
                const queue = await connection.createQueue('jobs');
                const exchange = await connection.createExchange('orders', 'topic');
                await queue.bindExchange(exchange, 'order.*');

                nativeChannels[0]!.emit('close');
                await queue.name();
                await exchange.name();

                expect(nativeChannels).toHaveLength(2);
                expect(nativeChannels[1]!.assertQueue).toHaveBeenCalledWith('jobs', expect.anything());
                expect(nativeChannels[1]!.assertExchange).toHaveBeenCalledWith('orders', 'topic', expect.anything());
                expect(nativeChannels[1]!.bindQueue).toHaveBeenCalledWith('jobs', 'orders', 'order.*', undefined);
            });
        });

        describe('listeners', () => {
            it('should not warn about too many listeners when many queues and exchanges are declared', async () => {
                const warningListener = vi.fn();
                process.on('warning', warningListener);
                try {
                    for (let i = 0; i < 25; i++) {
                        await connection.createQueue(`queue-${i}`);
                        await connection.createExchange(`exchange-${i}`, 'direct');
                    }
                    await new Promise(resolve => setImmediate(resolve));

                    expect(warningListener).not.toHaveBeenCalledWith(expect.objectContaining({ name: 'MaxListenersExceededWarning' }));
                } finally {
                    process.off('warning', warningListener);
                }
            });
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
