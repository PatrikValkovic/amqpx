import * as amqp from 'amqplib';
import { TestConnection, TestQueue, TestExchange } from '../extensions/vitest';
import { QueueImplementation } from '../queue';
import { ExchangeImplementation } from '../exchange';
import { DrainError } from '../errors';
import { ChannelImplementation } from './channel-implementation';

type EventHandlers = Record<string, (...args: unknown[]) => void>;

type MockAmqpChannel = {
    on: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
    publish: ReturnType<typeof vi.fn>;
    waitForConfirms: ReturnType<typeof vi.fn>;
    assertQueue: ReturnType<typeof vi.fn>;
    assertExchange: ReturnType<typeof vi.fn>;
    checkQueue: ReturnType<typeof vi.fn>;
};

type MockAmqpConnection = {
    createChannel: ReturnType<typeof vi.fn>;
    createConfirmChannel: ReturnType<typeof vi.fn>;
};

describe('ChannelImplementation', () => {
    let connection: TestConnection;
    let channel: ChannelImplementation;
    let eventHandlers: EventHandlers;
    let mockAmqpChannel: MockAmqpChannel;
    let mockAmqpConnection: MockAmqpConnection;


    beforeEach(async () => {
        vitest.useRealTimers();
        connection = new TestConnection();
        channel = new ChannelImplementation(connection, false);

        eventHandlers = {};
        mockAmqpChannel = {
            on: vi.fn().mockImplementation((evt: string, h: (...args: unknown[]) => void) => {
                eventHandlers[evt] = h;
            }),
            close: vi.fn().mockResolvedValue(undefined),
            publish: vi.fn().mockReturnValue(true),
            waitForConfirms: vi.fn().mockResolvedValue(undefined),
            assertQueue: vi.fn().mockResolvedValue({ queue: 'test-queue', messageCount: 0, consumerCount: 0 }),
            assertExchange: vi.fn().mockResolvedValue({ exchange: 'test-exchange' }),
            checkQueue: vi.fn().mockResolvedValue({ queue: 'test-queue', messageCount: 0, consumerCount: 0 }),
        };
        mockAmqpConnection = {
            createChannel: vi.fn().mockResolvedValue(mockAmqpChannel),
            createConfirmChannel: vi.fn().mockResolvedValue(mockAmqpChannel),
        };
        vi.mocked(connection.native).mockResolvedValue(mockAmqpConnection as unknown as amqp.ChannelModel);
    });

    describe('connect', () => {
        it('should call connection.native() then createChannel() on the result', async () => {
            const result = await channel.connect();

            expect(result).toBe(channel);
            expect(connection.native).toHaveBeenCalledTimes(1);
            expect(mockAmqpConnection.createChannel).toHaveBeenCalledTimes(1);
            expect(mockAmqpConnection.createConfirmChannel).not.toHaveBeenCalled();
        });

        it('should not call createChannel() a second time when connect() is called again', async () => {
            await channel.connect();
            await channel.connect();

            expect(mockAmqpConnection.createChannel).toHaveBeenCalledTimes(1);
            expect(connection.native).toHaveBeenCalledTimes(1);
        });

        it('should forward events from the amqp channel to the ChannelImplementation emitter', async () => {
            await channel.connect();

            const errorListener = vi.fn();
            channel.on('error', errorListener);
            const testError = new Error('amqp channel error');
            eventHandlers.error?.(testError);
            expect(errorListener).toHaveBeenCalledTimes(1);
            expect(errorListener).toHaveBeenCalledWith(testError);

            const closeListener = vi.fn();
            channel.on('close', closeListener);
            eventHandlers.close?.();
            expect(closeListener).toHaveBeenCalledTimes(1);

            const drainListener = vi.fn();
            channel.on('drain', drainListener);
            eventHandlers.drain?.();
            expect(drainListener).toHaveBeenCalledTimes(1);
        });

        it('should use createConfirmChannel() when isConfirmed is true', async () => {
            const confirmedChannel = new ChannelImplementation(connection, true);
            await confirmedChannel.connect();

            expect(mockAmqpConnection.createConfirmChannel).toHaveBeenCalledTimes(1);
            expect(mockAmqpConnection.createChannel).not.toHaveBeenCalled();
        });
    });

    describe('close', () => {
        it('should call amqp channel.close() when connected', async () => {
            await channel.connect();
            await channel.close();

            expect(mockAmqpChannel.close).toHaveBeenCalledTimes(1);
        });

        it('should no-op when not yet connected', async () => {
            await expect(channel.close()).resolves.toBeUndefined();
            expect(mockAmqpChannel.close).not.toHaveBeenCalled();
        });

        it('should be idempotent — two concurrent close() calls share the same promise', async () => {
            await channel.connect();

            const close1 = channel.close();
            const close2 = channel.close();
            const close3 = channel.close();
            const close4 = channel.close();

            await Promise.all([close1, close2, close3, close4]);

            expect(mockAmqpChannel.close).toHaveBeenCalledTimes(1);
        });

        it('should call waitForConfirms() before close() when channel is a confirm channel', async () => {
            const callOrder: string[] = [];
            mockAmqpChannel.waitForConfirms = vi.fn().mockImplementation(async () => {
                callOrder.push('waitForConfirms');
            });
            mockAmqpChannel.close = vi.fn().mockImplementation(async () => {
                callOrder.push('close');
            });

            const confirmedChannel = new ChannelImplementation(connection, true);
            await confirmedChannel.connect();
            await confirmedChannel.close();

            expect(mockAmqpChannel.waitForConfirms).toHaveBeenCalledTimes(1);
            expect(mockAmqpChannel.close).toHaveBeenCalledTimes(1);
            expect(callOrder).toEqual(['waitForConfirms', 'close']);
        });
    });

    describe('native', () => {
        it('should call connect() automatically and return the underlying amqp channel', async () => {
            const native = await channel.native();

            expect(mockAmqpConnection.createChannel).toHaveBeenCalledTimes(1);
            expect(native).toBe(mockAmqpChannel);
        });

        it('should return the same underlying channel on repeated calls', async () => {
            const native1 = await channel.native();
            const native2 = await channel.native();

            expect(native1).toBe(native2);
            expect(mockAmqpConnection.createChannel).toHaveBeenCalledTimes(1);
        });
    });

    describe('checkQueue', () => {
        it('should delegate to the native channel with the queue name and return the result', async () => {
            const result = await channel.checkQueue('test-queue');

            expect(mockAmqpChannel.checkQueue).toHaveBeenCalledTimes(1);
            expect(mockAmqpChannel.checkQueue).toHaveBeenCalledWith('test-queue');
            expect(result).toEqual({
                queue: 'test-queue',
                messageCount: 0,
                consumerCount: 0,
            });
        });

        it('should auto-connect via native() when not yet connected', async () => {
            await channel.checkQueue('test-queue');

            expect(mockAmqpConnection.createChannel).toHaveBeenCalledTimes(1);
        });
    });

    describe('publish — regular channel (isConfirmed=false)', () => {
        it('should call amqpChannel.publish with (exchange, routingKey, content, options) and return false', async () => {
            await channel.connect();

            const content = Buffer.from('hello');
            const result = await channel.publish('my-exchange', 'my-key', content, { drainTimeout: 1000 });

            expect(mockAmqpChannel.publish).toHaveBeenCalledTimes(1);
            expect(mockAmqpChannel.publish).toHaveBeenCalledWith('my-exchange', 'my-key', content, {});
            expect(result).toBe(false);
        });

        it('should handle backpressure: wait until drain event fires when amqpChannel.publish returns false', async () => {
            vitest.useFakeTimers();

            channel = new ChannelImplementation(connection, false);
            await channel.connect();

            mockAmqpChannel.publish = vi.fn()
                .mockReturnValueOnce(false)
                .mockReturnValueOnce(true);

            const content = Buffer.from('hello');
            const publishPromise = channel.publish('my-exchange', 'my-key', content, { drainTimeout: 5000 });

            await vitest.advanceTimersByTimeAsync(100);

            eventHandlers.drain?.();

            await expect(publishPromise).resolves.toEqual(false);
            expect(mockAmqpChannel.publish).toHaveBeenCalledTimes(2);
        });

        it('should reject with DrainError when drain timeout expires', async () => {
            vitest.useFakeTimers();
            await channel.connect();

            mockAmqpChannel.publish = vi.fn().mockReturnValue(false);
            mockAmqpChannel.close = vi.fn().mockResolvedValue(undefined);

            const content = Buffer.from('hello');
            const publishPromise = channel.publish('my-exchange', 'my-key', content, { drainTimeout: 1000 });

            // Set up the rejection handler BEFORE advancing timers to avoid unhandled rejection
            const rejectExpectation = expect(publishPromise).rejects.toThrow(DrainError);
            await vitest.advanceTimersByTimeAsync(1100);
            await rejectExpectation;
        });
    });

    describe('publish — confirm channel (isConfirmed=true)', () => {
        let confirmedChannel: ChannelImplementation;

        beforeEach(async () => {
            confirmedChannel = new ChannelImplementation(connection, true);
            // Use async callback to avoid TDZ (temporal dead zoe): the source captures the return value of publish()
            // into `status` and calls resolve(status) inside the callback. If the callback fires
            // synchronously, `status` is not yet assigned (temporal dead zone for `const`).
            mockAmqpChannel.publish = vi.fn().mockImplementation(
                (_exchange: string, _key: string, _content: Buffer, _opts: unknown, callback: (err: Error | null) => void) => {
                    setImmediate(() => callback(null));
                    return true;
                },
            );
            await confirmedChannel.connect();
        });

        it('should call createConfirmChannel() instead of createChannel()', () => {
            expect(mockAmqpConnection.createConfirmChannel).toHaveBeenCalledTimes(1);
            expect(mockAmqpConnection.createChannel).not.toHaveBeenCalled();
        });

        it('should publish with a callback and resolve to true on success', async () => {
            const content = Buffer.from('hello');
            const result = await confirmedChannel.publish('my-exchange', 'my-key', content, { drainTimeout: 1000 });

            expect(mockAmqpChannel.publish).toHaveBeenCalledTimes(1);
            expect(result).toBe(true);
        });

        it('should reject when the publish callback receives a non-retriable error', async () => {
            // Use a non-'message nacked' error so the retryLoop does not retry and rejects immediately
            mockAmqpChannel.publish = vi.fn().mockImplementation(
                (_exchange: string, _key: string, _content: Buffer, _opts: unknown, callback: (err: Error | null) => void) => {
                    setImmediate(() => callback(new Error('publish failed: connection reset')));
                    return true;
                },
            );

            const content = Buffer.from('hello');
            await expect(
                confirmedChannel.publish('my-exchange', 'my-key', content, { drainTimeout: 1000 }),
            ).rejects.toThrow('publish failed: connection reset');
        });
    });

    describe('factory methods', () => {
        beforeEach(async () => {
            await channel.connect();
        });

        it('createQueue returns a QueueImplementation', async () => {
            const queue = await channel.createQueue('test-queue');

            expect(queue).toBeInstanceOf(QueueImplementation);
        });

        it('createExchange returns an ExchangeImplementation', async () => {
            const exchange = await channel.createExchange('test-exchange', 'direct');

            expect(exchange).toBeInstanceOf(ExchangeImplementation);
        });

        it('createProducerForQueue passes the channel to the queue createProducer call', async () => {
            const queue = new TestQueue();
            await channel.createProducerForQueue(queue, { routingKey: 'my-key' });

            expect(queue.createProducer).toHaveBeenCalledTimes(1);
            expect(queue.createProducer).toHaveBeenCalledWith(
                expect.objectContaining({ channel, routingKey: 'my-key' }),
            );
        });

        it('createProducerForExchange passes the channel to the exchange createProducer call', async () => {
            const exchange = new TestExchange();
            await channel.createProducerForExchange(exchange, { routingKey: 'my-key' });

            expect(exchange.createProducer).toHaveBeenCalledTimes(1);
            expect(exchange.createProducer).toHaveBeenCalledWith(
                expect.objectContaining({ channel, routingKey: 'my-key' }),
            );
        });

        it('createConsumerForQueue passes the channel to the queue createConsumer call', async () => {
            const queue = new TestQueue();
            await channel.createConsumerForQueue(queue);

            expect(queue.createConsumer).toHaveBeenCalledTimes(1);
            expect(queue.createConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel }),
            );
        });

        it('createBatchConsumerForQueue passes the channel to the queue createBatchConsumer call', async () => {
            const queue = new TestQueue();
            await channel.createBatchConsumerForQueue(queue);

            expect(queue.createBatchConsumer).toHaveBeenCalledTimes(1);
            expect(queue.createBatchConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel }),
            );
        });

        it('createConsumerForExchange passes the channel to the exchange createConsumer call', async () => {
            const exchange = new TestExchange();
            await channel.createConsumerForExchange(exchange, { pattern: 'test-pattern' });

            expect(exchange.createConsumer).toHaveBeenCalledTimes(1);
            expect(exchange.createConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel, pattern: 'test-pattern' }),
                undefined,
            );
        });

        it('createBatchConsumerForExchange passes the channel to the exchange createBatchConsumer call', async () => {
            const exchange = new TestExchange();
            await channel.createBatchConsumerForExchange(exchange, { pattern: 'test-pattern' });

            expect(exchange.createBatchConsumer).toHaveBeenCalledTimes(1);
            expect(exchange.createBatchConsumer).toHaveBeenCalledWith(
                expect.objectContaining({ channel, pattern: 'test-pattern' }),
                undefined,
            );
        });
    });
});
