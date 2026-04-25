import { TestChannel, TestExchange } from '../extensions/vitest';
import { ConsumerImplementation, BatchConsumerImplementation } from '../consumer';
import { ProducerImplementation } from '../producer/producer-implementation';
import { AssertionMode } from '../types';
import { QueueImplementation } from './queue-implementation';

describe('QueueImplementation', () => {
    let channel: TestChannel;
    let queue: QueueImplementation;

    beforeEach(() => {
        channel = new TestChannel();
        queue = new QueueImplementation(channel, 'test-queue');
    });

    describe('assert', () => {
        beforeEach(() => {
            (channel.nativeChannel as any).assertQueue = vi.fn().mockResolvedValue({
                queue: 'test-queue',
                messageCount: 0,
                consumerCount: 0,
            });
            (channel.nativeChannel as any).checkQueue = vi.fn().mockResolvedValue({
                queue: 'test-queue',
                messageCount: 0,
                consumerCount: 0,
            });
        });

        it('should call assertQueue on native channel in Assert mode (default)', async () => {
            await queue.assert();

            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledTimes(1);
            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledWith(
                'test-queue',
                expect.objectContaining({ assertionMode: AssertionMode.Assert }),
            );
            expect((channel.nativeChannel as any).checkQueue).not.toHaveBeenCalled();
        });

        it('should pass queue options to assertQueue', async () => {
            const durableQueue = new QueueImplementation(channel, 'test-queue', { durable: true });
            await durableQueue.assert();

            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledWith(
                'test-queue',
                expect.objectContaining({ durable: true }),
            );
        });

        it('should call checkQueue in Check mode', async () => {
            const checkQueue = new QueueImplementation(channel, 'test-queue', {
                assertionMode: AssertionMode.Check,
            });
            await checkQueue.assert();

            expect((channel.nativeChannel as any).checkQueue).toHaveBeenCalledTimes(1);
            expect((channel.nativeChannel as any).checkQueue).toHaveBeenCalledWith('test-queue');
            expect((channel.nativeChannel as any).assertQueue).not.toHaveBeenCalled();
        });

        it('should skip native call in Passive mode', async () => {
            const passiveQueue = new QueueImplementation(channel, 'test-queue', {
                assertionMode: AssertionMode.Passive,
            });
            await passiveQueue.assert();

            expect((channel.nativeChannel as any).assertQueue).not.toHaveBeenCalled();
            expect((channel.nativeChannel as any).checkQueue).not.toHaveBeenCalled();
        });

        it('should only assert once even when assert() is called multiple times', async () => {
            await queue.assert();
            await queue.assert();

            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledTimes(1);
        });

        it('should re-assert after channel emits close', async () => {
            await queue.assert();
            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledTimes(1);

            channel.emit('close');

            await queue.assert();
            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledTimes(2);
        });
    });

    describe('name', () => {
        beforeEach(() => {
            (channel.nativeChannel as any).assertQueue = vi.fn().mockResolvedValue({
                queue: 'test-queue',
                messageCount: 0,
                consumerCount: 0,
            });
        });

        it('should return the queue name', async () => {
            const result = await queue.name();

            expect(result).toBe('test-queue');
        });

        it('should trigger assert when called', async () => {
            await queue.name();

            expect((channel.nativeChannel as any).assertQueue).toHaveBeenCalledTimes(1);
        });
    });

    describe('bindExchange', () => {
        it('should call exchange.bindQueue with the queue, pattern, and args', async () => {
            const exchange = new TestExchange();

            await queue.bindExchange(exchange, 'routing.key', { 'x-match': 'all' });

            expect(exchange.bindQueue).toHaveBeenCalledTimes(1);
            expect(exchange.bindQueue).toHaveBeenCalledWith(queue, 'routing.key', { 'x-match': 'all' });
        });

        it('should return the queue itself for chaining', async () => {
            const exchange = new TestExchange();

            const result = await queue.bindExchange(exchange, 'routing.key');

            expect(result).toBe(queue);
        });
    });

    describe('createConsumer', () => {
        it('returns ConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createConsumer();
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).not.toBe(channel);
        });

        it('returns ConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });

    describe('createBatchConsumer', () => {
        it('returns BatchConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createBatchConsumer();
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).not.toBe(channel);
        });

        it('returns BatchConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createBatchConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });

    describe('createProducer', () => {
        it('returns ProducerImplementation', async () => {
            const producer = await queue.createProducer();

            expect(producer).toBeInstanceOf(ProducerImplementation);
            await producer.close();
        });

        it('uses the channel provided in options', async () => {
            const otherChannel = new TestChannel();
            const producer = await queue.createProducer({ channel: otherChannel });

            expect(producer.channel).toBe(otherChannel);
            await producer.close();
        });

        it('uses connection.createChannel() as default channel', async () => {
            const producer = await queue.createProducer();

            // Default channel is created via connection.createChannel(), not the queue's own channel
            expect(producer.channel).not.toBe(channel);
            await producer.close();
        });

        it('should set the routing key to resolve to the queue name', async () => {
            (channel.nativeChannel as any).assertQueue = vi.fn().mockResolvedValue({
                queue: 'test-queue',
                messageCount: 0,
                consumerCount: 0,
            });
            const producer = await queue.createProducer() as ProducerImplementation<unknown>;

            // @ts-expect-error options is private
            const key = await producer.options.routingKey({});
            expect(key).toBe('test-queue');

            await producer.close();
        });

        it('should create a confirmed channel when isConfirmed=true', async () => {
            const createChannelMock = vi.fn().mockReturnValue(new TestChannel());
            (channel.connection as any).createChannel = createChannelMock;

            await queue.createProducer({ isConfirmed: true });

            expect(createChannelMock).toHaveBeenCalledWith(true);
        });
    });
});
