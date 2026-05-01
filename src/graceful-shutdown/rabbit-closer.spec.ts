import { TestConsumer, TestProducer, TestConnection } from '../extensions/vitest';
import { sleepPromise } from '../utils';
import { RabbitCloser } from './rabbit-closer';

describe('RabbitCloser', () => {
    let producer: TestProducer<unknown>;
    let consumer: TestConsumer<unknown>;
    let connection: TestConnection;

    beforeEach(() => {
        producer = new TestProducer();
        consumer = new TestConsumer();
        connection = new TestConnection();
    });

    describe('close with single entities', () => {
        it('should close the producer', async () => {
            const closer = new RabbitCloser([connection], [consumer], [producer]);
            await closer.close();

            expect(producer.close).toHaveBeenCalledTimes(1);
        });

        it('should close the consumer', async () => {
            const closer = new RabbitCloser([connection], [consumer], [producer]);
            await closer.close();

            expect(consumer.close).toHaveBeenCalledTimes(1);
        });

        it('should close the producer channel', async () => {
            const closer = new RabbitCloser([connection], [consumer], [producer]);
            await closer.close();

            expect(producer.channel.close).toHaveBeenCalledTimes(1);
        });

        it('should close the consumer channel', async () => {
            const closer = new RabbitCloser([connection], [consumer], [producer]);
            await closer.close();

            expect(consumer.channel.close).toHaveBeenCalledTimes(1);
        });

        it('should close the connection', async () => {
            const closer = new RabbitCloser([connection], [consumer], [producer]);
            await closer.close();

            expect(connection.close).toHaveBeenCalledTimes(1);
        });
    });

    describe('close ordering', () => {
        it('should close producer before its channel', async () => {
            const callOrder: string[] = [];
            producer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('producer.close');
            });
            producer.channel.close = vi.fn().mockImplementation(async () => {
                callOrder.push('channel.close');
            });

            const closer = new RabbitCloser([], [], [producer]);
            await closer.close();

            const producerCloseIndex = callOrder.indexOf('producer.close');
            const channelCloseIndex = callOrder.indexOf('channel.close');
            expect(producerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(channelCloseIndex).toBeGreaterThanOrEqual(0);
            expect(producerCloseIndex).toBeLessThan(channelCloseIndex);
        });

        it('should close consumer before its channel', async () => {
            const callOrder: string[] = [];
            consumer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('consumer.close');
            });
            consumer.channel.close = vi.fn().mockImplementation(async () => {
                callOrder.push('channel.close');
            });

            const closer = new RabbitCloser([], [consumer], []);
            await closer.close();

            const consumerCloseIndex = callOrder.indexOf('consumer.close');
            const channelCloseIndex = callOrder.indexOf('channel.close');
            expect(consumerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(channelCloseIndex).toBeGreaterThanOrEqual(0);
            expect(consumerCloseIndex).toBeLessThan(channelCloseIndex);
        });

        it('should close channels before connections', async () => {
            const callOrder: string[] = [];
            producer.channel.close = vi.fn().mockImplementation(async () => {
                callOrder.push('channel.close');
            });
            connection.close = vi.fn().mockImplementation(async () => {
                callOrder.push('connection.close');
            });

            const closer = new RabbitCloser([connection], [], [producer]);
            await closer.close();

            const channelCloseIndex = callOrder.indexOf('channel.close');
            const connectionCloseIndex = callOrder.indexOf('connection.close');
            expect(channelCloseIndex).toBeGreaterThanOrEqual(0);
            expect(connectionCloseIndex).toBeGreaterThanOrEqual(0);
            expect(channelCloseIndex).toBeLessThan(connectionCloseIndex);
        });

        it('should close producer before connection', async () => {
            const callOrder: string[] = [];
            producer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('producer.close');
            });
            connection.close = vi.fn().mockImplementation(async () => {
                callOrder.push('connection.close');
            });

            const closer = new RabbitCloser([connection], [], [producer]);
            await closer.close();

            const producerCloseIndex = callOrder.indexOf('producer.close');
            const connectionCloseIndex = callOrder.indexOf('connection.close');
            expect(producerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(connectionCloseIndex).toBeGreaterThanOrEqual(0);
            expect(producerCloseIndex).toBeLessThan(connectionCloseIndex);
        });

        it('should close consumer before connection', async () => {
            const callOrder: string[] = [];
            consumer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('consumer.close');
            });
            connection.close = vi.fn().mockImplementation(async () => {
                callOrder.push('connection.close');
            });

            const closer = new RabbitCloser([connection], [consumer], []);
            await closer.close();

            const consumerCloseIndex = callOrder.indexOf('consumer.close');
            const connectionCloseIndex = callOrder.indexOf('connection.close');
            expect(consumerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(connectionCloseIndex).toBeGreaterThanOrEqual(0);
            expect(consumerCloseIndex).toBeLessThan(connectionCloseIndex);
        });

        it('should close consumers after producers', async () => {
            const callOrder: string[] = [];
            producer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('producer.close');
            });
            consumer.close = vi.fn().mockImplementation(async () => {
                callOrder.push('consumer.close');
            });

            const closer = new RabbitCloser([], [consumer], [producer]);
            await closer.close();

            const producerCloseIndex = callOrder.indexOf('producer.close');
            const consumerCloseIndex = callOrder.indexOf('consumer.close');
            expect(producerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(consumerCloseIndex).toBeGreaterThanOrEqual(0);
            expect(producerCloseIndex).toBeLessThan(consumerCloseIndex);
        });
    });

    describe('multiple entities', () => {
        it('should close all producers', async () => {
            const producer2 = new TestProducer();
            const closer = new RabbitCloser([], [], [producer, producer2]);
            await closer.close();

            expect(producer.close).toHaveBeenCalledTimes(1);
            expect(producer2.close).toHaveBeenCalledTimes(1);
        });

        it('should close all consumers', async () => {
            const consumer2 = new TestConsumer();
            const closer = new RabbitCloser([], [consumer, consumer2], []);
            await closer.close();

            expect(consumer.close).toHaveBeenCalledTimes(1);
            expect(consumer2.close).toHaveBeenCalledTimes(1);
        });

        it('should close all connections', async () => {
            const connection2 = new TestConnection();
            const closer = new RabbitCloser([connection, connection2], [], []);
            await closer.close();

            expect(connection.close).toHaveBeenCalledTimes(1);
            expect(connection2.close).toHaveBeenCalledTimes(1);
        });

        it('should close channels for both producers and consumers', async () => {
            const closer = new RabbitCloser([], [consumer], [producer]);
            await closer.close();

            expect(producer.channel.close).toHaveBeenCalledTimes(1);
            expect(consumer.channel.close).toHaveBeenCalledTimes(1);
        });
    });

    describe('empty arrays', () => {
        it('should resolve without error when all arrays are empty', async () => {
            const closer = new RabbitCloser([], [], []);
            await expect(closer.close()).resolves.toBeUndefined();
        });

        it('should close the connection when no producers or consumers', async () => {
            const closer = new RabbitCloser([connection], [], []);
            await closer.close();

            expect(connection.close).toHaveBeenCalledTimes(1);
        });
    });

    describe('close with timeout', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('should pass decrementing timeout', async () => {
            vitest.useFakeTimers();
            const closer = new RabbitCloser([connection], [consumer], [producer]);

            producer.close.mockImplementation(() => sleepPromise(100));
            consumer.close.mockImplementation(() => sleepPromise(100));
            producer.channel.close.mockImplementation(() => sleepPromise(100));
            consumer.channel.close.mockImplementation(() => sleepPromise(100));
            connection.close.mockImplementation(() => sleepPromise(100));

            const closePromise = closer.close(5000);
            await vitest.advanceTimersByTimeAsync(5000);
            await closePromise;

            expect(producer.close).toHaveBeenCalledWith(expect.closeTo(5000, 0));
            expect(consumer.close).toHaveBeenCalledWith(expect.closeTo(4900, 0));
            expect(producer.channel.close).toHaveBeenCalledWith(expect.closeTo(4800, 0));
            expect(consumer.channel.close).toHaveBeenCalledWith(expect.closeTo(4800, 0));
            expect(connection.close).toHaveBeenCalledWith(expect.closeTo(4700, 0));
        });
    });
});
