import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Channel, ConnectionImplementation, Consumer, ConsumptionFailureStrategy, Queue } from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { RABBIT_CONTAINER, restartContainer, startContainer, stopContainer, waitForHealthy } from '../helpers/docker';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { externallyResolvedPromise, sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

type TestMessage = { value: number };

describe('SimpleConsumer integration', () => {
    let connection: ConnectionImplementation;
    let channel: Channel;
    let queue: Queue;
    let queueName: string;
    let dlqName: string;
    let consumer: Consumer<TestMessage>;

    beforeEach(async () => {
        queueName = uniqueName('consumer');
        dlqName = uniqueName('consumer-dlq');
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
        channel = connection.createChannel();
        await channel.createQueue(dlqName, { durable: true, autoDelete: false });
        queue = await channel.createQueue(queueName, {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': dlqName,
            },
        });
    });

    afterEach(async () => {
        await consumer?.close();
        await connection?.close();
    });

    describe('basic consume and ack', () => {
        it('should consume a message and ack it', async () => {
            consumer = await queue.createConsumer({ prefetch: 1 });

            const handler = vi.fn();
            await consumer.listen(async ({ message }) => {
                handler(message);
            });

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);

            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledWith({ value: 1 });

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.ack).toBe(1);
            expect(detail.message_stats.deliver).toBe(1);
        });

        it('should parse message as the expected type', async () => {
            consumer = await queue.createConsumer({ prefetch: 1 });

            const handler = vi.fn();
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 42 }), queueName);

            await vi.waitFor(() => expect(handler).toHaveBeenCalled(), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledWith(expect.objectContaining({
                message: { value: 42 },
            }));
        });

        it('should not allow to register handler twice', async () => {
            consumer = await queue.createConsumer();

            const handler1 = vi.fn();
            await consumer.listen(handler1);

            await expect(consumer.listen(vi.fn())).rejects.toThrow('Listener is already attached');
        });
    });

    describe('failure strategies', () => {
        it('should ack message on handler error with Drop strategy', async () => {
            consumer = await queue.createConsumer({
                prefetch: 1,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });

            const handler = vi.fn().mockRejectedValue(new Error('drop'));
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 2 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats?.ack).toBe(1);
            const dlqDetails = await rabbitApi.queueDetail(dlqName);
            expect(dlqDetails.messages).toBe(0);
        });

        it('should nack and dead-letter message with Reject strategy', async () => {
            consumer = await queue.createConsumer({
                prefetch: 1,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });

            const handler = vi.fn().mockRejectedValue(new Error('reject'));
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 3 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalled(), { timeout: 10_000 });

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.ack).toBe(0);
            expect(detail.message_stats.deliver).toBe(1);
            const dlqDetail = await rabbitApi.queueDetail(dlqName);
            expect(dlqDetail.messages).toBe(1);
        });

        it('should nack and requeue message with Requeue strategy, then ack on second attempt', async () => {
            consumer = await queue.createConsumer({
                prefetch: 1,
                failureStrategy: ConsumptionFailureStrategy.Requeue,
            });

            const handler = vi.fn()
                .mockRejectedValueOnce(new Error('requeue'))
                .mockResolvedValue(undefined);
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 4 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(2), { timeout: 10_000 });

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.redeliver).toBe(1);
            expect(detail.message_stats.ack).toBe(1);
            expect(detail.message_stats.deliver).toBe(2);
            const dlqDetail = await rabbitApi.queueDetail(dlqName);
            expect(dlqDetail.messages).toBe(0);
        });

        it('should fail for invalid strategy', async () => {
            consumer = await queue.createConsumer({
                prefetch: 1,
                // @ts-expect-error testing invalid failure strategy
                failureStrategy: 'invalid',
            });

            const errorHandler = vi.fn();
            consumer.on('error', errorHandler);

            const handler = vi.fn()
                .mockRejectedValue(new Error('error'));
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 4 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });

            expect(errorHandler).toHaveBeenCalled();
        });
    });

    describe('prefetch', () => {
        it('should not deliver more messages than prefetch limit simultaneously', async () => {
            const PREFETCH = 3;
            const MESSAGES = 10;
            const [latch, releaseLatch] = externallyResolvedPromise();

            consumer = await queue.createConsumer({
                prefetch: PREFETCH,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });

            let maxObservedInFlight = 0;
            let inFlight = 0;
            const handler = vi.fn().mockImplementation(async () => {
                inFlight++;
                maxObservedInFlight = Math.max(maxObservedInFlight, inFlight);
                await latch;
                inFlight--;
            });
            await consumer.listen(handler);

            for (let i = 0; i < MESSAGES; i++)
                await rabbitApi.publish(JSON.stringify({ value: i }), queueName);

            await sleepPromise(2000);
            expect(handler).toHaveBeenCalledTimes(PREFETCH);
            expect(maxObservedInFlight).toBe(PREFETCH);

            releaseLatch();

            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(MESSAGES), { timeout: 10_000 });
            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.ack).toEqual(MESSAGES);
            expect(detail.message_stats.deliver).toEqual(MESSAGES);
            const dlqDetail = await rabbitApi.queueDetail(dlqName);
            expect(dlqDetail.messages).toBe(0);
        });
    });

    describe('auto-ack mode', () => {
        it('should auto-ack all messages when prefetch=0 and strategy is Drop', async () => {
            const MESSAGES = 5;
            consumer = await queue.createConsumer({
                prefetch: 0,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });

            const handler = vi.fn();
            await consumer.listen(async ({ message }) => {
                handler(message);
            });

            for (let i = 0; i < MESSAGES; i++)
                await rabbitApi.publish(JSON.stringify({ value: i }), queueName);

            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(MESSAGES), { timeout: 10_000 });

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.deliver_no_ack).toBe(MESSAGES);
        });
    });

    describe('consumer close', () => {
        it('should stop consuming after close() is called', async () => {
            consumer = await queue.createConsumer({
                prefetch: 1,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const handler = vi.fn();
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });

            await consumer.close();
            await rabbitApi.publish(JSON.stringify({ value: 2 }), queueName);
            await sleepPromise(2000);
            expect(handler).toHaveBeenCalledTimes(1);
        });

        it('should wait for in-flight message to complete before resolving close()', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            consumer = await queue.createConsumer({
                prefetch: 1,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });

            const handler = vi.fn().mockImplementation(async () => {
                await latch;
            });
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalled(), { timeout: 10_000 });

            const closePromise = consumer.close();
            const raceResult = await Promise.race([
                closePromise.then(() => 'closed' as const),
                sleepPromise(200).then(() => 'timeout' as const),
            ]);
            expect(raceResult).toBe('timeout');

            releaseLatch();
            await expect(closePromise).resolves.toBeUndefined();
        });

        it('should emit close event after consumer.close() resolves', async () => {
            consumer = await queue.createConsumer({ prefetch: 1 });
            const closeFn = vi.fn();
            consumer.on('close', closeFn);
            await consumer.listen(vi.fn());
            await consumer.close();
            expect(closeFn).toHaveBeenCalledTimes(1);
        });

        it('should close based on broker information', async () => {
            const consumer = await queue.createConsumer();

            const handler = vi.fn();
            await consumer.listen(handler);

            const closeHandler = vi.fn();
            consumer.on('close', closeHandler);

            await sleepPromise(500);

            await rabbitApi.queueDelete(queueName);
            await vi.waitFor(() => expect(closeHandler).toHaveBeenCalled(), { timeout: 5000 });
        });
    });

    describe('channel close', () => {
        it('should not ack message when channel closes', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = await queue.createConsumer();

            const handler = vi.fn().mockImplementation(async () => {
                await latch;
            });
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalled(), { timeout: 10_000 });

            await stopContainer(RABBIT_CONTAINER);
            releaseLatch();
            await startContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(2), { timeout: 10_000 });
            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.ack).toEqual(1);
            expect(detail.message_stats.deliver).toEqual(1);
        });

        it('should not nack message when channel closes', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = await queue.createConsumer({
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });

            const handler = vi.fn().mockImplementation(async () => {
                await latch;
                throw new Error('test');
            });
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalled(), { timeout: 10_000 });

            await stopContainer(RABBIT_CONTAINER);
            releaseLatch();
            await startContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(2), { timeout: 10_000 });
            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(0);
            expect(detail.message_stats.ack).toEqual(0);
            expect(detail.message_stats.deliver).toEqual(1);
            const dlqDetail = await rabbitApi.queueDetail(dlqName);
            expect(dlqDetail.messages).toBe(1);
        });

        it('should reconnect when connection close', async () => {
            const consumer = await queue.createConsumer({
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });

            const handler = vi.fn();
            await consumer.listen(handler);

            await sleepPromise(500);

            await connection.close();
            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);

            await sleepPromise(PROPAGATION_DELAY);
            expect(handler).toHaveBeenCalledTimes(1);
        });
    });

    describe('custom parse function', () => {
        it('should parse non-JSON messages with parseMessageFn', async () => {
            type CsvRow = { name: string; age: number };
            const csvQueueName = uniqueName('q-csv');
            const csvQueue = await channel.createQueue(csvQueueName, { durable: true, autoDelete: false });

            const consumer = await csvQueue.createConsumer<CsvRow>({
                prefetch: 1,
                parseMessageFn: payload => {
                    const [name, ageStr] = payload.toString().split(',');
                    return { name: name!, age: Number(ageStr) };
                },
            });

            const handler = vi.fn();
            await consumer.listen(handler);

            await rabbitApi.publish('Alice,30', csvQueueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledWith(expect.objectContaining({
                message: {
                    name: 'Alice',
                    age: 30,
                },
            }));

            await consumer.close();
        });
    });

    describe('reconnection after connection loss', () => {
        it('should reconnect and resume consuming after broker restart', async () => {
            consumer = await queue.createConsumer({
                prefetch: 5,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });

            const handler = vi.fn();
            await consumer.listen(handler);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1), { timeout: 10_000 });

            await restartContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            await rabbitApi.publish(JSON.stringify({ value: 2 }), queueName);
            await vi.waitFor(async () => {
                expect(handler).toHaveBeenCalledTimes(2);
            }, { timeout: 60_000 });
        });
    });
});
