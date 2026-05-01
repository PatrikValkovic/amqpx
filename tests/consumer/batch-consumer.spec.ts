import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    BatchConsumerImplementation,
    BatchFailureStrategy,
    Channel,
    ConnectionImplementation,
    ConsumptionFailureStrategy,
    Queue,
} from '../../src';
import { DIRECT_OPTIONS, PROXIED_OPTIONS } from '../helpers/broker-urls';
import { RABBIT_CONTAINER, restartContainer, waitForHealthy } from '../helpers/docker';
import { withToxic } from '../helpers/toxiproxy';
import { uniqueName } from '../helpers/names';
import { externallyResolvedPromise, sleepPromise } from '../../src/utils';
import * as rabbitApi from '../helpers/rabbit-api';

type TestMessage = { value: number };

const PROPAGATION_DELAY = 10_000;

describe('BatchConsumer integration', () => {
    let connection: ConnectionImplementation;
    let consumerChannel: Channel;
    let managementChannel: Channel;
    let queue: Queue;
    let queueName: string;
    let dlqName: string;

    beforeEach(async () => {
        queueName = uniqueName('batch');
        dlqName = uniqueName('dlq');

        connection = new ConnectionImplementation(DIRECT_OPTIONS);

        consumerChannel = connection.createChannel();
        managementChannel = connection.createChannel();

        await managementChannel.createQueue(dlqName, {
            durable: true,
            autoDelete: false,
        });

        queue = await consumerChannel.createQueue(queueName, {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': dlqName,
            },
        });
    });

    afterEach(async () => {
        await connection?.close();
    });

    async function publishN(n: number): Promise<void> {
        for (let i = 0; i < n; i++) {
            await rabbitApi.publish(
                JSON.stringify({ value: i }),
                queueName,
            );
        }
    }

    async function expectQueueDepth(name: string, expected: number) {
        const queueDetail = await rabbitApi.queueDetail(name);
        expect(queueDetail.messages).toEqual(expected);
        return queueDetail;
    }

    describe('consuming', () => {
        it('should process a full batch', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
            });
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 5000 });

            const { messages } = listener.mock.calls[0]![0] as { messages: Array<{ message: TestMessage }> };
            expect(messages).toHaveLength(5);
            expect(messages.map(m => m.message.value)).toEqual([0, 1, 2, 3, 4]);
            await sleepPromise(PROPAGATION_DELAY);
            const queueDetail = await expectQueueDepth(queueName, 0);
            expect(queueDetail.message_stats.ack).toEqual(5);
        });

        it('should process multiple batches', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
            });
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(20);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(4), { timeout: 15000 });

            for (const call of listener.mock.calls) {
                const { messages } = call[0] as { messages: Array<{ message: TestMessage }> };
                expect(messages).toHaveLength(5);
            }
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            expect(queueStatus.message_stats.ack).toEqual(20);
        });

        it('should process a partial batch after maxWaitTimeForBatch elapses under network latency', async () => {
            await connection.close();

            connection = new ConnectionImplementation(PROXIED_OPTIONS);
            consumerChannel = connection.createChannel();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 500,
            });
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            await withToxic('rabbit', {
                type: 'latency',
                stream: 'downstream',
                toxicity: 1,
                attributes: { latency: 100, jitter: 0 },
            }, async () => {
                await publishN(3);
                await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 8000 });
            });

            const { messages } = listener.mock.calls[0]![0] as { messages: Array<{ message: TestMessage }> };
            expect(messages).toHaveLength(3);
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            expect(queueStatus.message_stats.ack).toEqual(3);
        });

        it('should process second batch when first one is delayed', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                prefetch: 10,
                maxWaitTimeForBatch: 5000,
                maxWaitTimeForAck: 500,
            });

            const [latch, releaseLatch] = externallyResolvedPromise();

            const listener = vi.fn()
                .mockImplementationOnce(() => latch)
                .mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(10);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(2), { timeout: 5000 });

            await sleepPromise(PROPAGATION_DELAY);
            const queueStatusBefore = await rabbitApi.queueDetail(queueName);
            expect(queueStatusBefore.messages_unacknowledged).toEqual(5);
            expect(queueStatusBefore.message_stats.ack).toEqual(5);
            expect(queueStatusBefore.message_stats.deliver).toEqual(10);

            releaseLatch();
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            expect(queueStatus.messages_unacknowledged).toEqual(0);
            expect(queueStatus.message_stats.ack).toEqual(10);
            expect(queueStatus.message_stats.deliver).toEqual(10);
        });
    });

    describe('failure with Fail strategy', () => {
        it('should dead-letter messages when Reject strategy is used', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 500,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });
            const listener = vi.fn().mockRejectedValue(new Error('test rejection'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 5);
            expect(queueStatus.message_stats.ack).toEqual(0);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should redeliver and succeed on second attempt with Requeue strategy', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Requeue,
            });
            const listener = vi.fn()
                .mockRejectedValueOnce(new Error('first attempt fails'))
                .mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(2), { timeout: 10000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(10);
            expect(queueStatus.message_stats.redeliver).toEqual(5);
        });

        it('should acknowledge when failure strategy is Drop', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                prefetch: 5,
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const listener = vi.fn().mockRejectedValue(new Error('test rejection'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should turn on auto-ack when batch size is 0 and failure strategy is Drop', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                prefetch: 0,
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const listener = vi.fn().mockRejectedValue(new Error('test rejection'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.deliver_no_ack).toEqual(5);
        });
    });

    describe('failure with Split strategy', () => {
        it('should retry each message individually after batch failure — all succeed', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Split,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });
            const listener = vi.fn()
                .mockRejectedValueOnce(new Error('batch fails — trigger split'))
                .mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(6), { timeout: 10000 });

            const [batchCall, ...individualCalls] = listener.mock.calls as Array<[{ messages: Array<{ message: TestMessage }> }]>;
            expect(batchCall![0].messages).toHaveLength(5);
            for (const call of individualCalls)
                expect(call[0].messages).toHaveLength(1);

            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should dead-letter failing messages after split', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Split,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });
            const listener = vi.fn()
                // batch of 5 — fails, triggers split
                .mockRejectedValueOnce(new Error('batch fails'))
                // individual messages: fail, succeed, fail, succeed, succeed
                .mockRejectedValueOnce(new Error('message 0 fails'))
                .mockResolvedValueOnce(undefined)
                .mockRejectedValueOnce(new Error('message 2 fails'))
                .mockResolvedValueOnce(undefined)
                .mockResolvedValueOnce(undefined);
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(6), { timeout: 10000 });

            const [batchCall, ...individualCalls] = listener.mock.calls as Array<[{ messages: Array<{ message: TestMessage }> }]>;
            expect(batchCall![0].messages).toHaveLength(5);
            for (const call of individualCalls)
                expect(call[0].messages).toHaveLength(1);

            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 2);
            expect(queueStatus.message_stats.ack).toEqual(3);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should acknowledge individually when failure strategy is Drop', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                prefetch: 20,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Split,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const listener = vi.fn()
                .mockRejectedValueOnce(new Error('batch fails — trigger split'))
                .mockRejectedValue(new Error('individual message fails'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(6), { timeout: 10000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should turn on auto-ack when prefetch is 0 and failure strategy is Drop', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                prefetch: 0,
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Split,
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const listener = vi.fn().mockRejectedValue(new Error('test rejection'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(6), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            expect(queueStatus.message_stats.deliver_no_ack).toEqual(5);
        });
    });

    describe('consumer close', () => {
        it('should wait for an in-flight batch to complete before resolving', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
            });

            const listener = vi.fn().mockImplementation(async () => {
                await latch;
            });
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 5000 });

            const closePromise = consumer.close();

            const raceResult = await Promise.race([
                closePromise.then(() => 'closed' as const),
                sleepPromise(200).then(() => 'timeout' as const),
            ]);
            expect(raceResult).toBe('timeout');

            releaseLatch();
            await expect(closePromise).resolves.toBeUndefined();
            expect(listener).toHaveBeenCalledTimes(1);
            await sleepPromise(PROPAGATION_DELAY);
            const queueStatus = await expectQueueDepth(queueName, 0);
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should close the consumer when empty message is received', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue);

            const listener = vi.fn();
            await consumer.listen(listener);

            const closeFn = vi.fn();
            consumer.on('close', closeFn);

            await sleepPromise(PROPAGATION_DELAY);
            await rabbitApi.queueDelete(queueName);
            await sleepPromise(PROPAGATION_DELAY);
            expect(closeFn).toHaveBeenCalled();
        });
    });

    describe('reconnect', () => {
        it('should reconnect and keep consuming after connection', async () => {
            await connection.close();

            connection = new ConnectionImplementation(PROXIED_OPTIONS);
            consumerChannel = connection.createChannel();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 500,
            });

            let consumedMessages = 0;
            const listener = vi.fn().mockImplementation(({ messages }) => {
                consumedMessages += messages.length;
            });
            await consumer.listen(listener);

            const publishedMessages = await withToxic('rabbit', {
                type: 'latency',
                stream: 'downstream',
                toxicity: 1,
                attributes: { latency: 100, jitter: 0 },
            }, async () => {
                let published = 0;
                const publishingFn = async () => {
                    publishN(1)
                        .then(() => published++)
                        .catch(_err => { /* ignore */ });
                };
                let publishInterval = setInterval(publishingFn, 50);
                await sleepPromise(1000);
                clearInterval(publishInterval);
                await restartContainer(RABBIT_CONTAINER);
                await waitForHealthy(RABBIT_CONTAINER);
                publishInterval = setInterval(publishingFn, 50);
                await sleepPromise(1000);
                clearInterval(publishInterval);
                await sleepPromise(1000);
                return published;
            });

            expect(publishedMessages).toEqual(consumedMessages);
            await sleepPromise(PROPAGATION_DELAY);
            await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
        });

        it('should not contact broker when connection is lost during processing', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                prefetch: 5,
                maxWaitTimeForBatch: 5000,
            });

            const errorHandler = vi.fn();
            consumer.on('error', errorHandler);

            const listener = vi.fn().mockImplementation(async () => {
                await latch;
            });
            await consumer.listen(listener);

            await publishN(5);
            await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const beforeQueueStatus = await rabbitApi.queueDetail(queueName);
            expect(beforeQueueStatus.messages_unacknowledged).toEqual(5);

            await restartContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            releaseLatch();

            await sleepPromise(PROPAGATION_DELAY);
            expect(errorHandler).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(2);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            // no redelivery because broker was restarted
            expect(queueStatus.message_stats.ack).toEqual(5);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should not contact broker when connection is lost even during error', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                prefetch: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });

            const errorHandler = vi.fn();
            consumer.on('error', errorHandler);

            const listener = vi.fn().mockImplementation(async () => {
                await latch;
                throw new Error('testing-error');
            });
            await consumer.listen(listener);

            await publishN(5);
            await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const beforeQueueStatus = await rabbitApi.queueDetail(queueName);
            expect(beforeQueueStatus.messages_unacknowledged).toEqual(5);

            await restartContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            releaseLatch();

            await sleepPromise(PROPAGATION_DELAY);
            expect(errorHandler).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(2);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 5);
            // no redelivery because broker was restarted
            expect(queueStatus.message_stats.ack).toEqual(0);
            expect(queueStatus.message_stats.deliver).toEqual(5);
        });

        it('should not contact broker with acknowledgement when waiting for previous batch and connection is lost', async () => {
            const [latch, releaseLatch] = externallyResolvedPromise();

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                prefetch: 10,
                maxWaitTimeForBatch: 5000,
                maxWaitTimeForAck: 30_000,
            });

            const errorHandler = vi.fn();
            consumer.on('error', errorHandler);

            const listener = vi.fn()
                .mockImplementationOnce(async () => {
                    await latch;
                    return undefined;
                })
                .mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(10);
            await vi.waitFor(() => expect(listener).toHaveBeenCalled(), { timeout: 5000 });
            await sleepPromise(PROPAGATION_DELAY);
            const beforeQueueStatus = await rabbitApi.queueDetail(queueName);
            expect(beforeQueueStatus.messages_unacknowledged).toEqual(10);
            expect(beforeQueueStatus.message_stats.ack).toEqual(0);

            await restartContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            await sleepPromise(20_000);
            releaseLatch();

            await sleepPromise(PROPAGATION_DELAY);
            expect(errorHandler).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(4);
            const queueStatus = await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
            // no redelivery because broker was restarted
            expect(queueStatus.message_stats.ack).toEqual(10);
            expect(queueStatus.message_stats.deliver).toEqual(10);
        });
    });
});
