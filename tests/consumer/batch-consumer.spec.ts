import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
    ConnectionImplementation,
    BatchConsumerImplementation,
    BatchFailureStrategy,
    ConsumptionFailureStrategy,
    Channel,
    Queue,
} from '../../src';
import { DIRECT_OPTIONS, PROXIED_OPTIONS } from '../helpers/broker-urls';
import { RABBIT_CONTAINER, dockerExec, restartContainer, waitForHealthy } from '../helpers/docker';
import { withToxic } from '../helpers/toxiproxy';
import { uniqueName } from '../helpers/names';
import { sleepPromise } from '../../src/utils';

type TestMessage = { value: number };

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
            await dockerExec(RABBIT_CONTAINER, [
                'rabbitmqadmin',
                'publish',
                `routing_key=${queueName}`,
                `payload=${JSON.stringify({ value: i })}`,
                `properties=${JSON.stringify({
                    delivery_mode: 2,
                })}`,
            ]);
        }
    }

    async function expectQueueDepth(name: string, expected: number): Promise<void> {
        const { stdout } = await dockerExec(RABBIT_CONTAINER, [
            'rabbitmq-diagnostics',
            'list_queues',
            '--formatter=json',
        ]);
        const parsed: Array<{ name: string; messages: number }> = JSON.parse(stdout);
        const relevantQueue = parsed.find(q => q.name === name);
        expect(relevantQueue?.messages).toEqual(expected);
    }

    function suppressErrors(consumer: BatchConsumerImplementation<TestMessage>): void {
        consumer.on('error', () => {});
        consumer.on('handlingFailed', () => {});
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
            await expectQueueDepth(queueName, 0);
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
            await expectQueueDepth(queueName, 0);
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
            await expectQueueDepth(queueName, 0);
        });
    });

    describe('failure with Fail strategy', () => {
        it('should dead-letter messages when Reject strategy is used', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });
            suppressErrors(consumer);
            const listener = vi.fn().mockRejectedValue(new Error('test rejection'));
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1), { timeout: 5000 });
            await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 5);
        });

        it('should redeliver and succeed on second attempt with Requeue strategy', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Fail,
                failureStrategy: ConsumptionFailureStrategy.Requeue,
            });
            suppressErrors(consumer);
            const listener = vi.fn()
                .mockRejectedValueOnce(new Error('first attempt fails'))
                .mockResolvedValue(undefined);
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(2), { timeout: 10000 });
            await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
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
            suppressErrors(consumer);
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

            await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 0);
        });

        it('should dead-letter failing messages after split', async () => {
            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
                batchFailureStrategy: BatchFailureStrategy.Split,
                failureStrategy: ConsumptionFailureStrategy.Reject,
            });
            suppressErrors(consumer);
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

            await expectQueueDepth(queueName, 0);
            await expectQueueDepth(dlqName, 2);
        });
    });

    describe('consumer close', () => {
        it('should wait for an in-flight batch to complete before resolving', async () => {
            let releaseLatch!: () => void;
            const latch = new Promise<void>(resolve => {
                releaseLatch = resolve;
            });
            let processingStarted = false;

            const consumer = new BatchConsumerImplementation<TestMessage>(consumerChannel, queue, {
                batchSize: 5,
                maxWaitTimeForBatch: 5000,
            });
            const listener = vi.fn().mockImplementation(async () => {
                processingStarted = true;
                await latch;
            });
            await consumer.listen(listener);

            await publishN(5);

            await vi.waitFor(() => expect(processingStarted).toBe(true), { timeout: 5000 });

            const closePromise = consumer.close();

            const raceResult = await Promise.race([
                closePromise.then(() => 'closed' as const),
                sleepPromise(200).then(() => 'timeout' as const),
            ]);
            expect(raceResult).toBe('timeout');

            releaseLatch();
            await expect(closePromise).resolves.toBeUndefined();
            expect(listener).toHaveBeenCalledTimes(1);
            await expectQueueDepth(queueName, 0);
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
                const publishInterval = setInterval(async () => {
                    publishN(1)
                        .then(() => published++)
                        .catch(_err => { /* ignore */ });
                }, 50);
                await sleepPromise(1000);
                await restartContainer(RABBIT_CONTAINER);
                await waitForHealthy(RABBIT_CONTAINER);
                await sleepPromise(1000);
                clearInterval(publishInterval);
                await sleepPromise(5000);
                return published;
            });

            expect(publishedMessages).toEqual(consumedMessages);
            await expectQueueDepth(queueName, 0);
        });
    });
});
