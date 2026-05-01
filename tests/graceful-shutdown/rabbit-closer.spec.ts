import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConnectionImplementation, ConnectionState, ConsumptionFailureStrategy, RabbitCloser } from '../../src';
import { DIRECT_OPTIONS, PROXIED_OPTIONS } from '../helpers/broker-urls';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';
import { withToxic } from '../helpers/toxiproxy';

const PROPAGATION_DELAY = 10_000;

type TestMessage = { value: number };

describe('RabbitCloser integration', () => {
    let connection: ConnectionImplementation;

    beforeEach(() => {
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
    });

    afterEach(async () => {
        await connection?.close().catch(() => {});
    });

    describe('close', () => {
        it('should close the connection after shutdown sequence', async () => {
            const channel = connection.createChannel();
            const queueName = uniqueName('closer-conn');
            await channel.createQueue(queueName, { durable: true, autoDelete: false });

            const closer = new RabbitCloser([connection], [], []);
            await closer.close();

            expect(connection.state()).toBe(ConnectionState.closed);
        });

        it('should prevent producer from publishing after shutdown', async () => {
            const channel = connection.createChannel();
            const queueName = uniqueName('closer-prod');
            const queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
            const producer = await queue.createProducer<TestMessage>();

            const closer = new RabbitCloser([connection], [], [producer]);
            await closer.close();

            await expect(producer.publish({ value: 1 })).rejects.toThrow('Producer is closed');
        });

        it('should stop consumer from receiving new messages after shutdown', async () => {
            const channel = connection.createChannel();
            const queueName = uniqueName('closer-consumer');
            const queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
            const consumer = await queue.createConsumer<TestMessage>({
                failureStrategy: ConsumptionFailureStrategy.Drop,
            });
            const handler = vi.fn();
            await consumer.listen(handler);

            const closer = new RabbitCloser([connection], [consumer], []);
            await closer.close();

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName);
            await sleepPromise(2000);
            expect(handler).toHaveBeenCalledTimes(0);
        });

        it('should handle all-empty arrays without error', async () => {
            const closer = new RabbitCloser([], [], []);
            await closer.close();
        });

        it('close on not-yet connected channel should not throw error', async () => {
            const closer = new RabbitCloser([connection], [], []);
            await closer.close();
        });

        it('should close all producers and connections when multiple provided', async () => {
            const connection2 = new ConnectionImplementation(DIRECT_OPTIONS);
            try {
                const channel1 = connection.createChannel();
                const channel2 = connection2.createChannel();
                const queueName1 = uniqueName('closer-multi-1');
                const queueName2 = uniqueName('closer-multi-2');
                const queue1 = await channel1.createQueue(queueName1, { durable: true, autoDelete: false });
                const queue2 = await channel2.createQueue(queueName2, { durable: true, autoDelete: false });
                const producer1 = await queue1.createProducer<TestMessage>();
                const producer2 = await queue2.createProducer<TestMessage>();

                const closer = new RabbitCloser([connection, connection2], [], [producer1, producer2]);
                await closer.close();

                expect(connection.state()).toBe(ConnectionState.closed);
                expect(connection2.state()).toBe(ConnectionState.closed);
                await expect(producer1.publish({ value: 1 })).rejects.toThrow('Producer is closed');
                await expect(producer2.publish({ value: 2 })).rejects.toThrow('Producer is closed');
            } finally {
                await connection2.close().catch(() => {});
            }
        });

        it('should wait for an in-flight confirm-channel publish to settle before resolving', async () => {
            const queueName = uniqueName('closer-inflight');
            const setupChannel = connection.createChannel();
            await setupChannel.createQueue(queueName, { durable: true, autoDelete: false });

            const toxiConnection = new ConnectionImplementation(PROXIED_OPTIONS);
            const confirmChannel = toxiConnection.createChannel(true);
            const toxiQueue = await confirmChannel.createQueue(queueName, { durable: true, autoDelete: false });
            const producer = await toxiQueue.createProducer<TestMessage>({ isConfirmed: true });

            try {
                await withToxic(
                    'rabbit',
                    {
                        type: 'latency',
                        stream: 'upstream',
                        toxicity: 1,
                        attributes: { latency: 500 },
                    },
                    async () => {
                        const publishPromise = producer.publish({ value: 1 });
                        await sleepPromise(100);
                        const closer = new RabbitCloser([toxiConnection], [], [producer]);
                        await Promise.all([publishPromise, closer.close()]);
                    },
                );

                await sleepPromise(PROPAGATION_DELAY);
                const detail = await rabbitApi.queueDetail(queueName);
                expect(detail.messages).toBe(1);
            } finally {
                await toxiConnection.close();
            }
        });
    });
});
