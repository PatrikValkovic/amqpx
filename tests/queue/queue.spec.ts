import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    AssertionMode,
    Channel,
    ConnectionImplementation,
} from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { compose, RABBIT_CONTAINER, waitForHealthy } from '../helpers/docker';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

type TestMessage = { value: number };

describe('Queue integration', () => {
    let connection: ConnectionImplementation;
    let channel: Channel;

    beforeEach(() => {
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
        channel = connection.createChannel();
    });

    afterEach(async () => {
        await connection?.close();
    });

    describe('assertion modes', () => {
        it('should assert queue in Assert mode', async () => {
            const name = uniqueName('q-assert');
            const queue = await channel.createQueue(name, {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Assert,
            });
            await queue.assert();
            const queueDetail = await rabbitApi.queueDetail(name);
            expect(queueDetail.name).toEqual(name);
        });

        it('should throw error when in Assert mode and queue exists with different properties', async () => {
            const name = uniqueName('q-assert');
            const queue = await channel.createQueue(name, {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Assert,
            });
            await queue.assert();

            const assertChannel = connection.createChannel();
            const channelErrorHandler = vi.fn();
            assertChannel.on('error', channelErrorHandler);
            const channelCloseHandler = vi.fn();
            assertChannel.on('close', channelCloseHandler);

            const e = `Operation failed: QueueDeclare; 406 (PRECONDITION-FAILED) with message "PRECONDITION_FAILED - inequivalent arg 'auto_delete' for queue '${name}' in vhost '/': received 'true' but current is 'false'`;
            await expect(async () => {
                const q = await assertChannel.createQueue(name, {
                    durable: true,
                    autoDelete: true,
                    assertionMode: AssertionMode.Assert,
                });
                await q.assert();
            }).rejects.toThrow(e);
            expect(channelErrorHandler).toHaveBeenCalled();
            expect(channelCloseHandler).toHaveBeenCalled();
        });

        it('should pass Check mode when queue exists', async () => {
            const name = uniqueName('q-check');
            await channel.createQueue(name, { durable: true, autoDelete: false });
            const checkChannel = connection.createChannel();
            const queue = await checkChannel.createQueue(name, {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Check,
            });
            await queue.assert();
            const queueDetail = await rabbitApi.queueDetail(name);
            expect(queueDetail.name).toEqual(name);
        });

        it('should pass Check mode when queue exists even with different properties declared', async () => {
            const name = uniqueName('q-check');
            await channel.createQueue(name, { durable: true, autoDelete: false });
            const checkChannel = connection.createChannel();
            const queue = await checkChannel.createQueue(name, {
                durable: true,
                autoDelete: true,
                assertionMode: AssertionMode.Check,
            });
            await queue.assert();
            const secondQueue = await checkChannel.createQueue(name, {
                durable: false,
                autoDelete: false,
                assertionMode: AssertionMode.Check,
            });
            await secondQueue.assert();
        });

        it('should throw in Check mode when queue does not exist', async () => {
            const name = uniqueName('q-check-missing');

            const checkChannel = connection.createChannel();
            const channelErrorHandler = vi.fn();
            checkChannel.on('error', channelErrorHandler);
            const channelCloseHandler = vi.fn();
            checkChannel.on('close', channelCloseHandler);

            await expect(
                checkChannel.createQueue(name, {
                    durable: true,
                    autoDelete: false,
                    assertionMode: AssertionMode.Check,
                }),
            ).rejects.toThrow();
            expect(channelErrorHandler).toHaveBeenCalled();
            expect(channelCloseHandler).toHaveBeenCalled();
        });

        it('should pass in Passive mode when queue does not exist', async () => {
            const name = uniqueName('q-passive');
            const queue = await channel.createQueue(name, {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Passive,
            });
            await queue.assert();
        });

        it('should fail for invalid Assert mode', async () => {
            const name = uniqueName('q-assert');
            await expect(
                channel.createQueue(name, {
                    durable: true,
                    autoDelete: false,
                    // @ts-expect-error on purpose
                    assertionMode: 'invalid',
                }),
            ).rejects.toThrow('Unknown assertion mode: invalid');
        });
    });

    describe('auto-delete', () => {
        it('should delete queue automatically when the last consumer disconnects', async () => {
            const name = uniqueName('q-autodelete');
            const autoDeleteConn = new ConnectionImplementation(DIRECT_OPTIONS);
            const autoDeleteChannel = autoDeleteConn.createChannel();
            const q = await autoDeleteChannel.createQueue(name, {
                durable: true,
                autoDelete: true,
            });

            // Start a consumer to make the queue "active"
            const consumer = await q.createConsumer<TestMessage>({ prefetch: 1 });
            await consumer.listen(vi.fn());

            // Close the consumer — auto-delete fires when consumers reach 0
            await consumer.close();
            await sleepPromise(PROPAGATION_DELAY);

            const status = await rabbitApi.queueDetail(name);
            expect(status).toEqual({
                error: 'Object Not Found',
                reason: 'Not Found',
            });
        });

        it('should create an exclusive queue that disappears when the connection closes', async () => {
            const name = uniqueName('q-exclusive');
            const exclusiveConn = new ConnectionImplementation(DIRECT_OPTIONS);
            const exclusiveChannel = exclusiveConn.createChannel();
            await exclusiveChannel.createQueue(name, {
                durable: false,
                exclusive: true,
            });

            const detailBefore = await rabbitApi.queueDetail(name);
            expect(detailBefore.exclusive).toBe(true);

            await exclusiveConn.close();
            await sleepPromise(PROPAGATION_DELAY);

            const status = await rabbitApi.queueDetail(name);
            expect(status).toEqual({
                error: 'Object Not Found',
                reason: 'Not Found',
            });
        });
    });

    describe('queue arguments', () => {
        it('should set x-dead-letter-exchange and x-dead-letter-routing-key in broker metadata', async () => {
            const dlqName = uniqueName('dlq');
            const mainName = uniqueName('q-dlx');
            await channel.createQueue(dlqName, { durable: true, autoDelete: false });
            await channel.createQueue(mainName, {
                durable: true,
                autoDelete: false,
                arguments: {
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': dlqName,
                },
            });

            const detail = await rabbitApi.queueDetail(mainName);
            expect(detail.arguments['x-dead-letter-exchange']).toBe('');
            expect(detail.arguments['x-dead-letter-routing-key']).toBe(dlqName);
        });

        it('should expire messages per x-message-ttl', async () => {
            const name = uniqueName('q-ttl');
            await channel.createQueue(name, {
                durable: true,
                autoDelete: false,
                arguments: { 'x-message-ttl': 500 },
            });

            await rabbitApi.publish(JSON.stringify({ value: 1 }), name);

            await sleepPromise(PROPAGATION_DELAY);
            const status = await rabbitApi.queueDetail(name);
            expect(status.messages).toBe(0);
        });
    });

    describe('queue binding to exchange', () => {
        it('should bind queue to exchange via queue.bindExchange and survive restart', async () => {
            const exchangeName = uniqueName('ex-qbind');
            const queueName = uniqueName('q-qbind');
            const exchange = await channel.createExchange(exchangeName, 'direct', { durable: true, autoDelete: false });
            const queue = await channel.createQueue(queueName);
            await queue.bindExchange(exchange, 'test-key');

            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: 'test-key',
            }));

            await compose(['down']);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await exchange.assert();
            const restartBindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: 'test-key',
            }));
        });
    });
});
