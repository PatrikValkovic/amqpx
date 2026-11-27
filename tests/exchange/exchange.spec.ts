import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    AssertionMode,
    BatchConsumer,
    Channel,
    ConnectionImplementation,
    Consumer,
    predefined,
} from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { compose, destroyContainer, RABBIT_CONTAINER, restartContainer, waitForHealthy } from '../helpers/docker';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

type TestMessage = { value: number };

describe('Exchange integration', () => {
    let connection: ConnectionImplementation;
    let channel: Channel;
    let consumers: Array<Consumer<TestMessage> | BatchConsumer<TestMessage>>;

    beforeEach(() => {
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
        channel = connection.createChannel();
        consumers = [];
    });

    afterEach(async () => {
        for (const consumer of consumers)
            await consumer.close();
        await connection?.close();
    });

    describe('assertion modes', () => {
        it('should assert exchange in Assert mode', async () => {
            const name = uniqueName('ex-assert');
            const exchange = await channel.createExchange(name, 'direct', {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Assert,
            });
            expect(await exchange.name()).toBe(name);
            const exchangeDetail = await rabbitApi.exchangeDetail(name);
            expect(exchangeDetail.name).toEqual(name);
        });

        it('should throw error when in Assert mode and exchange exists with different properties', async () => {
            const name = uniqueName('ex-assert');
            const exchange = await channel.createExchange(name, 'fanout', {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Assert,
            });
            await exchange.assert();

            const assertChannel = connection.createChannel();
            const channelErrorHandler = vi.fn();
            assertChannel.on('error', channelErrorHandler);
            const channelCloseHandler = vi.fn();
            assertChannel.on('close', channelCloseHandler);

            const e = `Operation failed: ExchangeDeclare; 406 (PRECONDITION-FAILED) with message "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '${name}' in vhost '/': received 'direct' but current is 'fanout'`;
            await expect(async () => {
                const exchange = await assertChannel.createExchange(name, 'direct', {
                    durable: true,
                    autoDelete: false,
                    assertionMode: AssertionMode.Assert,
                });
                await exchange.assert();
            }).rejects.toThrow(e);
            expect(channelErrorHandler).toHaveBeenCalled();
            expect(channelCloseHandler).toHaveBeenCalled();
        });

        it('should pass Check mode when exchange exists', async () => {
            const name = uniqueName('ex-check');
            await channel.createExchange(name, 'direct', {
                durable: true,
                autoDelete: false,
            });
            const checkChannel = connection.createChannel();
            const exchange = await checkChannel.createExchange(name, 'direct', {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Check,
            });
            expect(await exchange.name()).toBe(name);
            const exchangeDetail = await rabbitApi.exchangeDetail(name);
            expect(exchangeDetail.name).toEqual(name);
        });

        it('should pass Check mode when exchange exists even with different type', async () => {
            const name = uniqueName('ex-check');
            await channel.createExchange(name, 'fanout', {
                durable: true,
                autoDelete: false,
            });
            const checkChannel = connection.createChannel();
            const exchange = await checkChannel.createExchange(name, 'direct', {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Check,
            });
            expect(await exchange.name()).toBe(name);
            const exchangeDetail = await rabbitApi.exchangeDetail(name);
            expect(exchangeDetail.name).toEqual(name);
        });

        it('should throw in Check mode when exchange does not exist', async () => {
            const name = uniqueName('ex-check-missing');

            const checkChannel = connection.createChannel();
            const channelErrorHandler = vi.fn();
            checkChannel.on('error', channelErrorHandler);
            const channelCloseHandler = vi.fn();
            checkChannel.on('close', channelCloseHandler);

            await expect(
                checkChannel.createExchange(name, 'direct', {
                    durable: true,
                    autoDelete: false,
                    assertionMode: AssertionMode.Check,
                }),
            ).rejects.toThrow();
            expect(channelErrorHandler).toHaveBeenCalled();
            expect(channelCloseHandler).toHaveBeenCalled();
        });

        it('should pass in Passive mode when exchange does not exist', async () => {
            const name = uniqueName('ex-check-missing');
            const exchange = await channel.createExchange(name, 'direct', {
                durable: true,
                autoDelete: false,
                assertionMode: AssertionMode.Passive,
            });
            await exchange.assert();
        });

        it('should fail for invalid Assert mode', async () => {
            const name = uniqueName('ex-assert');
            await expect(
                channel.createExchange(name, 'direct', {
                    durable: true,
                    autoDelete: false,
                    // @ts-expect-error on purpose
                    assertionMode: 'invalid',
                }),
            ).rejects.toThrow('Unknown assertion mode: invalid');
        });
    });

    describe('exchange types', () => {
        it('should create a fanout exchange', async () => {
            const name = uniqueName('ex-fanout');
            const exchange = await channel.createExchange(name, 'fanout', { durable: true, autoDelete: false });
            expect(await exchange.name()).toBe(name);
            const status = await rabbitApi.exchangeDetail(name);
            expect(status.name).toBe(name);
            expect(status.type).toBe('fanout');
        });

        it('should create a topic exchange', async () => {
            const name = uniqueName('ex-topic');
            const exchange = await channel.createExchange(name, 'topic', { durable: true, autoDelete: false });
            expect(await exchange.name()).toBe(name);
            const status = await rabbitApi.exchangeDetail(name);
            expect(status.name).toBe(name);
            expect(status.type).toBe('topic');
        });

        it('should create a headers exchange', async () => {
            const name = uniqueName('ex-headers');
            const exchange = await channel.createExchange(name, 'headers', { durable: true, autoDelete: false });
            expect(await exchange.name()).toBe(name);
            const status = await rabbitApi.exchangeDetail(name);
            expect(status.name).toBe(name);
            expect(status.type).toBe('headers');
        });
    });

    describe('predefined exchanges', () => {
        it.each([
            ['directExchange', 'amq.direct', 'direct', predefined.directExchange],
            ['fanoutExchange', 'amq.fanout', 'fanout', predefined.fanoutExchange],
            ['headersExchange', 'amq.headers', 'headers', predefined.headersExchange],
            ['matchExchange', 'amq.match', 'headers', predefined.matchExchange],
            ['topicExchange', 'amq.topic', 'topic', predefined.topicExchange],
        ] as const)('should resolve %s to the broker built-in %s exchange of type %s', async (_fn, expectedName, expectedType, factory) => {
            const exchange = factory(channel);
            await expect(exchange.assert()).resolves.toBe(exchange);
            expect(await exchange.name()).toBe(expectedName);

            const detail = await rabbitApi.exchangeDetail(expectedName);
            expect(detail).toEqual(expect.objectContaining({
                name: expectedName,
                type: expectedType,
                durable: true,
            }));
        });

        it('should route a message through the predefined amq.direct exchange', async () => {
            const queueName = uniqueName('q-predefined-direct');
            const exchange = predefined.directExchange(channel);
            const queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue, queueName);

            const result = await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName, 'amq.direct');
            expect(result.routed).toBe(true);
            await sleepPromise(PROPAGATION_DELAY);
            expect((await rabbitApi.queueDetail(queueName)).messages).toBe(1);

            await rabbitApi.queueDelete(queueName);
        });
    });

    describe('createConsumer', () => {
        it('should receive a published message on a dedicated consumer channel', async () => {
            const exchangeName = uniqueName('ex-consumer');
            const exchange = await channel.createExchange(exchangeName, 'direct', { durable: false, autoDelete: true });
            const consumer = await exchange.createConsumer<TestMessage>({ pattern: 'my-key' });
            consumers.push(consumer);
            expect(consumer.channel).not.toBe(channel);

            const handler = vi.fn();
            await consumer.listen(({ message }) => handler(message));
            expect(await consumer.queue.name()).toMatch(/^amq\.gen-/);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), 'my-key', exchangeName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 1 }), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledTimes(1);
        });

        it('should give each consumer its own queue and deliver to both via fanout', async () => {
            const exchangeName = uniqueName('ex-consumer-fanout');
            const exchange = await channel.createExchange(exchangeName, 'fanout', { durable: false, autoDelete: true });
            const consumer1 = await exchange.createConsumer<TestMessage>({ pattern: '' });
            consumers.push(consumer1);
            const consumer2 = await exchange.createConsumer<TestMessage>({ pattern: '' });
            consumers.push(consumer2);

            const handler1 = vi.fn();
            const handler2 = vi.fn();
            await consumer1.listen(({ message }) => handler1(message));
            await consumer2.listen(({ message }) => handler2(message));

            const [queueName1, queueName2] = await Promise.all([consumer1.queue.name(), consumer2.queue.name()]);
            expect(queueName1).toMatch(/^amq\.gen-/);
            expect(queueName2).toMatch(/^amq\.gen-/);
            expect(queueName1).not.toBe(queueName2);

            await rabbitApi.publish(JSON.stringify({ value: 7 }), '', exchangeName);
            await vi.waitFor(() => {
                expect(handler1).toHaveBeenCalledWith({ value: 7 });
                expect(handler2).toHaveBeenCalledWith({ value: 7 });
            }, { timeout: 10_000 });
            expect(handler1).toHaveBeenCalledTimes(1);
            expect(handler2).toHaveBeenCalledTimes(1);
        });

        it('should keep receiving after broker restart with a re-declared and re-bound queue', async () => {
            const exchangeName = uniqueName('ex-consumer-restart');
            const exchange = await channel.createExchange(exchangeName, 'fanout', { durable: true, autoDelete: false });
            const consumer = await exchange.createConsumer<TestMessage>({ pattern: '' });
            consumers.push(consumer);

            const handler = vi.fn();
            await consumer.listen(({ message }) => handler(message));
            const queueNameBefore = await consumer.queue.name();

            await rabbitApi.publish(JSON.stringify({ value: 1 }), '', exchangeName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 1 }), { timeout: 10_000 });

            await restartContainer(RABBIT_CONTAINER);
            await waitForHealthy(RABBIT_CONTAINER);

            // wait until the consumer re-declared its exclusive queue and re-bound it to the exchange
            await vi.waitFor(async () => {
                const bindings = await rabbitApi.exchangeBindings(exchangeName);
                expect(bindings).toContainEqual(expect.objectContaining({ destination_type: 'queue' }));
            }, { timeout: 30_000, interval: 500 });

            const queueNameAfter = await consumer.queue.name();
            expect(queueNameAfter).toMatch(/^amq\.gen-/);
            expect(queueNameAfter).not.toBe(queueNameBefore);

            await rabbitApi.publish(JSON.stringify({ value: 2 }), '', exchangeName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 2 }), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledTimes(2);

            await sleepPromise(PROPAGATION_DELAY);
            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toEqual([expect.objectContaining({
                destination: queueNameAfter,
                destination_type: 'queue',
                routing_key: '',
            })]);
        }, 120_000);
    });

    describe('createBatchConsumer', () => {
        it('should receive a published message on a dedicated consumer channel', async () => {
            const exchangeName = uniqueName('ex-batch-consumer');
            const exchange = await channel.createExchange(exchangeName, 'direct', { durable: false, autoDelete: true });
            const consumer = await exchange.createBatchConsumer<TestMessage>({
                pattern: 'my-key',
                batchSize: 2,
                maxWaitTimeForBatch: 500,
            });
            consumers.push(consumer);
            expect(consumer.channel).not.toBe(channel);

            const handler = vi.fn();
            await consumer.listen(({ messages }) => handler(messages.map(m => m.message)));
            expect(await consumer.queue.name()).toMatch(/^amq\.gen-/);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), 'my-key', exchangeName);
            await rabbitApi.publish(JSON.stringify({ value: 2 }), 'my-key', exchangeName);
            await vi.waitFor(() => expect(handler.mock.calls.flatMap(([messages]) => messages)).toEqual([
                { value: 1 },
                { value: 2 },
            ]), { timeout: 10_000 });
        });
    });

    describe('connection.createConsumerForExchange', () => {
        it('should receive a published message on a dedicated consumer channel', async () => {
            const exchangeName = uniqueName('ex-conn-consumer');
            const exchange = await channel.createExchange(exchangeName, 'topic', { durable: false, autoDelete: true });
            const consumer = await connection.createConsumerForExchange<TestMessage>(exchange, { pattern: 'orders.*' });
            consumers.push(consumer);
            expect(consumer.channel).not.toBe(channel);

            const handler = vi.fn();
            await consumer.listen(({ message }) => handler(message));
            expect(await consumer.queue.name()).toMatch(/^amq\.gen-/);

            await rabbitApi.publish(JSON.stringify({ value: 3 }), 'orders.created', exchangeName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 3 }), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledTimes(1);
        });

        it('should receive a published message with a batch consumer', async () => {
            const exchangeName = uniqueName('ex-conn-batch-consumer');
            const exchange = await channel.createExchange(exchangeName, 'topic', { durable: false, autoDelete: true });
            const consumer = await connection.createBatchConsumerForExchange<TestMessage>(exchange, {
                pattern: 'orders.*',
                batchSize: 1,
                maxWaitTimeForBatch: 500,
            });
            consumers.push(consumer);
            expect(consumer.channel).not.toBe(channel);

            const handler = vi.fn();
            await consumer.listen(({ messages }) => handler(messages.map(m => m.message)));
            expect(await consumer.queue.name()).toMatch(/^amq\.gen-/);

            await rabbitApi.publish(JSON.stringify({ value: 4 }), 'orders.created', exchangeName);
            await vi.waitFor(() => expect(handler).toHaveBeenCalledWith([{ value: 4 }]), { timeout: 10_000 });
            expect(handler).toHaveBeenCalledTimes(1);
        });
    });

    describe('queue binding', () => {
        it('should route message to queue bound with matching routing key on a direct exchange', async () => {
            const exchangeName = uniqueName('ex-direct-bind');
            const q1 = uniqueName('q1-direct-bind');
            const q2 = uniqueName('q2-direct-bind');
            const exchange = await channel.createExchange(exchangeName, 'direct', { durable: true, autoDelete: false });
            const queue1 = await channel.createQueue(q1, { durable: true, autoDelete: false });
            const queue2 = await channel.createQueue(q2, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue1, 'my-key');
            await exchange.bindQueue(queue2, 'second-key');

            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: 'my-key',
            }));
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: 'second-key',
            }));

            await destroyContainer(RABBIT_CONTAINER);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            // queues own their bindings: re-declaring them re-asserts the exchange and re-binds
            await queue1.assert();
            await queue2.assert();
            const restartBindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: 'my-key',
            }));
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: 'second-key',
            }));
        });

        it('should route to all bound queues via fanout exchange', async () => {
            const exchangeName = uniqueName('ex-fanout-bind');
            const q1 = uniqueName('q-fan1');
            const q2 = uniqueName('q-fan2');
            const exchange = await channel.createExchange(exchangeName, 'fanout', { durable: true, autoDelete: false });
            const queue1 = await channel.createQueue(q1, { durable: true, autoDelete: false });
            const queue2 = await channel.createQueue(q2, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue1, '');
            await exchange.bindQueue(queue2, '');

            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: '',
            }));
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: '',
            }));

            await destroyContainer(RABBIT_CONTAINER);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            // queues own their bindings: re-declaring them re-asserts the exchange and re-binds
            await queue1.assert();
            await queue2.assert();
            const restartBindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: '',
            }));
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: '',
            }));
        });

        it('should route message to queue bound using headers exchange', async () => {
            const exchangeName = uniqueName('ex-headers-bind');
            const q1 = uniqueName('q1-headers-bind');
            const q2 = uniqueName('q-headers-bind');
            const exchange = await channel.createExchange(exchangeName, 'headers', { durable: true, autoDelete: false });
            const queue1 = await channel.createQueue(q1, { durable: true, autoDelete: false });
            const queue2 = await channel.createQueue(q2, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue1, '', {
                type: 'priority',
            });
            await exchange.bindQueue(queue2, '', {
                type: 'normal',
            });

            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'priority',
                },
            }));
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'normal',
                },
            }));

            await destroyContainer(RABBIT_CONTAINER);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            // queues own their bindings: re-declaring them re-asserts the exchange and re-binds
            await queue1.assert();
            await queue2.assert();
            const restartBindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q1,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'priority',
                },
            }));
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: q2,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'normal',
                },
            }));
        });

        it('should route message to same queue bound using headers exchange', async () => {
            const exchangeName = uniqueName('ex-headers-bind');
            const queueName = uniqueName('q1-headers-bind');
            const exchange = await channel.createExchange(exchangeName, 'headers', { durable: true, autoDelete: false });
            const queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue, '', {
                type: 'priority',
            });
            await exchange.bindQueue(queue, '', {
                type: 'normal',
            });

            const bindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'priority',
                },
            }));
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'normal',
                },
            }));

            await destroyContainer(RABBIT_CONTAINER);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            // the queue owns its bindings: re-declaring it re-asserts the exchange and re-binds
            await queue.assert();
            const restartBindings = await rabbitApi.exchangeBindings(exchangeName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'priority',
                },
            }));
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: queueName,
                destination_type: 'queue',
                routing_key: '',
                arguments: {
                    type: 'normal',
                },
            }));
        });
    });

    describe('exchange-to-exchange binding', () => {
        it('should forward messages from source exchange to destination exchange and into queue', async () => {
            const srcName = uniqueName('ex-src');
            const destName = uniqueName('ex-dest');
            const srcExchange = await channel.createExchange(srcName, 'fanout', { durable: true, autoDelete: false });
            const destExchange = await channel.createExchange(destName, 'fanout');
            await srcExchange.bindExchange(destExchange, '');

            const bindings = await rabbitApi.exchangeBindings(srcName);
            expect(bindings).toContainEqual(expect.objectContaining({
                destination: destName,
                destination_type: 'exchange',
                routing_key: '',
            }));

            await destroyContainer(RABBIT_CONTAINER);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await srcExchange.assert();
            const restartBindings = await rabbitApi.exchangeBindings(srcName);
            expect(restartBindings).toContainEqual(expect.objectContaining({
                destination: destName,
                destination_type: 'exchange',
                routing_key: '',
            }));
        });
    });
});
