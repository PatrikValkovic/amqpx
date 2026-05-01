import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    AssertionMode,
    Channel,
    ConnectionImplementation,
    predefined,
} from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { compose, RABBIT_CONTAINER, waitForHealthy } from '../helpers/docker';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

describe('Exchange integration', () => {
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
        it('should assert predefined amqp.direct exchange', async () => {
            const exchange = predefined.directExchange(channel);
            await expect(exchange.assert()).resolves.toBeDefined();
        });

        it('should assert predefined amqp.fanout exchange', async () => {
            const exchange = predefined.fanoutExchange(channel);
            await expect(exchange.assert()).resolves.toBeDefined();
        });

        it('should assert predefined amqp.topic exchange', async () => {
            const exchange = predefined.topicExchange(channel);
            await expect(exchange.assert()).resolves.toBeDefined();
        });

        it('should assert predefined amqp.headers exchange', async () => {
            const exchange = predefined.headersExchange(channel);
            await expect(exchange.assert()).resolves.toBeDefined();
        });

        it('should route a message through the predefined amqp.direct exchange', async () => {
            const queueName = uniqueName('q-predefined-direct');
            const exchange = predefined.directExchange(channel);
            const queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue, queueName);

            await rabbitApi.publish(JSON.stringify({ value: 1 }), queueName, 'amqp.direct');
            await sleepPromise(PROPAGATION_DELAY);
            expect((await rabbitApi.queueDetail(queueName)).messages).toBe(1);

            await rabbitApi.queueDelete(queueName);
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

            await compose(['down']);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await exchange.assert();
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

            await compose(['down']);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await exchange.assert();
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

            await compose(['down']);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await exchange.assert();
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

            await compose(['down']);
            await compose(['up', '-d']);
            await waitForHealthy(RABBIT_CONTAINER);

            await exchange.assert();
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

            await compose(['down']);
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
