import { describe, expect, afterEach, beforeEach, vi } from 'vitest';
import * as amqp from 'amqplib';
import { BatchConsumer, ConnectionImplementation, ConnectionState, Consumer, Producer, predefined } from '../../src';
import { DIRECT_OPTIONS, PROXIED_OPTIONS } from '../helpers/broker-urls';
import { compose, destroyContainer, RABBIT_CONTAINER, restartContainer, stopContainer, waitForHealthy } from '../helpers/docker';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { withToxic } from '../helpers/toxiproxy';
import { sleepPromise } from '../../src/utils';

const connectRef = vi.hoisted(() => ({
    fn: null as ((...args: Parameters<typeof import('amqplib').connect>) => ReturnType<typeof import('amqplib').connect>) | null,
}));

vi.mock('amqplib', async importOriginal => {
    const actual = await importOriginal<typeof import('amqplib')>();
    connectRef.fn = (...args) => actual.connect(...args);
    return {
        ...actual,
        connect: vi.fn((...args: Parameters<typeof actual.connect>) => connectRef.fn!(...args)),
    };
});

type TestMessage = { value: number };

describe('Connection integration', () => {
    let conn: ConnectionImplementation;

    afterEach(async () => {
        vi.mocked(amqp.connect).mockImplementation((...args) => connectRef.fn!(...args));
        await Promise.race([
            conn?.close().catch(() => { /* already closed or never opened */ }),
            sleepPromise(100),
        ]);
    });

    describe('connect', () => {
        it('transitions to connected state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.connect();
            expect(conn.state()).toBe(ConnectionState.connected);
            const native = await conn.native();
            expect(native).toBeDefined();
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        it('concurrent connect() calls coalesce to single attempt', async () => {
            vi.mocked(amqp.connect).mockClear();
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await Promise.all([conn.connect(), conn.connect()]);
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });

        it('calling native() should establish the connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.native();
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        it('calling connect() after close() should establish new connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.close();
            await conn.connect();
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });
    });

    describe('close', () => {
        it('transitions to closed state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closePromise = conn.close();
            expect(conn.state()).toBe(ConnectionState.closing);
            await closePromise;
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        it('emits close event', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closeHandlerMock = vi.fn();
            conn.on('close', closeHandlerMock);
            await conn.close();
            expect(closeHandlerMock).toHaveBeenCalledTimes(1);
        });

        it('close() on preconnect state is a no-op', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.preconnect);
        });

        it('concurrent close() calls coalesce to single teardown', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const native = await conn.native();
            const closeSpy = vi.spyOn(native, 'close');
            await Promise.all([conn.close(), conn.close()]);
            expect(conn.state()).toBe(ConnectionState.closed);
            expect(closeSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('reconnect', () => {
        it('emits reconnecting event when server drops connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
        });

        it('re-establishes connection and emits connected event after drop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectedMock = vi.fn();
            conn.once('connected', reconnectedMock);
            await restartContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectedMock).toHaveBeenCalledTimes(1), { timeout: 60_000 });
            expect(conn.state()).toBe(ConnectionState.connected);
        });

        it('close during reconnection stops retry loop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        it('emits connectionRetryExhausted when all retries fail', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS, { maxRetries: 1, reconnectionTimeoutMs: 0 });
            await conn.connect();
            const exhaustedMock = vi.fn();
            conn.once('connectionRetryExhausted', exhaustedMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(exhaustedMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            expect(conn.state()).toBe(ConnectionState.closed);
        });
    });

    describe('topology', () => {
        let consumers: Array<Consumer<TestMessage> | BatchConsumer<TestMessage>>;
        let producers: Array<Producer<TestMessage>>;

        beforeEach(() => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            consumers = [];
            producers = [];
        });

        afterEach(async () => {
            for (const consumer of consumers)
                await consumer.close();
            for (const producer of producers)
                await producer.close();
        });

        describe('createQueue', () => {
            it('should deliver a message when producer and consumer are created from a connection-declared queue', async () => {
                const queue = await conn.createQueue(uniqueName('conn-q'), { durable: false, exclusive: true });
                const consumer = await queue.createConsumer<TestMessage>();
                consumers.push(consumer);
                const producer = await queue.createProducer<TestMessage>();
                producers.push(producer);

                const handler = vi.fn();
                await consumer.listen(({ message }) => handler(message));
                await producer.publish({ value: 1 });

                await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 1 }), { timeout: 10_000 });
                expect(consumer.channel).not.toBe(producer.channel);
            });

            it('should deliver a message when the consumer is created through the connection', async () => {
                const queue = await conn.createQueue(uniqueName('conn-q'), { durable: false, exclusive: true });
                const consumer = await conn.createConsumerForQueue<TestMessage>(queue);
                consumers.push(consumer);
                const producer = await conn.createProducerForQueue<TestMessage>(queue);
                producers.push(producer);

                const handler = vi.fn();
                await consumer.listen(({ message }) => handler(message));
                await producer.publish({ value: 2 });

                await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 2 }), { timeout: 10_000 });
            });

            it('should declare the queue on the broker when called', async () => {
                const name = uniqueName('conn-q-declared');
                const queue = await conn.createQueue(name, { durable: true, autoDelete: false });

                expect(await queue.name()).toBe(name);
                const detail = await rabbitApi.queueDetail(name);
                expect(detail.name).toBe(name);
                await rabbitApi.queueDelete(name);
            });
        });

        describe('createExchange', () => {
            it('should route by routing key when the queue is bound to a connection-declared exchange', async () => {
                const exchange = await conn.createExchange(uniqueName('conn-ex'), 'direct', { durable: false, autoDelete: true });
                const matching = await conn.createQueue(uniqueName('conn-q-match'), { durable: false, exclusive: true });
                const other = await conn.createQueue(uniqueName('conn-q-other'), { durable: false, exclusive: true });
                await matching.bindExchange(exchange, 'orders.created');
                await other.bindExchange(exchange, 'orders.deleted');

                const matchingHandler = vi.fn();
                const otherHandler = vi.fn();
                const matchingConsumer = await matching.createConsumer<TestMessage>();
                const otherConsumer = await other.createConsumer<TestMessage>();
                consumers.push(matchingConsumer, otherConsumer);
                await matchingConsumer.listen(({ message }) => matchingHandler(message));
                await otherConsumer.listen(({ message }) => otherHandler(message));

                const producer = await exchange.createProducer<TestMessage>({ routingKey: 'orders.created' });
                producers.push(producer);
                await producer.publish({ value: 3 });

                await vi.waitFor(() => expect(matchingHandler).toHaveBeenCalledWith({ value: 3 }), { timeout: 10_000 });
                await sleepPromise(500);
                expect(otherHandler).not.toHaveBeenCalled();
            });

            it('should deliver a message when the consumer is created from a connection-declared exchange', async () => {
                const exchange = await conn.createExchange(uniqueName('conn-ex-consumer'), 'topic', { durable: false, autoDelete: true });
                const consumer = await exchange.createConsumer<TestMessage>({ pattern: 'orders.*' });
                consumers.push(consumer);
                const producer = await exchange.createProducer<TestMessage>({ routingKey: 'orders.created' });
                producers.push(producer);

                const handler = vi.fn();
                await consumer.listen(({ message }) => handler(message));
                expect(await consumer.queue.name()).toMatch(/^amq\.gen-/);
                await producer.publish({ value: 4 });

                await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 4 }), { timeout: 10_000 });
                expect(consumer.channel).not.toBe(producer.channel);
            });

            it('should declare the exchange on the broker when called', async () => {
                const name = uniqueName('conn-ex-declared');
                const exchange = await conn.createExchange(name, 'fanout', { durable: false, autoDelete: true });

                expect(await exchange.name()).toBe(name);
                const detail = await rabbitApi.exchangeDetail(name);
                expect(detail).toEqual(expect.objectContaining({ name, type: 'fanout' }));
            });
        });

        describe('checkQueue', () => {
            it('should resolve the queue metadata when the queue exists', async () => {
                const name = uniqueName('conn-q-check');
                await conn.createQueue(name, { durable: false, autoDelete: false, exclusive: true });
                await rabbitApi.publish(JSON.stringify({ value: 1 }), name);

                const reply = await conn.checkQueue(name);

                expect(reply).toEqual({ queue: name, messageCount: 1, consumerCount: 0 });
            });

            it('should reject when the queue does not exist and keep later topology calls working', async () => {
                const errorHandler = vi.fn();
                conn.on('error', errorHandler);
                const declared = await conn.createQueue(uniqueName('conn-q-before'), { durable: false, exclusive: true });
                const missing = uniqueName('conn-q-missing');

                await expect(conn.checkQueue(missing)).rejects.toThrow(/NOT_FOUND/);

                const name = uniqueName('conn-q-after');
                const queue = await conn.createQueue(name, { durable: false, exclusive: true });
                expect(await queue.name()).toBe(name);
                await expect(conn.checkQueue(name)).resolves.toEqual(expect.objectContaining({ queue: name }));
                // queues declared earlier re-assert on the next use
                const declaredName = await declared.name();
                await expect(conn.checkQueue(declaredName)).resolves.toEqual(expect.objectContaining({ queue: declaredName }));
                expect(errorHandler).not.toHaveBeenCalled();
                expect(conn.state()).toBe(ConnectionState.connected);
            });
        });

        describe('broker restart', () => {
            it('should keep topology calls working when the broker restarts with topology kept', async () => {
                const queueName = uniqueName('conn-q-restart');
                await conn.createQueue(queueName, { durable: true, autoDelete: false });

                await restartContainer(RABBIT_CONTAINER);
                await waitForHealthy(RABBIT_CONTAINER);

                await vi.waitFor(() => expect(conn.checkQueue(queueName)).resolves.toEqual(expect.objectContaining({ queue: queueName })), { timeout: 60_000, interval: 1_000 });
                const exchangeName = uniqueName('conn-ex-restart');
                const exchange = await conn.createExchange(exchangeName, 'direct', { durable: false, autoDelete: true });
                expect(await exchange.name()).toBe(exchangeName);
                await rabbitApi.queueDelete(queueName);
            }, 120_000);

            it('should re-declare queues, exchanges and bindings when the broker loses topology', async () => {
                const exchangeName = uniqueName('conn-ex-destroy');
                const queueName = uniqueName('conn-q-destroy');
                const exchange = await conn.createExchange(exchangeName, 'direct', { durable: true, autoDelete: false });
                const queue = await conn.createQueue(queueName, { durable: true, autoDelete: false });
                await queue.bindExchange(exchange, 'my-key');

                await destroyContainer(RABBIT_CONTAINER);
                await compose(['up', '-d']);
                await waitForHealthy(RABBIT_CONTAINER);

                await vi.waitFor(() => expect(queue.assert()).resolves.toBe(queue), { timeout: 60_000, interval: 1_000 });
                await expect(conn.checkQueue(queueName)).resolves.toEqual(expect.objectContaining({ queue: queueName }));
                const bindings = await rabbitApi.exchangeBindings(exchangeName);
                expect(bindings).toContainEqual(expect.objectContaining({
                    destination: queueName,
                    destination_type: 'queue',
                    routing_key: 'my-key',
                }));

                const consumer = await queue.createConsumer<TestMessage>();
                consumers.push(consumer);
                const producer = await exchange.createProducer<TestMessage>({ routingKey: 'my-key' });
                producers.push(producer);
                const handler = vi.fn();
                await consumer.listen(({ message }) => handler(message));
                await producer.publish({ value: 5 });
                await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 5 }), { timeout: 10_000 });
            }, 120_000);
        });

        describe('predefined', () => {
            it.each([
                ['directExchange', 'amq.direct', 'direct', predefined.directExchange],
                ['fanoutExchange', 'amq.fanout', 'fanout', predefined.fanoutExchange],
                ['headersExchange', 'amq.headers', 'headers', predefined.headersExchange],
                ['matchExchange', 'amq.match', 'headers', predefined.matchExchange],
                ['topicExchange', 'amq.topic', 'topic', predefined.topicExchange],
            ] as const)('should resolve %s to the broker built-in %s exchange of type %s when given a connection', async (_fn, expectedName, expectedType, factory) => {
                const exchange = await factory(conn);

                expect(await exchange.name()).toBe(expectedName);
                const detail = await rabbitApi.exchangeDetail(expectedName);
                expect(detail).toEqual(expect.objectContaining({
                    name: expectedName,
                    type: expectedType,
                    durable: true,
                }));
            });

            it('should deliver through amq.direct when the exchange comes from a connection', async () => {
                const routingKey = uniqueName('conn-amq-direct');
                const exchange = await predefined.directExchange(conn);
                const queue = await conn.createQueue(uniqueName('conn-q-amq'), { durable: false, exclusive: true });
                await queue.bindExchange(exchange, routingKey);
                const consumer = await queue.createConsumer<TestMessage>();
                consumers.push(consumer);
                const producer = await exchange.createProducer<TestMessage>({ routingKey });
                producers.push(producer);

                const handler = vi.fn();
                await consumer.listen(({ message }) => handler(message));
                await producer.publish({ value: 6 });

                await vi.waitFor(() => expect(handler).toHaveBeenCalledWith({ value: 6 }), { timeout: 10_000 });
            });

            it('should resolve the default exchange with an empty name when given a connection', async () => {
                const exchange = await predefined.defaultExchange(conn);

                expect(await exchange.name()).toBe('');
            });
        });
    });

    describe('networking', () => {
        it('connects successfully with 50ms upstream and 50ms downstream latency', async () => {
            conn = new ConnectionImplementation(PROXIED_OPTIONS);
            await withToxic('rabbit', { type: 'latency', stream: 'upstream', toxicity: 1, attributes: { latency: 50 } }, () =>
                withToxic('rabbit', { type: 'latency', stream: 'downstream', toxicity: 1, attributes: { latency: 50 } }, async () => {
                    await conn.connect();
                    expect(conn.state()).toBe(ConnectionState.connected);
                }),
            );
        });
    });
});
