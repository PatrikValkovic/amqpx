import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    Channel,
    ConnectionImplementation,
    Exchange,
    Producer,
    Queue,
} from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

type TestMessage = { value: number };

describe('Producer integration', () => {
    let connection: ConnectionImplementation;
    let channel: Channel;
    let exchange: Exchange;
    let exchangeName: string;
    let queue: Queue;
    let queueName: string;
    let producer: Producer<TestMessage>;

    beforeEach(async () => {
        queueName = uniqueName('producer');
        exchangeName = uniqueName('producer');
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
        channel = connection.createChannel();
        exchange = await channel.createExchange(exchangeName, 'fanout');
        queue = await channel.createQueue(queueName, { durable: true, autoDelete: false });
        await exchange.bindQueue(queue, '');
    });

    afterEach(async () => {
        await connection?.close();
    });

    describe('basic publish', () => {
        it('should publish a message and consumer receives it', async () => {
            producer = await exchange.createProducer();

            await producer.publish({ value: 1 });

            await sleepPromise(PROPAGATION_DELAY);

            const status = await rabbitApi.queueDetail(queueName);
            expect(status.messages).toBe(1);
        });

        it('should publish multiple messages', async () => {
            const producer = await exchange.createProducer();

            for (let i = 0; i < 5; i++)
                await producer.publish({ value: i });

            await sleepPromise(PROPAGATION_DELAY);

            const status = await rabbitApi.queueDetail(queueName);
            expect(status.messages).toBe(5);
        });

        it('should publish to confirmed channel', async () => {
            const confirmChannel = connection.createChannel(true);
            producer = await confirmChannel.createProducerForExchange(exchange);

            await producer.publish({ value: 1 });

            await sleepPromise(PROPAGATION_DELAY);

            const status = await rabbitApi.queueDetail(queueName);
            expect(status.messages).toBe(1);
        });
    });

    describe('publish to exchange with routing key', () => {
        it('should route message via direct exchange using a fixed routing key', async () => {
            const exchangeName = uniqueName('ex-prod');
            const boundQueueName = uniqueName('q-prod-bound');
            const exchange = await channel.createExchange(exchangeName, 'direct', {
                durable: true,
                autoDelete: false,
            });
            const boundQueue = await channel.createQueue(boundQueueName, {
                durable: true,
                autoDelete: false,
            });
            await exchange.bindQueue(boundQueue, 'route-key');

            const producer = await exchange.createProducer({
                routingKey: 'route-key',
            });
            await producer.publish({ value: 10 });

            await sleepPromise(PROPAGATION_DELAY);
            const status = await rabbitApi.queueDetail(boundQueueName);
            expect(status.messages).toBe(1);
        });

        it('should route message via direct exchange using a message routing key', async () => {
            const exchangeName = uniqueName('ex-prod');
            const boundQueueName = uniqueName('q-prod-bound');
            const exchange = await channel.createExchange(exchangeName, 'direct', {
                durable: true,
                autoDelete: false,
            });
            const boundQueue = await channel.createQueue(boundQueueName, {
                durable: true,
                autoDelete: false,
            });
            await exchange.bindQueue(boundQueue, 'route-key');

            const producer = await exchange.createProducer();
            await producer.publish({ value: 10 }, 'route-key');

            await sleepPromise(PROPAGATION_DELAY);
            const status = await rabbitApi.queueDetail(boundQueueName);
            expect(status.messages).toBe(1);
        });

        it('should derive routing key dynamically from message content', async () => {
            const exchangeName = uniqueName('ex-dynkey');
            const q1 = uniqueName('q-dynkey-1');
            const q2 = uniqueName('q-dynkey-2');
            const exchange = await channel.createExchange(exchangeName, 'direct', { durable: true, autoDelete: false });
            const queue1 = await channel.createQueue(q1, { durable: true, autoDelete: false });
            const queue2 = await channel.createQueue(q2, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue1, 'key-1');
            await exchange.bindQueue(queue2, 'key-2');

            const producer = await exchange.createProducer<TestMessage>({
                routingKey: msg => `key-${msg.value}`,
            });
            await producer.publish({ value: 1 });
            await producer.publish({ value: 2 });

            await sleepPromise(PROPAGATION_DELAY);
            const q1Status = await rabbitApi.queueDetail(q1);
            const q2Status = await rabbitApi.queueDetail(q1);
            expect(q1Status.messages).toBe(1);
            expect(q2Status.messages).toBe(1);
        });
    });

    describe('publish to fanout exchange', () => {
        it('should deliver message to all bound queues', async () => {
            const exchangeName = uniqueName('ex-fanout-prod');
            const q1 = uniqueName('q-fan-1');
            const q2 = uniqueName('q-fan-2');
            const exchange = await channel.createExchange(exchangeName, 'fanout', { durable: true, autoDelete: false });
            const queue1 = await channel.createQueue(q1, { durable: true, autoDelete: false });
            const queue2 = await channel.createQueue(q2, { durable: true, autoDelete: false });
            await exchange.bindQueue(queue1, '');
            await exchange.bindQueue(queue2, '');

            const producer = await exchange.createProducer();
            await producer.publish({ value: 42 });

            await sleepPromise(PROPAGATION_DELAY);
            const q1Status = await rabbitApi.queueDetail(q1);
            const q2Status = await rabbitApi.queueDetail(q2);
            expect(q1Status.messages).toBe(1);
            expect(q2Status.messages).toBe(1);
        });
    });

    describe('beforeSend / afterSend events', () => {
        it('should emit beforeSend and afterSend with message and buffer', async () => {
            const producer = await queue.createProducer();
            const beforeSend = vi.fn();
            const afterSend = vi.fn();
            producer.on('beforeSend', beforeSend);
            producer.on('afterSend', afterSend);

            await producer.publish({ value: 7 });

            expect(beforeSend).toHaveBeenCalledTimes(1);
            expect(afterSend).toHaveBeenCalledTimes(1);
            const [beforeMsg, beforeBuf] = beforeSend.mock.calls[0]!;
            expect(beforeMsg).toEqual({ value: 7 });
            expect(beforeBuf).toBeInstanceOf(Buffer);
            const [afterMsg, afterBuf] = afterSend.mock.calls[0]!;
            expect(afterMsg).toEqual({ value: 7 });
            expect(afterBuf).toBeInstanceOf(Buffer);
        });

        it('should emit beforeSend before afterSend for each publish call', async () => {
            const producer = await queue.createProducer();
            const order: string[] = [];
            producer.on('beforeSend', () => order.push('before'));
            producer.on('afterSend', () => order.push('after'));

            await producer.publish({ value: 1 });
            await producer.publish({ value: 2 });

            expect(order).toEqual(['before', 'after', 'before', 'after']);
        });
    });

    describe('custom serialization', () => {
        it('should serialize message with custom function', async () => {
            type FlatMsg = { id: string; count: number };
            const flatQueueName = uniqueName('q-custom-ser');
            const flatQueue = await channel.createQueue(flatQueueName, { durable: true, autoDelete: false });

            const producer = await flatQueue.createProducer<FlatMsg>({
                stringifyMessage: msg => Buffer.from(`id=${msg.id};count=${msg.count}`),
            });
            await producer.publish({ id: 'abc', count: 5 });

            await sleepPromise(PROPAGATION_DELAY);
            const status = await rabbitApi.queueDetail(flatQueueName);
            expect(status.messages).toBe(1);
            const [msg] = await rabbitApi.queueGet(flatQueueName);
            expect(msg.payload).toEqual('id=abc;count=5');
        });
    });

    describe('reconnect', () => {

    });
});
