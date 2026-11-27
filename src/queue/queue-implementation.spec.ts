import { TestChannel, TestExchange } from '../extensions/vitest';
import { ConsumerImplementation, BatchConsumerImplementation } from '../consumer';
import { ProducerImplementation } from '../producer';
import { AssertionMode } from '../types';
import { ExchangeImplementation } from '../exchange';
import { QueueImplementation } from './queue-implementation';

const deferred = <T = void>() => {
    let resolve!: (value: T) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
};

const track = <T>(promise: Promise<T>) => {
    const tracked = { promise, settled: false };
    const settle = () => {
        tracked.settled = true;
    };
    promise.then(settle, settle);
    return tracked;
};

const flush = () => new Promise(resolve => setImmediate(resolve));

describe('QueueImplementation', () => {
    let channel: TestChannel;
    let queue: QueueImplementation;

    beforeEach(() => {
        channel = new TestChannel();
        queue = new QueueImplementation(channel, 'test-queue');
    });

    describe('assert', () => {
        it('should call assertQueue on native channel in Assert mode (default)', async () => {
            await queue.assert();

            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledWith(
                'test-queue',
                expect.anything(),
            );
            expect(channel.nativeChannel.checkQueue).not.toHaveBeenCalled();
        });

        it('should pass queue options to assertQueue', async () => {
            const durableQueue = new QueueImplementation(channel, 'test-queue', {
                durable: true,
            });
            await durableQueue.assert();

            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledWith(
                'test-queue',
                expect.objectContaining({ durable: true }),
            );
        });

        it('should call checkQueue in Check mode', async () => {
            const checkQueue = new QueueImplementation(channel, 'test-queue', {
                assertionMode: AssertionMode.Check,
            });
            await checkQueue.assert();

            expect(channel.nativeChannel.checkQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.checkQueue).toHaveBeenCalledWith('test-queue');
            expect(channel.nativeChannel.assertQueue).not.toHaveBeenCalled();
        });

        it('should skip native call in Passive mode', async () => {
            const passiveQueue = new QueueImplementation(channel, 'test-queue', {
                assertionMode: AssertionMode.Passive,
            });
            await passiveQueue.assert();

            expect(channel.nativeChannel.assertQueue).not.toHaveBeenCalled();
            expect(channel.nativeChannel.checkQueue).not.toHaveBeenCalled();
        });

        it('should only assert once even when assert() is called multiple times', async () => {
            await queue.assert();
            await queue.assert();

            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);
        });

        it('should only check once even when assert() is called multiple times', async () => {
            queue = new QueueImplementation(channel, 'test-queue', {
                assertionMode: AssertionMode.Check,
            });
            await queue.assert();
            await queue.assert();

            expect(channel.nativeChannel.checkQueue).toHaveBeenCalledTimes(1);
        });

        it('should re-assert after channel emits close', async () => {
            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);

            channel.emit('close');

            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
        });

        it('should assert during name() call', async () => {
            const result = await queue.name();

            expect(result).toBe('test-queue');
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);
        });

        it('should fail with invalid assert mode', async () => {
            queue = new QueueImplementation(channel, 'test-queue', {
                // @ts-expect-error invalid on purpose
                assertionMode: 'invalid',
            });

            await expect(
                queue.assert(),
            ).rejects.toThrow('Unknown assertion mode: invalid');
        });
    });

    describe('name', () => {
        const serverNamed = () => {
            let generated = 0;
            channel.nativeChannel.assertQueue.mockImplementation(() => Promise.resolve({
                queue: `amq.gen-${++generated}`,
                messageCount: 0,
                consumerCount: 0,
            }));
        };

        it('should return the same generated name when name() is called again before the channel closes', async () => {
            serverNamed();
            queue = new QueueImplementation(channel, '');

            expect(await queue.name()).toBe('amq.gen-1');
            expect(await queue.name()).toBe('amq.gen-1');
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledWith('', expect.anything());
        });

        it('should return a new generated name when the queue is declared again after the channel closes', async () => {
            serverNamed();
            queue = new QueueImplementation(channel, '');
            expect(await queue.name()).toBe('amq.gen-1');

            channel.emit('close');

            expect(await queue.name()).toBe('amq.gen-2');
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.assertQueue).toHaveBeenNthCalledWith(1, '', expect.anything());
            expect(channel.nativeChannel.assertQueue).toHaveBeenNthCalledWith(2, '', expect.anything());
        });

        it('should return the constructor name when the assertion mode is Check', async () => {
            queue = new QueueImplementation(channel, 'test-queue', { assertionMode: AssertionMode.Check });
            channel.nativeChannel.checkQueue.mockResolvedValueOnce({ queue: 'other', messageCount: 0, consumerCount: 0 });

            expect(await queue.name()).toBe('test-queue');
        });

        it('should return the constructor name when the assertion mode is Passive', async () => {
            queue = new QueueImplementation(channel, 'test-queue', { assertionMode: AssertionMode.Passive });

            expect(await queue.name()).toBe('test-queue');
        });

        it('should use the generated name as the producer routing key when the queue is server-named', async () => {
            serverNamed();
            queue = new QueueImplementation(channel, '');
            const producer = await queue.createProducer();

            // @ts-expect-error options is private
            expect(await producer.options.routingKey({})).toBe('amq.gen-1');
            await producer.close();
        });
    });

    describe('bindExchange', () => {
        it('should bind on the native channel with queue name, exchange name, pattern and args when args are provided', async () => {
            const exchange = new TestExchange();

            await queue.bindExchange(exchange, 'routing.key', { 'x-match': 'all' });

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith('test-queue', 'test-exchange', 'routing.key', { 'x-match': 'all' });
        });

        it('should bind on the native channel with queue name, exchange name and pattern when args are omitted', async () => {
            const exchange = new TestExchange();

            await queue.bindExchange(exchange, 'routing.key');

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith('test-queue', 'test-exchange', 'routing.key', undefined);
        });

        it('should bind only after the queue is asserted and the exchange name is resolved when bindExchange is called', async () => {
            const exchange = new TestExchange();

            await queue.bindExchange(exchange, 'routing.key');

            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(1);
            expect(exchange.name).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.assertQueue.mock.invocationCallOrder[0])
                .toBeLessThan(channel.nativeChannel.bindQueue.mock.invocationCallOrder[0] ?? 0);
        });

        it('should bind and re-bind natively without calling exchange.bindQueue when the queue is bound and re-declared', async () => {
            const exchange = new TestExchange();

            await queue.bindExchange(exchange, 'routing.key');
            channel.emit('close');
            await queue.assert();

            expect(exchange.bindQueue).not.toHaveBeenCalled();
        });

        it('should return the queue when bindExchange resolves', async () => {
            const exchange = new TestExchange();

            const result = await queue.bindExchange(exchange, 'routing.key');

            expect(result).toBe(queue);
        });

        it('should not bind again when assert() is called again without the channel closing', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);
        });

        it('should re-apply the binding when the queue is re-declared after the channel closes', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key', { 'x-match': 'all' });

            channel.emit('close');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindQueue).toHaveBeenLastCalledWith('test-queue', 'test-exchange', 'routing.key', { 'x-match': 'all' });
            expect(exchange.name).toHaveBeenCalledTimes(2);
        });

        it('should re-apply the binding when name() re-declares the queue after the channel closes', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');

            channel.emit('close');
            await queue.name();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
        });

        it('should keep assert() pending when re-applying the binding has not finished', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            const rebind = deferred();
            channel.nativeChannel.bindQueue.mockImplementationOnce(() => rebind.promise);

            channel.emit('close');
            const assert = track(queue.assert());
            await flush();

            expect(assert.settled).toBe(false);

            rebind.resolve();
            await assert.promise;
        });

        it('should re-apply the binding once and resolve all concurrent callers after it when the queue is re-declared', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            const rebind = deferred();
            channel.nativeChannel.bindQueue.mockImplementationOnce(() => rebind.promise);

            channel.emit('close');
            const first = track(queue.assert());
            const second = track(queue.assert());
            await flush();
            const third = track(queue.assert());
            const named = track(queue.name());
            await flush();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
            expect([first.settled, second.settled, third.settled, named.settled]).toEqual([false, false, false, false]);

            rebind.resolve();
            await Promise.all([first.promise, second.promise, third.promise]);
            expect(await named.promise).toBe('test-queue');
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
        });

        it('should reject all concurrent callers when re-binding fails', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            channel.nativeChannel.bindQueue.mockRejectedValueOnce(new Error('rebind failed'));

            channel.emit('close');
            const results = await Promise.allSettled([queue.assert(), queue.assert(), queue.name()]);

            expect(results.map(result => result.status)).toEqual(['rejected', 'rejected', 'rejected']);
            for (const result of results)
                expect((result as PromiseRejectedResult).reason).toEqual(new Error('rebind failed'));
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
        });

        it('should retry the declare and the binding when assert() is called after a failed re-bind', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            channel.nativeChannel.bindQueue.mockRejectedValueOnce(new Error('rebind failed'));

            channel.emit('close');
            await expect(queue.assert()).rejects.toThrow('rebind failed');
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);

            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.bindQueue).toHaveBeenLastCalledWith('test-queue', 'test-exchange', 'routing.key', undefined);

            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(3);
        });

        it('should retry the declare when assert() is called after a failed declare', async () => {
            channel.nativeChannel.assertQueue.mockRejectedValueOnce(new Error('declare failed'));

            await expect(queue.assert()).rejects.toThrow('declare failed');
            await queue.assert();
            await queue.assert();

            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
        });

        it('should keep the newer assert when an older assert fails after the channel closed', async () => {
            const declare = deferred<{ queue: string; messageCount: number; consumerCount: number }>();
            channel.nativeChannel.assertQueue.mockImplementationOnce(() => declare.promise);

            const stale = queue.assert();
            await flush();
            channel.emit('close');
            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);

            declare.reject(new Error('channel closed'));
            await expect(stale).rejects.toThrow('channel closed');

            await queue.assert();
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
        });
    });

    describe('remembered bindings', () => {
        it('should re-apply only one binding when the same binding was bound twice', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key', { a: 1, b: 2 });
            await queue.bindExchange(exchange, 'routing.key', { b: 2, a: 1 });
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);

            channel.emit('close');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(3);
        });

        it('should re-apply every binding when patterns, args or exchanges differ', async () => {
            const exchange1 = new TestExchange();
            const exchange2 = new TestExchange();
            exchange2.name.mockResolvedValue('other-exchange');
            await queue.bindExchange(exchange1, 'one');
            await queue.bindExchange(exchange1, 'two');
            await queue.bindExchange(exchange1, 'two', { type: 'x' });
            await queue.bindExchange(exchange2, 'one');
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(4);

            channel.emit('close');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(8);
            const rebindCalls = channel.nativeChannel.bindQueue.mock.calls.slice(4);
            expect(rebindCalls).toEqual(expect.arrayContaining([
                ['test-queue', 'test-exchange', 'one', undefined],
                ['test-queue', 'test-exchange', 'two', undefined],
                ['test-queue', 'test-exchange', 'two', { type: 'x' }],
                ['test-queue', 'other-exchange', 'one', undefined],
            ]));
        });

        it('should not re-apply a binding when its bind failed', async () => {
            const exchange = new TestExchange();
            channel.nativeChannel.bindQueue.mockRejectedValueOnce(new Error('bind failed'));
            await expect(queue.bindExchange(exchange, 'routing.key')).rejects.toThrow('bind failed');

            channel.emit('close');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);
        });

        it('should not re-apply a binding when the exchange could not be asserted', async () => {
            const exchange = new TestExchange();
            exchange.name.mockRejectedValueOnce(new Error('exchange missing'));
            await expect(queue.bindExchange(exchange, 'routing.key')).rejects.toThrow('exchange missing');

            channel.emit('close');
            await queue.assert();

            expect(channel.nativeChannel.bindQueue).not.toHaveBeenCalled();
        });

        it('should reject assert() when re-applying a binding fails', async () => {
            const exchange = new TestExchange();
            await queue.bindExchange(exchange, 'routing.key');
            channel.nativeChannel.bindQueue.mockRejectedValueOnce(new Error('rebind failed'));

            channel.emit('close');

            await expect(queue.assert()).rejects.toThrow('rebind failed');
        });

        it('should re-bind under the new generated name when a server-named queue is re-declared', async () => {
            let generated = 0;
            channel.nativeChannel.assertQueue.mockImplementation(() => Promise.resolve({
                queue: `amq.gen-${++generated}`,
                messageCount: 0,
                consumerCount: 0,
            }));
            queue = new QueueImplementation(channel, '');
            const exchange = new ExchangeImplementation(channel, 'my-exchange', 'fanout');

            await queue.bindExchange(exchange, '');
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.bindQueue).toHaveBeenLastCalledWith('amq.gen-1', 'my-exchange', '', undefined);

            channel.emit('close');
            expect(await queue.name()).toBe('amq.gen-2');

            // only the queue re-binds; the exchange does not keep a record of queue bindings
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindQueue).toHaveBeenLastCalledWith('amq.gen-2', 'my-exchange', '', undefined);
            expect(channel.nativeChannel.assertQueue).toHaveBeenCalledTimes(2);
            // the exchange was re-asserted on the way, because it shares the closed channel
            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(2);
        });

        it('should re-apply a binding made through exchange.bindQueue when the queue, but not the exchange, is asserted again', async () => {
            const exchange = new ExchangeImplementation(channel, 'my-exchange', 'direct');

            await exchange.bindQueue(queue, 'routing.key');
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);

            channel.emit('close');
            await exchange.assert();
            // re-asserting the exchange alone does not restore queue bindings
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(1);

            await queue.assert();
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindQueue).toHaveBeenLastCalledWith('test-queue', 'my-exchange', 'routing.key', undefined);
        });
    });

    describe('createConsumer', () => {
        it('returns ConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createConsumer();
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).not.toBe(channel);
        });

        it('returns ConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });

    describe('createBatchConsumer', () => {
        it('returns BatchConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createBatchConsumer();
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).not.toBe(channel);
        });

        it('returns BatchConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createBatchConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });

    describe('createProducer', () => {
        it('returns ProducerImplementation with new channel', async () => {
            const producer = await queue.createProducer();

            expect(producer).toBeInstanceOf(ProducerImplementation);
            expect(producer.channel).not.toBe(channel);
            await producer.close();
        });

        it('uses the channel provided in options', async () => {
            const otherChannel = new TestChannel();
            const producer = await queue.createProducer({ channel: otherChannel });

            expect(producer.channel).toBe(otherChannel);
            await producer.close();
        });

        it('should set the routing key to resolve to the queue name', async () => {
            const producer = await queue.createProducer();

            // @ts-expect-error options is private
            const key = await producer.options.routingKey({});
            expect(key).toBe('test-queue');

            await producer.close();
        });

        it('should create a confirmed channel when isConfirmed=true', async () => {
            await queue.createProducer({ isConfirmed: true });

            expect(channel.connection.createChannel).toHaveBeenCalledWith(true);
        });
    });
});
