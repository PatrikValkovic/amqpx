import { TestChannel, TestExchange, TestQueue } from '../extensions/vitest';
import { ProducerImplementation } from '../producer';
import { AssertionMode } from '../types';
import { ExchangeImplementation } from './exchange-implementation';

describe('ExchangeImplementation', () => {
    let channel: TestChannel;
    let exchange: ExchangeImplementation;

    beforeEach(() => {
        channel = new TestChannel();
        exchange = new ExchangeImplementation(channel, 'my-exchange', 'direct');
    });

    describe('assert', () => {
        it('should call assertExchange on native channel in Assert mode when using default options', async () => {
            await exchange.assert();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledWith(
                'my-exchange',
                'direct',
                expect.anything(),
            );
        });

        it('should call assertExchange on native channel when assertionMode is explicitly Assert', async () => {
            const assertExchange = new ExchangeImplementation(channel, 'my-exchange', 'direct', {
                assertionMode: AssertionMode.Assert,
            });

            await assertExchange.assert();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.checkExchange).not.toHaveBeenCalled();
        });

        it('should call assertExchange on native channel with correct exchange type', async () => {
            const assertExchange = new ExchangeImplementation(channel, 'my-exchange', 'fanout', {
                assertionMode: AssertionMode.Assert,
            });

            await assertExchange.assert();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledWith(
                'my-exchange',
                'fanout',
                expect.anything(),
            );
            expect(channel.nativeChannel.checkExchange).not.toHaveBeenCalled();
        });

        it('should call checkExchange on native channel in Check mode', async () => {
            const checkExchange = new ExchangeImplementation(channel, 'my-exchange', 'direct', {
                assertionMode: AssertionMode.Check,
            });

            await checkExchange.assert();

            expect(channel.nativeChannel.checkExchange).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.checkExchange).toHaveBeenCalledWith('my-exchange');
            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
        });

        it('should skip any native call in Passive mode', async () => {
            const passiveExchange = new ExchangeImplementation(channel, 'my-exchange', 'direct', {
                assertionMode: AssertionMode.Passive,
            });

            await passiveExchange.assert();

            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
            expect(channel.nativeChannel.checkExchange).not.toHaveBeenCalled();
        });

        it('should not assert twice when assert is called multiple times', async () => {
            await exchange.assert();
            await exchange.assert();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);
        });

        it('should re-assert after channel emits close event', async () => {
            await exchange.assert();
            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);

            channel.emit('close');

            await exchange.assert();
            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(2);
        });

        it('should pass exchange options to assertExchange', async () => {
            const durableExchange = new ExchangeImplementation(channel, 'my-exchange', 'topic', {
                durable: true,
                autoDelete: false,
            });

            await durableExchange.assert();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledWith(
                'my-exchange',
                'topic',
                expect.objectContaining({ durable: true, autoDelete: false }),
            );
        });

        it('should fail for invalid assert mode', async () => {
            exchange = new ExchangeImplementation(channel, 'my-exchange', 'direct', {
                // @ts-expect-error testing invalid mode
                assertionMode: 'invalid-mode',
            });
            await expect(
                exchange.assert(),
            ).rejects.toThrow('Unknown assertion mode: invalid-mode');
            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
            expect(channel.nativeChannel.checkExchange).not.toHaveBeenCalled();
        });
    });

    describe('name', () => {
        it('should return the exchange name', async () => {
            const result = await exchange.name();

            expect(result).toBe('my-exchange');
        });

        it('should trigger assert when called', async () => {
            await exchange.name();

            expect(channel.nativeChannel.assertExchange).toHaveBeenCalledTimes(1);
        });
    });

    describe('bindQueue', () => {
        it('should call nativeChannel.bindQueue with queueName, exchangeName, and pattern', async () => {
            const queue = new TestQueue();

            await exchange.assert();
            await exchange.bindQueue(queue, 'my.routing.key');

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith(
                'test-queue',
                'my-exchange',
                'my.routing.key',
                undefined,
            );
        });

        it('should pass binding args when provided', async () => {
            const queue = new TestQueue();
            const args = { 'x-match': 'all', priority: 10 };

            await exchange.assert();
            await exchange.bindQueue(queue, 'my.routing.key', args);

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith(
                'test-queue',
                'my-exchange',
                'my.routing.key',
                args,
            );
        });

        it('should not register duplicate bindings when bindQueue is called twice with same queue and pattern', async () => {
            const queue = new TestQueue();

            await exchange.assert();
            await exchange.bindQueue(queue, 'routing.key');
            await exchange.bindQueue(queue, 'routing.key');
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);

            // Second call still calls nativeChannel.bindQueue, but internal binding list should deduplicate
            // after channel close, rebind should only call bindQueue once for this queue+pattern combination
            channel.emit('close');
            await exchange.assert();

            // bindQueue is called: 2 times during initial binding + 1 time during rebind (deduplicated)
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(3);
        });

        it('should allow different patterns for the same queue', async () => {
            const queue = new TestQueue();

            await exchange.assert();
            await exchange.bindQueue(queue, 'pattern.one');
            await exchange.bindQueue(queue, 'pattern.two');
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(2);

            channel.emit('close');
            await exchange.assert();

            // After rebind, both patterns should be re-registered
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledTimes(4);
            const bindQueueCalls = channel.nativeChannel.bindQueue.mock.calls;
            const rebindCalls = bindQueueCalls.slice(2); // first 2 are initial bindings
            const rebindPatterns = rebindCalls.map((call: unknown[]) => call[2]);
            expect(rebindPatterns).toContain('pattern.one');
            expect(rebindPatterns).toContain('pattern.two');
        });
    });

    describe('bindExchange', () => {
        it('should call nativeChannel.bindExchange with sourceExchangeName, destExchangeName, and pattern', async () => {
            const sourceExchange = new TestExchange();

            await exchange.assert();
            await exchange.bindExchange(sourceExchange, 'fanout.key');

            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledWith(
                'test-exchange',
                'my-exchange',
                'fanout.key',
                undefined,
            );
        });

        it('should pass binding args to bindExchange when provided', async () => {
            const sourceExchange = new TestExchange();
            const args = { 'x-match': 'any' };

            await exchange.assert();
            await exchange.bindExchange(sourceExchange, 'topic.key', args);

            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledWith(
                'test-exchange',
                'my-exchange',
                'topic.key',
                args,
            );
        });

        it('should not register duplicate bindings when bindExchange is called twice with same exchange and pattern', async () => {
            const sourceExchange = new TestExchange();

            await exchange.assert();
            await exchange.bindExchange(sourceExchange, 'fanout.key');
            await exchange.bindExchange(sourceExchange, 'fanout.key');
            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(2);

            channel.emit('close');
            await exchange.assert();

            // bindExchange called: 2 during initial + 1 during rebind (deduplicated)
            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(3);
        });

        it('should call bind again after channel close and re-assert', async () => {
            const sourceExchange = new TestExchange();

            await exchange.assert();
            await exchange.bindExchange(sourceExchange, 'routing.key');
            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(1);

            channel.emit('close');
            await exchange.assert();

            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.bindExchange).toHaveBeenNthCalledWith(
                2,
                'test-exchange',
                'my-exchange',
                'routing.key',
                undefined,
            );
        });

        it('should bind same exchange twice with two patterns', async () => {
            const sourceExchange = new TestExchange();

            await exchange.assert();
            await exchange.bindExchange(sourceExchange, 'fanout.key');
            await exchange.bindExchange(sourceExchange, 'fanout.key2');
            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(2);

            channel.emit('close');
            await exchange.assert();

            // bindExchange called: 2 during initial + 1 during rebind (deduplicated)
            expect(channel.nativeChannel.bindExchange).toHaveBeenCalledTimes(4);
        });
    });

    describe('createConsumer', () => {
        it('should create consumer on the exchange with the new queue and provided pattern', async () => {
            const consumer = await exchange.createConsumer({
                pattern: 'test.pattern',
            });

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith(
                'test-queue',
                'my-exchange',
                'test.pattern',
                undefined,
            );
            expect(channel.createQueue).toHaveBeenCalledWith('', expect.objectContaining({
                durable: false,
                exclusive: true,
            }));
            expect(consumer.channel).not.toBe(channel);
        });

        it('should pass additional queueOptions to createQueue', async () => {
            await exchange.createConsumer(
                { pattern: 'test.pattern' },
                { arguments: { 'x-message-ttl': 5000 } },
            );

            expect(channel.createQueue).toHaveBeenCalledWith('', expect.objectContaining({
                durable: false,
                exclusive: true,
                arguments: { 'x-message-ttl': 5000 },
            }));
        });
    });

    describe('createBatchConsumer', () => {
        it('should create batch consumer with empty name, durable=false, exclusive=true', async () => {
            await exchange.createBatchConsumer({
                pattern: 'test.pattern',
            });

            expect(channel.createQueue).toHaveBeenCalledWith('', expect.objectContaining({
                durable: false,
                exclusive: true,
            }));
            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith(
                'test-queue',
                'my-exchange',
                'test.pattern',
                undefined,
            );
        });

        it('should pass additional queueOptions to createQueue', async () => {
            await exchange.createBatchConsumer(
                { pattern: 'test.pattern' },
                { arguments: { 'x-message-ttl': 5000 } },
            );

            expect(channel.createQueue).toHaveBeenCalledWith('', expect.objectContaining({
                durable: false,
                exclusive: true,
                arguments: { 'x-message-ttl': 5000 },
            }));
        });
    });

    describe('createProducer', () => {
        it('should return a ProducerImplementation instance', async () => {
            const producer = await exchange.createProducer();

            expect(producer).toBeInstanceOf(ProducerImplementation);
            expect(producer.channel).not.toBe(channel);
        });

        it('should use the channel provided in options when present', async () => {
            const otherChannel = new TestChannel();

            const producer = await exchange.createProducer({ channel: otherChannel });

            expect(producer.channel).toBe(otherChannel);
        });

        it('should fall back to channel.connection.createChannel when no channel option is provided', async () => {
            const fallbackChannel = new TestChannel();
            const createChannelMock = vi.fn().mockReturnValue(fallbackChannel);
            channel.connection.createChannel = createChannelMock;

            const producer = await exchange.createProducer();

            expect(createChannelMock).toHaveBeenCalledTimes(1);
            expect(producer.channel).toBe(fallbackChannel);
        });
    });
});
