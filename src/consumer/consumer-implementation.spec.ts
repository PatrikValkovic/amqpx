import { TestChannel, TestQueue } from '../extensions/vitest';
import { sleepPromise } from '../utils';
import { processGeneratedMessages } from '../test/generate-messages';
import { ConsumerImplementation } from './consumer-implementation';
import { ConsumptionFailureStrategy } from './types';

describe('Consumer', () => {
    let channel: TestChannel;
    let consumer: ConsumerImplementation<{ value: number }>;

    beforeEach(() => {
        vitest.useRealTimers();
        channel = new TestChannel();
        consumer = new ConsumerImplementation(
            channel,
            new TestQueue(),
            {},
        );
    });

    afterEach(async () => {
        const messages: object[] = [];
        channel.nativeChannel.ack.mock.calls.forEach(([message]) => {
            expect(messages).not.toContain(message);
            messages.push(message);
        });
        channel.nativeChannel.nack.mock.calls.forEach(([message]) => {
            expect(messages).not.toContain(message);
            messages.push(message);
        });
    });

    describe('consuming', () => {
        it('should process message and invoke callback with parsed message, channel, and rabbitMessage', async () => {
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            const content = { value: 42 };
            const { rabbitMessages: [msg], consumePromise } = processGeneratedMessages(channel, [content]);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith({
                message: content,
                rabbitMessage: msg,
                channel,
            });
        });

        it('should not allow two listeners — rejects with "Listener is already attached"', async () => {
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);
            await expect(consumer.listen(listener)).rejects.toThrow('Listener is already attached');
        });

        it('should close consumer when cancelled via null message (emit close, no ack, no nack)', async () => {
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            const closeFn = vi.fn();
            consumer.on('close', closeFn);

            const handler = channel.nativeChannel.consume.mock.lastCall![1];
            await handler(null);

            expect(closeFn).toHaveBeenCalledTimes(1);
            expect(listener).not.toHaveBeenCalled();
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should use custom sync parseMessageFn', async () => {
            const parseFn = vi.fn().mockImplementation((buf: Buffer) => ({ parsed: buf.toString() }));
            const consumerWithParseFn = new ConsumerImplementation<{ parsed: string }>(
                channel,
                new TestQueue(),
                { parseMessageFn: parseFn },
            );

            const listener = vi.fn().mockResolvedValue(undefined);
            await consumerWithParseFn.listen(listener);

            const handler = channel.nativeChannel.consume.mock.lastCall![1];
            const rawContent = Buffer.from('hello');
            const msg = { content: rawContent };
            await handler(msg);

            expect(parseFn).toHaveBeenCalledTimes(1);
            expect(parseFn).toHaveBeenCalledWith(rawContent);
            expect(listener).toHaveBeenCalledWith(expect.objectContaining({
                message: { parsed: 'hello' },
            }));
        });

        it('should support async parseMessageFn', async () => {
            const parseFn = vi.fn().mockImplementation(async (buf: Buffer) => Promise.resolve({ parsed: buf.toString() }));
            const consumerWithParseFn = new ConsumerImplementation<{ parsed: string }>(
                channel,
                new TestQueue(),
                { parseMessageFn: parseFn },
            );

            const listener = vi.fn().mockResolvedValue(undefined);
            await consumerWithParseFn.listen(listener);

            const handler = channel.nativeChannel.consume.mock.lastCall![1];
            const rawContent = Buffer.from('world');
            const msg = { content: rawContent };
            await handler(msg);

            expect(parseFn).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith(expect.objectContaining({
                message: { parsed: 'world' },
            }));
        });

        it('should pass consumeOptions to channel.consume (priority option reaching nativeChannel.consume)', async () => {
            const consumerWithOptions = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { consumeOptions: { priority: 10 } },
            );
            await consumerWithOptions.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toMatchObject({ priority: 10 });
        });

        it('should set prefetch on native channel before consuming', async () => {
            const consumerWithPrefetch = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { prefetch: 7 },
            );
            await consumerWithPrefetch.listen(vi.fn().mockResolvedValue(undefined));

            expect(channel.nativeChannel.prefetch).toHaveBeenCalledWith(7);
            const prefetchCallOrder = channel.nativeChannel.prefetch.mock.invocationCallOrder[0]!;
            const consumeCallOrder = channel.nativeChannel.consume.mock.invocationCallOrder[0]!;
            expect(prefetchCallOrder).toBeLessThan(consumeCallOrder);
        });
    });

    describe('noAck and shouldAcknowledge logic', () => {
        it('should set noAck=true when failureStrategy=Drop and prefetch is 0', async () => {
            const consumerDrop = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Drop },
            );
            await consumerDrop.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', true);
        });

        it('should set noAck=false when failureStrategy=Drop and prefetch>0', async () => {
            const consumerDrop = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Drop, prefetch: 5 },
            );
            await consumerDrop.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', false);
        });

        it.each([
            [0],
            [-3],
            [5],
            [undefined],
        ])('should set noAck=false when failureStrategy=Reject and prefetch %d', async prefetch => {
            const consumerReject = new ConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    prefetch,
                },
            );
            await consumerReject.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', false);
        });

        it.each([
            [0],
            [-3],
            [5],
            [undefined],
        ])('should set noAck=false when failureStrategy=Requeue and prefetch %d', async prefetch => {
            const consumerRequeue = new ConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            await consumerRequeue.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', false);
        });

        it('should ack message after successful callback when shouldAcknowledge is true', async () => {
            const consumerReject = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Reject },
            );
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumerReject.listen(listener);

            const { rabbitMessages: [msg], consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(msg);
        });

        it('should not ack or nack after successful callback when shouldAcknowledge is false (Drop + no prefetch)', async () => {
            const consumerDrop = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Drop },
            );
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumerDrop.listen(listener);

            const { consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });
    });

    describe('failure handling', () => {
        it('should nack(msg, false, false) when failureStrategy=Reject and callback throws', async () => {
            const consumerReject = new ConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );

            const listener = vi.fn().mockRejectedValue(new Error('handler error'));
            await consumerReject.listen(listener);

            const { rabbitMessages: [msg], consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(msg, false, false);
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
        });

        it('should nack(msg, false, true) when failureStrategy=Requeue and callback throws', async () => {
            const consumerRequeue = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Requeue },
            );

            const listener = vi.fn().mockRejectedValue(new Error('handler error'));
            await consumerRequeue.listen(listener);

            const { rabbitMessages: [msg], consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(msg, false, true);
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
        });

        it('should ack(msg) on failure when failureStrategy=Drop and prefetch is set', async () => {
            const consumerDrop = new ConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    prefetch: 5,
                },
            );

            const listener = vi.fn().mockRejectedValue(new Error('handler error'));
            await consumerDrop.listen(listener);

            const { rabbitMessages: [msg], consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(msg);
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should not ack or nack on failure when failureStrategy=Drop and no prefetch', async () => {
            const consumerDrop = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy: ConsumptionFailureStrategy.Drop },
            );

            const listener = vi.fn().mockRejectedValue(new Error('handler error'));
            await consumerDrop.listen(listener);

            const { consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it.each([
            [ConsumptionFailureStrategy.Drop],
            [ConsumptionFailureStrategy.Requeue],
            [ConsumptionFailureStrategy.Reject],
        ])('should emit handlingFailed when callback throws and failure strategy is %s', async failureStrategy => {
            const consumer = new ConsumerImplementation(
                channel,
                new TestQueue(),
                { failureStrategy },
            );

            const callbackError = new Error('callback failure');
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            const listener = vi.fn().mockRejectedValue(callbackError);
            await consumer.listen(listener);

            const { consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(1);
            expect(handlingFailedListener).toHaveBeenCalledWith(callbackError);
        });

        it('should emit handlingFailed on parse failure', async () => {
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new ConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    parseMessageFn: () => {
                        throw parseError;
                    },
                },
            );

            const handlingFailedListener = vi.fn();
            consumerWithBadParser.on('handlingFailed', handlingFailedListener);

            await consumerWithBadParser.listen(vi.fn());

            const { consumePromise } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(1);
            expect(handlingFailedListener).toHaveBeenCalledWith(parseError);
        });

        it('should nack(msg, false, false) on parse failure with Reject strategy', async () => {
            const consumerWithBadParser = new ConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise, rabbitMessages: [msg] } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(msg, false, false);
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
        });

        it('should nack(msg, false, true) on parse failure with Requeue strategy', async () => {
            const consumerWithBadParser = new ConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );
            consumerWithBadParser.on('handlingFailed', () => {});

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise, rabbitMessages: [msg] } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(msg, false, true);
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
        });

        it('should ack on parse failure with Drop strategy and prefetch set', async () => {
            const consumerWithBadParser = new ConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    prefetch: 5,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );
            consumerWithBadParser.on('handlingFailed', () => {});

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise, rabbitMessages: [msg] } = processGeneratedMessages(channel, 1);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(msg);
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });
    });

    describe('channel close', () => {
        it('should not ack when channel closes during message processing', async () => {
            vitest.useFakeTimers();
            // Use a short sleep so the message are acknowledge immediately
            const listener = vi.fn().mockImplementation(() => sleepPromise(500));
            await consumer.listen(listener);

            const { consumePromise: promise } = processGeneratedMessages(channel, 1);

            await vitest.advanceTimersByTimeAsync(200);
            // message still running
            channel.emit('close');

            await vitest.advanceTimersByTimeAsync(500);
            // messages processed
            await promise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should try to reconnect after channel close', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockResolvedValue(undefined);
            await consumer.listen(listener);

            await vitest.advanceTimersByTimeAsync(500);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(500);

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
        });

        it('should emit reconnectError when reconnect fails', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockResolvedValue(undefined);

            const reconnectErrorListener = vi.fn();
            consumer.on('reconnectError', reconnectErrorListener);

            channel.nativeChannel.prefetch
                .mockImplementationOnce(() => Promise.resolve())
                // fail on second attempt
                .mockImplementationOnce(() => Promise.reject(new Error('prefetch failure')));

            await consumer.listen(listener);

            await vitest.advanceTimersByTimeAsync(200);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(200);

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(1);
            expect(reconnectErrorListener).toHaveBeenCalledTimes(1);
        });
    });

    describe('consumer close', () => {
        it('should resolve immediately when called before listen', async () => {
            await expect(consumer.close()).resolves.toBeUndefined();
        });

        it('should wait for in-flight message before resolving', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(() => sleepPromise(500));
            await consumer.listen(listener);

            const { consumePromise: messagePromise } = processGeneratedMessages(channel, 1);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();

            await vitest.advanceTimersByTimeAsync(500);
            await messagePromise;
            await closePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
        });

        it('should reject with "Consumer close timeout" when timeout expires', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(() => sleepPromise(2000));
            await consumer.listen(listener);

            processGeneratedMessages(channel, 1);

            await vitest.advanceTimersByTimeAsync(100);
            const closeExpectation = expect(consumer.close(500)).rejects.toThrow('Consumer close timeout');
            await vitest.advanceTimersByTimeAsync(1000);
            await closeExpectation;
        });

        it('should allow listen again after close (full listen → close → listen cycle)', async () => {
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);
            await consumer.close();

            await consumer.listen(listener);
            const { rabbitMessages: [msg], consumePromise: promise } = processGeneratedMessages(channel, 1);
            await promise;

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(msg);
        });
    });
});
