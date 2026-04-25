import { TestChannel, TestQueue } from '../extensions/vitest';
import { sleepPromise, zip } from '../utils';
import { processGeneratedMessages } from '../test/generate-messages';
import { BatchConsumerImplementation } from './batch-consumer-implementation';
import { BatchFailureStrategy, ConsumptionFailureStrategy } from './types';

describe('Batch consumer', () => {
    let channel: TestChannel;
    let consumer: BatchConsumerImplementation<{  value: number }>;

    beforeEach(() => {
        vitest.useRealTimers();
        channel = new TestChannel();
        consumer = new BatchConsumerImplementation(
            channel,
            new TestQueue(),
            {
                batchSize: 5,
            },
        );
    });

    afterEach(() => {
        // @ts-expect-error batches is private property
        expect(consumer.batches).toHaveLength(0);
        // @ts-expect-error currentlyProcessingMessages is private property
        expect(consumer.currentlyProcessingMessages).toEqual(0);
        // each message were ack/nack at most once
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
        it('should process batch', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);

            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent, rabbitMessages).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });

        it('should process multiple batches', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 20);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(4);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[9], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[14], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[19], true);

            expect(listener).toHaveBeenCalledTimes(4);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });

        it('should not allow to hook in two listeners', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            await expect(consumer.listen(listener)).rejects.toThrow('Listener is already attached');
        });

        it('should close consumer when canceled via null message', async () => {
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

        it('should set batch size based on prefetch when batch size is not provided', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch: 5,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(channel, 20);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(4);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);

            expect(listener).toHaveBeenCalledTimes(4);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });

        it('should set batch size to default 20 when batch size nor prefetch is specified', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {},
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(channel, 40);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[19], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[39], true);

            expect(listener).toHaveBeenCalledTimes(2);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(0, 20), rabbitMessages.slice(0, 20)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });

        it('should process batch exactly once when zero-delay timer fires concurrently with batch fill', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 4,
                    maxWaitTimeForBatch: 0,
                },
            );
            const listener = vi.fn().mockResolvedValue(undefined);

            const realClearTimeout = global.clearTimeout;
            const clearTimeoutMock = vitest.spyOn(global, 'clearTimeout').mockImplementation(async handler => {
                await sleepPromise(10);
                realClearTimeout(handler);
            });

            try {
                await consumer.listen(listener);
                const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 4);
                await consumePromise;

                expect(listener).toHaveBeenCalledTimes(1);
                expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
                expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
            } finally {
                clearTimeoutMock.mockRestore();
            }
        });

        it('should process individual batches when maxWaitTimeForBatch=0 and there is delay', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 4,
                    maxWaitTimeForBatch: 0,
                },
            );
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);

            const {
                consumePromise: consumePromise1,
            } = processGeneratedMessages(channel, 1);
            await vitest.advanceTimersByTimeAsync(10);
            const {
                consumePromise: consumePromise2,
            } = processGeneratedMessages(channel, 1);
            await vitest.advanceTimersByTimeAsync(10);
            const {
                consumePromise: consumePromise3,
            } = processGeneratedMessages(channel, 2);
            await vitest.advanceTimersByTimeAsync(10);

            await Promise.all([
                consumePromise1,
                consumePromise2,
                consumePromise3,
            ]);

            expect(listener).toHaveBeenCalledTimes(3);
        });

        it('should process partial batch', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForBatch: 400,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(channel, 8);

            // acknowledge first promise
            await vitest.advanceTimersByTimeAsync(200);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);

            // acknowledge second batch
            await vitest.advanceTimersByTimeAsync(400);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[7], true);

            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(2);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(5, 8), rabbitMessages.slice(5, 8)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });

        it('should use custom parseMessageFn to parse messages', async () => {
            const parseFn = vi.fn().mockImplementation((buf: Buffer): { parsed: string } => ({ parsed: buf.toString() }));
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 3,
                    parseMessageFn: parseFn,
                },
            );
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);

            const handler = channel.nativeChannel.consume.mock.lastCall![1];
            const contents = [Buffer.from('hello'), Buffer.from('world'), Buffer.from('!')];
            const messages = contents.map(content => ({ content }));
            await Promise.all(messages.map(msg => handler(msg)));

            expect(parseFn).toHaveBeenCalledTimes(3);
            expect(parseFn).toHaveBeenCalledWith(contents[0]);
            expect(parseFn).toHaveBeenCalledWith(contents[1]);
            expect(parseFn).toHaveBeenCalledWith(contents[2]);
            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: [
                    { message: { parsed: 'hello' }, rabbitMessage: messages[0] },
                    { message: { parsed: 'world' }, rabbitMessage: messages[1] },
                    { message: { parsed: '!' }, rabbitMessage: messages[2] },
                ],
            });
        });

        it('should support async parseMessageFn', async () => {
            const parseFn = vi.fn().mockImplementation(async (buf: Buffer): Promise<{ parsed: string }> => ({ parsed: buf.toString() }));
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 2,
                    parseMessageFn: parseFn,
                },
            );
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);

            const handler = channel.nativeChannel.consume.mock.lastCall![1];
            const contents = [Buffer.from('foo'), Buffer.from('bar')];
            const messages = contents.map(content => ({ content }));
            await Promise.all(messages.map(msg => handler(msg)));

            expect(parseFn).toHaveBeenCalledTimes(2);
            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: [
                    { message: { parsed: 'foo' }, rabbitMessage: messages[0] },
                    { message: { parsed: 'bar' }, rabbitMessage: messages[1] },
                ],
            });
        });

        it('should pass consumeOptions to channel.consume', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    consumeOptions: { priority: 10 },
                },
            );
            await consumer.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toMatchObject({ priority: 10 });
        });

        it('should set noAck to true when failureStrategy is Drop and prefetch is not set', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            await consumer.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', true);
        });

        it('should set noAck to false when failureStrategy is Drop but prefetch is set', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    prefetch: 5,
                },
            );
            await consumer.listen(vi.fn().mockResolvedValue(undefined));

            const consumeCallOptions = channel.nativeChannel.consume.mock.lastCall![2];
            expect(consumeCallOptions).toHaveProperty('noAck', false);
        });
    });

    describe('batch out of order processing', () => {
        test.each([
            ['zero', 0],
            ['negative', -3],
        ])('should send the confirmation message immediately when wait time for ack is %s', async (_, waitTime) => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForAck: waitTime,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(
                    () => Promise.resolve(),
                );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // now second batch has been processed
            await vitest.advanceTimersByTimeAsync(500);
            // now the first batch has been processed
            await consumePromise;

            // separately second batch and then first batch as a whole
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(6);
            const ackCalls = channel.nativeChannel.ack.mock.calls;
            expect(ackCalls).toEqual([
                [rabbitMessages[5], false],
                [rabbitMessages[6], false],
                [rabbitMessages[7], false],
                [rabbitMessages[8], false],
                [rabbitMessages[9], false],
                [rabbitMessages[4], true],
            ]);

            expect(listener).toHaveBeenCalledTimes(2);
            const listenerCalls = listener.mock.calls;
            expect(listenerCalls).toEqual([
                [{
                    channel,
                    messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
                [{
                    channel,
                    messages: zip(messagesContent.slice(5, 10), rabbitMessages.slice(5, 10)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
            ]);
        });

        it('should wait for batch but send confirmation anyway when previous batch is not processed within specified time', async () => {
            vitest.useFakeTimers();
            const sleepTime = 1000;
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForAck: 400,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(sleepTime))
                .mockImplementation(
                    () => Promise.resolve(),
                );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(sleepTime / 2);
            // now should the second batch fire
            await vitest.advanceTimersByTimeAsync(sleepTime);
            // now should the first batch fire
            await consumePromise;

            // separately second batch and then first batch as a whole
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(6);
            const ackCalls = channel.nativeChannel.ack.mock.calls;
            expect(ackCalls).toEqual([
                [rabbitMessages[5], false],
                [rabbitMessages[6], false],
                [rabbitMessages[7], false],
                [rabbitMessages[8], false],
                [rabbitMessages[9], false],
                [rabbitMessages[4], true],
            ]);

            expect(listener).toHaveBeenCalledTimes(2);
            const listenerCalls = listener.mock.calls;
            expect(listenerCalls).toEqual([
                [{
                    channel,
                    messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
                [{
                    channel,
                    messages: zip(messagesContent.slice(5, 10), rabbitMessages.slice(5, 10)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
            ]);
        });

        it('should wait for the delayed batch and send single confirmation message', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForAck: 1000,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(
                    () => Promise.resolve(),
                );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // now the second batch is waiting
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            await vitest.advanceTimersByTimeAsync(500);
            // now single confirmation for second batch should fire
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            const ackCalls = channel.nativeChannel.ack.mock.calls;
            expect(ackCalls).toEqual([
                [rabbitMessages[9], true],
            ]);

            expect(listener).toHaveBeenCalledTimes(2);
            const listenerCalls = listener.mock.calls;
            expect(listenerCalls).toEqual([
                [{
                    channel,
                    messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
                [{
                    channel,
                    messages: zip(messagesContent.slice(5, 10), rabbitMessages.slice(5, 10)).map(([message, rabbitMessage]) => ({
                        message,
                        rabbitMessage,
                    })),
                }],
            ]);
        });
    });

    describe('processing failure with fail batch failure strategy', () => {
        it('should reject messages', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.reject(new Error('Testing rejection')),
            );

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
        });

        it('should reject the messages individually when first batch is delayed', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // second batch is done and should reject immediately
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, false);
            // finish first batch
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        it('should reject the messages individually and then in batch', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(async () => {
                    await sleepPromise(500);
                    throw new Error('Delayed testing error');
                })
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // second batch is done and should reject immediately
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, false);
            // finish first batch
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
        });

        it('should requeue the messages', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.reject(new Error('Testing rejection')),
            );

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        it('should requeue the messages individually when first batch is delayed', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // second batch is done and should requeue immediately
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, true);
            // finish first batch
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        it('should requeue the messages individually and then in batch', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(async () => {
                    await sleepPromise(500);
                    throw new Error('Delayed testing error');
                })
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(100);
            // second batch is done and should requeue immediately
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, true);
            // finish first batch
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        it('should have auto acknowledge on when failure strategy is drop', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it.each([
            ['zero', 0],
            ['negative', -3],
        ])('should have auto acknowledge on when failure strategy is drop and prefetch is %s', async (_, prefetch) => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch,
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should explicitly ack message when failure strategy is drop and prefetch is set', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch: 10,
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 10);
            await consumePromise;

            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[9], true);
        });
    });

    describe('processing failure with split batch failure strategy', () => {
        it('should split batch and process messages one at a time', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                .mockImplementation(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[0], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[2], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        it('after split it should respect acknowledgement delay', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    maxWaitTimeForAck: 1000,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await vitest.advanceTimersByTimeAsync(100);
            // batch rejected at this point and last 4 messages are waiting for the delayed one
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            await vitest.advanceTimersByTimeAsync(500);
            // first message should now be resolved

            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        it('should reprocess messages even when failure strategy is drop and auto-acknowledgement is enabled', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing split')));

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should reprocess and acknowledge messages even when failure strategy is drop and auto-acknowledgement is disabled', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch: 10,
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing split')));

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[0], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[2], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should handle each message individually for reject strategy', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn()
                // for batch
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                // for messages
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.nack.mock.calls).toEqual([
                [rabbitMessages[0], true, false],
                [rabbitMessages[2], false, false],
            ]);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack.mock.calls).toEqual([
                [rabbitMessages[4], true],
            ]);

            const nackCallOrder = channel.nativeChannel.nack.mock.invocationCallOrder;
            const ackCallOrder = channel.nativeChannel.ack.mock.invocationCallOrder;
            // nack first, nack third, ack fifth
            expect(nackCallOrder[0]).toBeLessThan(nackCallOrder[1] as number);
            expect(nackCallOrder[1]).toBeLessThan(ackCallOrder[0] as number);
        });

        it('should handle each message individually for reject strategy with delayed message', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    maxWaitTimeForAck: 1000,
                },
            );
            const listener = vi.fn()
                // for batch
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                // for messages
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementationOnce(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            // now messages should be rejected, first message acknowledged, and last one waitinf ro fourth
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.nack.mock.calls).toEqual([
                [rabbitMessages[0], true, false],
                [rabbitMessages[2], false, false],
            ]);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);

            await vitest.advanceTimersByTimeAsync(500);
            // fourth message should be resolved
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);

            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
        });

        it('should handle each message individually for requeue strategy', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            const listener = vi.fn()
                // for batch
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                // for messages
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.nack.mock.calls).toEqual([
                [rabbitMessages[0], true, true],
                [rabbitMessages[2], false, true],
            ]);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack.mock.calls).toEqual([
                [rabbitMessages[4], true],
            ]);

            const nackCallOrder = channel.nativeChannel.nack.mock.invocationCallOrder;
            const ackCallOrder = channel.nativeChannel.ack.mock.invocationCallOrder;
            // nack first, nack third, ack fifth
            expect(nackCallOrder[0]).toBeLessThan(nackCallOrder[1] as number);
            expect(nackCallOrder[1]).toBeLessThan(ackCallOrder[0] as number);
        });

        it('should handle each message individually for requeue strategy with delayed message', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                    maxWaitTimeForAck: 1000,
                },
            );
            const listener = vi.fn()
                // for batch
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                // for messages
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.reject(new Error('Testing error for message')))
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementationOnce(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            // now messages should be rejected, first message acknowledged, and last one waitinf ro fourth
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.nack.mock.calls).toEqual([
                [rabbitMessages[0], true, true],
                [rabbitMessages[2], false, true],
            ]);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);

            await vitest.advanceTimersByTimeAsync(500);
            // fourth message should be resolved
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);

            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
        });
    });

    describe('failure strategy has invalid input', () => {
        it('should throw error when invalid batch failure strategy is used', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    // @ts-expect-error Invalid failure strategy for test purpose
                    batchFailureStrategy: 'invalid',
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);

            let err: Error | undefined;
            consumer.on('error', error => {
                err = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow(`Not supported batch failure strategy: invalid`);
            expect(err?.message).toEqual('Not supported batch failure strategy: invalid');
        });

        it('should throw error when invalid failure strategy is used with reject batch failure', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    // @ts-expect-error Invalid failure strategy for test purpose
                    failureStrategy: 'invalid',
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);

            let err: Error | undefined;
            consumer.on('error', error => {
                err = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow(`Not supported failure strategy: invalid`);
            expect(err?.message).toEqual('Not supported failure strategy: invalid');
        });

        it('should throw error when invalid failure strategy is used with split batch failure', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    // @ts-expect-error Invalid failure strategy
                    failureStrategy: 'invalid',
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing split')));

            await consumer.listen(listener);

            let err: Error | undefined;
            consumer.on('error', error => {
                err = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow('Not supported failure strategy: invalid');
            expect(err?.message).toEqual('Not supported failure strategy: invalid');
        });

        it.each([
            [0],
            [1],
        ])('should not allow split batch failure strategy with batch size of %d', batchSize => {
            expect(() => new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: batchSize,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            )).toThrow('Cannot have split batch failure strategy when batch size is 1');

            expect(() => new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    prefetch: 1,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            )).toThrow('Cannot have split batch failure strategy when batch size is 1');
        });
    });

    describe('channel close', () => {
        it('should try to reconnect after channel close', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 20,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );
            const listener = vi.fn();

            await consumer.listen(listener);

            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(1000);

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
        });

        it('should close when reconnect encounter error', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 20,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );
            const listener = vi.fn();

            const closeListener = vi.fn();
            consumer.on('close', closeListener);
            const reconnectErrorListener = vi.fn();
            consumer.on('reconnectError', reconnectErrorListener);

            channel.nativeChannel.prefetch
                .mockImplementationOnce(() => Promise.resolve())
                .mockImplementationOnce(() => Promise.reject(new Error('prefetch failure')));

            await consumer.listen(listener);
            await vitest.advanceTimersByTimeAsync(100);

            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(1000);

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(1);
            expect(reconnectErrorListener).toHaveBeenCalledTimes(1);
        });

        it('should not send ack nor nock even if processing is successful', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(
                () => sleepPromise(500),
            );

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(1000);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should not send ack nor nock when processing is not successful', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(async () => {
                await sleepPromise(500);
                throw new Error('Testing error');
            });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should not send ack nor nock when batch is split', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Test failure')))
                .mockImplementation(async () => {
                    await sleepPromise(500);
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should not send ack nor nock when batch is split and split fails', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 20,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );

            let shouldFail = true;
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Test failure')))
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    if (shouldFail)
                        throw new Error('Test failure');
                    shouldFail = !shouldFail;
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 20);

            await vitest.advanceTimersByTimeAsync(100);
            channel.emit('close');
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        it('should skip batch processing when channel disconnects before fill timer fires', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(channel, new TestQueue(), {
                batchSize: 5,
                maxWaitTimeForBatch: 1000,
            });
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);

            const { consumePromise: cp1 } = processGeneratedMessages(channel, 3);

            channel.emit('close');

            await vitest.advanceTimersByTimeAsync(2000);
            await cp1;
            expect(listener).not.toHaveBeenCalled();

            const { consumePromise: cp2, rabbitMessages } = processGeneratedMessages(channel, 5);
            await vitest.advanceTimersByTimeAsync(1000);
            await cp2;

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
            expect(listener).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should not ack when channel disconnects before confirmTimer fires', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(channel, new TestQueue(), {
                batchSize: 5,
                maxWaitTimeForAck: 500,
            });
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(1000))
                .mockResolvedValue(undefined);

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 10);

            await vitest.advanceTimersByTimeAsync(200);
            // second batch processed and waiting, first batch processing

            channel.emit('close');

            await vitest.advanceTimersByTimeAsync(2000);
            // both batch processed
            await consumePromise;

            // as second batch was waiting and disconnected during waiting,
            // no ack nor nack should be called
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(2);
        });
    });

    describe('consumer close', () => {
        it('should wait for processing all batches', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(
                () => sleepPromise(500),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(1000);

            await consumePromise;
            await closePromise;
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[9], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[14], true);
        });

        it('should wait for reject of batches', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(1000);
            await closePromise;
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], true, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[14], true, false);
        });

        it('should wait for ack of messages after split', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );

            const closeListener = vitest.fn();
            consumer.on('close', closeListener);

            const listener = vi.fn()
                .mockImplementationOnce(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                })
                .mockImplementation(async () => {
                    await sleepPromise(200);
                });

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();

            await vitest.advanceTimersByTimeAsync(200);
            // second and third batch processed
            expect(closeListener).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(3);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(10);
            expect(channel.nativeChannel.ack.mock.calls).toEqual([
                [rabbitMessages[5], false],
                [rabbitMessages[6], false],
                [rabbitMessages[7], false],
                [rabbitMessages[8], false],
                [rabbitMessages[9], false],
                [rabbitMessages[10], false],
                [rabbitMessages[11], false],
                [rabbitMessages[12], false],
                [rabbitMessages[13], false],
                [rabbitMessages[14], false],
            ]);

            await vitest.advanceTimersByTimeAsync(300);
            // first batch failed
            expect(closeListener).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(8);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(10);

            await vitest.advanceTimersByTimeAsync(1000);
            // first (now splitted) batch success
            expect(closeListener).toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(8);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(15);
            expect(channel.nativeChannel.ack.mock.calls).toEqual([
                [rabbitMessages[5], false],
                [rabbitMessages[6], false],
                [rabbitMessages[7], false],
                [rabbitMessages[8], false],
                [rabbitMessages[9], false],
                [rabbitMessages[10], false],
                [rabbitMessages[11], false],
                [rabbitMessages[12], false],
                [rabbitMessages[13], false],
                [rabbitMessages[14], false],
                [rabbitMessages[0], true],
                [rabbitMessages[1], true],
                [rabbitMessages[2], true],
                [rabbitMessages[3], true],
                [rabbitMessages[4], true],
            ]);

            await closePromise;
            await consumePromise;
        });

        it('should wait for nack of messages after split', async () => {
            vitest.useFakeTimers();
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );

            const closeListener = vitest.fn();
            consumer.on('close', closeListener);

            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();

            await vitest.advanceTimersByTimeAsync(500);
            // all 3 batches failed, split into individual messages
            expect(closeListener).not.toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(3+15);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);

            await vitest.advanceTimersByTimeAsync(500);
            // all individual messages failed and nacked
            expect(closeListener).toHaveBeenCalled();
            expect(listener).toHaveBeenCalledTimes(18);
            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(15);

            await closePromise;
            await consumePromise;
        });

        it('should handle close timeout with error', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(1000);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closeExpectation = expect(consumer.close(500)).rejects.toThrow('Consumer close timeout');
            await vitest.advanceTimersByTimeAsync(1000);
            await closeExpectation;

            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });
    });

    describe('parse failure', () => {
        it('should nack batch when parse fails and failureStrategy is Reject', async () => {
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
        });

        it('should requeue batch when parse fails and failureStrategy is Requeue', async () => {
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        it('should ack when parse fails and failureStrategy is Drop with prefetch', async () => {
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    prefetch: 20,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise, rabbitMessages } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should not ack or nack when parse fails and failureStrategy is Drop with no prefetch', async () => {
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Fail,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    parseMessageFn: () => {
                        throw new Error('parse failure');
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        it('should emit handlingFailed when parse fails', async () => {
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    parseMessageFn: () => {
                        throw parseError;
                    },
                },
            );

            let emittedError: unknown;
            consumerWithBadParser.on('handlingFailed', err => {
                emittedError = err;
            });

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(emittedError).toBe(parseError);
        });

        it('close() should resolve after parse failure', async () => {
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    parseMessageFn: () => {
                        throw parseError;
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            await expect(consumerWithBadParser.close(500)).resolves.toBeUndefined();
        });

        it('close() should resolve after async parse failure', async () => {
            vitest.useFakeTimers();
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    parseMessageFn: async () => {
                        await sleepPromise(500);
                        throw parseError;
                    },
                },
            );

            const closeListener = vitest.fn();
            consumerWithBadParser.on('close', closeListener);

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise } = processGeneratedMessages(channel, 5);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumerWithBadParser.close(500);
            expect(closeListener).not.toHaveBeenCalled();

            await vitest.advanceTimersByTime(200);
            expect(closeListener).not.toHaveBeenCalled();

            await vitest.advanceTimersByTimeAsync(500);
            expect(closeListener).toHaveBeenCalled();

            await closePromise;
            await consumePromise;
        });

        it('should nack only failing message when split strategy and one message has bad content', async () => {
            const consumerWithSplit = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn().mockImplementation(() => Promise.resolve());

            await consumerWithSplit.listen(listener);

            const consumerHandler = channel.nativeChannel.consume.mock.lastCall![1];
            const validContent = Buffer.from(JSON.stringify({ value: 1 }));
            const invalidContent = Buffer.from('not valid json');

            const messages = [
                { content: invalidContent },
                { content: validContent },
                { content: validContent },
                { content: validContent },
                { content: validContent },
            ];

            await Promise.all(messages.map(msg => consumerHandler(msg)));

            // Invalid message is nacked; valid singletons are each acked individually (each at index 0 when processed)
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(messages[0], true, false);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(4);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(messages[1], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(messages[2], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(messages[3], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(messages[4], true);
            // Listener is called once per valid singleton (full batch parse fails before reaching callback)
            expect(listener).toHaveBeenCalledTimes(4);
        });
    });

    describe('acknowledgment', () => {
        it('should not ack the same message twice when batches complete concurrently', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 10,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    maxWaitTimeForAck: 0,
                },
            );

            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('trigger split')))
                .mockResolvedValue(undefined);

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(10);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[0], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[2], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[5], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[6], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[7], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[8], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[9], true);
        });
    });

    describe('handlingFailed event', () => {
        it('should emit handlingFailed when listener callback throws', async () => {
            const callbackError = new Error('callback failure');
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            await consumer.listen(vi.fn().mockRejectedValue(callbackError));
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(1);
            expect(handlingFailedListener).toHaveBeenCalledWith(callbackError);
        });

        it('should emit handlingFailed once per batch not once per message in the batch', async () => {
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            await consumer.listen(vi.fn().mockRejectedValue(new Error('failure')));
            const { consumePromise } = processGeneratedMessages(channel, 10);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(2);
        });

        it('should emit handlingFailed for each batch across multiple failed batches', async () => {
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            await consumer.listen(vi.fn().mockRejectedValue(new Error('failure')));
            const { consumePromise } = processGeneratedMessages(channel, 20);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(4);
        });

        it('should emit handlingFailed for batch failure and for each individual message failure in split strategy', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            await consumer.listen(vi.fn().mockRejectedValue(new Error('failure')));
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            // 1 for the batch + 5 for each individual message = 6 total
            expect(handlingFailedListener).toHaveBeenCalledTimes(6);
        });

        it('should emit handlingFailed only once when batch fails but individual messages succeed in split strategy', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                },
            );
            const handlingFailedListener = vi.fn();
            consumer.on('handlingFailed', handlingFailedListener);

            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('batch failure')))
                .mockResolvedValue(undefined);

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledTimes(1);
        });
    });

    describe('lifecycle', () => {
        it('should resolve close without error when called before listen', async () => {
            await expect(consumer.close()).resolves.toBeUndefined();
        });

        it('should allow listen to be called again after close', async () => {
            const listener = vi.fn().mockResolvedValue(undefined);

            await consumer.listen(listener);
            await consumer.close();

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });
    });
});
