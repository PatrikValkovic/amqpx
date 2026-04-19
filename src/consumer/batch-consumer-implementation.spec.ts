import { TestChannel, TestQueue } from '../extensions/vitest';
import { sleepPromise, zip } from '../utils';
import { processGeneratedMessages } from '../test/generate-messages';
import { BatchConsumerImplementation } from './batch-consumer-implementation';
import { ConsumptionFailureStrategy, BatchFailureStrategy } from './types';

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

    describe('listen', () => {
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
                {
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(channel, 40);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[19], true);

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

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 4);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
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
                    maxWaitTimeForAck: 400,
                },
            );
            const listener = vi.fn().mockImplementation(
                () => Promise.resolve(),
            );

            await consumer.listen(listener);
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(channel, 8);
            await consumePromise;

            // wait for acknowledge to fire
            await vitest.advanceTimersByTimeAsync(800);

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(2);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[7], true);

            expect(listener).toHaveBeenCalledTimes(2);
            expect(listener).toHaveBeenCalledWith({
                channel,
                messages: zip(messagesContent.slice(0, 5), rabbitMessages.slice(0, 5)).map(([message, rabbitMessage]) => ({
                    message,
                    rabbitMessage,
                })),
            });
        });
    });

    describe('batch out of order processing', () => {
        test.each([
            ['zero', 0],
            ['negative', -3],
        ])('should send the confirmation message immediately when wait time for ack is %s', async (_, waitTime) => {
            const sleepMs = 500;
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
                .mockImplementationOnce(() => sleepPromise(sleepMs))
                .mockImplementation(
                    () => Promise.resolve(),
                );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(sleepMs/2);
            await vitest.advanceTimersByTimeAsync(sleepMs);
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
            const sleepTime = 500;
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForAck: sleepTime * 2,
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
            // here is second batch waiting
            await vitest.advanceTimersByTimeAsync(sleepTime);
            // now single confirmation for second batch should fire
            await consumePromise;

            // separately second batch and then first batch as a whole
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

    describe('processing failure with rejection batch failure strategy', () => {
        it('should reject messages', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
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
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;


            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, false);
        });

        it('should requeue the messages', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
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
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(500))
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 10);
            await vitest.advanceTimersByTimeAsync(500);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, true);
        });

        it('should have auto acknowledge on when failure strategy is drop', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
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
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    maxWaitTimeForAck: 100,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
                .mockImplementationOnce(() => sleepPromise(50))
                .mockImplementation(() => Promise.resolve());

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        it('should reprocess messages even when failure strategy is drop', async () => {
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
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
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
                [rabbitMessages[0], true, false],
                [rabbitMessages[2], false, false],
            ]);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack.mock.calls).toEqual([
                [rabbitMessages[4], true],
            ]);

            const nackCallOrder = channel.nativeChannel.nack.mock.invocationCallOrder;
            const ackCallOrder = channel.nativeChannel.ack.mock.invocationCallOrder;
            expect(ackCallOrder).toHaveLength(1);
            expect(nackCallOrder).toHaveLength(2);
            expect(nackCallOrder[0]).toBeLessThan(nackCallOrder[1] as number);
            expect(nackCallOrder[1]).toBeLessThan(ackCallOrder[0] as number);
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
                .mockImplementationOnce(() => Promise.reject(new Error('Testing split')))
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
            expect(ackCallOrder).toHaveLength(1);
            expect(nackCallOrder).toHaveLength(2);
            expect(nackCallOrder[0]).toBeLessThan(nackCallOrder[1] as number);
            expect(nackCallOrder[1]).toBeLessThan(ackCallOrder[0] as number);
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

            let errorMsg: Error | undefined;
            consumer.on('error', error => {
                errorMsg = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow(`Not supported batch failure strategy: invalid`);
            expect(errorMsg?.message).toEqual('Not supported batch failure strategy: invalid');
        });

        it('should throw error when invalid failure strategy is used with reject batch failure', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    // @ts-expect-error Invalid failure strategy for test purpose
                    failureStrategy: 'invalid',
                },
            );
            const listener = vi.fn()
                .mockImplementation(() => Promise.reject(new Error('Testing rejection')));

            await consumer.listen(listener);

            let errorMsg: Error | undefined;
            consumer.on('error', error => {
                errorMsg = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow(`Not supported failure strategy: invalid`);
            expect(errorMsg?.message).toEqual('Not supported failure strategy: invalid');
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

            let errorMsg: Error | undefined;
            consumer.on('error', error => {
                errorMsg = error;
            });

            const { consumePromise } = processGeneratedMessages(channel, 5);

            await expect(consumePromise).rejects.toThrow('Not supported failure strategy: invalid');
            expect(errorMsg?.message).toEqual('Not supported failure strategy: invalid');
        });

        it('should not allow split batch failure strategy with batch size of 1', () => {
            expect(() => new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 1,
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
            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('Test failure')))
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    if (Math.random() < 0.5)
                        throw new Error('Test failure');
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
    });

    describe('consumer close', () => {
        it('should wait for processing all batches', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn().mockImplementation(
                () => sleepPromise(500),
            );

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(1000);
            await closePromise;
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
        });

        it('should wait for reject of batches', async () => {
            vitest.useFakeTimers();
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(1000);
            await closePromise;
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalled();
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
            const listener = vi.fn()
                .mockImplementationOnce(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                })
                .mockImplementation(async () => {
                    await sleepPromise(500);
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(2000);
            await closePromise;
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
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
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(channel, 15);

            await vitest.advanceTimersByTimeAsync(100);
            const closePromise = consumer.close();
            await vitest.advanceTimersByTimeAsync(2000);
            await closePromise;
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalled();
        });

        it('should handle close timeout', async () => {
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
        });
    });

    describe('parse failure', () => {
        it('should nack batch when parse fails and failureStrategy is Reject', async () => {
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    parseMessageFn: () => {
                        throw parseError;
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
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    failureStrategy: ConsumptionFailureStrategy.Requeue,
                    parseMessageFn: () => {
                        throw parseError;
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        it('should not ack or nack when parse fails and failureStrategy is Drop', async () => {
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    batchFailureStrategy: BatchFailureStrategy.Reject,
                    failureStrategy: ConsumptionFailureStrategy.Drop,
                    parseMessageFn: () => {
                        throw parseError;
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
            const parseError = new Error('parse failure');
            const consumerWithBadParser = new BatchConsumerImplementation<{ value: number }>(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    parseMessageFn: async () => {
                        await sleepPromise(250);
                        throw parseError;
                    },
                },
            );

            await consumerWithBadParser.listen(vi.fn());
            const { consumePromise } = processGeneratedMessages(channel, 5);
            const closePromise = consumerWithBadParser.close(500);
            await consumePromise;

            await expect(closePromise).resolves.toBeUndefined();
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
                    batchSize: 4,
                    batchFailureStrategy: BatchFailureStrategy.Split,
                    failureStrategy: ConsumptionFailureStrategy.Reject,
                    maxWaitTimeForAck: 0,
                },
            );

            const listener = vi.fn()
                .mockImplementationOnce(() => Promise.reject(new Error('trigger split')))
                .mockResolvedValue(undefined);

            await consumer.listen(listener);
            const { rabbitMessages, consumePromise } = processGeneratedMessages(channel, 4);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
        });
    });
});
