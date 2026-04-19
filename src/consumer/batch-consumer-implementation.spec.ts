import { TestChannel, TestQueue } from '../extensions/vitest';
import { sleepPromise, zip } from '../utils';
import { BatchConsumerImplementation } from './batch-consumer-implementation';
import { ConsumptionFailureStrategy, BatchFailureStrategy } from './types';

describe('Batch consumer implementation', () => {
    let channel: TestChannel;
    let consumer: BatchConsumerImplementation<{  value: number }>;

    beforeEach(() => {
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
        // each messages was ack/nack at most once
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

    const processGeneratedMessages = (numOfMessages: number) => {
        const messagesContent = Array.from({ length: numOfMessages }, (_, i) => i).map(
            value => ({ value }),
        );
        const rabbitMessages = messagesContent.map((content, i) => ({
            content: Buffer.from(JSON.stringify(content)),
            __index: i,
        }));

        const consumerHandler = channel.nativeChannel.consume.mock.lastCall![1];
        const consumePromise = Promise.all(rabbitMessages.map(message => consumerHandler(message)));

        return {
            messagesContent,
            rabbitMessages,
            consumePromise,
        };
    };

    describe('consuming', () => {
        test('should process batch', async () => {
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
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(5);
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
            expect(listener).toHaveBeenCalledTimes(1);
        });

        test('should set batch size based on prefetch when batch size is not provided', async () => {
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
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(20);
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

        test('should set batch size to default 20 when batch size nor prefetch is specified', async () => {
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
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(40);
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

        test('should process partial batch', async () => {
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
            const { consumePromise, rabbitMessages, messagesContent } = processGeneratedMessages(8);
            await consumePromise;

            // wait for acknowledge to fire
            await sleepPromise(800);

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
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(10);
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

        test('should wait for batch but send confirmation anyway when previous batch is not processed within specified time', async () => {
            const consumer = new BatchConsumerImplementation(
                channel,
                new TestQueue(),
                {
                    batchSize: 5,
                    maxWaitTimeForAck: 500,
                },
            );
            const listener = vi.fn()
                .mockImplementationOnce(() => sleepPromise(1000))
                .mockImplementation(
                    () => Promise.resolve(),
                );

            await consumer.listen(listener);
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(10);
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

        test('should wait for the delayed batch and send single confirmation message', async () => {
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
            const { rabbitMessages, messagesContent, consumePromise } = processGeneratedMessages(10);
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
        test('should reject messages', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
        });

        test('should reject the messages individually when first batch is delayed', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(10);
            await consumePromise;


            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, false);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, false);
        });

        test('should requeue the messages', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        test('should requeue the messages individually when first batch is delayed', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(10);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[5], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[6], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[7], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[8], false, true);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[9], false, true);
        });

        test('should have auto acknowledge on when failure strategy is drop', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });
    });

    describe('processing failure with split batch failure strategy', () => {
        test('should split batch and process messages one at a time', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(5);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[0], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[1], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[2], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        test('after split it should respect acknowledgement delay', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[4], true);
        });

        test('should reprocess messages even when failure strategy is drop', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(listener).toHaveBeenCalledTimes(6);
            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        test('should handle each message individually for reject strategy', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
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

        test('should handle each message individually for requeue strategy', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
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
        test('should throw error when invalid batch failure strategy is used', async () => {
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

            const { consumePromise } = processGeneratedMessages(5);

            await expect(consumePromise).rejects.toThrow(`Not supported batch failure strategy: invalid`);
            expect(errorMsg?.message).toEqual('Not supported batch failure strategy: invalid');
        });

        test('should throw error when invalid failure strategy is used with reject batch failure', async () => {
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

            const { consumePromise } = processGeneratedMessages(5);

            await expect(consumePromise).rejects.toThrow(`Not supported failure strategy: invalid`);
            expect(errorMsg?.message).toEqual('Not supported failure strategy: invalid');
        });

        test('should throw error when invalid failure strategy is used with split batch failure', async () => {
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

            const { consumePromise } = processGeneratedMessages(5);

            await expect(consumePromise).rejects.toThrow('Not supported failure strategy: invalid');
            expect(errorMsg?.message).toEqual('Not supported failure strategy: invalid');
        });
    });

    describe('channel close', () => {
        test('should not send ack nor nock when processing is successful', async () => {
            const listener = vi.fn().mockImplementation(
                () => sleepPromise(500),
            );

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(5);

            await sleepPromise(100);
            channel.emit('close');

            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        test('should not send ack nor nock when processing is not successful', async () => {
            const listener = vi.fn().mockImplementation(async () => {
                await sleepPromise(500);
                throw new Error('Testing error');
            });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(5);

            await sleepPromise(100);
            channel.emit('close');

            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        test('should not send ack nor nock when batch is split', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);

            await sleepPromise(100);
            channel.emit('close');

            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        test('should not send ack nor nock when batch is split and split fails', async () => {
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
            const { consumePromise } = processGeneratedMessages(20);

            await sleepPromise(100);
            channel.emit('close');

            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalledTimes(0);
            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(0);
        });

        test('should try to reconnect after channel close', async () => {
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

            await sleepPromise(100);
            channel.emit('close');
            await sleepPromise(400);

            expect(channel.nativeChannel.consume).toHaveBeenCalledTimes(2);
        });
    });

    describe('consumer close', () => {
        test('should wait for processing all batches', async () => {
            const listener = vi.fn().mockImplementation(
                () => sleepPromise(500),
            );

            await consumer.listen(listener);
            const consumerHandler = channel.nativeChannel.consume.mock.lastCall![1];

            const messagesContent = Array.from({ length: 15 }, (_, i) => i).map(
                value => ({ value }),
            );
            const messages = messagesContent.map(content => ({
                content: Buffer.from(JSON.stringify(content)),
            }));
            const consumePromises = messages.map(message => consumerHandler(message));

            await sleepPromise(100);
            await consumer.close();
            await Promise.all(consumePromises);

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
        });

        test('should wait for reject of batches', async () => {
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(500);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(15);

            await sleepPromise(100);
            await consumer.close();
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalled();
        });

        test('should wait for ack of messages after split', async () => {
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
            const { consumePromise } = processGeneratedMessages(15);

            await sleepPromise(100);
            await consumer.close();
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
        });

        test('should wait for nack of messages after split', async () => {
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
            const { consumePromise } = processGeneratedMessages(15);

            await sleepPromise(100);
            await consumer.close();
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalled();
        });

        test('should handle close timeout', async () => {
            const listener = vi.fn()
                .mockImplementation(async () => {
                    await sleepPromise(1000);
                    throw new Error('Testing error');
                });

            await consumer.listen(listener);
            const { consumePromise } = processGeneratedMessages(15);

            await sleepPromise(100);

            await expect(consumer.close(500)).rejects.toThrow('Consumer close timeout');

            await consumePromise;
        });
    });

    describe('parse failure', () => {
        test('should nack batch when parse fails and failureStrategy is Reject', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, false);
        });

        test('should requeue batch when parse fails and failureStrategy is Requeue', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.nack).toHaveBeenCalledTimes(1);
            expect(channel.nativeChannel.nack).toHaveBeenCalledWith(rabbitMessages[4], true, true);
        });

        test('should not ack or nack when parse fails and failureStrategy is Drop', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(channel.nativeChannel.ack).not.toHaveBeenCalled();
            expect(channel.nativeChannel.nack).not.toHaveBeenCalled();
        });

        test('should emit handlingFailed when parse fails', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            expect(emittedError).toBe(parseError);
        });

        test('close() should resolve after parse failure', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            await consumePromise;

            await expect(consumerWithBadParser.close(500)).resolves.toBeUndefined();
        });

        test('close() should resolve after async parse failure', async () => {
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
            const { consumePromise } = processGeneratedMessages(5);
            const closePromise = consumerWithBadParser.close(500);
            await consumePromise;

            await expect(closePromise).resolves.toBeUndefined();
        });

        test('should nack only failing message when split strategy and one message has bad content', async () => {
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
        test('should not ack the same message twice when batches complete concurrently', async () => {
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
            const { rabbitMessages, consumePromise } = processGeneratedMessages(4);
            await consumePromise;

            expect(channel.nativeChannel.ack).toHaveBeenCalled();
            expect(channel.nativeChannel.ack).toHaveBeenCalledWith(rabbitMessages[3], true);
        });
    });

    test('should not allow split batch failure strategy with batch size of 1', () => {
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
