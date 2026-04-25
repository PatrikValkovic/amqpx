import { TestChannel, TestExchange } from '../extensions/vitest';
import { externallyResolvedPromise } from '../utils';
import { ProducerImplementation } from './producer-implementation';
import { ProducerEvents } from './types';

describe('Producer', () => {
    let channel: TestChannel;
    let exchange: TestExchange;
    let producer: ProducerImplementation<{ value: number }>;

    beforeEach(() => {
        vitest.useRealTimers();
        channel = new TestChannel();
        exchange = new TestExchange();
        producer = new ProducerImplementation(channel, exchange);
    });

    afterEach(async () => {
        await producer.close();
    });

    describe('publish', () => {
        it('should return the published message', async () => {
            const message = { value: 42 };
            const result = await producer.publish(message);
            expect(result).toBe(message);
        });

        it('should call channel.publish with exchange name, routing key, serialized buffer, and options', async () => {
            const message = { value: 7 };
            await producer.publish(message);

            expect(channel.publish).toHaveBeenCalledTimes(1);
            const [exchangeName, routingKey, buffer] = channel.publish.mock.calls[0]!;
            expect(exchangeName).toBe('test-exchange');
            expect(routingKey).toBe('');
            expect(buffer).toEqual(Buffer.from(JSON.stringify(message)));
        });

        it('should use an empty string as routing key by default', async () => {
            await producer.publish({ value: 1 });

            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('');
        });

        it('should use a string routing key when provided as second argument', async () => {
            await producer.publish({ value: 1 }, 'my.routing.key');

            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('my.routing.key');
        });

        it('should call routing key generator function with the message when a function is provided', async () => {
            const keyFn = vi.fn().mockReturnValue('generated-key');
            const message = { value: 99 };

            await producer.publish(message, keyFn);

            expect(keyFn).toHaveBeenCalledTimes(1);
            expect(keyFn).toHaveBeenCalledWith(message);
        });

        it('should call routingKey function and use its return value', async () => {
            const keyFn = vi.fn().mockReturnValue('dynamic-key');
            await producer.publish({ value: 5 }, keyFn);

            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('dynamic-key');
        });

        it('should call async routingKey function and use its return value', async () => {
            const keyFn = vi.fn().mockResolvedValue('async-key');
            await producer.publish({ value: 5 }, keyFn);

            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('async-key');
        });

        it('should use custom stringifyMessage function when provided in options', async () => {
            const customBuffer = Buffer.from('custom');
            const stringifyMessage = vi.fn().mockReturnValue(customBuffer);
            const customProducer = new ProducerImplementation(channel, exchange, { stringifyMessage });

            await customProducer.publish({ value: 3 });
            await customProducer.close();

            expect(stringifyMessage).toHaveBeenCalledTimes(1);
            const [, , buffer] = channel.publish.mock.calls[0]!;
            expect(buffer).toBe(customBuffer);
        });

        it('should emit beforeSend event with message and buffer before calling channel.publish', async () => {
            const beforeSendListener = vi.fn();
            producer.on(ProducerEvents.beforeSend, beforeSendListener);

            const message = { value: 10 };
            await producer.publish(message);

            expect(beforeSendListener).toHaveBeenCalledTimes(1);
            const [emittedMessage, emittedBuffer] = beforeSendListener.mock.calls[0]!;
            expect(emittedMessage).toBe(message);
            expect(emittedBuffer).toEqual(Buffer.from(JSON.stringify(message)));

            // beforeSend must fire before channel.publish
            const beforeSendOrder = beforeSendListener.mock.invocationCallOrder[0]!;
            const publishOrder = channel.publish.mock.invocationCallOrder[0]!;
            expect(beforeSendOrder).toBeLessThan(publishOrder);
        });

        it('should emit afterSend event with message and buffer after channel.publish resolves', async () => {
            const afterSendListener = vi.fn();
            producer.on(ProducerEvents.afterSend, afterSendListener);

            const message = { value: 20 };
            await producer.publish(message);

            expect(afterSendListener).toHaveBeenCalledTimes(1);
            const [emittedMessage, emittedBuffer] = afterSendListener.mock.calls[0]!;
            expect(emittedMessage).toBe(message);
            expect(emittedBuffer).toEqual(Buffer.from(JSON.stringify(message)));

            // afterSend must fire after channel.publish
            const publishOrder = channel.publish.mock.invocationCallOrder[0]!;
            const afterSendOrder = afterSendListener.mock.invocationCallOrder[0]!;
            expect(publishOrder).toBeLessThan(afterSendOrder);
        });

        it('should throw "Producer is closed" when publish called after close()', async () => {
            await producer.close();

            await expect(producer.publish({ value: 1 })).rejects.toThrow('Producer is closed');

            // Re-create so afterEach close() doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });
    });

    describe('in-flight tracking', () => {
        it('should add message to in-flight when channel.publish returns false (unconfirmed)', async () => {
            channel.publish.mockResolvedValue(false);
            await producer.publish({ value: 1 });

            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(1);
        });

        it('should not add message to in-flight when channel.publish returns true (confirmed)', async () => {
            channel.publish.mockResolvedValue(true);
            await producer.publish({ value: 1 });

            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(0);
        });

        it('should republish in-flight messages when channel emits "error" event', async () => {
            channel.publish.mockResolvedValue(false);
            await producer.publish({ value: 1 });

            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(1);

            // second publish triggered by the republish will return confirmed
            channel.publish.mockResolvedValue(true);
            channel.emit('error', new Error('channel error'));

            // Allow the republish microtask to complete
            await Promise.resolve();
            await Promise.resolve();

            expect(channel.publish).toHaveBeenCalledTimes(2);
            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(0);
        });

        it('should not republish expired in-flight entries when channel emits "error" (advance past errorWindow)', async () => {
            vitest.useFakeTimers();
            const errorWindowProducer = new ProducerImplementation(channel, exchange, {
                errorWindow: 100,
            });

            try {
                channel.publish.mockResolvedValue(false);
                await errorWindowProducer.publish({ value: 1 });

                // @ts-expect-error inFlight is private
                expect(errorWindowProducer.inFlight.size).toBe(1);

                // Advance past the errorWindow so the entry expires
                await vitest.advanceTimersByTimeAsync(200);

                // @ts-expect-error inFlight is private
                expect(errorWindowProducer.inFlight.size).toBe(0);

                channel.emit('error', new Error('channel error'));

                // Only the first publish should have been made; no republish
                expect(channel.publish).toHaveBeenCalledTimes(1);
            } finally {
                await errorWindowProducer.close();
            }
        });

        it('should emit republishFailed when republish attempt throws', async () => {
            channel.publish.mockResolvedValue(false);
            await producer.publish({ value: 1 });

            const republishFailedListener = vi.fn();
            producer.on(ProducerEvents.republishFailed, republishFailedListener);

            const republishError = new Error('republish failed');
            channel.publish.mockRejectedValue(republishError);
            channel.emit('error', new Error('channel error'));

            // republishFailed fires after several async hops inside publish():
            // Promise.all (key+buffer+exchangeName) → channel.publish rejection → .then().catch()
            await vi.waitFor(() => expect(republishFailedListener).toHaveBeenCalledTimes(1));
            const [, emittedError] = republishFailedListener.mock.calls[0]!;
            expect(emittedError).toBe(republishError);
        });

        it('should not republish expired in-flight entries on channel error when errorWindow=0', async () => {
            // With errorWindow=0 entries have expiresAt=now, so they fail the expiresAt >= now
            // guard in handleChannelError and are never republished.
            const noWindowProducer = new ProducerImplementation(channel, exchange, {
                errorWindow: 0,
            });

            try {
                channel.publish.mockResolvedValue(false);
                await noWindowProducer.publish({ value: 1 });

                // Add a no-op error handler so emit('error') doesn't throw
                channel.on('error', () => {});
                channel.emit('error', new Error('channel error'));

                // No republish should happen (entry is already expired)
                await Promise.resolve();
                expect(channel.publish).toHaveBeenCalledTimes(1);
            } finally {
                await noWindowProducer.close();
            }
        });
    });

    describe('close', () => {
        it('should resolve immediately when no publishes are in-flight', async () => {
            await expect(producer.close()).resolves.toBeUndefined();

            // Re-create so afterEach close() doesn't double-close
            producer = new ProducerImplementation(channel, exchange);
        });

        it('should wait for a pending channel.publish to finish before resolving', async () => {
            const [publishPromise, releasePublish] = externallyResolvedPromise<boolean>();
            channel.publish.mockReturnValue(publishPromise);

            let publishSettled = false;
            const publishCall = producer.publish({ value: 1 }).then(() => {
                publishSettled = true;
            });

            // Allow the publish to start and register the pending promise
            await Promise.resolve();

            let closedSettled = false;
            const closeCall = producer.close().then(() => {
                closedSettled = true;
            });

            // close should not have resolved yet
            await Promise.resolve();
            expect(closedSettled).toBe(false);

            // Release the blocked publish
            releasePublish(true);
            await publishCall;
            await closeCall;

            expect(publishSettled).toBe(true);
            expect(closedSettled).toBe(true);

            // Already closed — recreate so afterEach doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });

        it('should not republish after close (channel error emitted after close does nothing)', async () => {
            channel.publish.mockResolvedValue(false);
            await producer.publish({ value: 1 });

            await producer.close();

            // After close(), the channel error listener is removed. Emitting 'error' with no handler
            // would throw in Node.js, so we add a no-op listener first.
            channel.on('error', () => {});

            const publishCountBeforeError = channel.publish.mock.calls.length;
            channel.emit('error', new Error('late channel error'));

            // Allow any potential microtasks to run
            await Promise.resolve();
            await Promise.resolve();

            expect(channel.publish).toHaveBeenCalledTimes(publishCountBeforeError);

            // Recreate so afterEach close() doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });
    });

    describe('events', () => {
        it('should pass correct buffer to beforeSend (verify JSON serialization)', async () => {
            const message = { value: 123 };
            const capturedBuffers: Buffer[] = [];
            producer.on(ProducerEvents.beforeSend, (_msg, buf) => {
                capturedBuffers.push(buf);
            });

            await producer.publish(message);

            expect(capturedBuffers).toHaveLength(1);
            expect(capturedBuffers[0]).toEqual(Buffer.from(JSON.stringify(message)));
            expect(capturedBuffers[0]!.toString()).toBe(JSON.stringify(message));
        });

        it('should pass same buffer to afterSend as beforeSend', async () => {
            let beforeBuffer: Buffer | undefined;
            let afterBuffer: Buffer | undefined;

            producer.on(ProducerEvents.beforeSend, (_msg, buf) => {
                beforeBuffer = buf;
            });
            producer.on(ProducerEvents.afterSend, (_msg, buf) => {
                afterBuffer = buf;
            });

            await producer.publish({ value: 7 });

            expect(beforeBuffer).toBeDefined();
            expect(afterBuffer).toBeDefined();
            expect(afterBuffer).toEqual(beforeBuffer);
        });
    });
});
