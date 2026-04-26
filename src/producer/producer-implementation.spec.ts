import { TestChannel, TestExchange } from '../extensions/vitest';
import { externallyResolvedPromise } from '../utils';
import { ProducerImplementation } from './producer-implementation';


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
            expect(routingKey).toBe(''); // empty string by default
            expect(buffer).toEqual(Buffer.from(JSON.stringify(message)));
        });

        it('should use custom stringifyMessage function when provided in options', async () => {
            const customBuffer = Buffer.from('custom');
            const stringifyMessage = vi.fn().mockReturnValue(customBuffer);
            const customProducer = new ProducerImplementation(channel, exchange, {
                stringifyMessage,
            });

            await customProducer.publish({ value: 3 });
            await customProducer.close();

            expect(stringifyMessage).toHaveBeenCalledTimes(1);
            const [, , buffer] = channel.publish.mock.calls[0]!;
            expect(buffer).toBe(customBuffer);
        });

        it('should emit beforeSend event with message before calling channel.publish', async () => {
            const beforeSendListener = vi.fn();
            producer.on('beforeSend', beforeSendListener);

            const message = { value: 10 };
            await producer.publish(message);

            expect(channel.publish).toHaveBeenCalled();
            expect(beforeSendListener).toHaveBeenCalledTimes(1);
            const [emittedMessage, emittedBuffer] = beforeSendListener.mock.calls[0]!;
            expect(emittedMessage).toBe(message);
            expect(emittedBuffer).toEqual(Buffer.from(JSON.stringify(message)));

            // beforeSend must fire before channel.publish
            const beforeSendOrder = beforeSendListener.mock.invocationCallOrder[0]!;
            const publishOrder = channel.publish.mock.invocationCallOrder[0]!;
            expect(beforeSendOrder).toBeLessThan(publishOrder);
        });

        it('should emit afterSend event with message after channel.publish resolves', async () => {
            const afterSendListener = vi.fn();
            producer.on('afterSend', afterSendListener);

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

            await expect(
                producer.publish({ value: 1 }),
            ).rejects.toThrow('Producer is closed');

            // Re-create so afterEach close() doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });
    });

    describe('routing key', () => {
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
            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('generated-key');
        });

        it('should call async routingKey function and use its return value', async () => {
            const keyFn = vi.fn().mockResolvedValue('async-key');
            const message = { value: 5 };

            await producer.publish(message, keyFn);

            expect(keyFn).toHaveBeenCalledTimes(1);
            expect(keyFn).toHaveBeenCalledWith(message);
            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('async-key');
        });

        it('should use a string routing key when provided in options', async () => {
            producer = new ProducerImplementation(channel, exchange, {
                routingKey: 'my.routing.key',
            });

            await producer.publish({ value: 1 });

            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('my.routing.key');
        });

        it('should call routing key generator function when provided in options', async () => {
            const keyFn = vi.fn().mockReturnValue('generated-key');
            producer = new ProducerImplementation(channel, exchange, {
                routingKey: keyFn,
            });

            const message = { value: 99 };
            await producer.publish(message);

            expect(keyFn).toHaveBeenCalledTimes(1);
            expect(keyFn).toHaveBeenCalledWith(message);
            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('generated-key');
        });

        it('should call async routingKey function when provided in options', async () => {
            const keyFn = vi.fn().mockResolvedValue('async-key');
            producer = new ProducerImplementation(channel, exchange, {
                routingKey: keyFn,
            });

            const message = { value: 5 };
            await producer.publish(message);

            expect(keyFn).toHaveBeenCalledTimes(1);
            expect(keyFn).toHaveBeenCalledWith(message);
            const [, routingKey] = channel.publish.mock.calls[0]!;
            expect(routingKey).toBe('async-key');
        });
    });

    describe('in-flight tracking', () => {
        it('should add message to in-flight when channel.publish returns false (unconfirmed)', async () => {
            channel.publish.mockResolvedValue(false);
            await producer.publish({ value: 1 });

            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(1);
        });

        it('should not add message to in-flight when channel is confirmed', async () => {
            channel.publish.mockResolvedValue(true);
            await producer.publish({ value: 1 });

            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(0);
        });

        it('should republish in-flight messages when channel emits "error" event', async () => {
            vitest.useFakeTimers();
            // must be created a new to respect fake timers
            producer = new ProducerImplementation(channel, exchange);

            await producer.publish({ value: 1 });
            // @ts-expect-error inFlight is private
            expect(producer.inFlight.size).toBe(1);

            channel.emit('error', new Error('channel error'));

            // Allow the republish microtask to complete
            await vitest.advanceTimersByTimeAsync(10_000);

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
            const message = { value: 1 };
            await producer.publish(message);

            const republishFailedListener = vi.fn();
            producer.on('republishFailed', republishFailedListener);

            const republishError = new Error('republish failed');
            channel.publish.mockRejectedValue(republishError);
            channel.emit('error', new Error('channel error'));

            // republishFailed fires after several async hops inside publish():
            // Promise.all (key+buffer+exchangeName) → channel.publish rejection → .then().catch()
            await vi.waitFor(() => expect(republishFailedListener).toHaveBeenCalledTimes(1));
            const [msg, emittedError] = republishFailedListener.mock.calls[0]!;
            expect(emittedError).toBe(republishError);
            expect(msg).toBe(message);
        });

        it('should not republish expired in-flight entries on channel error when errorWindow=0', async () => {
            vitest.useFakeTimers();

            // With errorWindow=0 entries have expiresAt=now, so they fail the expiresAt >= now
            // guard in handleChannelError and are never republished.
            const noWindowProducer = new ProducerImplementation(channel, exchange, {
                errorWindow: 0,
            });

            try {
                await noWindowProducer.publish({ value: 1 });

                channel.emit('error', new Error('channel error'));

                await vitest.advanceTimersByTimeAsync(10_000);
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
            vitest.useFakeTimers();
            const [latch, releaseLatch] = externallyResolvedPromise<boolean>();
            channel.publish.mockReturnValue(latch);

            const publishPromise = producer.publish({ value: 1 });

            // Allow the publish to start and register the pending promise
            await vitest.advanceTimersByTimeAsync(100);

            let closedSettled = false;
            const closePromise = producer.close().then(() => {
                closedSettled = true;
            });

            // close should not have resolved yet
            await vitest.advanceTimersByTimeAsync(100);
            expect(closedSettled).toBe(false);

            // Release the blocked publish
            releaseLatch(true);
            await publishPromise;
            await closePromise;

            expect(closedSettled).toBe(true);

            // Already closed — recreate so afterEach doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });

        it('should not republish after close (channel error emitted after close does nothing)', async () => {
            vitest.useFakeTimers();
            await producer.publish({ value: 1 });

            await producer.close();

            // After close(), the channel error listener is removed. Emitting 'error' with no handler
            // would throw in Node.js, so we add a no-op listener first.
            channel.on('error', () => {});

            expect(channel.publish).toHaveBeenCalledTimes(1);
            channel.emit('error', new Error('late channel error'));

            // Allow any potential microtasks to run
            await vitest.advanceTimersByTimeAsync(100);

            expect(channel.publish).toHaveBeenCalledTimes(1);

            // Recreate so afterEach close() doesn't fail
            producer = new ProducerImplementation(channel, exchange);
        });
    });
});
