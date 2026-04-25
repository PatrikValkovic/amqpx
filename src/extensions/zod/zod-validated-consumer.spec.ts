import { z, ZodError } from 'zod';
import { TestChannel, TestConsumer, TestQueue } from '../vitest';
import { processGeneratedMessages } from '../../test/generate-messages';
import { ConsumerImplementation } from '../../consumer';
import { ZodValidatedConsumer } from './zod-validated-consumer';

describe('ZodValidatedConsumer', () => {
    const schema = z.object({ value: z.number() });
    let innerConsumer: TestConsumer<{ value: number }>;
    let validatedConsumer: ZodValidatedConsumer<{ value: number }, object>;

    beforeEach(() => {
        innerConsumer = new TestConsumer();
        validatedConsumer = new ZodValidatedConsumer(innerConsumer, schema);
    });

    describe('listen', () => {
        it('should call the callback with the validated message when it is valid', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await innerConsumer.deliverMessage({ value: 42 });

            expect(callback).toHaveBeenCalledTimes(1);
            expect(callback).toHaveBeenCalledWith(expect.objectContaining({
                message: { value: 42 },
            }));
        });

        it('should throw ZodError when the message fails schema validation', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await expect(
                // @ts-expect-error types don't match on purpose
                innerConsumer.deliverMessage({ value: 'not-a-number' }),
            ).rejects.toThrow(ZodError);

            expect(callback).not.toHaveBeenCalled();
        });

        it('should throw ZodError with message describing the failure', async () => {
            await validatedConsumer.listen(vi.fn());

            let caughtError: unknown;
            try {
                // @ts-expect-error types don't match on purpose
                await innerConsumer.deliverMessage({ value: 'bad' });
            } catch (err) {
                caughtError = err;
            }

            expect(caughtError).toBeInstanceOf(ZodError);
            expect((caughtError as ZodError).message).toContain('Invalid input: expected number, received string');
            expect((caughtError as ZodError).issues.length).toBeGreaterThan(0);
        });

        it('should support Zod schemas that transform the message', async () => {
            const transformSchema = z.object({
                value: z.string().transform(s => s.toUpperCase()),
            });

            const channel = new TestChannel();
            const rawConsumer = new ConsumerImplementation(channel, new TestQueue(), {});
            const transformConsumer = new ZodValidatedConsumer(
                rawConsumer,
                transformSchema,
            );

            const callback = vi.fn().mockResolvedValue(undefined);
            await transformConsumer.listen(callback);

            const { consumePromise } = processGeneratedMessages(channel, [{ value: 'hello' }]);
            await consumePromise;

            expect(callback).toHaveBeenCalledWith(expect.objectContaining({
                message: { value: 'HELLO' },
            }));
        });
    });

    describe('close', () => {
        it('should delegate close() to the inner consumer', async () => {
            await validatedConsumer.close(1000);

            expect(innerConsumer.close).toHaveBeenCalledTimes(1);
            expect(innerConsumer.close).toHaveBeenCalledWith(1000);
        });
    });

    describe('handlingFailed event', () => {
        it('should forward handlingFailed events emitted by the inner consumer', () => {
            const handlingFailedListener = vi.fn();
            validatedConsumer.on('handlingFailed', handlingFailedListener);

            const error = new Error('processing failed');
            innerConsumer.emit('handlingFailed', error);

            expect(handlingFailedListener).toHaveBeenCalledTimes(1);
            expect(handlingFailedListener).toHaveBeenCalledWith(error);
        });

        it('should forward ZodError when emitted as handlingFailed by inner consumer', async () => {
            const channel = new TestChannel();
            const rawConsumer = new ConsumerImplementation(channel, new TestQueue(), {});
            const validatedConsumer = new ZodValidatedConsumer(
                rawConsumer,
                schema,
            );

            const handlingFailedListener = vi.fn();
            validatedConsumer.on('handlingFailed', handlingFailedListener);

            await validatedConsumer.listen(vi.fn());

            const { consumePromise } = processGeneratedMessages(channel, [{ value: 'invalid' }]);
            await consumePromise;

            expect(handlingFailedListener).toHaveBeenCalledWith(expect.any(ZodError));
        });
    });

    describe('queue and channel getters', () => {
        it('should expose the inner consumer queue', () => {
            expect(validatedConsumer.queue).toBe(innerConsumer.queue);
        });

        it('should expose the inner consumer channel', () => {
            expect(validatedConsumer.channel).toBe(innerConsumer.channel);
        });
    });
});
