import { z } from 'zod';
import { TestConsumer } from '../vitest';
import { ZodValidatedConsumer } from './zod-validated-consumer';
import { ZodValidationError } from './zod-validation-error';

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

        it('should throw ZodValidationError when the message fails schema validation', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await expect(
                innerConsumer.deliverMessage({ value: 'not-a-number' } as any),
            ).rejects.toThrow(ZodValidationError);

            expect(callback).not.toHaveBeenCalled();
        });

        it('should throw ZodValidationError with message describing the failure', async () => {
            await validatedConsumer.listen(vi.fn());

            let caughtError: unknown;
            try {
                await innerConsumer.deliverMessage({ value: 'bad' } as any);
            } catch (err) {
                caughtError = err;
            }

            expect(caughtError).toBeInstanceOf(ZodValidationError);
            expect((caughtError as ZodValidationError).message).toContain('validation failed');
        });

        it('should attach the zodError property to the thrown ZodValidationError', async () => {
            await validatedConsumer.listen(vi.fn());

            let caughtError: unknown;
            try {
                await innerConsumer.deliverMessage({ value: 'bad' } as any);
            } catch (err) {
                caughtError = err;
            }

            expect(caughtError).toBeInstanceOf(ZodValidationError);
            expect((caughtError as ZodValidationError).zodError).toBeInstanceOf(z.ZodError);
            expect((caughtError as ZodValidationError).zodError.issues.length).toBeGreaterThan(0);
        });

        it('should pass channel and rabbitMessage through to the callback unchanged', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await innerConsumer.deliverMessage({ value: 1 });

            const { channel, rabbitMessage } = callback.mock.calls[0]![0];
            expect(channel).toBe(innerConsumer.channel);
            expect(rabbitMessage).toBeDefined();
            expect(rabbitMessage.content).toBeDefined();
        });

        it('should return the ZodValidatedConsumer itself', async () => {
            const result = await validatedConsumer.listen(vi.fn());

            expect(result).toBe(validatedConsumer);
        });

        it('should support Zod schemas that transform the message', async () => {
            const transformSchema = z.object({ value: z.string().transform(s => s.toUpperCase()) });
            const transformConsumer = new ZodValidatedConsumer(
                new TestConsumer<{ value: string }>(),
                transformSchema,
            );

            const callback = vi.fn().mockResolvedValue(undefined);
            await transformConsumer.listen(callback);

            await (transformConsumer as any).consumer.deliverMessage({ value: 'hello' });

            expect(callback).toHaveBeenCalledWith(expect.objectContaining({
                message: { value: 'HELLO' },
            }));
        });
    });

    describe('close', () => {
        it('should delegate close() to the inner consumer', async () => {
            await validatedConsumer.close();

            expect(innerConsumer.close).toHaveBeenCalledTimes(1);
        });

        it('should pass the timeout argument to the inner consumer', async () => {
            await validatedConsumer.close(1000);

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

        it('should forward ZodValidationError when emitted as handlingFailed by inner consumer', () => {
            const handlingFailedListener = vi.fn();
            validatedConsumer.on('handlingFailed', handlingFailedListener);

            const zodError = new ZodValidationError('bad', new z.ZodError([]));
            innerConsumer.emit('handlingFailed', zodError);

            expect(handlingFailedListener).toHaveBeenCalledWith(zodError);
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
