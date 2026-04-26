import { z, ZodError } from 'zod';
import { TestBatchConsumer } from '../vitest';
import { ZodValidatedBatchConsumer } from './zod-validated-batch-consumer';

describe('ZodValidatedBatchConsumer', () => {
    const schema = z.object({ value: z.number() });
    let innerConsumer: TestBatchConsumer<{ value: number }>;
    let validatedConsumer: ZodValidatedBatchConsumer<{ value: number }, object>;

    beforeEach(() => {
        innerConsumer = new TestBatchConsumer();
        validatedConsumer = new ZodValidatedBatchConsumer(innerConsumer, schema);
    });

    describe('listen', () => {
        it('should call the callback with validated messages when all are valid', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await innerConsumer.deliverMessages([{ value: 1 }, { value: 2 }]);

            expect(callback).toHaveBeenCalledTimes(1);
            expect(callback).toHaveBeenCalledWith(expect.objectContaining({
                messages: expect.arrayContaining([
                    expect.objectContaining({ message: { value: 1 } }),
                    expect.objectContaining({ message: { value: 2 } }),
                ]),
            }));
        });

        it('should throw ZodError when a message fails schema validation', async () => {
            const callback = vi.fn().mockResolvedValue(undefined);
            await validatedConsumer.listen(callback);

            await expect(
                // @ts-expect-error types don't match on purpose
                innerConsumer.deliverMessages([{ value: 'not-a-number' }]),
            ).rejects.toThrow(ZodError);

            expect(callback).not.toHaveBeenCalled();
        });

        it('should throw ZodError describing the failure', async () => {
            await validatedConsumer.listen(vi.fn());

            let caughtError: unknown;
            try {
                // @ts-expect-error types don't match on purpose
                await innerConsumer.deliverMessages([{ value: 'bad' }]);
            } catch (err) {
                caughtError = err;
            }

            expect(caughtError).toBeInstanceOf(ZodError);
            expect((caughtError as ZodError).message).toContain('Invalid input: expected number, received string');
        });

        it('should support Zod schemas that transform messages', async () => {
            const transformSchema = z.object({
                value: z.string().transform(s => s.toUpperCase()),
            });
            const innerTransformConsumer = new TestBatchConsumer<{ value: string }>();
            const transformConsumer = new ZodValidatedBatchConsumer(innerTransformConsumer, transformSchema);

            const callback = vi.fn().mockResolvedValue(undefined);
            await transformConsumer.listen(callback);

            await innerTransformConsumer.deliverMessages([{ value: 'hello' }, { value: 'world' }]);

            expect(callback).toHaveBeenCalledWith(expect.objectContaining({
                messages: expect.arrayContaining([
                    expect.objectContaining({ message: { value: 'HELLO' } }),
                    expect.objectContaining({ message: { value: 'WORLD' } }),
                ]),
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
            const listener = vi.fn();
            validatedConsumer.on('handlingFailed', listener);

            const error = new Error('processing failed');
            innerConsumer.emit('handlingFailed', error);

            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith(error);
        });
    });

    describe('error event', () => {
        it('should forward error events emitted by the inner consumer', () => {
            const listener = vi.fn();
            validatedConsumer.on('error', listener);

            const error = new Error('internal error');
            innerConsumer.emit('error', error);

            expect(listener).toHaveBeenCalledTimes(1);
            expect(listener).toHaveBeenCalledWith(error);
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
