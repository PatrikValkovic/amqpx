import { TooManyRetriesError } from '../utils';
import { retryLoop } from './retry-implementation';

describe('Retry', () => {
    describe('retryLoop', () => {
        const zeroDelayStrategy = { reconnectionTimeoutMs: 0, waitTimeoutMs: 0, maxRetries: 3 };

        test('returns callback result on first success', async () => {
            const result = await retryLoop(zeroDelayStrategy, () => 42);
            expect(result).toBe(42);
        });

        test('retries and succeeds after initial failures', async () => {
            let calls = 0;
            const result = await retryLoop(zeroDelayStrategy, () => {
                calls++;
                if (calls < 3)
                    throw new Error('fail');
                return 'ok';
            });
            expect(result).toBe('ok');
            expect(calls).toBe(3);
        });

        test('throws TooManyRetriesError after maxRetries exhausted', async () => {
            const callback = vi.fn().mockRejectedValue(new Error('always fails'));
            await expect(retryLoop(zeroDelayStrategy, callback)).rejects.toThrow(TooManyRetriesError);
            expect(callback).toHaveBeenCalledTimes(3);
        });

        test('throws immediately when shouldRetry returns false', async () => {
            let calls = 0;
            const err = new Error('un-retryable');
            await expect(
                retryLoop(zeroDelayStrategy, () => {
                    calls++;
                    throw err;
                }, () => false),
            ).rejects.toThrow(err);
            expect(calls).toBe(1);
        });

        test('rethrows non-Error exceptions without retrying', async () => {
            const thrown = 'string error';
            await expect(
                retryLoop(zeroDelayStrategy, () => {
                    throw thrown;
                }),
            ).rejects.toBe(thrown);
        });

        test('works with async callback', async () => {
            const result = await retryLoop(zeroDelayStrategy, async () => 'async-result');
            expect(result).toBe('async-result');
        });

        test('shouldRetry is called with the thrown error', async () => {
            const err = new Error('check me');
            const shouldRetry = vi.fn().mockReturnValue(false);
            await expect(
                retryLoop(zeroDelayStrategy, () => {
                    throw err;
                }, shouldRetry),
            ).rejects.toThrow(err);
            expect(shouldRetry).toHaveBeenCalledWith(err);
        });

        test('uses default strategy when partial strategy provided', async () => {
            // maxRetries defaults to 10, but callback succeeds on first try
            const result = await retryLoop({}, () => 'default-ok');
            expect(result).toBe('default-ok');
        });
    });
});
