import { sleepPromise, TooManyRetriesError } from '../utils';
import { MaybePromise } from '../types';
import { normalizeRetryStrategy, RetryStrategy } from './retry-strategy';

/**
 * Retries callback until it succeeds or reaches max retries.
 *
 * You can decide whether the retry (up to max attempts) should be done based on the error thrown by the callback.
 *
 * @param strategy Retry strategy to use. Missing properties will be taken from the default retry strategy,
 * that by default retries 10 times with exponential delay. See {@link DEFAULT_RETRY_STRATEGY} for default values.
 * @param callback Callback to call in the loop. If the callback throws an error, it will be caught and the loop will retry.
 * If the callback returns a promise, it will be awaited and the loop will retry if the promise is rejected.
 * @param shouldRetry Function that decides whether the retry should be performed based on the error thrown by the callback.
 * If it returns true, the callback will be retried. If it returns false, the error will be thrown and the loop will end.
 * @throws TooManyRetriesError if callback fails after max retries as specified by the retry strategy.
 * @throws Error if callback throws an error for which `shouldRetry` returns false.
 * @returns Result of the callback.
 */
export const retryLoop = async <T>(
    strategy: RetryStrategy,
    callback: () => MaybePromise<T>,
    shouldRetry: (error: Error) => boolean = () => true,
) => {
    const normalizedStrategy = normalizeRetryStrategy(strategy);
    let attempt = 0;
    let lastErr: Error | null = null;

    while (attempt < normalizedStrategy.maxRetries) {
        attempt++;
        try {
            return await callback();
        } catch (err) {
            if (err instanceof Error) {
                lastErr = err;
                if (shouldRetry(err))
                    await sleepPromise(normalizedStrategy.reconnectionTimeoutMs(attempt));
                else
                    throw err;
            } else {
                throw err;
            }
        }
    }

    throw new TooManyRetriesError(`Too many retries: ${lastErr?.message}`);
};
