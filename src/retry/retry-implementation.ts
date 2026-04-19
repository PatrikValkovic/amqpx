import { debuglog } from 'util';
import { errToMessage, LIB_NAME, sleepPromise } from '../utils';
import { TooManyRetriesError } from '../errors';
import { MaybePromise } from '../types';
import { normalizeRetryStrategy, RetryStrategy } from './retry-strategy';

const debug = debuglog(`${LIB_NAME}:retry`);

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
    shouldRetry: (error: unknown) => boolean = () => true,
) => {
    const normalizedStrategy = normalizeRetryStrategy(strategy);
    let attempt = 0;
    let lastErr: unknown | null = null;

    while (attempt < normalizedStrategy.maxRetries) {
        attempt++;
        debug('attempt current=%d total=%d', attempt, normalizedStrategy.maxRetries);
        try {
            return await callback();
        } catch (err) {
            lastErr = err;
            if (shouldRetry(err)) {
                const delay = normalizedStrategy.reconnectionTimeoutMs(attempt);
                debug('attempt-failed retrying-in=%dms error=%s', delay, errToMessage(err));
                await sleepPromise(delay);
            } else {
                debug('non-retryable-error attempt=%d error=%s', attempt, errToMessage(err));
                throw err;
            }
        }
    }

    debug('max-retries-exhausted attempts=%d error=%s', normalizedStrategy.maxRetries, errToMessage(lastErr));
    throw new TooManyRetriesError(`Too many retries`, lastErr);
};
