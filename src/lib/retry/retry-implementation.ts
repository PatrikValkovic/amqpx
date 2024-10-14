import { TooManyRetriesError } from '../../errors';
import { sleepPromise } from '../utils';
import { normalizeRetryStrategy, RetryStrategy } from './retry-strategy';

type MaybePromise<T> = Promise<T> | T;

export const retryLoop = async <T>(strategy: RetryStrategy, callback: () => MaybePromise<T>, shouldRetry: (error: Error) => boolean = () => true) => {
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
            } else {
                throw err;
            }
        }
    }

    throw new TooManyRetriesError(`Too many retries: ${lastErr?.message}`);
};
