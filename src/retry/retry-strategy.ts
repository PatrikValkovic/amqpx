import { TimeStrategy, linearBackoff } from './time-strategies';
import { DEFAULT_RETRY_STRATEGY } from './default-retry-strategy';
// Kept for JSDoc



/**
 * Retry strategy that controls the delay between attempts and the maximum number of retries.
 *
 * Delays may be fixed or dynamic, using the time strategies accessible from {@link retryStrategies}.
 */
export interface RetryStrategy {
    /**
     * Delay before the next retry attempt, in milliseconds.
     * Can be a fixed number or a {@link TimeStrategy} function from {@link retryStrategies} for dynamic backoff.
     */
    reconnectionTimeoutMs?: number | TimeStrategy;

    /**
     * Maximum number of retry attempts.
     * After this many failed attempts, a `TooManyRetriesError` is thrown.
     */
    maxRetries?: number;
}

/**
 * Normalizes a retry strategy so that all properties are defined.
 * Missing properties are filled in from {@link DEFAULT_RETRY_STRATEGY}.
 * Always converts `reconnectionTimeoutMs` to a {@link TimeStrategy} function.
 *
 * @param retryStrategy - Retry strategy to normalize.
 */
export const normalizeRetryStrategy = (retryStrategy: RetryStrategy): Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: TimeStrategy } => {
    const mergedWithDefault = {
        ...DEFAULT_RETRY_STRATEGY,
        ...retryStrategy,
    };
    return typeof mergedWithDefault.reconnectionTimeoutMs === 'number' ? {
        ...mergedWithDefault,
        reconnectionTimeoutMs: linearBackoff(mergedWithDefault.reconnectionTimeoutMs),
    } : {
        ...mergedWithDefault,
        reconnectionTimeoutMs: mergedWithDefault.reconnectionTimeoutMs as TimeStrategy,
    };
};
