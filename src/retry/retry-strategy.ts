import { TimeStrategy, linearBackoff } from './time-strategies';
import { DEFAULT_RETRY_STRATEGY } from './default-retry-strategy';
// Kept for JSDoc
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { retryStrategies } from './index';


/**
 * Retry strategy allowing to specify gaps between retries and maximum
 * number of retries.
 *
 * Gaps may be specified constantly or dynamically, using time strategies
 * accessible from {@link retryStrategies}.
 */
export interface RetryStrategy {
    /**
     * How much time to wait before next attempt to reconnect in ms.
     * May be number (it will wait specified amount of ms between reconnects) or time strategy
     * accessible from {@link retryStrategies}.
     */
    reconnectionTimeoutMs?: number | TimeStrategy;

    /**
     * Maximum number the connection tries to reconnect.
     * After this number of attempts, the connection will be closed.
     */
    maxRetries?: number;
}

/**
 * Normalizes retry strategy to have all properties defined.
 * Missing properties will be taken from the default retry strategy. See {@link DEFAULT_RETRY_STRATEGY} for default values.
 *
 * It will always turn reconnectionTimeoutMs into ITimeStrategy.
 *
 * @param retryStrategy Retry strategy to normalize.
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
