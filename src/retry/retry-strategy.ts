import { ITimeStrategy, linearTimeStrategy } from './time-strategies';
import { DEFAULT_RETRY_STRATEGY } from './default-retry-strategy';
// noinspection ES6UnusedImports Allowed for JSDOc link
import * as timeStrategies from './time-strategies';

/**
 * Retry strategy allowing to specify gaps between retries and maximum
 * number of retries.
 *
 * Gaps may be specified constantly or dynamically, using time strategies
 * accessible from {@link timeStrategies}.
 */
export interface RetryStrategy {
    /**
     * For how long to wait in minimum before application is trying to reconnect.
     */
    waitTimeoutMs?: number; // TODO is doc correct?

    /**
     * How much time to wait before next attempt to reconnect in ms.
     * May be number (it will wait specified amount of ms between reconnects) or time strategy
     * accessible from {@link timeStrategies}.
     */
    reconnectionTimeoutMs?: number | ITimeStrategy;

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
export const normalizeRetryStrategy = (retryStrategy: RetryStrategy): Required<Omit<RetryStrategy, 'reconnectionTimeoutMs'>> & { reconnectionTimeoutMs: ITimeStrategy } => {
    const mergedWithDefault = {
        ...DEFAULT_RETRY_STRATEGY,
        ...retryStrategy,
    };
    return typeof mergedWithDefault.reconnectionTimeoutMs === 'number' ? {
        ...mergedWithDefault,
        reconnectionTimeoutMs: linearTimeStrategy(mergedWithDefault.reconnectionTimeoutMs),
    } : {
        ...mergedWithDefault,
        reconnectionTimeoutMs: mergedWithDefault.reconnectionTimeoutMs as ITimeStrategy,
    };
};
