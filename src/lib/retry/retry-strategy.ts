import { ITimeStrategy, linearTimeStrategy } from './time-strategies';
import { DEFAULT_RETRY_STRATEGY } from './default-retry-strategy';

export interface RetryStrategy {
    /**
     * For how long to wait of other part of application is trying to reconnect.
     */
    waitTimeoutMs?: number;

    /**
     * How much time to wait before next attempt to reconnect in ms.
     * May be number (it will wait specified amount of ms between reconnects) or time strategy.
     */
    reconnectionTimeoutMs?: number | ITimeStrategy;

    /**
     * Maximum number the connection tries to reconnect.
     */
    maxRetries?: number;
}

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
