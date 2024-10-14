import { RetryStrategy } from './retry-strategy';
import { doublingTimeStrategy } from './time-strategies';

export const DEFAULT_RETRY_STRATEGY = {
    maxRetries: 10,
    reconnectionTimeoutMs: doublingTimeStrategy(100),
    waitTimeoutMs: 100,
} as const satisfies Required<RetryStrategy>;
