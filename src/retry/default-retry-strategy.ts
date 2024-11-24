import { RetryStrategy } from './retry-strategy';
import { doublingTimeStrategy } from './time-strategies';

/**
 * Default retry strategy that is used thorough *amqp-oop* library, if user doesn't provide its own.
 *
 * It will by default retry 10 times, with exponential delay between retries.
 */
export const DEFAULT_RETRY_STRATEGY = {
    maxRetries: 10,
    reconnectionTimeoutMs: doublingTimeStrategy(100),
    waitTimeoutMs: 100,
} as const satisfies Required<RetryStrategy>;
