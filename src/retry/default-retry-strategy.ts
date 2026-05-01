import { RetryStrategy } from './retry-strategy';
import { exponentialBackoff } from './time-strategies';

/**
 * Default retry strategy used throughout the library when the user does not provide their own.
 *
 * Retries up to 10 times with exponential backoff starting at 100ms.
 */
export const DEFAULT_RETRY_STRATEGY = {
    maxRetries: 10,
    reconnectionTimeoutMs: exponentialBackoff(100),
} as const satisfies Required<RetryStrategy>;
