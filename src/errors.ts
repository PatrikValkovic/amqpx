/**
 * Thrown when a retryable operation exhausts all retry attempts
 * as defined by the configured {@link RetryStrategy}.
 */
export class TooManyRetriesError extends Error {}

/**
 * Thrown when the channel's internal write buffer does not drain
 * within the configured drain timeout.
 * The underlying amqplib channel is closed when this error is thrown.
 */
export class DrainError extends Error {}
