/**
 * A value that is either already resolved (`T`) or wrapped in a `Promise<T>`.
 * Used throughout the library to accept both synchronous and asynchronous callbacks.
 */
export type MaybePromise<T> = Promise<T> | T;

/**
 * Controls how an entity is verified against the broker when `assert()` is called.
 *
 * - `Assert` (default) — declare the entity, creating it if absent; fail on config mismatch.
 * - `Check` — verify the entity exists without creating it; fail if absent.
 * - `Passive` — skip all broker interaction; no network call is made.
 */
export enum AssertionMode {
    /**
     * Assert the entity against the broker.
     * Creates it if absent; throws if it exists with incompatible options.
     * This is the default mode.
     */
    Assert = 'Assert',
    /**
     * Only check that the entity exists on the broker.
     * Does not create it; throws if absent.
     */
    Check = 'Check',
    /**
     * Skip all broker-side verification.
     * No network call is made when `assert()` is invoked.
     */
    Passive = 'Passive',
}
