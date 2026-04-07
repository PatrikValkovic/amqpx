/**
 * Function specification for time strategy. It takes step number and returns delay for this step.
 * @param step Step number. Starts from 1.
 */
export type ITimeStrategy = (step: number) => number;

/**
 * Adds jitter to the delay by randomizing it within a symmetric interval around the original value.
 * The jitter range is `± (delay * jitterFactor)`.
 *
 * @param delay Base delay in ms.
 * @param jitterFactor Fraction of `delay` to use as the randomization window. Clamped to [0, 1].
 *   A value of `0.1` randomizes within ±10% of `delay` (e.g. 1000 ms → 900–1100 ms).
 *   A value of `0.25` randomizes within ±25% of `delay` (e.g. 1000 ms → 750–1250 ms).
 *   A value of `0` disables jitter entirely and returns `delay` unchanged.
 * @returns Delay with jitter applied, rounded down to the nearest integer.
 *
 * @example
 * // No jitter — always returns 500
 * addJitter(500, 0); // → 500
 *
 * @example
 * // jitterFactor=0.1 → ±10% window → result in [450, 550]
 * addJitter(500, 0.1); // → e.g. 487
 *
 * @example
 * // jitterFactor=0.25 → ±25% window → result in [375, 625]
 * addJitter(500, 0.25); // → e.g. 521
 */
const addJitter = (delay: number, jitterFactor: number) => {
    const clampedFactor = Math.min(1, Math.max(0, jitterFactor));
    return Math.floor(clampedFactor > 0 ? delay + (Math.random() * 2 - 1) * (delay * clampedFactor) : delay);
};

/**
 * Returns `delay` in each step, ignoring step number, turning this strategy into constant delays with random jitter.
 * @param delay Delay in ms.
 * @param jitter Jitter factor in [0, 1]. Randomizes the delay within ±`jitter * delay`. Use `0` to disable jitter.
 */
export const linearBackoff = (delay: number, jitter = 0.25) =>
    () => addJitter(delay, jitter);

/**
 * Increase the value each step using formula `multiplier * base^step`.
 * For `base=2` and `multiplier=100` will result in series: 100, 200, 400, 800, ...
 * @param multiplier Multiplier of the function, specifying the delay in the first step. Default is 100.
 * @param base Base of the exponential function. Default is 2, resulting in doubling the time each step.
 * @param jitter Jitter factor in [0, 1]. Randomizes the delay within ±`jitter * delay`. Use `0` to disable jitter.
 */
export const exponentialBackoff = (multiplier: number, base = 2, jitter = 0.25) =>
    (step: number) => {
        const delay = multiplier * Math.pow(base, step - 1);
        return addJitter(delay, jitter);
    };

/**
 * Increase the value each step using formula `multiplier * base^step`, capped at `maxDelay`.
 * Behaves identically to {@link exponentialBackoff} but clamps the result to `maxDelay` before applying jitter.
 * For `base=2`, `multiplier=100`, and `maxDelay=500` will result in series: 100, 200, 400, 500, 500, ...
 * @param multiplier Multiplier of the function, specifying the delay in the first step.
 * @param maxDelay Maximum delay in ms. The raw exponential value is clamped to this before jitter is applied.
 * @param base Base of the exponential function. Default is 2, resulting in doubling the time each step.
 * @param jitter Jitter factor in [0, 1]. Randomizes the delay within ±`jitter * delay`. Use `0` to disable jitter.
 */
export const cappedExponentialBackoff = (multiplier: number, maxDelay: number, base = 2, jitter = 0.25) =>
    (step: number) => {
        const delay = Math.min(maxDelay, multiplier * Math.pow(base, step - 1));
        return addJitter(delay, jitter);
    };

/**
 * Increase the value each step using formula `multiplier * step^exponent`.
 * For `exponent=2` (quadratic) and `multiplier=100` will result in series: 100, 400, 900, 1600, ...
 * @param multiplier Multiplier of the function, specifying the delay in the first step.
 * @param exponent Exponent of the polynomial. Default is 2 (quadratic).
 * @param jitter Jitter factor in [0, 1]. Randomizes the delay within ±`jitter * delay`. Use `0` to disable jitter.
 */
export const polynomialBackoff = (multiplier: number, exponent = 2, jitter = 0.25) =>
    (step: number) => {
        const delay = multiplier * Math.pow(step, exponent);
        return addJitter(delay, jitter);
    };

/**
 * Increase the value each step following the Fibonacci sequence scaled by `multiplier`.
 * Results in series: `multiplier * 1, multiplier * 1, multiplier * 2, multiplier * 3, multiplier * 5, ...`
 * Grows slower than exponential, useful when exponential backoff is too aggressive.
 * @param multiplier Scale factor applied to each Fibonacci number. Specifying the delay at step 1.
 * @param jitter Jitter factor in [0, 1]. Randomizes the delay within ±`jitter * delay`. Use `0` to disable jitter.
 */
export const fibonacciBackoff = (multiplier: number, jitter = 0.25) =>
    (step: number) => {
        let a = 1;
        let b = 1;
        for (let i = 2; i < step; i++)
            [a, b] = [b, a + b];
        return addJitter(multiplier * (step === 1 ? 1 : b), jitter);
    };
