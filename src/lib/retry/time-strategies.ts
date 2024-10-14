/**
 * Interface that should generate series of number.
 * Most often used for reconnection.
 */
export type ITimeStrategy = (step: number) => number;

/**
 * Returns `delay` in each step, ignoring step number.
 * @param delay
 * @param jitter Whether to use jitter. Jitter randomizes time in 10% interval.
 */
export const linearTimeStrategy = (delay: number, jitter = true) =>
    (_: number) => delay + (jitter ? (Math.random() * 2 - 1) * (delay / 10) : 0);

/**
 * Increase the value each step using formula `multiplier * base^step`.
 * For `base=2` and `multiplier=100` will result in series: 100, 200, 400, 800, ...
 * @param multiplier
 * @param base
 * @param jitter Whether to use jitter. Jitter randomizes time in 10% interval.
 */
export const doublingTimeStrategy = (multiplier: number, base = 2, jitter = true) =>
    (step: number) => {
        const constantDelay = multiplier * base ** (step - 1);
        const delay = constantDelay + (jitter ? (Math.random() * 2 - 1) * (constantDelay / 10) : 0);
        return Math.floor(delay);
    };
