/**
 * Function specification for time strategy. It takes step number and returns delay for this step.
 * @param step Step number. Starts from 1.
 */
export type ITimeStrategy = (step: number) => number;

/**
 * Adds jitter to the delay allowing to randomize time in specified interval.
 * @param delay Delay in ms.
 * @param jitterPercentage Percentage of the delay to randomize. For example, 10 means randomize time in 10% interval.
 * @returns Delay with jitter.
 */
const addJitter = (delay: number, jitterPercentage: number) => Math.floor(jitterPercentage > 0 ? delay + (Math.random() * 2 - 1) * (delay / jitterPercentage) : delay);

/**
 * Returns `delay` in each step, ignoring step number, turning this strategy into constant delays with random jitter.
 * @param delay Delay in ms.
 * @param jitter Whether to use jitter. Jitter randomizes time in 10% interval.
 */
export const linearTimeStrategy = (delay: number, jitter = true) =>
    () => addJitter(delay, jitter ? 10 : 0);

/**
 * Increase the value each step using formula `multiplier * base^step`.
 * For `base=2` and `multiplier=100` will result in series: 100, 200, 400, 800, ...
 * @param multiplier Multiplier of the function, specifying the delay in the first step. Default is 100.
 * @param base Base of the exponential function. Default is 2, resulting in doubling the time each step.
 * @param jitter Whether to use jitter. Jitter randomizes time in 10% interval.
 */
export const doublingTimeStrategy = (multiplier: number, base = 2, jitter = true) =>
    (step: number) => {
        const delay = multiplier * Math.pow(base, step - 1);
        return addJitter(delay, jitter ? 10 : 0);
    };
