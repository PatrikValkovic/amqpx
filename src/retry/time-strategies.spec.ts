import { linearBackoff, exponentialBackoff, cappedExponentialBackoff, polynomialBackoff, fibonacciBackoff } from './time-strategies';

describe('Retry', () => {
    describe('linearBackoff', () => {
        it('returns constant delay regardless of step when jitter=0', () => {
            const strategy = linearBackoff(500, 0);
            expect(strategy(1)).toBe(500);
            expect(strategy(2)).toBe(500);
            expect(strategy(10)).toBe(500);
        });

        it('result is within jitter range', () => {
            const delay = 1000;
            const jitter = 0.1;
            const strategy = linearBackoff(delay, jitter);
            for (let i = 0; i < 50; i++) {
                const result = strategy(1);
                expect(result).toBeGreaterThanOrEqual(Math.floor(delay * (1 - jitter)));
                expect(result).toBeLessThanOrEqual(Math.floor(delay * (1 + jitter)));
            }
        });

        it('default jitter produces values within ±25% of delay', () => {
            const delay = 1000;
            const strategy = linearBackoff(delay);
            for (let i = 0; i < 50; i++) {
                const result = strategy(1);
                expect(result).toBeGreaterThanOrEqual(750);
                expect(result).toBeLessThanOrEqual(1250);
            }
        });
    });

    describe('exponentialBackoff', () => {
        it('doubles delay each step for base=2, jitter=0', () => {
            const strategy = exponentialBackoff(100, 2, 0);
            expect(strategy(1)).toBe(100);
            expect(strategy(2)).toBe(200);
            expect(strategy(3)).toBe(400);
            expect(strategy(4)).toBe(800);
        });

        it('uses custom base, jitter=0', () => {
            const strategy = exponentialBackoff(10, 3, 0);
            expect(strategy(1)).toBe(10);
            expect(strategy(2)).toBe(30);
            expect(strategy(3)).toBe(90);
        });

        it('result is within jitter range', () => {
            const strategy = exponentialBackoff(100, 2, 0.1);
            const step = 3; // base delay = 400
            const base = 400;
            for (let i = 0; i < 50; i++) {
                const result = strategy(step);
                expect(result).toBeGreaterThanOrEqual(Math.floor(base * 0.9));
                expect(result).toBeLessThanOrEqual(Math.floor(base * 1.1));
            }
        });
    });

    describe('cappedExponentialBackoff', () => {
        it('caps at maxDelay, jitter=0', () => {
            const strategy = cappedExponentialBackoff(100, 500, 2, 0);
            expect(strategy(1)).toBe(100);
            expect(strategy(2)).toBe(200);
            expect(strategy(3)).toBe(400);
            expect(strategy(4)).toBe(500); // 800 capped to 500
            expect(strategy(5)).toBe(500); // 1600 capped to 500
        });

        it('result is within jitter range after cap', () => {
            const strategy = cappedExponentialBackoff(100, 500, 2, 0.1);
            for (let i = 0; i < 50; i++) {
                const result = strategy(10); // well past cap
                expect(result).toBeGreaterThanOrEqual(Math.floor(500 * 0.9));
                expect(result).toBeLessThanOrEqual(Math.floor(500 * 1.1));
            }
        });
    });

    describe('polynomialBackoff', () => {
        it('quadratic (exponent=2) series, jitter=0', () => {
            const strategy = polynomialBackoff(100, 2, 0);
            expect(strategy(1)).toBe(100);  // 100 * 1^2
            expect(strategy(2)).toBe(400);  // 100 * 2^2
            expect(strategy(3)).toBe(900);  // 100 * 3^2
            expect(strategy(4)).toBe(1600); // 100 * 4^2
        });

        it('linear (exponent=1) series, jitter=0', () => {
            const strategy = polynomialBackoff(50, 1, 0);
            expect(strategy(1)).toBe(50);
            expect(strategy(2)).toBe(100);
            expect(strategy(3)).toBe(150);
        });

        it('result is within jitter range', () => {
            const strategy = polynomialBackoff(100, 2, 0.1);
            const step = 2; // base = 400
            for (let i = 0; i < 50; i++) {
                const result = strategy(step);
                expect(result).toBeGreaterThanOrEqual(Math.floor(400 * 0.9));
                expect(result).toBeLessThanOrEqual(Math.floor(400 * 1.1));
            }
        });
    });

    describe('fibonacciBackoff', () => {
        it('fibonacci series scaled by multiplier, jitter=0', () => {
            const strategy = fibonacciBackoff(100, 0);
            expect(strategy(1)).toBe(100);  // fib(1)=1
            expect(strategy(2)).toBe(100);  // fib(2)=1
            expect(strategy(3)).toBe(200);  // fib(3)=2
            expect(strategy(4)).toBe(300);  // fib(4)=3
            expect(strategy(5)).toBe(500);  // fib(5)=5
            expect(strategy(6)).toBe(800);  // fib(6)=8
        });

        it('result is within jitter range', () => {
            const strategy = fibonacciBackoff(100, 0.1);
            const step = 5; // base = 500
            for (let i = 0; i < 50; i++) {
                const result = strategy(step);
                expect(result).toBeGreaterThanOrEqual(Math.floor(500 * 0.9));
                expect(result).toBeLessThanOrEqual(Math.floor(500 * 1.1));
            }
        });
    });
});
