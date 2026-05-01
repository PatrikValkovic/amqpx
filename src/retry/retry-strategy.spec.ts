import { normalizeRetryStrategy } from './retry-strategy';
import { DEFAULT_RETRY_STRATEGY } from './default-retry-strategy';

describe('Retry', () => {
    describe('normalizeRetryStrategy', () => {
        it('fills missing fields from default strategy', () => {
            const result = normalizeRetryStrategy({});
            expect(result.maxRetries).toBe(DEFAULT_RETRY_STRATEGY.maxRetries);
            expect(typeof result.reconnectionTimeoutMs).toBe('function');
        });

        it('converts numeric reconnectionTimeoutMs to TimeStrategy', () => {
            const result = normalizeRetryStrategy({ reconnectionTimeoutMs: 500 });
            expect(typeof result.reconnectionTimeoutMs).toBe('function');
            expect(result.reconnectionTimeoutMs(1)).toBeGreaterThanOrEqual(0);
        });

        it('preserves TimeStrategy reconnectionTimeoutMs as-is', () => {
            const strategy = (step: number) => step * 100;
            const result = normalizeRetryStrategy({ reconnectionTimeoutMs: strategy });
            expect(result.reconnectionTimeoutMs).toBe(strategy);
        });

        it('overrides maxRetries from provided strategy', () => {
            const result = normalizeRetryStrategy({ maxRetries: 5 });
            expect(result.maxRetries).toBe(5);
        });

        it('numeric reconnectionTimeoutMs is wrapped in linearBackoff (constant per step)', () => {
            const result = normalizeRetryStrategy({ reconnectionTimeoutMs: 1000 });
            // linearBackoff with no jitter override uses default 0.25 jitter, so values vary
            // but should always be within ±25% of 1000
            for (let i = 0; i < 20; i++) {
                const delay = result.reconnectionTimeoutMs(i);
                expect(delay).toBeGreaterThanOrEqual(750);
                expect(delay).toBeLessThanOrEqual(1250);
            }
        });
    });
});
