import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        include: ['tests/**/*.spec.ts'],
        fileParallelism: false,
        coverage: {
            exclude: [
                'tests/**',
                'src/test/**',
            ],
        },
        globals: true,
        testTimeout: 30_000,
        hookTimeout: 60_000,
        globalSetup: './tests/setup/global-setup.ts',
        setupFiles: ['./tests/setup/before-each.ts'],
        pool: 'forks',
    },
});
