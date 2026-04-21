import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        include: ['**/*.spec.ts'],
        exclude: ['tests/**', 'node_modules/**'],
        globals: true,
        coverage: {
            exclude: [
                'src/test/**',
            ],
        },
    },
});
