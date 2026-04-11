const typescript = require('@patrikvalkovic/eslint-config/typescript');

module.exports = [
    ...typescript('tsconfig.test.json'),
    {
        rules: {
            'no-await-in-loop': 'off',
            '@typescript-eslint/unified-signatures': 'off',
        },
    },
    {
        ignores: [
            'eslint.config.js',
            'coverage/**/*',
            'dist/**/*',
            'vitest.config.mts',
            'vitest.integration.config.mts',
        ]
    },
    {
        files: [
          '**/*.spec.ts',
        ],
        rules: {
            '@typescript-eslint/no-non-null-assertion': 'off',
        }
    },
    {
        // compatibility issues
        rules: {
            '@typescript-eslint/no-empty-function': 'off',
        }
    }
];
