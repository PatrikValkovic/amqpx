const typescript = require('@patrikvalkovic/eslint-config/typescript');

module.exports = [
    ...typescript('tsconfig.test.json'),
    {
        rules: {
            'no-await-in-loop': 'off',
        },
    },
    {
        ignores: [
            'eslint.config.js',
            'coverage/**/*',
            'dist/**/*',
            'vitest.config.mts',
        ]
    },
    {
        // compatibility issues
        rules: {
            '@typescript-eslint/no-empty-function': 'off',
        }
    }
];
