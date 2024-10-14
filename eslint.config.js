const typescript = require('@patrikvalkovic/eslint-config/typescript');

module.exports = [
    ...typescript('tsconfig.json'),
    {
        rules: {
            'no-await-in-loop': 'off',
        },
    },
    {
        ignores: [
            'eslint.config.js',
        ]
    },
];
