/* eslint-env node */

/** @type {import('eslint').Linter.Config} */
module.exports = {
    root: true,
    extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
    parser: '@typescript-eslint/parser',
    plugins: ['@typescript-eslint'],
    reportUnusedDisableDirectives: true,
    overrides: [
        {
            files: ['*.ts', '*.tsx'],
            rules: {
                '@typescript-eslint/array-type': [
                    'error',
                    {
                        default: 'array-simple',
                        readonly: 'array-simple',
                    },
                ],
                '@typescript-eslint/member-ordering': 'off',
                '@typescript-eslint/no-floating-promises': 'error',
                'eqeqeq': [
                    'error',
                    'always',
                    {
                        null: 'ignore',
                    },
                ],
                'no-console': 'error',
            },
        },
        {
            files: ['*.ts', '*.tsx'],
            extends: ['plugin:@typescript-eslint/recommended-requiring-type-checking'],
            parserOptions: {
                project: 'test-suite/tsconfig.json',
            },
            rules: {
                '@typescript-eslint/unbound-method': 'off',
                '@typescript-eslint/prefer-readonly': 'error',
                '@typescript-eslint/no-unsafe-assignment': 'off',
                '@typescript-eslint/no-unused-vars': 'off',
                '@typescript-eslint/promise-function-async': [
                    'error',
                    {
                        checkArrowFunctions: false,
                    },
                ],
                '@typescript-eslint/restrict-plus-operands': 'off',
                '@typescript-eslint/restrict-template-expressions': [
                    'error',
                    {
                        allowBoolean: true,
                        allowRegExp: true,
                    },
                ],
                '@typescript-eslint/require-await': 'off',
            },
        },
    ],
};
