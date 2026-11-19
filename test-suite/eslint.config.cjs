const js = require('@eslint/js');
const tsPlugin = require('@typescript-eslint/eslint-plugin');
const prettierConfig = require('eslint-config-prettier/flat');

/** @type {import('eslint').Linter.Config[]} */
module.exports = [
    {
        linterOptions: {
            reportUnusedDisableDirectives: 'error',
        },
    },
    js.configs.recommended,
    ...tsPlugin.configs['flat/recommended-type-checked'].map(config => ({
        ...config,
        files: ['**/*.ts', '**/*.tsx'],
    })),
    {
        files: ['**/*.ts', '**/*.tsx'],
        languageOptions: {
            parserOptions: {
                tsconfigRootDir: __dirname,
                project: './tsconfig.json',
            },
        },
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
            '@typescript-eslint/no-unsafe-assignment': 'off',
            '@typescript-eslint/no-unused-expressions': [
                'error',
                {
                    allowShortCircuit: true,
                    allowTernary: true,
                },
            ],
            '@typescript-eslint/no-unused-vars': 'off',
            '@typescript-eslint/prefer-readonly': 'error',
            '@typescript-eslint/promise-function-async': [
                'error',
                {
                    checkArrowFunctions: false,
                },
            ],
            '@typescript-eslint/require-await': 'off',
            '@typescript-eslint/restrict-plus-operands': 'off',
            '@typescript-eslint/restrict-template-expressions': [
                'error',
                {
                    allowBoolean: true,
                    allowRegExp: true,
                },
            ],
            '@typescript-eslint/unbound-method': 'off',
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
        files: ['tests/7-descendants-query.test.ts'],
        rules: {
            '@typescript-eslint/no-explicit-any': 'off',
            '@typescript-eslint/no-unsafe-call': 'off',
            '@typescript-eslint/no-unsafe-member-access': 'off',
            'prefer-const': 'off',
        },
    },
    {
        files: ['tests/9-pipeline-operators.test.ts'],
        rules: {
            '@typescript-eslint/no-explicit-any': 'off',
            '@typescript-eslint/no-unsafe-return': 'off',
        },
    },
    {
        files: ['tests/5-live-queries.test.ts'],
        rules: {
            '@typescript-eslint/prefer-promise-reject-errors': 'off',
        },
    },
    {
        files: ['tests/11-vector-search.test.ts', 'tests/utils/common.ts'],
        rules: {
            '@typescript-eslint/no-base-to-string': 'off',
        },
    },
    prettierConfig,
];
