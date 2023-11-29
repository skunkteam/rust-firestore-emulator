/* eslint-env node */

/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    testMatch: ['<rootDir>/tests/**/*.test.ts'],
    setupFilesAfterEnv: ['jest-extended/all'],
    testTimeout: 60_000,
};
