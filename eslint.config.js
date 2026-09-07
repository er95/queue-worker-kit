import js from '@eslint/js'
import tseslint from 'typescript-eslint'

/**
 * Flat config, kept small on purpose.
 *
 * The rules that earn their place are the ones that catch real bugs in async
 * code: an unawaited promise in a shutdown path, an async function passed
 * where a void callback is expected. Stylistic rules are Prettier's job and are
 * not duplicated here.
 */
export default tseslint.config(
  {
    ignores: ['dist/**', 'coverage/**', 'node_modules/**'],
  },

  js.configs.recommended,

  // Type-aware linting: `no-floating-promises` and friends need the type
  // checker, which means the project service has to be enabled.
  ...tseslint.configs.recommendedTypeChecked,

  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // A dropped promise in a queue system means a job that silently never
      // runs, so this is an error rather than a warning.
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': 'error',
      '@typescript-eslint/await-thenable': 'error',
      '@typescript-eslint/return-await': ['error', 'in-try-catch'],

      /**
       * Off deliberately. Fastify plugins, BullMQ processors and shutdown steps
       * all have async signatures the interface requires, so "async function
       * with no await" is almost always a false positive here. The rules that
       * catch real async bugs are no-floating-promises and no-misused-promises,
       * both errors above.
       */
      '@typescript-eslint/require-await': 'off',

      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unnecessary-condition': 'error',
      '@typescript-eslint/consistent-type-imports': [
        'error',
        { prefer: 'type-imports', fixStyle: 'inline-type-imports' },
      ],
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrors: 'all' },
      ],

      // Application code logs through pino so that output is structured and
      // redacted. Bootstrap failures write to stderr directly, which is
      // allowed below.
      'no-console': 'error',

      eqeqeq: ['error', 'always', { null: 'ignore' }],
      'no-return-await': 'off',
    },
  },

  {
    // Plain JS (config files, the dev runner) sits outside the type-checked
    // program, so the type-aware rules cannot apply to it.
    files: ['**/*.js', '**/*.mjs', '**/*.cjs'],
    ...tseslint.configs.disableTypeChecked,
  },

  {
    files: ['tests/**/*.ts'],
    rules: {
      // Tests deliberately construct malformed payloads to prove they are
      // rejected, which means stepping outside the declared types. Fastify's
      // `response.json()` is also `any` by design.
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unnecessary-condition': 'off',
    },
  },
)
