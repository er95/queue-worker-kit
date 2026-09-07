import { defineConfig } from 'vitest/config'

/**
 * Two projects, because the suites have genuinely different needs.
 *
 * Unit tests are pure and run in parallel. Integration tests talk to a real
 * Redis and run in a single fork: they each namespace their own keys, but
 * running them sequentially keeps timing assertions (backoff, delayed jobs)
 * from competing for the same CPU and turning into flakes.
 */
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['tests/unit/**/*.test.ts'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['tests/integration/**/*.test.ts'],
          environment: 'node',
          // Retries and exponential backoff take real wall-clock time.
          testTimeout: 45_000,
          hookTimeout: 45_000,
          pool: 'forks',
          // One file at a time. Each suite namespaces its own Redis keys, but
          // running them sequentially keeps the timing assertions (backoff
          // curves, delayed-job promotion) from competing for CPU and flaking.
          fileParallelism: false,
          maxWorkers: 1,
        },
      },
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: [
        // Process bootstraps: exercised by running the app, not by unit tests,
        // and covering them would mean binding ports and signalling the process.
        'src/api/server.ts',
        'src/worker/server.ts',
        'src/index.ts',
      ],
      thresholds: {
        lines: 80,
        functions: 80,
        statements: 80,
        branches: 70,
      },
    },
  },
})
