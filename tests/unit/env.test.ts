import { describe, expect, it } from 'vitest'

import { ConfigError, loadEnv, shouldUsePrettyLogs } from '../../src/config/env.js'

/**
 * Configuration is the one place where being permissive is actively harmful:
 * a worker that boots with a silently defaulted concurrency is much harder to
 * diagnose than one that refuses to boot at all.
 */
describe('loadEnv', () => {
  it('applies documented defaults to an empty environment', () => {
    const env = loadEnv({})

    expect(env.NODE_ENV).toBe('development')
    expect(env.PORT).toBe(3000)
    expect(env.QUEUE_PREFIX).toBe('qwk')
    expect(env.WORKER_CONCURRENCY).toBe(10)
    expect(env.JOB_ATTEMPTS).toBe(5)
    expect(env.JOB_BACKOFF_DELAY_MS).toBe(2000)
    expect(env.JOB_TIMEOUT_MS).toBe(30_000)
    expect(env.ENABLE_BULL_BOARD).toBe(false)
  })

  it('coerces numeric strings to numbers', () => {
    const env = loadEnv({ PORT: '8080', WORKER_CONCURRENCY: '25' })

    expect(env.PORT).toBe(8080)
    expect(env.WORKER_CONCURRENCY).toBe(25)
  })

  // The regression this guards: `Number('')` is 0, so a blank variable would
  // otherwise sail through as a valid-looking zero.
  it('rejects an empty numeric variable instead of reading it as zero', () => {
    expect(() => loadEnv({ JOB_BACKOFF_DELAY_MS: '' })).toThrow(ConfigError)
  })

  it.each([
    ['whitespace padding', { PORT: ' 8080 ' }],
    ['exponential notation', { JOB_TIMEOUT_MS: '1e3' }],
    ['hex', { PORT: '0x50' }],
    ['decimal', { WORKER_CONCURRENCY: '2.5' }],
    ['trailing garbage', { PORT: '8080abc' }],
  ])('rejects %s rather than coercing it', (_label, source) => {
    expect(() => loadEnv(source)).toThrow(ConfigError)
  })

  it('enforces numeric bounds', () => {
    expect(() => loadEnv({ PORT: '0' })).toThrow(ConfigError)
    expect(() => loadEnv({ PORT: '70000' })).toThrow(ConfigError)
    expect(() => loadEnv({ WORKER_CONCURRENCY: '0' })).toThrow(ConfigError)
  })

  it.each(['http://localhost:6379', 'localhost:6379', 'redis://', 'not a url'])(
    'rejects %s as a Redis URL',
    (url) => {
      expect(() => loadEnv({ REDIS_URL: url })).toThrow(ConfigError)
    },
  )

  it.each(['redis://localhost:6379', 'rediss://user:pass@redis.example:6380/2'])(
    'accepts %s as a Redis URL',
    (url) => {
      expect(loadEnv({ REDIS_URL: url }).REDIS_URL).toBe(url)
    },
  )

  /**
   * Crash logs are rarely private. A validation message that quotes the
   * offending value would put a Redis password wherever those logs are shipped.
   */
  it('never echoes the offending value in an error message', () => {
    const secret = 'sup3r-s3cret-passw0rd'

    let error: ConfigError | undefined
    try {
      loadEnv({ REDIS_URL: `http://user:${secret}@localhost:6379` })
    } catch (thrown) {
      error = thrown as ConfigError
    }

    expect(error).toBeInstanceOf(ConfigError)
    expect(error?.message).not.toContain(secret)
    expect(error?.issues.join(' ')).not.toContain(secret)
    expect(error?.message).toContain('REDIS_URL')
  })

  it('does not leak a bad enum value', () => {
    let error: ConfigError | undefined
    try {
      loadEnv({ NODE_ENV: 'internal-secret-stage-name' })
    } catch (thrown) {
      error = thrown as ConfigError
    }

    expect(error?.message).not.toContain('internal-secret-stage-name')
  })

  it('reports every problem at once rather than one per restart', () => {
    let error: ConfigError | undefined
    try {
      loadEnv({ PORT: 'nope', WORKER_CONCURRENCY: 'nope', REDIS_URL: 'nope' })
    } catch (thrown) {
      error = thrown as ConfigError
    }

    expect(error?.issues).toHaveLength(3)
  })

  describe('boolean parsing', () => {
    it.each([
      ['true', true],
      ['false', false],
      ['1', true],
      ['0', false],
    ] as const)('parses %s', (input, expected) => {
      expect(loadEnv({ ENABLE_JOB_SCHEDULERS: input }).ENABLE_JOB_SCHEDULERS).toBe(expected)
    })

    it('rejects a value that is not boolean-shaped', () => {
      expect(() => loadEnv({ ENABLE_JOB_SCHEDULERS: 'maybe' })).toThrow(ConfigError)
    })
  })

  describe('cron validation', () => {
    it('accepts 5- and 6-field expressions', () => {
      expect(loadEnv({ MAINTENANCE_CLEANUP_CRON: '0 * * * *' }).MAINTENANCE_CLEANUP_CRON).toBe(
        '0 * * * *',
      )
      expect(loadEnv({ MAINTENANCE_CLEANUP_CRON: '0 0 * * * *' }).MAINTENANCE_CLEANUP_CRON).toBe(
        '0 0 * * * *',
      )
    })

    it('rejects the wrong number of fields', () => {
      expect(() => loadEnv({ MAINTENANCE_CLEANUP_CRON: '0 *' })).toThrow(ConfigError)
    })
  })

  /**
   * Docker Compose interpolation of an unset variable (`${FOO:-}`) yields an
   * empty string rather than omitting it, which is what lets docker-compose.yml
   * declare optional credentials with no committed default.
   */
  describe('empty values from Compose interpolation', () => {
    it('treats an empty optional credential as unset', () => {
      const env = loadEnv({
        NODE_ENV: 'production',
        ENABLE_BULL_BOARD: '',
        BULL_BOARD_USERNAME: '',
        BULL_BOARD_PASSWORD: '',
      })

      expect(env.ENABLE_BULL_BOARD).toBe(false)
      expect(env.BULL_BOARD_USERNAME).toBeUndefined()
      expect(env.BULL_BOARD_PASSWORD).toBeUndefined()
    })

    it.each([
      ['ENABLE_JOB_SCHEDULERS', true],
      ['ENABLE_QUEUE_EVENTS', true],
      ['ENABLE_WORKER_METRICS_SERVER', true],
      ['ENABLE_BULL_BOARD', false],
    ] as const)('falls back to the default for an empty %s', (key, expected) => {
      expect(loadEnv({ [key]: '' })[key]).toBe(expected)
    })

    it('leaves an empty LOG_PRETTY undefined so the NODE_ENV rule applies', () => {
      expect(loadEnv({ LOG_PRETTY: '' }).LOG_PRETTY).toBeUndefined()
    })

    // The leniency is scoped: a blank numeric is still a misconfiguration.
    it.each(['PORT', 'WORKER_CONCURRENCY', 'JOB_ATTEMPTS', 'JOB_BACKOFF_DELAY_MS'])(
      'still rejects an empty %s',
      (key) => {
        expect(() => loadEnv({ [key]: '' })).toThrow(ConfigError)
      },
    )

    it('still rejects a non-boolean value', () => {
      expect(() => loadEnv({ ENABLE_BULL_BOARD: 'maybe' })).toThrow(ConfigError)
    })

    it('still enforces the credential rules when the dashboard is enabled', () => {
      expect(() =>
        loadEnv({
          NODE_ENV: 'production',
          ENABLE_BULL_BOARD: 'true',
          BULL_BOARD_USERNAME: '',
          BULL_BOARD_PASSWORD: '',
        }),
      ).toThrow(/is required when ENABLE_BULL_BOARD is true/)
    })
  })

  describe('Bull Board credentials', () => {
    it('requires credentials whenever the dashboard is enabled', () => {
      expect(() => loadEnv({ ENABLE_BULL_BOARD: 'true' })).toThrow(ConfigError)
    })

    // Local development should not need a password manager.
    it('allows the demo password outside production', () => {
      const env = loadEnv({
        ENABLE_BULL_BOARD: 'true',
        BULL_BOARD_USERNAME: 'admin',
        BULL_BOARD_PASSWORD: 'change-me',
      })
      expect(env.ENABLE_BULL_BOARD).toBe(true)
    })

    it('refuses a well-known example password in production', () => {
      expect(() =>
        loadEnv({
          NODE_ENV: 'production',
          ENABLE_BULL_BOARD: 'true',
          BULL_BOARD_USERNAME: 'admin',
          BULL_BOARD_PASSWORD: 'change-me',
        }),
      ).toThrow(/well-known example password/)
    })

    it('refuses a short password in production', () => {
      expect(() =>
        loadEnv({
          NODE_ENV: 'production',
          ENABLE_BULL_BOARD: 'true',
          BULL_BOARD_USERNAME: 'admin',
          BULL_BOARD_PASSWORD: 'short',
        }),
      ).toThrow(/at least 16 characters/)
    })

    it('accepts a strong production password', () => {
      const env = loadEnv({
        NODE_ENV: 'production',
        ENABLE_BULL_BOARD: 'true',
        BULL_BOARD_USERNAME: 'ops',
        BULL_BOARD_PASSWORD: 'PrKq2vTz8mWnHs4LdXbY',
      })
      expect(env.BULL_BOARD_USERNAME).toBe('ops')
    })

    it('ignores credential rules when the dashboard is off', () => {
      expect(() => loadEnv({ NODE_ENV: 'production' })).not.toThrow()
    })
  })
})

describe('shouldUsePrettyLogs', () => {
  // pino-pretty is a devDependency, so it is absent from the production image.
  it('defaults to off in production and on elsewhere', () => {
    expect(shouldUsePrettyLogs(loadEnv({ NODE_ENV: 'production' }))).toBe(false)
    expect(shouldUsePrettyLogs(loadEnv({ NODE_ENV: 'development' }))).toBe(true)
  })

  it('honours an explicit override', () => {
    expect(shouldUsePrettyLogs(loadEnv({ NODE_ENV: 'production', LOG_PRETTY: 'true' }))).toBe(true)
    expect(shouldUsePrettyLogs(loadEnv({ NODE_ENV: 'development', LOG_PRETTY: 'false' }))).toBe(
      false,
    )
  })
})
