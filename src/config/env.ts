import { z } from 'zod'

/**
 * Runtime configuration.
 *
 * Two rules govern this file:
 *  1. The process refuses to start on invalid configuration. A malformed number
 *     never silently becomes a default, because a worker quietly running with
 *     `concurrency = NaN → 1` is far harder to diagnose than a failed boot.
 *  2. Error messages never echo the offending value. `REDIS_URL` and the
 *     dashboard password are secrets, and crash logs are rarely private.
 */

const LOG_LEVELS = ['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'silent'] as const

/**
 * Integer from an environment string, within a bounded range.
 *
 * Intentionally not `z.coerce.number()`: coercion goes through `Number()`,
 * which maps `''` to `0`, `' 10 '` to `10` and `'1e3'` to `1000`. A blank
 * variable turning into a valid-looking `0` is precisely the silent
 * misconfiguration this whole module exists to prevent, so the input must
 * look like an integer before it is one.
 */
const intFromEnv = (opts: { min: number; max: number }) =>
  z
    .string({ error: 'must be an integer' })
    .regex(/^-?\d+$/, 'must be an integer')
    .transform((value) => Number(value))
    .refine(Number.isSafeInteger, 'must be a safe integer')
    .refine((value) => value >= opts.min, `must be >= ${opts.min}`)
    .refine((value) => value <= opts.max, `must be <= ${opts.max}`)

/** An unset Compose variable interpolates to '', which means "not provided". */
const emptyToUndefined = (value: unknown): unknown =>
  typeof value === 'string' && value.trim().length === 0 ? undefined : value

/**
 * Boolean from an environment string, where an empty value falls through to the
 * field's default.
 *
 * The default has to live *inside* the preprocess pipe: `z.preprocess(...)
 * .default(x)` checks for undefined on the raw input, before '' has been
 * normalised, so the substitution would never happen.
 *
 * Deliberately not applied to the numeric fields, where an empty value is a
 * misconfiguration worth failing on rather than silently defaulting.
 */
const boolFromEnv = (defaultValue?: boolean) =>
  z.preprocess(
    emptyToUndefined,
    defaultValue === undefined
      ? z.stringbool({ error: 'must be a boolean' }).optional()
      : z.stringbool({ error: 'must be a boolean' }).default(defaultValue),
  )

const redisUrlSchema = z
  .string()
  .min(1, 'must not be empty')
  .refine(
    (value) => {
      let url: URL
      try {
        url = new URL(value)
      } catch {
        return false
      }
      // `rediss:` is the TLS scheme. Anything else (http, file, ...) is a
      // configuration mistake we would rather catch at boot.
      return (url.protocol === 'redis:' || url.protocol === 'rediss:') && url.hostname.length > 0
    },
    // Deliberately value-free: a Redis URL routinely embeds credentials.
    { error: 'must be a valid redis:// or rediss:// URL' },
  )

/**
 * A cron expression, checked only for shape. BullMQ (via cron-parser) is the
 * authority on semantics; this catches the common "wrong number of fields"
 * typo at boot instead of at the first scheduler upsert.
 */
const cronSchema = z
  .string()
  .trim()
  .refine((value) => {
    const fields = value.split(/\s+/).filter((field) => field.length > 0)
    return fields.length === 5 || fields.length === 6
  }, 'must be a 5- or 6-field cron expression')

/**
 * An optional string where an empty value means "not set".
 *
 * Docker Compose interpolation of an unset variable (`${FOO:-}`) produces an
 * empty string rather than omitting the variable, and `FOO=` in an .env file
 * does the same. Treating that as absent is what lets a stack declare an
 * optional credential without a committed default.
 *
 * Deliberately not applied to the numeric fields: there, an empty value is a
 * misconfiguration worth failing on rather than silently defaulting.
 */
const optionalSecret = () => z.preprocess(emptyToUndefined, z.string().min(1).optional())

const baseEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),

  HOST: z.string().min(1, 'must not be empty').default('0.0.0.0'),
  PORT: intFromEnv({ min: 1, max: 65_535 }).default(3000),

  REDIS_URL: redisUrlSchema.default('redis://localhost:6379'),

  LOG_LEVEL: z
    .enum(LOG_LEVELS, { error: `must be one of: ${LOG_LEVELS.join(', ')}` })
    .default('info'),
  /** Human-readable logs via pino-pretty. Defaults to on outside production. */
  LOG_PRETTY: boolFromEnv(),

  /** Namespaces every Redis key, so one Redis can host several environments. */
  QUEUE_PREFIX: z
    .string()
    .min(1, 'must not be empty')
    .regex(/^[A-Za-z0-9_.:-]+$/, 'may only contain letters, digits and _ . : -')
    .default('qwk'),

  /** Jobs processed in parallel *per worker process*, not per cluster. */
  WORKER_CONCURRENCY: intFromEnv({ min: 1, max: 1_000 }).default(10),

  JOB_ATTEMPTS: intFromEnv({ min: 1, max: 50 }).default(5),
  JOB_BACKOFF_DELAY_MS: intFromEnv({ min: 0, max: 3_600_000 }).default(2_000),

  JOB_REMOVE_ON_COMPLETE_COUNT: intFromEnv({ min: 0, max: 1_000_000 }).default(1_000),
  JOB_REMOVE_ON_FAIL_COUNT: intFromEnv({ min: 0, max: 1_000_000 }).default(5_000),

  WORKER_RATE_LIMIT_MAX: intFromEnv({ min: 1, max: 1_000_000 }).default(100),
  WORKER_RATE_LIMIT_DURATION_MS: intFromEnv({ min: 1, max: 3_600_000 }).default(1_000),

  JOB_TIMEOUT_MS: intFromEnv({ min: 100, max: 3_600_000 }).default(30_000),

  /** Hard deadline for graceful shutdown before the process is forced down. */
  SHUTDOWN_TIMEOUT_MS: intFromEnv({ min: 1_000, max: 300_000 }).default(30_000),

  /** Oldest dead-letter records are evicted beyond this count. */
  DEAD_LETTER_MAX_ENTRIES: intFromEnv({ min: 1, max: 1_000_000 }).default(5_000),

  /** Worker processes upsert the recurring job schedulers on boot. */
  ENABLE_JOB_SCHEDULERS: boolFromEnv(true),

  /**
   * Queue-wide lifecycle logging via BullMQ QueueEvents. Costs one extra Redis
   * connection per queue per worker process.
   */
  ENABLE_QUEUE_EVENTS: boolFromEnv(true),

  /**
   * The worker's own /metrics and /health/live listener. On by default: job
   * lifecycle counters are recorded in the worker process, so without it they
   * cannot be scraped at all. Bind it to an internal port.
   */
  ENABLE_WORKER_METRICS_SERVER: boolFromEnv(true),
  WORKER_METRICS_HOST: z.string().min(1, 'must not be empty').default('0.0.0.0'),
  WORKER_METRICS_PORT: intFromEnv({ min: 1, max: 65_535 }).default(9090),
  MAINTENANCE_CLEANUP_CRON: cronSchema.default('0 * * * *'),

  ENABLE_BULL_BOARD: boolFromEnv(false),
  BULL_BOARD_USERNAME: optionalSecret(),
  BULL_BOARD_PASSWORD: optionalSecret(),
})

/** Rejected outright, so a copy-pasted example cannot become production auth. */
const INSECURE_PASSWORDS = new Set(['change-me', 'changeme', 'password', 'admin', 'secret'])

const MIN_PRODUCTION_PASSWORD_LENGTH = 16

export const envSchema = baseEnvSchema.superRefine((env, ctx) => {
  if (!env.ENABLE_BULL_BOARD) return

  const isProduction = env.NODE_ENV === 'production'

  if (env.BULL_BOARD_USERNAME === undefined) {
    ctx.addIssue({
      code: 'custom',
      path: ['BULL_BOARD_USERNAME'],
      message: 'is required when ENABLE_BULL_BOARD is true',
    })
  }

  if (env.BULL_BOARD_PASSWORD === undefined) {
    ctx.addIssue({
      code: 'custom',
      path: ['BULL_BOARD_PASSWORD'],
      message: 'is required when ENABLE_BULL_BOARD is true',
    })
    return
  }

  // Outside production the demo credentials are allowed on purpose: local
  // development should not need a password manager.
  if (!isProduction) return

  if (INSECURE_PASSWORDS.has(env.BULL_BOARD_PASSWORD.toLowerCase())) {
    ctx.addIssue({
      code: 'custom',
      path: ['BULL_BOARD_PASSWORD'],
      message: 'must not be a well-known example password when NODE_ENV=production',
    })
  } else if (env.BULL_BOARD_PASSWORD.length < MIN_PRODUCTION_PASSWORD_LENGTH) {
    ctx.addIssue({
      code: 'custom',
      path: ['BULL_BOARD_PASSWORD'],
      message: `must be at least ${MIN_PRODUCTION_PASSWORD_LENGTH} characters when NODE_ENV=production`,
    })
  }
})

export type Env = z.output<typeof baseEnvSchema>

/** Thrown on invalid configuration. `issues` never contains input values. */
export class ConfigError extends Error {
  readonly issues: readonly string[]

  constructor(issues: readonly string[]) {
    super(`Invalid environment configuration:\n  - ${issues.join('\n  - ')}`)
    this.name = 'ConfigError'
    this.issues = issues
  }
}

/**
 * Parses and validates configuration. Pass an explicit source in tests rather
 * than mutating `process.env`.
 */
export function loadEnv(source: Record<string, string | undefined> = process.env): Env {
  const result = envSchema.safeParse(source)

  if (!result.success) {
    // Only the variable name and our own message escape: zod's `received` field
    // can contain the raw input, which may be a credential.
    const issues = result.error.issues.map((issue) => {
      const key = issue.path.length > 0 ? issue.path.join('.') : '(root)'
      return `${key} ${issue.message}`
    })
    throw new ConfigError(issues)
  }

  return result.data
}

/** Whether pino-pretty should be used; it is a devDependency, so never in production. */
export function shouldUsePrettyLogs(env: Env): boolean {
  return env.LOG_PRETTY ?? env.NODE_ENV !== 'production'
}
