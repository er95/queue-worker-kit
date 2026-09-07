import { createRequire } from 'node:module'
import { pino, type Logger, type LoggerOptions } from 'pino'

import type { Env } from '../config/env.js'
import { shouldUsePrettyLogs } from '../config/env.js'

export type AppLogger = Logger

/**
 * Fields scrubbed from every log record.
 *
 * Kept deliberately narrow: over-broad redaction (e.g. a bare `*.id`) destroys
 * the fields that make these logs worth having. Everything listed here is a
 * credential, never a diagnostic.
 */
export const REDACT_PATHS: readonly string[] = [
  'req.headers.authorization',
  'req.headers.cookie',
  'req.headers["set-cookie"]',
  'res.headers["set-cookie"]',
  'password',
  '*.password',
  'payload.password',
  'data.payload.password',
  'REDIS_URL',
  'redisUrl',
  'connection.password',
  // Defence in depth for the Redis-credential leak that `serializeError`
  // already prevents at every call site. ioredis attaches the failed command
  // to its errors, and a failed AUTH or HELLO handshake puts the plaintext
  // password in `args`. Any future site that logs a raw driver error is
  // covered here too.
  'err.command',
  '*.command.args',
  'command.args',
]

export interface CreateLoggerOptions {
  level: string
  /** Appears as `name` on every record; use it to tell api from worker. */
  name: string
  pretty: boolean
}

/**
 * pino-pretty is a devDependency, so it is absent from the production image.
 * Resolving it up front turns a would-be crash inside pino's transport worker
 * into a silent fall back to JSON.
 */
function isPrettyAvailable(): boolean {
  try {
    createRequire(import.meta.url).resolve('pino-pretty')
    return true
  } catch {
    return false
  }
}

export function createLogger(options: CreateLoggerOptions): AppLogger {
  const base: LoggerOptions = {
    level: options.level,
    name: options.name,
    redact: { paths: [...REDACT_PATHS], censor: '[redacted]' },
    // ISO timestamps survive log shipping far better than pino's default epoch millis.
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
  }

  if (options.pretty && isPrettyAvailable()) {
    return pino({
      ...base,
      transport: {
        target: 'pino-pretty',
        options: { colorize: true, translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname' },
      },
    })
  }

  return pino(base)
}

/** Convenience wrapper used by both bootstraps. */
export function createLoggerFromEnv(env: Env, name: string): AppLogger {
  return createLogger({
    level: env.LOG_LEVEL,
    name,
    pretty: shouldUsePrettyLogs(env),
  })
}
