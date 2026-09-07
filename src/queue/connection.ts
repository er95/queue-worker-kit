import { Redis, type RedisOptions } from 'ioredis'

import {
  REDIS_CONNECT_TIMEOUT_MS,
  REDIS_RECONNECT_MAX_DELAY_MS,
  REDIS_RECONNECT_MIN_DELAY_MS,
} from '../config/defaults.js'
import type { Env } from '../config/env.js'
import type { AppLogger } from '../observability/logger.js'
import { serializeError } from '../utils/serialize-error.js'

/**
 * Redis connection ownership, in one place.
 *
 * BullMQ v6 no longer depends on a Redis client: the backend is pluggable and
 * the driver (`ioredis` here) is an optional peer dependency. Two consequences
 * shape this file:
 *
 *  - Passing a *client instance* to Queue/Worker marks the connection as
 *    "shared", so BullMQ will not close it. Whoever created it must.
 *  - Passing *options* instead makes BullMQ create and own a client per
 *    Queue/Worker/QueueEvents, which quietly multiplies connections.
 *
 * So this kit creates one client per process role and hands the instance to
 * BullMQ. BullMQ still duplicates it where a blocking command requires a
 * dedicated socket (Worker fetch, QueueEvents `XREAD`), and closes those
 * duplicates itself.
 */

export type ConnectionRole = 'producer' | 'worker'

/**
 * Strips credentials from a Redis URL so it can be logged.
 * `redis://app:s3cret@redis:6379/2` becomes `redis://***@redis:6379/2`.
 */
export function redactRedisUrl(url: string): string {
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    return '[unparseable redis url]'
  }

  const hasCredentials = parsed.username !== '' || parsed.password !== ''
  const auth = hasCredentials ? '***@' : ''
  const port = parsed.port === '' ? '' : `:${parsed.port}`
  return `${parsed.protocol}//${auth}${parsed.hostname}${port}${parsed.pathname}`
}

export function createRedisOptions(role: ConnectionRole) {
  return {
    // BullMQ requires this for any connection that issues blocking commands,
    // and it also stops ordinary commands from failing mid-reconnect.
    maxRetriesPerRequest: null,

    // A producer should fail fast: an HTTP caller would rather get a 503 now
    // than have its request buffered indefinitely while Redis is down. A worker
    // wants the opposite, buffering across a blip instead of erroring.
    enableOfflineQueue: role === 'worker',

    connectTimeout: REDIS_CONNECT_TIMEOUT_MS,
    // Bounded exponential backoff with jitter, so a fleet restarting together
    // does not reconnect in lockstep.
    retryStrategy: (times: number) => {
      const backoff = Math.min(
        REDIS_RECONNECT_MIN_DELAY_MS * 2 ** (times - 1),
        REDIS_RECONNECT_MAX_DELAY_MS,
      )
      return backoff / 2 + Math.floor(Math.random() * (backoff / 2))
    },
    keepAlive: 30_000,
  } satisfies RedisOptions
}

export interface RedisConnectionHandle {
  readonly client: Redis
  /** Idempotent. Quits politely, then forces the socket shut if that hangs. */
  close(): Promise<void>
}

export interface CreateRedisConnectionOptions {
  env: Env
  logger: AppLogger
  role: ConnectionRole
}

export function createRedisConnection(
  options: CreateRedisConnectionOptions,
): RedisConnectionHandle {
  const { env, logger, role } = options
  const safeUrl = redactRedisUrl(env.REDIS_URL)

  const client = new Redis(env.REDIS_URL, {
    ...createRedisOptions(role),
    connectionName: `${env.QUEUE_PREFIX}:${role}`,
  })

  const log = logger.child({ component: 'redis', role, redis: safeUrl })

  client.on('error', (error: Error) => {
    // ioredis emits `error` on every failed reconnect attempt, so this is a
    // warning, not a fatal: the retry strategy is still working.
    //
    // `serializeError` is not optional here. ioredis attaches the failed
    // command to its errors as `err.command = { name, args }`, and pino's
    // default `err` serializer copies every own enumerable property onto the
    // record. For a failed AUTH or HELLO handshake those args are the
    // plaintext credentials from REDIS_URL, so logging the raw error would
    // write the Redis password to stdout. This projects to name/message/
    // stack/code only.
    log.warn({ err: serializeError(error) }, 'redis error')
  })
  client.on('ready', () => {
    log.info('redis ready')
  })
  client.on('reconnecting', (delay: number) => {
    log.warn({ delayMs: delay }, 'redis reconnecting')
  })
  client.on('close', () => {
    log.warn('redis connection closed')
  })
  client.on('end', () => {
    log.info('redis connection ended')
  })

  let closing: Promise<void> | undefined

  return {
    client,
    close(): Promise<void> {
      closing ??= (async () => {
        try {
          await client.quit()
        } catch (error) {
          // `quit` fails if the socket is already gone, which is fine: we only
          // care that the handle is released.
          log.debug({ err: serializeError(error) }, 'redis quit failed, forcing disconnect')
          client.disconnect()
        }
      })()
      return closing
    },
  }
}

/**
 * Waits for the initial connection, bounded.
 *
 * Resolves `false` rather than throwing when Redis is not up yet. Both
 * processes are designed to boot anyway: crash-looping a container because a
 * dependency is briefly unavailable just moves the outage, whereas an API that
 * starts and reports `503` from `/health/ready` gives the orchestrator exactly
 * the signal it needs, and a worker that starts will process jobs as soon as
 * ioredis reconnects.
 */
export async function waitUntilRedisReady(client: Redis, timeoutMs: number): Promise<boolean> {
  if (client.status === 'ready') return true

  return new Promise<boolean>((resolve) => {
    let settled = false

    const finish = (ready: boolean): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      client.off('ready', onReady)
      resolve(ready)
    }

    const onReady = (): void => {
      finish(true)
    }
    const timer = setTimeout(() => {
      finish(false)
    }, timeoutMs)

    client.once('ready', onReady)
  })
}

/**
 * `PING` with a hard deadline, for readiness probes.
 *
 * A probe that can block for the driver's full retry budget is worse than no
 * probe at all, so the timeout is enforced here rather than trusted to Redis.
 */
export async function pingRedis(client: Redis, timeoutMs: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined
  try {
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        reject(new Error(`Redis ping timed out after ${timeoutMs}ms`))
      }, timeoutMs)
    })
    await Promise.race([client.ping(), timeout])
    return true
  } catch {
    return false
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}
