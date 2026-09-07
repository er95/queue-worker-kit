import { randomBytes } from 'node:crypto'

import type { Queue, Worker } from 'bullmq'
import type { Redis } from 'ioredis'

import { loadEnv, type Env } from '../../src/config/env.js'
import { createLogger, type AppLogger } from '../../src/observability/logger.js'
import { createRedisConnection, type RedisConnectionHandle } from '../../src/queue/connection.js'

/**
 * Integration test harness.
 *
 * Every suite gets its own queue prefix, so tests never see each other's jobs
 * even when a previous run left keys behind. Everything opened through here is
 * tracked and closed in reverse order, because a leaked BullMQ blocking
 * connection is what makes a Vitest run hang at the end, and reaching for
 * `--force-exit` to hide it would also hide real bugs.
 */

/** Overridable so CI can point at a service container. */
export const TEST_REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6380'

export function uniquePrefix(label: string): string {
  return `qwk:test:${label}:${randomBytes(4).toString('hex')}`
}

export function testEnv(overrides: Record<string, string | undefined> = {}): Env {
  return loadEnv({
    NODE_ENV: 'test',
    REDIS_URL: TEST_REDIS_URL,
    LOG_LEVEL: 'silent',
    // Off by default: a test that wants a scheduler or queue events opts in.
    ENABLE_JOB_SCHEDULERS: 'false',
    ENABLE_QUEUE_EVENTS: 'false',
    ...overrides,
  })
}

export const silentLogger: AppLogger = createLogger({
  level: 'silent',
  name: 'test',
  pretty: false,
})

export interface TestContext {
  env: Env
  prefix: string
  logger: AppLogger
  connection: RedisConnectionHandle
  redis: Redis
  /** Registers a resource for teardown. Closed in reverse order. */
  track: <T extends { close: (...args: never[]) => Promise<unknown> }>(resource: T) => T
  /** Registers an arbitrary teardown callback. */
  onCleanup: (fn: () => Promise<void>) => void
  cleanup: () => Promise<void>
}

export function createTestContext(
  label: string,
  envOverrides: Record<string, string | undefined> = {},
): TestContext {
  const prefix = uniquePrefix(label)
  const env = testEnv({ QUEUE_PREFIX: prefix, ...envOverrides })

  const connection = createRedisConnection({ env, logger: silentLogger, role: 'worker' })

  const teardown: (() => Promise<void>)[] = []

  const context: TestContext = {
    env,
    prefix,
    logger: silentLogger,
    connection,
    redis: connection.client,

    track(resource) {
      teardown.push(async () => {
        await resource.close()
      })
      return resource
    },

    onCleanup(fn) {
      teardown.push(fn)
    },

    async cleanup() {
      // Reverse order, so workers stop fetching before the queues and the
      // connection they depend on are torn down.
      for (const fn of teardown.reverse()) {
        try {
          await fn()
        } catch {
          // Teardown failures must not mask the assertion that actually failed.
        }
      }
      teardown.length = 0

      await deleteKeysByPrefix(connection.client, prefix)
      await connection.close()
    },
  }

  return context
}

/**
 * Removes a suite's keys with `SCAN`, never `KEYS`, which blocks the server for
 * the duration of the scan.
 */
export async function deleteKeysByPrefix(client: Redis, prefix: string): Promise<void> {
  if (client.status !== 'ready') return

  let cursor = '0'
  do {
    const [next, keys] = await client.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', 500)
    cursor = next
    if (keys.length > 0) {
      await client.del(...keys)
    }
  } while (cursor !== '0')
}

/** Closes a worker and waits for its active jobs, as production shutdown does. */
export async function closeWorker(worker: Worker<unknown, unknown, string>): Promise<void> {
  await worker.close()
}

export async function closeQueue(queue: Queue): Promise<void> {
  await queue.close()
}
