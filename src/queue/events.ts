import { QueueEvents } from 'bullmq'
import type { Redis } from 'ioredis'

import type { Env } from '../config/env.js'
import type { AppLogger } from '../observability/logger.js'
import { serializeError } from '../utils/serialize-error.js'

/**
 * Queue-wide lifecycle visibility via BullMQ `QueueEvents`.
 *
 * An important caveat drives how this is used: `QueueEvents` is a *broadcast*
 * stream. Every listener in the fleet sees every event for the queue, so with
 * three replicas each event arrives three times. That makes it a fine source of
 * observability and a bad source of counters, which would end up tripled the
 * moment anyone summed them across instances.
 *
 * So metrics come from worker-local events (see `create-worker.ts`) and this
 * exists purely for logging, which is why the events below are logged at debug
 * level. Set `ENABLE_QUEUE_EVENTS=false` to drop it entirely.
 *
 * Each `QueueEvents` instance also needs its own blocking connection for its
 * `XREAD`. BullMQ duplicates the client passed here and owns the duplicate, so
 * this costs one extra Redis connection per queue per process.
 */

export interface QueueEventsHandle {
  /** Stops consuming and releases the duplicated connections. Idempotent. */
  close(): Promise<void>
}

export interface CreateQueueEventsOptions {
  queueNames: readonly string[]
  env: Env
  connection: Redis
  logger: AppLogger
}

export function createQueueEventsListeners(options: CreateQueueEventsOptions): QueueEventsHandle {
  const { queueNames, env, connection, logger } = options

  const instances = queueNames.map((queueName) => {
    const log = logger.child({ component: 'queue-events', queue: queueName })

    const events = new QueueEvents(queueName, {
      connection,
      prefix: env.QUEUE_PREFIX,
    })

    events.on('completed', ({ jobId }) => {
      log.debug({ jobId }, 'queue event: completed')
    })

    events.on('failed', ({ jobId, failedReason }) => {
      log.debug({ jobId, failedReason }, 'queue event: failed')
    })

    events.on('stalled', ({ jobId }) => {
      log.info({ jobId }, 'queue event: stalled')
    })

    events.on('error', (error: Error) => {
      log.warn({ err: serializeError(error) }, 'queue events error')
    })

    return events
  })

  let closing: Promise<void> | undefined

  return {
    close() {
      closing ??= (async () => {
        await Promise.all(
          instances.map(async (events) => {
            // `removeAllListeners` before `close` so nothing fires while the
            // blocking read is being torn down. Left attached, these listeners
            // are exactly the kind that survive a test file and leak.
            events.removeAllListeners()
            await events.close()
          }),
        )
      })()
      return closing
    },
  }
}
