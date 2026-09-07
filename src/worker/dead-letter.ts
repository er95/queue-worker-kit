import type { Job, Queue } from 'bullmq'
import { z } from 'zod'

import { DEAD_LETTER_REASON_MAX_CHARS, QUEUE_NAMES, type QueueName } from '../config/defaults.js'
import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'
import { formatErrorReason, serializeError } from '../utils/serialize-error.js'

/**
 * Dead-letter handling.
 *
 * When a job exhausts its attempts, BullMQ leaves it in the failed set, which
 * is bounded by `removeOnFail` and therefore eventually forgets it. That is the
 * right policy for the queue and the wrong one for an incident: the record you
 * need at 3am is the one that rolled off the list yesterday.
 *
 * So terminal failures are copied into a dedicated queue as a self-contained
 * record. Three properties matter:
 *
 *  - **No recursion.** Nothing consumes the dead-letter queue, and a record is
 *    refused if its source queue is the dead-letter queue itself.
 *  - **No duplicates.** The record's job id is derived from the failure, so a
 *    repeated write is a no-op in Redis rather than a second entry.
 *  - **No cascade.** A failed dead-letter write is logged and counted, never
 *    rethrown. The original job has already failed; failing the writer too
 *    would only take the worker down with it.
 */

export const deadLetterRecordSchema = z.object({
  sourceQueue: z.string(),
  sourceJobId: z.string(),
  sourceJobName: z.string(),
  payload: z.unknown(),
  failedReason: z.string(),
  error: z.object({
    name: z.string(),
    message: z.string(),
    stack: z.string().optional(),
    code: z.string().optional(),
  }),
  attemptsMade: z.number().int(),
  correlationId: z.string(),
  failedAt: z.iso.datetime(),
})

export type DeadLetterRecord = z.output<typeof deadLetterRecordSchema>

/** Job name used for every dead-letter entry. */
export const DEAD_LETTER_JOB_NAME = 'dead-letter.record'

export interface DeadLetterInput {
  sourceQueue: string
  job: Job<unknown, unknown, string>
  error: unknown
  correlationId: string
  attemptsMade: number
}

export interface DeadLetterWriter {
  /** Best-effort. Resolves even when the write failed; never throws. */
  record(input: DeadLetterInput): Promise<void>
  /** In-flight writes, so shutdown can wait for them. */
  pending(): number
  /** Waits for in-flight writes, up to a deadline. */
  drain(timeoutMs: number): Promise<void>
}

export interface CreateDeadLetterWriterOptions {
  queue: Queue
  logger: AppLogger
  metrics: Metrics
  /** Oldest records are evicted past this count. */
  maxEntries: number
}

/**
 * Redis job ids may not contain `:`, which BullMQ uses as its own key
 * separator, so queue names are flattened into a safe token.
 */
function toIdToken(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]/g, '_')
}

/**
 * Deterministic id for a terminal failure.
 *
 * `Queue.add` ignores a job whose id already exists, so including
 * `attemptsMade` gives exactly one record per terminal failure while still
 * allowing a genuinely retried-then-failed-again job to record a second one.
 */
export function deadLetterJobId(input: {
  sourceQueue: string
  sourceJobId: string
  attemptsMade: number
}): string {
  return `dlq-${toIdToken(input.sourceQueue)}-${toIdToken(input.sourceJobId)}-${input.attemptsMade}`
}

export function createDeadLetterWriter(options: CreateDeadLetterWriterOptions): DeadLetterWriter {
  const { queue, logger, metrics, maxEntries } = options
  const log = logger.child({ component: 'dead-letter' })

  const inFlight = new Set<Promise<void>>()

  async function write(input: DeadLetterInput): Promise<void> {
    const { job, sourceQueue, error, correlationId, attemptsMade } = input

    if (sourceQueue === (QUEUE_NAMES.deadLetter as QueueName)) {
      // The guard that makes a dead-letter loop structurally impossible.
      log.error(
        { queue: sourceQueue, jobId: job.id, correlationId },
        'refusing to dead-letter a job from the dead-letter queue',
      )
      return
    }

    const sourceJobId = job.id ?? 'unknown'
    const serialized = serializeError(error)

    const record: DeadLetterRecord = {
      sourceQueue,
      sourceJobId,
      sourceJobName: job.name,
      payload: job.data,
      failedReason: formatErrorReason(error, DEAD_LETTER_REASON_MAX_CHARS),
      error: {
        name: serialized.name,
        message: serialized.message.slice(0, DEAD_LETTER_REASON_MAX_CHARS),
        ...(serialized.stack !== undefined ? { stack: serialized.stack } : {}),
        ...(serialized.code !== undefined ? { code: serialized.code } : {}),
      },
      attemptsMade,
      correlationId,
      failedAt: new Date(job.finishedOn ?? Date.now()).toISOString(),
    }

    await queue.add(DEAD_LETTER_JOB_NAME, record, {
      jobId: deadLetterJobId({ sourceQueue, sourceJobId, attemptsMade }),
      // Nothing processes this queue, so retries and backoff are meaningless.
      attempts: 1,
      // Records are the point; never auto-remove them on completion.
      removeOnComplete: false,
      removeOnFail: false,
    })

    metrics.jobsDeadLettered.inc({ queue: sourceQueue, job_name: job.name })

    log.warn(
      {
        queue: sourceQueue,
        jobId: sourceJobId,
        jobName: job.name,
        correlationId,
        attemptsMade,
        failedReason: record.failedReason,
      },
      'job moved to dead-letter',
    )

    await trim()
  }

  /**
   * Keeps the queue bounded.
   *
   * Records sit in `waiting` forever because nothing consumes them, so BullMQ's
   * `removeOnComplete`/`removeOnFail` retention never applies. Without this the
   * dead-letter queue is the one unbounded structure in the system. Oldest
   * records are evicted first, so recent failures are the ones that survive.
   */
  async function trim(): Promise<void> {
    const waiting = await queue.getWaitingCount()
    if (waiting <= maxEntries) return

    const excess = waiting - maxEntries
    const oldest = await queue.getWaiting(0, excess - 1)

    await Promise.all(
      oldest.map(async (entry) => {
        try {
          await entry.remove()
        } catch (error) {
          log.debug(
            { err: serializeError(error), jobId: entry.id },
            'could not evict dead-letter record',
          )
        }
      }),
    )

    log.warn({ evicted: oldest.length, maxEntries }, 'dead-letter queue trimmed')
  }

  return {
    async record(input) {
      const promise = write(input).catch((error: unknown) => {
        // Swallowed deliberately, but never silently: the counter is here so a
        // broken dead-letter path is alertable rather than invisible.
        metrics.deadLetterWriteFailures.inc({ queue: input.sourceQueue })
        log.error(
          {
            err: serializeError(error),
            queue: input.sourceQueue,
            jobId: input.job.id,
            correlationId: input.correlationId,
          },
          'failed to write dead-letter record',
        )
      })

      inFlight.add(promise)
      try {
        await promise
      } finally {
        inFlight.delete(promise)
      }
    },

    pending() {
      return inFlight.size
    },

    async drain(timeoutMs) {
      if (inFlight.size === 0) return

      let timer: NodeJS.Timeout | undefined
      const deadline = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs)
      })

      try {
        await Promise.race([Promise.all([...inFlight]).then(() => undefined), deadline])
      } finally {
        if (timer !== undefined) clearTimeout(timer)
      }
    },
  }
}
