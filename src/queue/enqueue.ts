import type { JobsOptions } from 'bullmq'

import { DEFAULT_IDEMPOTENCY_TTL_MS, MAX_ENQUEUE_DELAY_MS } from '../config/defaults.js'
import { NonRetryableError, PayloadValidationError, UnknownJobError } from '../errors/errors.js'
import { getJobDefinition, isJobName, type JobName, type PayloadOf } from '../jobs/registry.js'
import type { JobEnvelope } from '../jobs/types.js'
import { resolveCorrelationId } from '../observability/context.js'
import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'
import type { QueueRegistry } from './registry.js'

export interface EnqueueOptions {
  /** Milliseconds to wait before the job becomes eligible. */
  delay?: number
  /** Lower runs first. Leave unset unless you need it; it costs a little throughput. */
  priority?: number
  /** Overrides the configured default for this job only. */
  attempts?: number
  /** Propagates an upstream correlation id instead of minting a new one. */
  correlationId?: string
  /**
   * Collapses repeat submissions of the same logical request.
   *
   * Maps onto BullMQ's deduplication: while the key is live, adding the same
   * key again does not create a second job and returns the existing job's id.
   * This is queue-level protection only. It cannot un-send an email that a
   * previous attempt already sent, so handlers still need to be idempotent.
   */
  idempotencyKey?: string
  /** How long the idempotency key stays live. Defaults to 24h. */
  idempotencyTtlMs?: number
}

export interface EnqueueResult {
  id: string
  name: string
  queue: string
  /**
   * Present when an idempotency key was used. If two calls with the same key
   * return the same `id`, the second one created no new work.
   */
  deduplicationId?: string
}

export interface Enqueuer {
  enqueue<TName extends JobName>(
    name: TName,
    payload: PayloadOf<TName>,
    options?: EnqueueOptions,
  ): Promise<EnqueueResult>
}

export interface CreateEnqueuerOptions {
  queues: QueueRegistry
  logger: AppLogger
  metrics: Metrics
}

export function createEnqueuer(options: CreateEnqueuerOptions): Enqueuer {
  const { queues, logger, metrics } = options

  return {
    async enqueue(name, payload, enqueueOptions = {}) {
      if (!isJobName(name)) {
        // Unreachable through the typed API; reachable from a plain-JS caller.
        throw new UnknownJobError(String(name))
      }

      const definition = getJobDefinition(name)

      // Validate before Redis, not after. A payload that never enters the queue
      // cannot fail five times and land in the dead-letter queue.
      const parsed = definition.schema.safeParse(payload)
      if (!parsed.success) {
        throw new PayloadValidationError(
          `Invalid payload for job "${name}"`,
          parsed.error.issues.map(
            (issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`,
          ),
        )
      }

      if (enqueueOptions.delay !== undefined) {
        if (
          !Number.isSafeInteger(enqueueOptions.delay) ||
          enqueueOptions.delay < 0 ||
          enqueueOptions.delay > MAX_ENQUEUE_DELAY_MS
        ) {
          throw new NonRetryableError(
            `delay must be an integer between 0 and ${MAX_ENQUEUE_DELAY_MS}ms`,
          )
        }
      }

      const queue = queues.getOrThrow(definition.queue)
      const correlationId = resolveCorrelationId(enqueueOptions.correlationId)

      const envelope: JobEnvelope<unknown> = {
        payload: parsed.data,
        meta: {
          correlationId,
          enqueuedAt: new Date().toISOString(),
          ...(enqueueOptions.idempotencyKey !== undefined
            ? { idempotencyKey: enqueueOptions.idempotencyKey }
            : {}),
        },
      }

      // Precedence: queue-wide defaults (set on the Queue itself) are the base,
      // then the definition's overrides, then this call's. Spreads are
      // conditional because `exactOptionalPropertyTypes` distinguishes an absent
      // key from an explicit `undefined`, and BullMQ treats the latter as a value.
      const jobOptions: JobsOptions = {
        ...definition.defaultJobOptions,
        ...(enqueueOptions.attempts !== undefined ? { attempts: enqueueOptions.attempts } : {}),
        ...(enqueueOptions.delay !== undefined ? { delay: enqueueOptions.delay } : {}),
        ...(enqueueOptions.priority !== undefined ? { priority: enqueueOptions.priority } : {}),
        ...(enqueueOptions.idempotencyKey !== undefined
          ? {
              deduplication: {
                id: enqueueOptions.idempotencyKey,
                ttl: enqueueOptions.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS,
              },
            }
          : {}),
      }

      const job = await queue.add(definition.name, envelope, jobOptions)

      if (job.id === undefined) {
        // BullMQ always assigns an id on a successful add; treat its absence as
        // a broken invariant rather than papering over it with an empty string.
        throw new Error(`Queue "${definition.queue}" returned a job without an id`)
      }

      metrics.jobsEnqueued.inc({ queue: definition.queue, job_name: definition.name })

      logger.info(
        {
          queue: definition.queue,
          jobId: job.id,
          jobName: definition.name,
          correlationId,
          ...(enqueueOptions.delay !== undefined ? { delayMs: enqueueOptions.delay } : {}),
          ...(enqueueOptions.idempotencyKey !== undefined
            ? { idempotencyKey: enqueueOptions.idempotencyKey }
            : {}),
        },
        'job enqueued',
      )

      return {
        id: job.id,
        name: definition.name,
        queue: definition.queue,
        ...(enqueueOptions.idempotencyKey !== undefined
          ? { deduplicationId: enqueueOptions.idempotencyKey }
          : {}),
      }
    },
  }
}
