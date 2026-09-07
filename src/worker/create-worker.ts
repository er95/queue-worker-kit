import { Worker, type Job } from 'bullmq'
import type { Redis } from 'ioredis'

import type { Env } from '../config/env.js'
import type { JobRegistry } from '../jobs/registry.js'
import { jobMetaSchema } from '../jobs/types.js'
import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'
import { buildWorkerOptions } from '../queue/defaults.js'
import { serializeError } from '../utils/serialize-error.js'
import type { DeadLetterWriter } from './dead-letter.js'
import { createJobProcessor } from './processor.js'

export interface CreateWorkerOptions {
  queueName: string
  env: Env
  connection: Redis
  logger: AppLogger
  metrics: Metrics
  registry: JobRegistry
  deadLetter: DeadLetterWriter
}

/**
 * Reads the correlation id back off a job, for logging and dead-lettering.
 *
 * The `failed` handler has to work even when the payload is the reason the job
 * failed, so this cannot assume the envelope ever parsed.
 */
function correlationIdOf(job: Job<unknown, unknown, string> | undefined): string {
  if (job === undefined) return 'unknown'
  const meta = (job.data as { meta?: unknown } | null | undefined)?.meta
  const parsed = jobMetaSchema.safeParse(meta)
  return parsed.success ? parsed.data.correlationId : 'unknown'
}

/**
 * Whether BullMQ has decided this failure is final.
 *
 * `finishedOn` is the authoritative signal, and asking BullMQ beats
 * reimplementing its rule. `Job.moveToFailed` sets it *only* on the branch
 * where the job is not being retried, and the `failed` event is emitted after
 * that call resolves, so by the time this runs the answer is already known.
 *
 * That covers cases a hand-rolled `attemptsMade >= attempts` check would miss:
 * an `UnrecoverableError` failing early, `maxStartedAttempts`, and a job failed
 * for exceeding `maxStalledCount`. It also correctly declines to dead-letter a
 * job whose `moveToFailed` did not land because the lock was lost, since that
 * job will be retried by the stalled checker instead.
 *
 * No Redis round trip, so the handler stays free of I/O.
 */
function isTerminalFailure(job: Job<unknown, unknown, string>): boolean {
  return job.finishedOn !== undefined
}

export function createWorker(options: CreateWorkerOptions): Worker<unknown, unknown, string> {
  const { queueName, env, connection, logger, metrics, registry, deadLetter } = options

  const log = logger.child({ component: 'worker', queue: queueName })

  const worker = new Worker<unknown, unknown, string>(
    queueName,
    createJobProcessor({ queueName, registry, env, logger, metrics }),
    {
      ...buildWorkerOptions(env),
      connection,
      name: `${env.QUEUE_PREFIX}-${queueName}`,
    },
  )

  worker.on('ready', () => {
    log.info(
      {
        concurrency: env.WORKER_CONCURRENCY,
        rateLimit: `${env.WORKER_RATE_LIMIT_MAX}/${env.WORKER_RATE_LIMIT_DURATION_MS}ms`,
        jobs: registry.namesForQueue(queueName),
      },
      'worker ready',
    )
  })

  worker.on('error', (error: Error) => {
    // Worker-level errors are usually connection trouble. The worker recovers
    // on its own, so this must not be fatal, but it must be visible.
    log.warn({ err: serializeError(error) }, 'worker error')
  })

  // The worker's own `stalled` event, not QueueEvents'. This fires only on the
  // worker that detected the stall, so the counter is not multiplied by the
  // number of replicas the way a broadcast event would be.
  worker.on('stalled', (jobId: string) => {
    metrics.jobsStalled.inc({ queue: queueName })
    log.warn({ jobId }, 'job stalled and was returned to wait')
  })

  worker.on('failed', (job: Job<unknown, unknown, string> | undefined, error: Error) => {
    if (job === undefined) {
      // BullMQ reports the job as undefined when a stalled job hit its limit
      // and had already been removed by `removeOnFail`.
      log.warn({ err: serializeError(error) }, 'job failed without a job reference')
      return
    }

    if (!isTerminalFailure(job)) return

    // Deliberately not awaited: BullMQ's event handlers are synchronous, so an
    // async listener would just create a floating promise. The writer tracks
    // its own in-flight work instead, and shutdown drains it.
    void deadLetter.record({
      sourceQueue: queueName,
      job,
      error,
      correlationId: correlationIdOf(job),
      attemptsMade: job.attemptsMade,
    })
  })

  worker.on('closing', () => {
    log.info('worker closing, no new jobs will be fetched')
  })

  worker.on('closed', () => {
    log.info('worker closed')
  })

  return worker
}
