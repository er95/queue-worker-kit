import type { Job, Processor } from 'bullmq'

import type { Env } from '../config/env.js'
import {
  JobCancelledError,
  JobTimeoutError,
  UnknownJobError,
  classifyError,
} from '../errors/errors.js'
import type { JobRegistry } from '../jobs/registry.js'
import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'
import { serializeError } from '../utils/serialize-error.js'

/**
 * The wrapper every job runs inside. It owns the concerns no handler should
 * have to repeat: dispatch, payload validation, a scoped logger, timing
 * metrics, and the deadline.
 */

export interface CreateJobProcessorOptions {
  queueName: string
  registry: JobRegistry
  env: Env
  logger: AppLogger
  metrics: Metrics
}

/** Reason attached to the abort signal when a job runs past its deadline. */
const TIMEOUT_REASON = 'job timeout'

export function createJobProcessor(
  options: CreateJobProcessorOptions,
): Processor<unknown, unknown, string> {
  const { queueName, registry, env, logger, metrics } = options

  /**
   * Three declared parameters, and it matters: BullMQ inspects
   * `processor.length` and only creates an `AbortController` for the job when
   * the processor looks like it wants the signal. Drop the third parameter and
   * `signal` silently becomes `undefined` forever.
   */
  return async function processJob(
    job: Job<unknown, unknown, string>,
    _token?: string,
    workerSignal?: AbortSignal,
  ): Promise<unknown> {
    const entry = registry.get(job.name)

    if (entry === undefined) {
      // A job name with no definition in this build: a rolled-back deploy, or a
      // producer running ahead of its workers. Retrying will not help.
      metrics.jobsFailed.inc({ queue: queueName, job_name: job.name })
      logger.error(
        { queue: queueName, jobId: job.id, jobName: job.name },
        'no processor registered for job',
      )
      throw new UnknownJobError(job.name)
    }

    const jobName = entry.definition.name
    const attempt = job.attemptsMade + 1
    const timeoutMs = entry.definition.timeoutMs ?? env.JOB_TIMEOUT_MS
    const labels = { queue: queueName, job_name: jobName }

    // A cooperative deadline, not a kill switch. Node cannot terminate an
    // arbitrary async operation; aborting the signal lets `fetch` and any
    // abort-aware sleep unwind, and failing the attempt stops the worker from
    // waiting on it forever. Work that ignores the signal keeps running until
    // it finishes, holding a concurrency slot. That limit is documented.
    const timeoutController = new AbortController()
    const timer = setTimeout(() => {
      timeoutController.abort(TIMEOUT_REASON)
    }, timeoutMs)

    // BullMQ aborts `workerSignal` when it loses the job's lock or when the
    // worker cancels active jobs during a forced shutdown.
    const signal =
      workerSignal === undefined
        ? timeoutController.signal
        : AbortSignal.any([workerSignal, timeoutController.signal])

    let scopedLogger = logger.child({ queue: queueName, jobId: job.id, jobName, attempt })

    /**
     * Timing starts when the *processor* starts, not when this wrapper does, so
     * the histogram measures handler work rather than schema validation. Held
     * on an object because it is assigned from the callback below, and a plain
     * `let` reassigned inside a closure defeats narrowing at the read site.
     */
    const timing: { startedAt?: bigint } = {}

    try {
      const { result } = await raceWithAbort(
        entry.invoke(job, (meta) => {
          // Called once the envelope has validated, which is the first moment a
          // correlation id exists. Everything logged from here on carries it.
          scopedLogger = scopedLogger.child({ correlationId: meta.correlationId })
          metrics.jobsStarted.inc(labels)
          timing.startedAt = process.hrtime.bigint()
          scopedLogger.info('job started')
          return { logger: scopedLogger, signal, attempt }
        }),
        signal,
        timeoutMs,
        timeoutController.signal,
      )

      const durationSeconds = elapsedSeconds(timing.startedAt)
      metrics.jobDuration.observe(labels, durationSeconds)
      metrics.jobsCompleted.inc(labels)
      scopedLogger.info({ durationSeconds }, 'job completed')

      return result
    } catch (error) {
      const durationSeconds = elapsedSeconds(timing.startedAt)

      // A validation failure throws before `invoke` calls back, so no attempt
      // ever started and a duration sample would be meaningless.
      if (timing.startedAt !== undefined) {
        metrics.jobDuration.observe(labels, durationSeconds)
      }
      metrics.jobsFailed.inc(labels)

      const classification = classifyError(error)
      const willRetry = classification === 'retryable' && attempt < (job.opts.attempts ?? 1)

      if (willRetry) {
        metrics.jobsRetrying.inc(labels)
      }

      scopedLogger.warn(
        {
          err: serializeError(error),
          durationSeconds,
          classification,
          attemptsAllowed: job.opts.attempts ?? 1,
        },
        willRetry ? 'job failed, retrying' : 'job failed',
      )

      // Rethrown so BullMQ owns the outcome: it applies backoff, decides
      // whether attempts remain, and moves the job to `failed`. The
      // dead-letter record is written from the worker's `failed` event, once
      // that decision has actually been made.
      throw error
    } finally {
      clearTimeout(timer)
    }
  }
}

function elapsedSeconds(startedAt: bigint | undefined): number {
  if (startedAt === undefined) return 0
  return Number(process.hrtime.bigint() - startedAt) / 1e9
}

/**
 * Resolves the handler, or rejects as soon as the job is aborted.
 *
 * Without this a timed-out job would hold its concurrency slot until the
 * underlying work finished, defeating the purpose of having a deadline.
 */
async function raceWithAbort<T>(
  work: Promise<T>,
  signal: AbortSignal,
  timeoutMs: number,
  timeoutSignal: AbortSignal,
): Promise<T> {
  if (signal.aborted) {
    throw abortError(signal, timeoutSignal, timeoutMs)
  }

  let onAbort: (() => void) | undefined

  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => {
      reject(abortError(signal, timeoutSignal, timeoutMs))
    }
    signal.addEventListener('abort', onAbort, { once: true })
  })

  try {
    return await Promise.race([work, aborted])
  } finally {
    if (onAbort !== undefined) {
      // Removed explicitly: a completed job must not leave a listener behind on
      // a signal, or a long-lived worker slowly leaks one per job.
      signal.removeEventListener('abort', onAbort)
    }
    // The losing promise is still pending. Attach a no-op handler so a later
    // rejection from abandoned work is not reported as unhandled.
    void work.catch(() => undefined)
  }
}

function abortError(signal: AbortSignal, timeoutSignal: AbortSignal, timeoutMs: number): Error {
  if (timeoutSignal.aborted) {
    return new JobTimeoutError(timeoutMs)
  }
  const reason: unknown = signal.reason
  if (reason instanceof Error) return reason
  return new JobCancelledError(typeof reason === 'string' ? reason : 'worker cancelled the job')
}
