import { UnrecoverableError } from 'bullmq'

/**
 * Error semantics for job processing.
 *
 * BullMQ decides whether to retry by asking one question: is this error an
 * `UnrecoverableError`? Everything else is retried until `attempts` runs out.
 * So rather than bolt a parallel retry policy on top, non-retryable errors here
 * extend BullMQ's own class and get its native behaviour for free.
 */

/**
 * A permanent failure. Retrying cannot change the outcome, so BullMQ fails the
 * job immediately regardless of how many attempts remain.
 *
 * Use for: malformed payloads, unknown job names, unsupported operations, a
 * resource that is known to be permanently gone.
 */
export class NonRetryableError extends UnrecoverableError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message)
    this.name = 'NonRetryableError'
    if (options?.cause !== undefined) {
      this.cause = options.cause
    }
  }
}

/** A payload that failed schema validation. Permanently broken by definition. */
export class PayloadValidationError extends NonRetryableError {
  readonly issues: readonly string[]

  constructor(message: string, issues: readonly string[]) {
    super(message)
    this.name = 'PayloadValidationError'
    this.issues = issues
  }
}

/** A job name with no registered definition. Code or data is out of sync. */
export class UnknownJobError extends NonRetryableError {
  constructor(jobName: string) {
    super(`No job definition registered for "${jobName}"`)
    this.name = 'UnknownJobError'
  }
}

/**
 * A transient failure worth retrying: a timeout, a 502, a dropped connection.
 * Plain `Error`s are treated the same way; this exists to say so explicitly at
 * the point of failure.
 */
export class TransientError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message)
    this.name = 'TransientError'
    if (options?.cause !== undefined) {
      this.cause = options.cause
    }
  }
}

/**
 * The job exceeded its deadline. Retryable: the usual cause is a slow
 * dependency, which is exactly the sort of thing that succeeds on attempt two.
 */
export class JobTimeoutError extends Error {
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`Job exceeded its ${timeoutMs}ms timeout`)
    this.name = 'JobTimeoutError'
    this.timeoutMs = timeoutMs
  }
}

/**
 * The worker asked the job to stop: either the process is shutting down past
 * its deadline, or BullMQ lost the job's lock and another worker may pick it
 * up. Retryable.
 */
export class JobCancelledError extends Error {
  constructor(reason: string) {
    super(`Job cancelled: ${reason}`)
    this.name = 'JobCancelledError'
  }
}

/**
 * True when BullMQ will refuse to retry this error.
 *
 * The `name` check mirrors BullMQ's own logic and covers errors that crossed a
 * module or process boundary, where `instanceof` silently stops working.
 */
export function isNonRetryableError(error: unknown): boolean {
  if (error instanceof UnrecoverableError) return true
  return (
    error instanceof Error &&
    (error.name === 'UnrecoverableError' ||
      error.name === 'NonRetryableError' ||
      error.name === 'PayloadValidationError' ||
      error.name === 'UnknownJobError')
  )
}

export type ErrorClassification = 'non_retryable' | 'retryable'

export function classifyError(error: unknown): ErrorClassification {
  return isNonRetryableError(error) ? 'non_retryable' : 'retryable'
}
