/**
 * Central home for every operational constant that is not worth an environment
 * variable. Anything that a deployment realistically needs to tune lives in
 * `env.ts` instead; anything here is a considered default that only changes
 * with a code review.
 */

/** Canonical queue names. The HTTP API can only ever address these. */
export const QUEUE_NAMES = {
  email: 'email',
  reports: 'reports',
  maintenance: 'maintenance',
  deadLetter: 'dead-letter',
} as const

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES]

export const ALL_QUEUE_NAMES: readonly QueueName[] = Object.values(QUEUE_NAMES)

/**
 * Queues a worker process consumes. The dead-letter queue is deliberately
 * absent: nothing consumes it, which is what makes re-queue loops impossible.
 */
export const WORKER_QUEUE_NAMES: readonly QueueName[] = [
  QUEUE_NAMES.email,
  QUEUE_NAMES.reports,
  QUEUE_NAMES.maintenance,
]

/** Prometheus metric name prefix. Constant on purpose: dashboards hard-code it. */
export const METRIC_PREFIX = 'qwk_'

/**
 * Histogram buckets for job processing duration, in seconds. Skewed towards
 * sub-second I/O work while still resolving the multi-second tail.
 */
export const JOB_DURATION_BUCKETS_SECONDS: readonly number[] = [
  0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
]

/** Maximum accepted HTTP request body. Enqueue payloads are small by design. */
export const HTTP_BODY_LIMIT_BYTES = 64 * 1024

/** Fastify connection timeout: a request may not occupy a socket forever. */
export const HTTP_REQUEST_TIMEOUT_MS = 15_000

/**
 * Readiness probes must answer quickly even when Redis is wedged, otherwise the
 * orchestrator's own probe timeout becomes the only bound.
 */
export const READINESS_TIMEOUT_MS = 1_000

/** How long a brand-new Redis socket may take to become usable. */
export const REDIS_CONNECT_TIMEOUT_MS = 10_000

/** Reconnect backoff bounds for ioredis `retryStrategy`. */
export const REDIS_RECONNECT_MIN_DELAY_MS = 200
export const REDIS_RECONNECT_MAX_DELAY_MS = 5_000

/**
 * BullMQ lock duration. A job whose lock is not renewed within this window is
 * considered stalled and may be handed to another worker, so it must comfortably
 * exceed the longest event-loop stall the worker can experience.
 */
export const WORKER_LOCK_DURATION_MS = 30_000

/** How often each worker scans for stalled jobs belonging to dead workers. */
export const WORKER_STALLED_INTERVAL_MS = 30_000

/**
 * Times a job may be recovered from `stalled` back to `wait` before it is
 * failed outright. Keeping this at 1 stops a job that reliably kills its worker
 * from cycling through the whole fleet.
 */
export const WORKER_MAX_STALLED_COUNT = 1

/** Approximate cap on the BullMQ events stream, so it cannot grow without bound. */
export const QUEUE_EVENTS_STREAM_MAX_LEN = 10_000

/** Stack traces are truncated before they reach logs or dead-letter records. */
export const ERROR_STACK_MAX_LINES = 12

/** Upper bound on a serialized error message stored in a dead-letter record. */
export const DEAD_LETTER_REASON_MAX_CHARS = 2_000

/** Depth limit when walking `error.cause` chains. */
export const ERROR_CAUSE_MAX_DEPTH = 3

/**
 * Grace period between resolving all shutdown steps and forcing the process
 * down, giving log transports a chance to flush.
 */
export const SHUTDOWN_EXIT_GRACE_MS = 250

/** How long shutdown waits for in-flight dead-letter writes to land. */
export const DEAD_LETTER_DRAIN_TIMEOUT_MS = 5_000

/** Deterministic id of the recurring maintenance schedule. */
export const MAINTENANCE_SCHEDULER_ID = 'maintenance-cleanup'

/** How far back `maintenance.cleanup` sweeps on each scheduled run. */
export const MAINTENANCE_CLEANUP_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1_000

/** Default TTL applied to an `idempotencyKey` when the caller does not set one. */
export const DEFAULT_IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1_000

/** Largest accepted `delay` on an enqueue request (7 days). */
export const MAX_ENQUEUE_DELAY_MS = 7 * 24 * 60 * 60 * 1_000

/** Rejects oversized job payloads at `Queue.add` time rather than in Redis. */
export const JOB_PAYLOAD_SIZE_LIMIT_BYTES = 16 * 1024
