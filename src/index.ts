/**
 * Public surface of queue-worker-kit.
 *
 * Deliberately smaller than the source tree. These are the pieces you compose
 * to build a producer or a worker; everything else is an implementation detail
 * and may change without a major version. If you find yourself reaching past
 * this file, that is a signal the export list should grow on purpose.
 */

// Configuration
export { ConfigError, envSchema, loadEnv, shouldUsePrettyLogs, type Env } from './config/env.js'
export {
  ALL_QUEUE_NAMES,
  QUEUE_NAMES,
  WORKER_QUEUE_NAMES,
  type QueueName,
} from './config/defaults.js'

// Job definitions
export {
  defineJob,
  envelopeSchema,
  jobMetaSchema,
  type JobDefaultOptions,
  type JobDefinition,
  type JobEnvelope,
  type JobMeta,
  type JobProcessor,
  type ProcessorContext,
} from './jobs/types.js'
export {
  JOB_NAMES,
  createJobRegistry,
  getJobDefinition,
  isJobName,
  jobDefinitions,
  queueForJob,
  type JobName,
  type JobRegistry,
  type PayloadOf,
  type RegisteredJob,
  type ResultOf,
} from './jobs/registry.js'

// Demo job definitions, exported so their payload types are usable by callers.
export {
  cleanupJob,
  cleanupPayloadSchema,
  type CleanupPayload,
  type CleanupResult,
} from './jobs/definitions/cleanup.job.js'
export {
  generateReportJob,
  generateReportPayloadSchema,
  type GenerateReportPayload,
  type GenerateReportResult,
  type ReportProgress,
} from './jobs/definitions/generate-report.job.js'
export {
  createDemoEmailProvider,
  sendEmailJob,
  sendEmailPayloadSchema,
  type EmailProvider,
  type SendEmailPayload,
  type SendEmailResult,
} from './jobs/definitions/send-email.job.js'

// Queues
export {
  createRedisConnection,
  createRedisOptions,
  pingRedis,
  redactRedisUrl,
  waitUntilRedisReady,
  type ConnectionRole,
  type RedisConnectionHandle,
} from './queue/connection.js'
export { createQueue, type CreateQueueOptions } from './queue/create-queue.js'
export { createQueueRegistry, type QueueRegistry } from './queue/registry.js'
export {
  buildDefaultJobOptions,
  buildRateLimiterOptions,
  buildWorkerOptions,
} from './queue/defaults.js'
export {
  createEnqueuer,
  type EnqueueOptions,
  type EnqueueResult,
  type Enqueuer,
} from './queue/enqueue.js'
export {
  buildDefaultSchedules,
  listSchedules,
  removeSchedule,
  upsertSchedules,
  type ScheduleSpec,
} from './queue/scheduler.js'
export { createQueueEventsListeners, type QueueEventsHandle } from './queue/events.js'

// Workers
export { createWorker, type CreateWorkerOptions } from './worker/create-worker.js'
export { createJobProcessor } from './worker/processor.js'
export {
  DEAD_LETTER_JOB_NAME,
  createDeadLetterWriter,
  deadLetterJobId,
  deadLetterRecordSchema,
  type DeadLetterRecord,
  type DeadLetterWriter,
} from './worker/dead-letter.js'
export { startWorkerMetricsServer, type WorkerMetricsServer } from './worker/metrics-server.js'
export {
  createShutdownController,
  installSignalHandlers,
  type ShutdownController,
  type ShutdownStep,
} from './worker/graceful-shutdown.js'

// HTTP
export { buildApp, type BuildAppOptions } from './api/app.js'

// Observability
export { createLogger, createLoggerFromEnv, type AppLogger } from './observability/logger.js'
export { createMetrics, type Metrics } from './observability/metrics.js'
export {
  CORRELATION_ID_HEADER,
  getCorrelationId,
  newCorrelationId,
  resolveCorrelationId,
  runWithCorrelationId,
} from './observability/context.js'

// Errors
export {
  JobCancelledError,
  JobTimeoutError,
  NonRetryableError,
  PayloadValidationError,
  TransientError,
  UnknownJobError,
  classifyError,
  isNonRetryableError,
  type ErrorClassification,
} from './errors/errors.js'
export { formatErrorReason, serializeError, type SerializedError } from './utils/serialize-error.js'
