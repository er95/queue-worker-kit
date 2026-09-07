import type { Worker } from 'bullmq'

import {
  ALL_QUEUE_NAMES,
  DEAD_LETTER_DRAIN_TIMEOUT_MS,
  QUEUE_NAMES,
  REDIS_CONNECT_TIMEOUT_MS,
  WORKER_QUEUE_NAMES,
} from '../config/defaults.js'
import { ConfigError, loadEnv } from '../config/env.js'
import { createJobRegistry } from '../jobs/registry.js'
import { createLoggerFromEnv } from '../observability/logger.js'
import { createMetrics } from '../observability/metrics.js'
import { createRedisConnection, redactRedisUrl, waitUntilRedisReady } from '../queue/connection.js'
import { createQueueEventsListeners } from '../queue/events.js'
import { createQueueRegistry } from '../queue/registry.js'
import { buildDefaultSchedules, upsertSchedules } from '../queue/scheduler.js'
import { serializeError } from '../utils/serialize-error.js'
import { createWorker } from './create-worker.js'
import { createDeadLetterWriter } from './dead-letter.js'
import { startWorkerMetricsServer } from './metrics-server.js'
import {
  createShutdownController,
  installSignalHandlers,
  type ShutdownStep,
} from './graceful-shutdown.js'

/**
 * Worker process entrypoint.
 *
 * Deliberately separate from the API. Running workers inside the HTTP process
 * couples two things with opposite scaling and shutdown needs: an API wants to
 * drain in seconds and scales with request rate, while a worker may need
 * minutes to finish an in-flight job and scales with queue depth. It also means
 * a CPU-heavy job cannot stall health checks.
 *
 * Nothing here happens at import time, so the modules above stay testable.
 */
async function main(): Promise<void> {
  const env = loadEnv()
  const logger = createLoggerFromEnv(env, 'worker')

  logger.info(
    {
      nodeEnv: env.NODE_ENV,
      redis: redactRedisUrl(env.REDIS_URL),
      queuePrefix: env.QUEUE_PREFIX,
      queues: WORKER_QUEUE_NAMES,
      concurrency: env.WORKER_CONCURRENCY,
    },
    'worker starting',
  )

  const metrics = createMetrics({ collectDefaults: true })
  const registry = createJobRegistry()

  const connection = createRedisConnection({ env, logger, role: 'worker' })

  // Bounded wait so the first log line reflects reality. Not fatal: ioredis
  // keeps retrying, and the worker starts consuming as soon as it connects.
  const redisReady = await waitUntilRedisReady(connection.client, REDIS_CONNECT_TIMEOUT_MS)
  if (!redisReady) {
    logger.warn(
      { timeoutMs: REDIS_CONNECT_TIMEOUT_MS },
      'redis not ready yet, continuing and relying on reconnect',
    )
  }

  // Workers need queues too: to write dead-letter records and to upsert
  // schedulers. All of them are registered so the dead-letter queue is present.
  const queues = createQueueRegistry({
    env,
    connection: connection.client,
    logger,
    names: ALL_QUEUE_NAMES,
  })

  const deadLetter = createDeadLetterWriter({
    queue: queues.getOrThrow(QUEUE_NAMES.deadLetter),
    logger,
    metrics,
    maxEntries: env.DEAD_LETTER_MAX_ENTRIES,
  })

  if (env.ENABLE_JOB_SCHEDULERS) {
    // Safe from every replica: `upsertJobScheduler` keys off a deterministic id.
    await upsertSchedules({ queues, schedules: buildDefaultSchedules(env), logger })
  }

  const queueEvents = env.ENABLE_QUEUE_EVENTS
    ? createQueueEventsListeners({
        queueNames: WORKER_QUEUE_NAMES,
        env,
        connection: connection.client,
        logger,
      })
    : undefined

  const workers: Worker<unknown, unknown, string>[] = WORKER_QUEUE_NAMES.map((queueName) =>
    createWorker({
      queueName,
      env,
      connection: connection.client,
      logger,
      metrics,
      registry,
      deadLetter,
    }),
  )

  let shuttingDown = false

  // Started after the workers, so a scrape never sees a process that is not
  // yet consuming.
  const metricsServer = env.ENABLE_WORKER_METRICS_SERVER
    ? await startWorkerMetricsServer({
        host: env.WORKER_METRICS_HOST,
        port: env.WORKER_METRICS_PORT,
        metrics,
        logger,
        isShuttingDown: () => shuttingDown,
      })
    : undefined

  /**
   * Order matters more here than anywhere else in the codebase.
   *
   * 1. `worker.close()` stops fetching new jobs and waits for active ones. This
   *    is the step that makes a rolling deploy safe: without it, in-flight jobs
   *    are abandoned and another replica re-runs them once their locks expire.
   * 2. Dead-letter writes are fired from event handlers and may still be in
   *    flight after the last job settles, so they are drained next.
   * 3. Queues and event listeners close, and the shared Redis connection they
   *    all use goes last. Closing it earlier would make every other close fail
   *    against a dead socket.
   */
  const steps: ShutdownStep[] = [
    {
      // Flips /health/live to 503 first, so an orchestrator can tell a
      // container that is draining from one that is wedged.
      name: 'mark-draining',
      run: async () => {
        shuttingDown = true
        await Promise.resolve()
      },
    },
    {
      name: 'workers',
      run: async () => {
        await Promise.all(workers.map((worker) => worker.close()))
      },
    },
    {
      name: 'dead-letter-drain',
      run: () => deadLetter.drain(DEAD_LETTER_DRAIN_TIMEOUT_MS),
    },
    // Closed after the jobs drain, so a final scrape can still collect the
    // counters for the work this process just finished.
    ...(metricsServer === undefined
      ? []
      : [{ name: 'metrics-server', run: () => metricsServer.close() }]),
    ...(queueEvents === undefined
      ? []
      : [{ name: 'queue-events', run: () => queueEvents.close() }]),
    { name: 'queues', run: () => queues.close() },
    { name: 'redis', run: () => connection.close() },
  ]

  const controller = createShutdownController({
    steps,
    logger,
    timeoutMs: env.SHUTDOWN_TIMEOUT_MS,
  })

  installSignalHandlers({ controller, logger })

  logger.info(
    { queues: WORKER_QUEUE_NAMES, metricsPort: metricsServer?.port ?? null },
    'worker started',
  )
}

main().catch((error: unknown) => {
  // No logger is guaranteed to exist this early, and a configuration failure
  // must not be reported through a logger built from that configuration.
  if (error instanceof ConfigError) {
    process.stderr.write(`${error.message}\n`)
  } else {
    process.stderr.write(`worker failed to start: ${JSON.stringify(serializeError(error))}\n`)
  }
  process.exitCode = 1
})
