import {
  ALL_QUEUE_NAMES,
  READINESS_TIMEOUT_MS,
  REDIS_CONNECT_TIMEOUT_MS,
} from '../config/defaults.js'
import { ConfigError, loadEnv } from '../config/env.js'
import { createLoggerFromEnv } from '../observability/logger.js'
import { createMetrics } from '../observability/metrics.js'
import { createRedisConnection, redactRedisUrl, waitUntilRedisReady } from '../queue/connection.js'
import { createEnqueuer } from '../queue/enqueue.js'
import { createQueueRegistry } from '../queue/registry.js'
import { serializeError } from '../utils/serialize-error.js'
import {
  createShutdownController,
  installSignalHandlers,
  type ShutdownStep,
} from '../worker/graceful-shutdown.js'
import { buildApp } from './app.js'

/**
 * API (producer) process entrypoint.
 *
 * This process validates and enqueues; it never processes. Keeping the two
 * apart is what lets the API drain in seconds during a deploy while workers
 * take as long as their in-flight jobs need.
 */
async function main(): Promise<void> {
  const env = loadEnv()
  const logger = createLoggerFromEnv(env, 'api')

  logger.info(
    {
      nodeEnv: env.NODE_ENV,
      // Redacted: a Redis URL routinely carries a password.
      redis: redactRedisUrl(env.REDIS_URL),
      queuePrefix: env.QUEUE_PREFIX,
      bullBoard: env.ENABLE_BULL_BOARD,
    },
    'api starting',
  )

  const metrics = createMetrics({ collectDefaults: true })
  const connection = createRedisConnection({ env, logger, role: 'producer' })

  // Not fatal if this times out: the server still starts and `/health/ready`
  // reports 503 until Redis answers, which is the signal an orchestrator wants.
  const redisReady = await waitUntilRedisReady(connection.client, REDIS_CONNECT_TIMEOUT_MS)
  if (!redisReady) {
    logger.warn(
      { timeoutMs: REDIS_CONNECT_TIMEOUT_MS },
      'redis not ready yet, starting anyway and reporting not-ready',
    )
  }

  const queues = createQueueRegistry({
    env,
    connection: connection.client,
    logger,
    names: ALL_QUEUE_NAMES,
  })

  const enqueuer = createEnqueuer({ queues, logger, metrics })

  // Queue depth is sampled on scrape rather than pushed, so the numbers are
  // never stale and no background timer is required.
  metrics.registerQueueDepthCollector(async (report) => {
    await Promise.all(
      queues.all().map(async (queue) => {
        try {
          const counts = await queue.getJobCounts(
            'waiting',
            'active',
            'delayed',
            'failed',
            'completed',
          )
          for (const [state, count] of Object.entries(counts)) {
            report.set({ queue: queue.name, state }, count)
          }
        } catch (error) {
          // A scrape must not fail because Redis is momentarily unavailable:
          // the queue simply reports no samples this time round.
          logger.debug({ err: serializeError(error), queue: queue.name }, 'queue depth unavailable')
        }
      }),
    )
  })

  let shuttingDown = false

  const app = await buildApp({
    env,
    logger,
    metrics,
    queues,
    enqueuer,
    redis: connection.client,
    isShuttingDown: () => shuttingDown,
  })

  /**
   * Shutdown order for an HTTP producer:
   *
   * 1. Flip readiness to 503 first, then pause briefly. The load balancer needs
   *    at least one failed probe to take this instance out of rotation;
   *    closing the server before that is what produces connection-refused
   *    errors during an otherwise clean deploy.
   * 2. `app.close()` stops accepting connections and waits for in-flight
   *    requests, so a request that has already been accepted still gets to
   *    finish enqueueing its job.
   * 3. Only then close the queues, and the shared Redis connection last.
   */
  const steps: ShutdownStep[] = [
    {
      name: 'drain-readiness',
      run: async () => {
        shuttingDown = true
        await new Promise<void>((resolve) => {
          setTimeout(resolve, READINESS_TIMEOUT_MS)
        })
      },
    },
    { name: 'http-server', run: () => app.close() },
    { name: 'queues', run: () => queues.close() },
    { name: 'redis', run: () => connection.close() },
  ]

  const controller = createShutdownController({
    steps,
    logger,
    timeoutMs: env.SHUTDOWN_TIMEOUT_MS,
  })

  installSignalHandlers({ controller, logger })

  await app.listen({ host: env.HOST, port: env.PORT })

  logger.info({ host: env.HOST, port: env.PORT }, 'api started')
}

main().catch((error: unknown) => {
  // A configuration failure cannot be reported through a logger built from
  // that same configuration, so this writes to stderr directly.
  if (error instanceof ConfigError) {
    process.stderr.write(`${error.message}\n`)
  } else {
    process.stderr.write(`api failed to start: ${JSON.stringify(serializeError(error))}\n`)
  }
  process.exitCode = 1
})
