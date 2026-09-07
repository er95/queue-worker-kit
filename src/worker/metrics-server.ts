import Fastify, { type FastifyBaseLogger } from 'fastify'

import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'

/**
 * A tiny HTTP surface for the worker process.
 *
 * Without this, the most interesting metrics in the system are unreachable.
 * Jobs are started, completed, failed and dead-lettered by the *worker*, so
 * those counters live in the worker's registry; the API only ever knows how
 * many jobs it enqueued. A worker with no HTTP server means
 * `qwk_jobs_completed_total` is never scraped by anything.
 *
 * Two endpoints, deliberately. This is not a second API: it cannot enqueue and
 * it exposes no queue operations. Bind it to an internal port and do not route
 * public traffic to it.
 *
 * Queue depth is intentionally *not* reported here. It is a property of the
 * queue, not of this process, so every replica would publish the same numbers;
 * the API reports it once instead.
 */

export interface WorkerMetricsServer {
  /** The port actually bound, which matters when port 0 was requested. */
  readonly port: number
  close(): Promise<void>
}

export interface StartWorkerMetricsServerOptions {
  host: string
  port: number
  metrics: Metrics
  logger: AppLogger
  /** Reports whether shutdown has begun. */
  isShuttingDown: () => boolean
}

export async function startWorkerMetricsServer(
  options: StartWorkerMetricsServerOptions,
): Promise<WorkerMetricsServer> {
  const { host, port, metrics, logger, isShuttingDown } = options

  // Raised to `warn`, because a scrape every 15 seconds is not worth a log
  // line and would bury the job logs this process exists to produce. Problems
  // still come through.
  const baseLogger: FastifyBaseLogger = logger.child(
    { component: 'worker-metrics' },
    { level: 'warn' },
  )

  const app = Fastify({
    loggerInstance: baseLogger,
    // Nothing here accepts a body.
    bodyLimit: 1024,
  })

  app.get('/metrics', async (_request, reply) => {
    const body = await metrics.registry.metrics()
    return reply.header('content-type', metrics.registry.contentType).status(200).send(body)
  })

  /**
   * Liveness only. A worker has no meaningful readiness signal: it pulls work
   * rather than receiving it, so there is no load balancer to remove it from.
   * It reports unhealthy once draining starts, which is what tells an
   * orchestrator the container is on its way out rather than wedged.
   */
  app.get('/health/live', async (_request, reply) => {
    if (isShuttingDown()) {
      return reply.status(503).send({ status: 'shutting_down' })
    }
    return reply.status(200).send({ status: 'ok' })
  })

  await app.listen({ host, port })

  const address = app.addresses()[0]
  const boundPort = address?.port ?? port

  logger.info({ host, port: boundPort }, 'worker metrics server listening')

  return {
    port: boundPort,
    close: () => app.close(),
  }
}
