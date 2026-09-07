import type { FastifyInstance, FastifyPluginAsync } from 'fastify'

import type { Metrics } from '../../observability/metrics.js'

export interface MetricsRoutesOptions {
  metrics: Metrics
}

/**
 * Prometheus scrape endpoint.
 *
 * Note the `Content-Type` comes from the registry rather than being hard-coded:
 * it carries the exposition format version, and Prometheus needs it to parse
 * the body correctly.
 */
export function createMetricsRoutes(options: MetricsRoutesOptions): FastifyPluginAsync {
  const { metrics } = options

  return async function metricsRoutes(app: FastifyInstance): Promise<void> {
    app.get('/metrics', async (_request, reply) => {
      // `registry.metrics()` awaits every registered `collect` hook, including
      // the queue-depth gauge, which talks to Redis.
      const body = await metrics.registry.metrics()
      return reply.header('content-type', metrics.registry.contentType).status(200).send(body)
    })
  }
}
