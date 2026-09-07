import type { FastifyInstance, FastifyPluginAsync } from 'fastify'
import type { Redis } from 'ioredis'

import { READINESS_TIMEOUT_MS } from '../../config/defaults.js'
import { pingRedis } from '../../queue/connection.js'

/**
 * Liveness and readiness are different questions, and conflating them causes
 * outages.
 *
 * Liveness asks "is this process broken beyond recovery", and a `no` gets the
 * container killed. Readiness asks "should traffic come here right now", and a
 * `no` only removes it from the load balancer. If liveness checked Redis, a
 * brief Redis blip would restart every API replica at once, turning a
 * recoverable dependency failure into a full outage.
 */

export interface HealthResponse {
  status: 'ok' | 'not_ready'
  checks?: Record<string, boolean>
}

export interface HealthRoutesOptions {
  redis: Redis
  /** Reports whether shutdown has begun, so readiness can fail during drain. */
  isShuttingDown: () => boolean
}

export function createHealthRoutes(options: HealthRoutesOptions): FastifyPluginAsync {
  const { redis, isShuttingDown } = options

  return async function healthRoutes(app: FastifyInstance): Promise<void> {
    /**
     * Liveness. Deliberately checks nothing external: if this handler runs at
     * all, the event loop is turning and the server is accepting connections.
     */
    app.get<{ Reply: HealthResponse }>('/health/live', async (_request, reply) =>
      reply.status(200).send({ status: 'ok' }),
    )

    /**
     * Readiness. Fails while draining so the load balancer stops sending
     * requests before the server actually closes, and fails when Redis is
     * unreachable because an enqueue would only 503 anyway.
     */
    app.get<{ Reply: HealthResponse }>('/health/ready', async (_request, reply) => {
      if (isShuttingDown()) {
        return reply.status(503).send({ status: 'not_ready', checks: { redis: false } })
      }

      // Bounded: a probe that can hang for the driver's full retry budget is
      // worse than no probe, because the orchestrator waits on it.
      const redisOk = await pingRedis(redis, READINESS_TIMEOUT_MS)

      if (!redisOk) {
        return reply.status(503).send({ status: 'not_ready', checks: { redis: false } })
      }

      return reply.status(200).send({ status: 'ok', checks: { redis: true } })
    })
  }
}
