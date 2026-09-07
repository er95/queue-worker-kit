import helmet from '@fastify/helmet'
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify'
import type { Redis } from 'ioredis'
import type { z } from 'zod'

import { HTTP_BODY_LIMIT_BYTES, HTTP_REQUEST_TIMEOUT_MS } from '../config/defaults.js'
import type { Env } from '../config/env.js'
import {
  CORRELATION_ID_HEADER,
  newCorrelationId,
  runWithCorrelationId,
} from '../observability/context.js'
import type { AppLogger } from '../observability/logger.js'
import type { Metrics } from '../observability/metrics.js'
import type { Enqueuer } from '../queue/enqueue.js'
import type { QueueRegistry } from '../queue/registry.js'
import { BULL_BOARD_BASE_PATH, registerBullBoard } from './bull-board.js'
import { createErrorHandler, toValidationError } from './errors.js'
import { createHealthRoutes } from './routes/health.routes.js'
import { createJobsRoutes } from './routes/jobs.routes.js'
import { createMetricsRoutes } from './routes/metrics.routes.js'

/**
 * Accepted shape of a caller-supplied correlation id.
 *
 * Bounded and restricted to printable ASCII without whitespace, because the
 * value is echoed back in a response header: a newline would make Node reject
 * the header outright and turn a malformed client request into a 500.
 */
const SAFE_CORRELATION_ID = /^[A-Za-z0-9._:-]{1,200}$/

export interface BuildAppOptions {
  env: Env
  logger: AppLogger
  metrics: Metrics
  queues: QueueRegistry
  enqueuer: Enqueuer
  redis: Redis
  /** Lets readiness fail as soon as draining starts. */
  isShuttingDown: () => boolean
}

/**
 * Builds the HTTP app without starting it, so tests can drive it through
 * `app.inject()` and never bind a port.
 */
export async function buildApp(options: BuildAppOptions): Promise<FastifyInstance> {
  const { env, logger, metrics, queues, enqueuer, redis, isShuttingDown } = options

  // Widened deliberately. Fastify infers its instance type from this value, and
  // a pino `Logger` would make the instance narrower than the
  // `FastifyPluginAsync` signature every route plugin is declared with.
  const baseLogger: FastifyBaseLogger = logger

  const app = Fastify({
    // Fastify shares the app logger, so HTTP logs and job logs are one stream
    // with the same redaction rules.
    loggerInstance: baseLogger,
    // Enqueue payloads are small; anything larger is a mistake or an attack.
    bodyLimit: HTTP_BODY_LIMIT_BYTES,
    // A request may not hold a socket indefinitely.
    connectionTimeout: HTTP_REQUEST_TIMEOUT_MS,
    // Trusting X-Forwarded-* without knowing the proxy lets a caller spoof its
    // own IP, so it stays off. Enable it only behind a proxy you control.
    trustProxy: false,
    genReqId: () => newCorrelationId(),
  })

  /**
   * Route schemas are zod, not JSON Schema.
   *
   * One schema per payload then serves three jobs: HTTP validation, the
   * enqueue-time check, and the worker-side envelope check. The alternative,
   * a hand-written JSON Schema beside each zod schema, is a guarantee that the
   * two eventually disagree.
   */
  app.setValidatorCompiler(({ schema }) => {
    const zodSchema = schema as unknown as z.ZodType
    return (data: unknown) => {
      const result = zodSchema.safeParse(data)
      if (result.success) {
        return { value: result.data }
      }
      return { error: toValidationError(result.error) }
    }
  })

  // Sensible security headers. CSP is off because Bull Board serves its own
  // inline assets and a broken dashboard is worse than a missing header here;
  // the API's JSON responses are not a CSP target.
  await app.register(helmet, { contentSecurityPolicy: false })

  /**
   * Correlation id per request. Accepts a caller-supplied one so a trace can
   * span services, echoes it, and puts it in `AsyncLocalStorage` so `enqueue`
   * stamps it onto jobs without every handler passing it down by hand.
   */
  app.addHook('onRequest', (request, reply, done) => {
    const incoming = request.headers[CORRELATION_ID_HEADER]
    const correlationId =
      typeof incoming === 'string' && SAFE_CORRELATION_ID.test(incoming) ? incoming : request.id

    reply.header(CORRELATION_ID_HEADER, correlationId)
    runWithCorrelationId(correlationId, done)
  })

  app.setErrorHandler(
    createErrorHandler({
      // Stack traces and internal messages stay out of production responses.
      exposeInternals: env.NODE_ENV !== 'production',
    }),
  )

  app.setNotFoundHandler((request, reply) =>
    reply.status(404).send({
      error: { code: 'NOT_FOUND', message: `Route ${request.method} ${request.url} not found` },
    }),
  )

  await app.register(createHealthRoutes({ redis, isShuttingDown }))
  await app.register(createMetricsRoutes({ metrics }))
  await app.register(
    createJobsRoutes({
      enqueuer,
      queues,
      // Every registered queue is readable, including dead-letter, so operators
      // can fetch a record by id.
      readableQueues: queues.names(),
    }),
  )

  if (env.ENABLE_BULL_BOARD) {
    // `loadEnv` has already refused to start with the dashboard on and no
    // credentials, so these are present.
    if (env.BULL_BOARD_USERNAME === undefined || env.BULL_BOARD_PASSWORD === undefined) {
      throw new Error('Bull Board is enabled but its credentials are missing')
    }

    await registerBullBoard({
      app,
      queues,
      username: env.BULL_BOARD_USERNAME,
      password: env.BULL_BOARD_PASSWORD,
    })

    logger.info({ path: BULL_BOARD_BASE_PATH }, 'bull board mounted behind basic auth')
  }

  return app
}
