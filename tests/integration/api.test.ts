import type { FastifyInstance } from 'fastify'
import { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'

import { ALL_QUEUE_NAMES, QUEUE_NAMES } from '../../src/config/defaults.js'
import { buildApp } from '../../src/api/app.js'
import { createJobRegistry } from '../../src/jobs/registry.js'
import { CORRELATION_ID_HEADER } from '../../src/observability/context.js'
import { createMetrics } from '../../src/observability/metrics.js'
import { createEnqueuer } from '../../src/queue/enqueue.js'
import { createQueueRegistry } from '../../src/queue/registry.js'
import { createWorker } from '../../src/worker/create-worker.js'
import { createDeadLetterWriter } from '../../src/worker/dead-letter.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { waitFor } from '../helpers/wait.js'

/**
 * HTTP surface, driven through `app.inject()` so no port is bound.
 */
describe('HTTP API', () => {
  let context: TestContext
  let app: FastifyInstance
  let shuttingDown = false

  beforeAll(async () => {
    context = createTestContext('api')

    const metrics = createMetrics()
    const queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: ALL_QUEUE_NAMES,
    })
    context.track(queues)

    const enqueuer = createEnqueuer({ queues, logger: context.logger, metrics })

    metrics.registerQueueDepthCollector(async (report) => {
      for (const queue of queues.all()) {
        const counts = await queue.getJobCounts('waiting', 'active')
        for (const [state, count] of Object.entries(counts)) {
          report.set({ queue: queue.name, state }, count)
        }
      }
    })

    app = await buildApp({
      env: context.env,
      logger: context.logger,
      metrics,
      queues,
      enqueuer,
      redis: context.redis,
      isShuttingDown: () => shuttingDown,
    })
    context.onCleanup(() => app.close())
  })

  afterAll(async () => {
    await context.cleanup()
  })

  afterEach(() => {
    shuttingDown = false
  })

  describe('POST /jobs/email', () => {
    it('accepts a valid request and reports the queued job', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: { to: 'user@example.com', template: 'welcome', userId: 'usr_123' },
      })

      expect(response.statusCode).toBe(202)
      expect(response.json()).toMatchObject({
        data: { queue: QUEUE_NAMES.email, name: 'email.send' },
      })
      expect(response.json().data.id).toBeTruthy()
    })

    it('rejects an invalid email with a 400 and the offending field', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: { to: 'not-an-email', template: 'welcome', userId: 'usr_123' },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json().error.code).toBe('VALIDATION_ERROR')
      expect(JSON.stringify(response.json().error.details)).toContain('to')
    })

    it('rejects a missing field', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: { to: 'user@example.com' },
      })

      expect(response.statusCode).toBe(400)
    })

    // `.strict()` on the request schema: an unrecognised key is far more often
    // a client bug than an intentional extra.
    it('rejects an unknown field rather than ignoring it', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: {
          to: 'user@example.com',
          template: 'welcome',
          userId: 'usr_1',
          isAdmin: true,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('rejects a malformed JSON body', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        headers: { 'content-type': 'application/json' },
        payload: '{"to": ',
      })

      expect(response.statusCode).toBe(400)
    })

    it('accepts a delay and leaves the job delayed', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: {
          to: 'user@example.com',
          template: 'welcome',
          userId: 'usr_delayed',
          delay: 60_000,
        },
      })

      expect(response.statusCode).toBe(202)

      const detail = await app.inject({
        method: 'GET',
        url: `/jobs/${QUEUE_NAMES.email}/${response.json().data.id}`,
      })

      expect(detail.json().data.state).toBe('delayed')
    })

    it('rejects a delay beyond the allowed window', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: {
          to: 'user@example.com',
          template: 'welcome',
          userId: 'usr_1',
          delay: 99_999_999_999,
        },
      })

      expect(response.statusCode).toBe(400)
    })

    it('collapses two requests sharing an idempotency key', async () => {
      const payload = {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_idem_http',
        idempotencyKey: 'http:welcome:usr_idem_http',
      }

      const first = await app.inject({ method: 'POST', url: '/jobs/email', payload })
      const second = await app.inject({ method: 'POST', url: '/jobs/email', payload })

      expect(second.json().data.id).toBe(first.json().data.id)
    })
  })

  describe('POST /jobs/report and /jobs/cleanup', () => {
    it('queues a report', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/report',
        payload: { reportId: 'r-1', requestedBy: 'ops', format: 'csv' },
      })

      expect(response.statusCode).toBe(202)
      expect(response.json().data).toMatchObject({
        queue: QUEUE_NAMES.reports,
        name: 'report.generate',
      })
    })

    it('rejects an unsupported report format', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/report',
        payload: { reportId: 'r-1', requestedBy: 'ops', format: 'pdf' },
      })

      expect(response.statusCode).toBe(400)
    })

    it('queues a cleanup', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/cleanup',
        payload: { before: '2026-01-01T00:00:00.000Z', dryRun: true },
      })

      expect(response.statusCode).toBe(202)
      expect(response.json().data.name).toBe('maintenance.cleanup')
    })

    it('rejects a non-ISO cutoff', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/cleanup',
        payload: { before: 'yesterday', dryRun: true },
      })

      expect(response.statusCode).toBe(400)
    })
  })

  describe('GET /jobs/:queue/:id', () => {
    it('returns a whitelisted projection of the job', async () => {
      const created = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        payload: { to: 'user@example.com', template: 'welcome', userId: 'usr_status' },
      })

      const response = await app.inject({
        method: 'GET',
        url: `/jobs/${QUEUE_NAMES.email}/${created.json().data.id}`,
      })

      expect(response.statusCode).toBe(200)

      const { data } = response.json()

      expect(Object.keys(data).sort()).toEqual([
        'attemptsMade',
        'correlationId',
        'createdAt',
        'failedReason',
        'finishedAt',
        'id',
        'name',
        'processedAt',
        'progress',
        'queue',
        'result',
        'state',
      ])
      // Not a serialized BullMQ job: internal fields cannot leak by accident.
      expect(data).not.toHaveProperty('opts')
      expect(data).not.toHaveProperty('data')
      expect(data).not.toHaveProperty('stacktrace')
    })

    it('exposes the correlation id and result of a completed job', async () => {
      const registry = createJobRegistry()
      const metrics = createMetrics()
      const queues = createQueueRegistry({
        env: context.env,
        connection: context.redis,
        logger: context.logger,
        names: ALL_QUEUE_NAMES,
      })
      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: context.env,
        connection: context.redis,
        logger: context.logger,
        metrics,
        registry,
        deadLetter: createDeadLetterWriter({
          queue: queues.getOrThrow(QUEUE_NAMES.deadLetter),
          logger: context.logger,
          metrics,
          maxEntries: 100,
        }),
      })

      try {
        const created = await app.inject({
          method: 'POST',
          url: '/jobs/email',
          headers: { [CORRELATION_ID_HEADER]: 'trace-http-1' },
          payload: { to: 'user@example.com', template: 'receipt', userId: 'usr_done' },
        })

        const id = created.json().data.id

        const data = await waitFor(
          async () => {
            const response = await app.inject({
              method: 'GET',
              url: `/jobs/${QUEUE_NAMES.email}/${id}`,
            })
            const body = response.json().data
            return body.state === 'completed' ? body : undefined
          },
          { description: 'the job to complete' },
        )

        expect(data.correlationId).toBe('trace-http-1')
        expect(data.result).toMatchObject({ delivered: true })
        expect(data.finishedAt).toBeTruthy()
      } finally {
        await worker.close()
        await queues.close()
      }
    })

    it('returns 404 for an unknown job id', async () => {
      const response = await app.inject({
        method: 'GET',
        url: `/jobs/${QUEUE_NAMES.email}/999999999`,
      })

      expect(response.statusCode).toBe(404)
      expect(response.json().error.code).toBe('NOT_FOUND')
    })

    /**
     * The registry lookup, from the outside. An unknown name must be a 404, not
     * a newly constructed Queue in a caller-chosen Redis keyspace.
     */
    it.each(['unknown-queue', 'bull', '../../etc', 'email;flushall'])(
      'returns 404 for the queue name %s',
      async (queue) => {
        const response = await app.inject({ method: 'GET', url: `/jobs/${queue}/1` })
        expect(response.statusCode).toBe(404)
      },
    )

    it('reads the dead-letter queue so operators can fetch a record', async () => {
      const response = await app.inject({
        method: 'GET',
        url: `/jobs/${QUEUE_NAMES.deadLetter}/nonexistent`,
      })

      // 404 for the id, not for the queue: the queue itself is addressable.
      expect(response.statusCode).toBe(404)
      expect(response.json().error.message).toContain('not found')
    })
  })

  describe('health', () => {
    // Liveness must not depend on Redis: a brief Redis blip would otherwise
    // restart every replica at once and turn a recoverable failure into an outage.
    it('reports live while Redis is up', async () => {
      const response = await app.inject({ method: 'GET', url: '/health/live' })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ status: 'ok' })
    })

    it('reports ready while Redis is reachable', async () => {
      const response = await app.inject({ method: 'GET', url: '/health/ready' })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ status: 'ok', checks: { redis: true } })
    })

    // Readiness has to fail before the server stops accepting connections, or
    // the load balancer keeps routing to a draining instance.
    it('reports not ready while draining', async () => {
      shuttingDown = true

      const response = await app.inject({ method: 'GET', url: '/health/ready' })

      expect(response.statusCode).toBe(503)
      expect(response.json()).toEqual({ status: 'not_ready', checks: { redis: false } })
    })

    it('still reports live while draining', async () => {
      shuttingDown = true
      expect((await app.inject({ method: 'GET', url: '/health/live' })).statusCode).toBe(200)
    })
  })

  describe('GET /metrics', () => {
    it('serves the Prometheus exposition format', async () => {
      const response = await app.inject({ method: 'GET', url: '/metrics' })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('text/plain')
      expect(response.body).toContain('qwk_jobs_enqueued_total')
    })

    it('samples queue depth at scrape time', async () => {
      const response = await app.inject({ method: 'GET', url: '/metrics' })
      expect(response.body).toContain('qwk_queue_jobs')
    })
  })

  describe('security basics', () => {
    it('sets security headers', async () => {
      const response = await app.inject({ method: 'GET', url: '/health/live' })

      expect(response.headers).toHaveProperty('x-content-type-options', 'nosniff')
      expect(response.headers).toHaveProperty('x-frame-options')
    })

    it('rejects a body past the limit with 413', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/jobs/email',
        headers: { 'content-type': 'application/json' },
        payload: JSON.stringify({
          to: 'user@example.com',
          template: 'welcome',
          userId: 'x'.repeat(200_000),
        }),
      })

      expect(response.statusCode).toBe(413)
      expect(response.json().error.code).toBe('PAYLOAD_TOO_LARGE')
    })

    it('echoes a caller-supplied correlation id', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/health/live',
        headers: { [CORRELATION_ID_HEADER]: 'trace-echo' },
      })

      expect(response.headers[CORRELATION_ID_HEADER]).toBe('trace-echo')
    })

    it('generates a correlation id when the caller sends none', async () => {
      const response = await app.inject({ method: 'GET', url: '/health/live' })

      expect(response.headers[CORRELATION_ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/)
    })

    /**
     * The echoed value goes into a response header, and Node rejects invalid
     * header characters, so an unvalidated one would turn a malformed client
     * header into a 500. Anything unacceptable is replaced, not rejected.
     */
    it.each([
      ['a header-injection attempt', 'trace\r\nx-injected: 1'],
      ['a space', 'trace id'],
      ['an over-long value', 'a'.repeat(300)],
      ['an empty value', ''],
    ])('ignores %s in the correlation id header', async (_label, value) => {
      const response = await app.inject({
        method: 'GET',
        url: '/health/live',
        headers: { [CORRELATION_ID_HEADER]: value },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers[CORRELATION_ID_HEADER]).toMatch(/^[0-9a-f-]{36}$/)
    })

    it('returns a consistent error shape for an unknown route', async () => {
      const response = await app.inject({ method: 'GET', url: '/nope' })

      expect(response.statusCode).toBe(404)
      expect(response.json().error).toMatchObject({ code: 'NOT_FOUND' })
    })

    it('does not mount the dashboard when it is disabled', async () => {
      const response = await app.inject({ method: 'GET', url: '/admin/queues' })
      expect(response.statusCode).toBe(404)
    })

    // Operationally dangerous actions are not public API routes.
    it.each([
      ['POST', '/queues/email/obliterate'],
      ['POST', '/queues/email/pause'],
      ['DELETE', '/queues/email'],
    ])('does not expose %s %s', async (method, url) => {
      const response = await app.inject({ method: method as 'POST', url })
      expect(response.statusCode).toBe(404)
    })
  })
})

/**
 * Readiness against a Redis that is not there. A separate app instance, because
 * the point is a connection that cannot succeed.
 */
describe('readiness with unreachable Redis', () => {
  let app: FastifyInstance
  let redis: Redis

  beforeAll(async () => {
    const context = createTestContext('api-down')

    // A closed port on localhost: connection refused rather than a slow DNS
    // timeout, so the test stays fast and deterministic.
    redis = new Redis('redis://127.0.0.1:6399', {
      maxRetriesPerRequest: null,
      enableOfflineQueue: false,
      lazyConnect: true,
      retryStrategy: () => 50,
    })
    redis.on('error', () => undefined)

    const metrics = createMetrics()
    const queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: ALL_QUEUE_NAMES,
    })

    app = await buildApp({
      env: context.env,
      logger: context.logger,
      metrics,
      queues,
      enqueuer: createEnqueuer({ queues, logger: context.logger, metrics }),
      redis,
      isShuttingDown: () => false,
    })

    await queues.close()
    await context.connection.close()
  })

  afterAll(async () => {
    await app.close()
    redis.disconnect()
  })

  it('reports not ready with a 503', async () => {
    const response = await app.inject({ method: 'GET', url: '/health/ready' })

    expect(response.statusCode).toBe(503)
    expect(response.json()).toEqual({ status: 'not_ready', checks: { redis: false } })
  })

  // Liveness is about this process, not its dependencies.
  it('still reports live', async () => {
    expect((await app.inject({ method: 'GET', url: '/health/live' })).statusCode).toBe(200)
  })

  // The readiness probe must never outlast the orchestrator's own timeout.
  it('answers within its own timeout rather than hanging', async () => {
    const startedAt = Date.now()
    await app.inject({ method: 'GET', url: '/health/ready' })
    expect(Date.now() - startedAt).toBeLessThan(5_000)
  })
})

describe('Bull Board', () => {
  let context: TestContext
  let app: FastifyInstance

  beforeAll(async () => {
    context = createTestContext('board', {
      ENABLE_BULL_BOARD: 'true',
      BULL_BOARD_USERNAME: 'admin',
      BULL_BOARD_PASSWORD: 'local-dev-password',
    })

    const metrics = createMetrics()
    const queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: ALL_QUEUE_NAMES,
    })
    context.track(queues)

    app = await buildApp({
      env: context.env,
      logger: context.logger,
      metrics,
      queues,
      enqueuer: createEnqueuer({ queues, logger: context.logger, metrics }),
      redis: context.redis,
      isShuttingDown: () => false,
    })
    context.onCleanup(() => app.close())
  })

  afterAll(async () => {
    await context.cleanup()
  })

  const basic = (user: string, password: string) =>
    `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`

  // The dashboard can retry, remove and drain queues, so it is never anonymous.
  it('refuses an unauthenticated request', async () => {
    const response = await app.inject({ method: 'GET', url: '/admin/queues' })
    expect(response.statusCode).toBe(401)
  })

  it.each([
    ['wrong password', 'admin', 'nope'],
    ['wrong username', 'root', 'local-dev-password'],
    ['both wrong', 'root', 'nope'],
    ['empty', '', ''],
  ])('refuses %s', async (_label, user, password) => {
    const response = await app.inject({
      method: 'GET',
      url: '/admin/queues',
      headers: { authorization: basic(user, password) },
    })
    expect(response.statusCode).toBe(401)
  })

  it('serves the dashboard with valid credentials', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/admin/queues',
      headers: { authorization: basic('admin', 'local-dev-password') },
    })

    expect(response.statusCode).toBe(200)
  })

  // Authentication is an onRequest hook on the whole prefix, so it covers the
  // JSON API the UI calls, not just the entry page.
  it('protects the dashboard API too', async () => {
    const response = await app.inject({ method: 'GET', url: '/admin/queues/api/queues' })
    expect(response.statusCode).toBe(401)
  })

  it('leaves the rest of the API unauthenticated', async () => {
    expect((await app.inject({ method: 'GET', url: '/health/live' })).statusCode).toBe(200)
  })
})
