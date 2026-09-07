import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { QUEUE_NAMES } from '../../src/config/defaults.js'
import { createJobRegistry } from '../../src/jobs/registry.js'
import { createMetrics, type Metrics } from '../../src/observability/metrics.js'
import { createEnqueuer } from '../../src/queue/enqueue.js'
import { createQueueRegistry } from '../../src/queue/registry.js'
import { createWorker } from '../../src/worker/create-worker.js'
import { createDeadLetterWriter } from '../../src/worker/dead-letter.js'
import {
  startWorkerMetricsServer,
  type WorkerMetricsServer,
} from '../../src/worker/metrics-server.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { waitFor } from '../helpers/wait.js'

/**
 * The worker's scrape endpoint.
 *
 * This exists because the counters that matter most are recorded where the jobs
 * run. The API knows how many jobs it enqueued and nothing about how many
 * succeeded, so a worker with no HTTP server leaves
 * `qwk_jobs_completed_total` unreachable by any scraper.
 */
describe('worker metrics server', () => {
  let context: TestContext
  let server: WorkerMetricsServer
  let metrics: Metrics
  let shuttingDown = false

  beforeAll(async () => {
    context = createTestContext('worker-metrics')
    metrics = createMetrics({ collectDefaults: true })

    const queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: [QUEUE_NAMES.email, QUEUE_NAMES.deadLetter],
    })
    context.track(queues)

    const worker = createWorker({
      queueName: QUEUE_NAMES.email,
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      metrics,
      registry: createJobRegistry(),
      deadLetter: createDeadLetterWriter({
        queue: queues.getOrThrow(QUEUE_NAMES.deadLetter),
        logger: context.logger,
        metrics,
        maxEntries: 100,
      }),
    })
    context.track(worker)

    // Port 0 lets the OS pick a free one, so the suite cannot collide with
    // anything already listening.
    server = await startWorkerMetricsServer({
      host: '127.0.0.1',
      port: 0,
      metrics,
      logger: context.logger,
      isShuttingDown: () => shuttingDown,
    })
    context.onCleanup(() => server.close())

    // One job through the worker, so there is something to report.
    const enqueuer = createEnqueuer({ queues, logger: context.logger, metrics })
    await enqueuer.enqueue('email.send', {
      to: 'user@example.com',
      template: 'welcome',
      userId: 'usr_metrics',
    })

    await waitFor(
      async () => ((await metrics.jobsCompleted.get()).values.length > 0 ? true : undefined),
      { description: 'the job to complete' },
    )
  })

  afterAll(async () => {
    await context.cleanup()
  })

  const get = (path: string) => fetch(`http://127.0.0.1:${server.port}${path}`)

  it('serves the lifecycle counters the API cannot see', async () => {
    const response = await get('/metrics')
    const body = await response.text()

    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toContain('text/plain')

    // The whole reason this server exists.
    expect(body).toContain('qwk_jobs_started_total')
    expect(body).toContain('qwk_jobs_completed_total')
    expect(body).toContain('qwk_job_duration_seconds')
    expect(body).toMatch(/qwk_jobs_completed_total\{queue="email",job_name="email\.send"\} 1/)
  })

  it('includes Node process metrics', async () => {
    expect(await (await get('/metrics')).text()).toContain('process_cpu')
  })

  /**
   * Queue depth is a property of the queue, not of this process. Reporting it
   * from every replica would publish the same numbers N times and add a Redis
   * round trip per worker per scrape; the API reports it once instead.
   */
  it('publishes no queue depth samples', async () => {
    const body = await (await get('/metrics')).text()

    // The gauge is declared by `createMetrics`, so its HELP/TYPE lines are
    // present. What matters is that no worker registers a collector for it, so
    // there are no labelled samples.
    expect(body).not.toMatch(/^qwk_queue_jobs\{/m)
  })

  it('reports liveness', async () => {
    const response = await get('/health/live')

    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({ status: 'ok' })
  })

  // Lets an orchestrator distinguish a draining container from a wedged one.
  it('reports 503 once draining starts', async () => {
    shuttingDown = true
    try {
      const response = await get('/health/live')
      expect(response.status).toBe(503)
      expect(await response.json()).toEqual({ status: 'shutting_down' })
    } finally {
      shuttingDown = false
    }
  })

  // It is a scrape endpoint, not a second API.
  it.each(['/jobs/email', '/health/ready', '/admin/queues'])('does not expose %s', async (path) => {
    expect((await get(path)).status).toBe(404)
  })

  it('rejects an attempt to enqueue through it', async () => {
    const response = await fetch(`http://127.0.0.1:${server.port}/jobs/email`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ to: 'user@example.com', template: 'welcome', userId: 'u' }),
    })

    expect(response.status).toBe(404)
  })
})
