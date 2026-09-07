import { describe, expect, it } from 'vitest'

import { METRIC_PREFIX } from '../../src/config/defaults.js'
import { createMetrics } from '../../src/observability/metrics.js'

describe('createMetrics', () => {
  /**
   * prom-client's default registry is process-global and throws on duplicate
   * registration. Handing every caller its own registry is what keeps a test
   * file, a hot reload, or two apps in one process from colliding.
   */
  it('can be created repeatedly without a duplicate-registration error', () => {
    expect(() => {
      createMetrics()
      createMetrics()
      createMetrics()
    }).not.toThrow()
  })

  it('keeps separate instances isolated', async () => {
    const a = createMetrics()
    const b = createMetrics()

    a.jobsEnqueued.inc({ queue: 'email', job_name: 'email.send' }, 5)

    expect(
      await b.registry.getSingleMetricAsString(`${METRIC_PREFIX}jobs_enqueued_total`),
    ).not.toContain(' 5')
  })

  it('exposes the documented metric names', async () => {
    const metrics = createMetrics()

    metrics.jobsEnqueued.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsStarted.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsCompleted.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsFailed.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsRetrying.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsDeadLettered.inc({ queue: 'email', job_name: 'email.send' })
    metrics.jobsStalled.inc({ queue: 'email' })
    metrics.jobDuration.observe({ queue: 'email', job_name: 'email.send' }, 0.5)

    const exposition = await metrics.registry.metrics()

    for (const name of [
      'jobs_enqueued_total',
      'jobs_started_total',
      'jobs_completed_total',
      'jobs_failed_total',
      'jobs_dead_lettered_total',
      'job_duration_seconds',
    ]) {
      expect(exposition).toContain(`${METRIC_PREFIX}${name}`)
    }
  })

  /**
   * The cardinality rule this locks in: a label set of {queue, job_name} is
   * bounded by the codebase. Adding a job id or a correlation id would mint a
   * new time series per job and eventually take Prometheus down.
   */
  it('labels job metrics only by queue and job name', async () => {
    const metrics = createMetrics()
    metrics.jobsEnqueued.inc({ queue: 'email', job_name: 'email.send' })

    const [sample] = (await metrics.jobsEnqueued.get()).values

    expect(Object.keys(sample?.labels ?? {}).sort()).toEqual(['job_name', 'queue'])
  })

  it('records durations into the configured histogram buckets', async () => {
    const metrics = createMetrics()
    metrics.jobDuration.observe({ queue: 'reports', job_name: 'report.generate' }, 0.3)

    const exposition = await metrics.registry.metrics()

    expect(exposition).toContain(`${METRIC_PREFIX}job_duration_seconds_bucket`)
    expect(exposition).toContain('le="0.5"')
  })

  describe('queue depth collector', () => {
    // Sampled at scrape time so the numbers are never stale and no background
    // timer is needed.
    it('is invoked on scrape', async () => {
      const metrics = createMetrics()
      let calls = 0

      metrics.registerQueueDepthCollector(async (report) => {
        calls += 1
        report.set({ queue: 'email', state: 'waiting' }, 7)
        await Promise.resolve()
      })

      const exposition = await metrics.registry.metrics()

      expect(calls).toBe(1)
      expect(exposition).toContain(`${METRIC_PREFIX}queue_jobs`)
      expect(exposition).toContain('7')
    })

    it('drops label combinations that no longer exist', async () => {
      const metrics = createMetrics()
      let queues = ['email', 'reports']

      metrics.registerQueueDepthCollector(async (report) => {
        for (const queue of queues) {
          report.set({ queue, state: 'waiting' }, 1)
        }
        await Promise.resolve()
      })

      expect(await metrics.registry.metrics()).toContain('queue="reports"')

      queues = ['email']
      expect(await metrics.registry.metrics()).not.toContain('queue="reports"')
    })

    it('scrapes cleanly before any collector is registered', async () => {
      await expect(createMetrics().registry.metrics()).resolves.toBeTypeOf('string')
    })
  })

  it('adds Node process metrics only when asked', async () => {
    expect(await createMetrics().registry.metrics()).not.toContain('process_cpu')
    expect(await createMetrics({ collectDefaults: true }).registry.metrics()).toContain(
      'process_cpu',
    )
  })
})
