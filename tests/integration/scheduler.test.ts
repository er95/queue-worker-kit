import { Worker } from 'bullmq'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { MAINTENANCE_SCHEDULER_ID, QUEUE_NAMES } from '../../src/config/defaults.js'
import { buildWorkerOptions } from '../../src/queue/defaults.js'
import { createQueueRegistry, type QueueRegistry } from '../../src/queue/registry.js'
import {
  buildDefaultSchedules,
  listSchedules,
  removeSchedule,
  upsertSchedules,
} from '../../src/queue/scheduler.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { waitFor } from '../helpers/wait.js'

/**
 * Recurring jobs, against BullMQ v6 Job Schedulers.
 *
 * v6 removed the legacy repeatable-job API, so `queue.add(name, data, { repeat:
 * { cron } })` no longer exists. The tests below pin the two properties that
 * made the old API painful: a deterministic id, so redeploying updates the
 * schedule instead of accumulating orphans, and an upsert, so every replica
 * calling it on boot converges rather than multiplying.
 */
describe('job schedulers', () => {
  let context: TestContext
  let queues: QueueRegistry

  beforeEach(() => {
    context = createTestContext('scheduler')
    queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: [QUEUE_NAMES.maintenance],
    })
    context.track(queues)
  })

  afterEach(async () => {
    await context.cleanup()
  })

  it('registers the maintenance schedule', async () => {
    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules(context.env),
      logger: context.logger,
    })

    const schedules = await listSchedules(queues, QUEUE_NAMES.maintenance)

    expect(schedules).toHaveLength(1)
    expect(schedules[0]).toMatchObject({
      key: MAINTENANCE_SCHEDULER_ID,
      name: 'maintenance.cleanup',
      pattern: '0 * * * *',
    })
  })

  /**
   * The property that makes booting N replicas safe. With the legacy API, a
   * changed cron expression left the old repeat key in place and you ended up
   * running two schedules.
   */
  it('converges on one schedule no matter how many times it is upserted', async () => {
    const schedules = buildDefaultSchedules(context.env)

    for (let i = 0; i < 5; i += 1) {
      await upsertSchedules({ queues, schedules, logger: context.logger })
    }

    expect(await queues.getOrThrow(QUEUE_NAMES.maintenance).getJobSchedulersCount()).toBe(1)
  })

  it('handles concurrent upserts from several replicas', async () => {
    const schedules = buildDefaultSchedules(context.env)

    await Promise.all(
      Array.from({ length: 4 }, () =>
        upsertSchedules({ queues, schedules, logger: context.logger }),
      ),
    )

    expect(await queues.getOrThrow(QUEUE_NAMES.maintenance).getJobSchedulersCount()).toBe(1)
  })

  it('updates an existing schedule in place when the pattern changes', async () => {
    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules(context.env),
      logger: context.logger,
    })

    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules({ ...context.env, MAINTENANCE_CLEANUP_CRON: '*/5 * * * *' }),
      logger: context.logger,
    })

    const schedules = await listSchedules(queues, QUEUE_NAMES.maintenance)

    expect(schedules).toHaveLength(1)
    expect(schedules[0]?.pattern).toBe('*/5 * * * *')
  })

  it('produces a delayed job for the next iteration', async () => {
    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules(context.env),
      logger: context.logger,
    })

    const queue = queues.getOrThrow(QUEUE_NAMES.maintenance)

    // A scheduler is a job factory: it immediately materialises the next
    // iteration as an ordinary delayed job.
    const delayed = await waitFor(
      async () => {
        const jobs = await queue.getDelayed(0, 10)
        return jobs.length > 0 ? jobs : undefined
      },
      { description: 'the first scheduled job' },
    )

    expect(delayed[0]?.name).toBe('maintenance.cleanup')
  })

  it('gives scheduled jobs a valid envelope the worker will accept', async () => {
    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules(context.env),
      logger: context.logger,
    })

    const queue = queues.getOrThrow(QUEUE_NAMES.maintenance)
    const delayed = await waitFor(
      async () => {
        const jobs = await queue.getDelayed(0, 10)
        return jobs.length > 0 ? jobs : undefined
      },
      { description: 'the first scheduled job' },
    )

    const data = delayed[0]?.data as {
      payload: { before: string; dryRun: boolean }
      meta: { correlationId: string }
    }

    expect(Date.parse(data.payload.before)).not.toBeNaN()
    expect(data.payload.dryRun).toBe(false)
    // Scheduler templates are stored once, so every iteration shares this id.
    // It identifies the schedule; `jobId` is what distinguishes the runs.
    expect(data.meta.correlationId).toBe(`scheduler:${MAINTENANCE_SCHEDULER_ID}`)
  })

  it('removes a schedule', async () => {
    await upsertSchedules({
      queues,
      schedules: buildDefaultSchedules(context.env),
      logger: context.logger,
    })

    expect(await removeSchedule(queues, QUEUE_NAMES.maintenance, MAINTENANCE_SCHEDULER_ID)).toBe(
      true,
    )
    expect(await listSchedules(queues, QUEUE_NAMES.maintenance)).toHaveLength(0)
  })

  it('reports removal of a schedule that is not there', async () => {
    expect(await removeSchedule(queues, QUEUE_NAMES.maintenance, 'no-such-schedule')).toBe(false)
  })

  /**
   * A scheduler only materialises its *next* iteration once the current one
   * leaves the delayed set, so this needs a worker consuming jobs. Without
   * one, exactly one job is ever produced.
   */
  it('keeps producing iterations while a worker consumes them', async () => {
    const queue = queues.getOrThrow(QUEUE_NAMES.maintenance)
    const runs: string[] = []

    const worker = new Worker<unknown, unknown, string>(
      QUEUE_NAMES.maintenance,
      async (job) => {
        runs.push(job.id ?? '')
        return { ok: true }
      },
      { ...buildWorkerOptions(context.env), connection: context.redis },
    )
    context.track(worker)

    // `every` rather than a cron pattern: the shortest cron interval is one
    // minute, which is far too long for a test.
    await queue.upsertJobScheduler(
      'fast-cleanup',
      { every: 200 },
      {
        name: 'maintenance.cleanup',
        data: {
          payload: { before: new Date().toISOString(), dryRun: true },
          meta: {
            correlationId: 'scheduler:fast-cleanup',
            enqueuedAt: new Date().toISOString(),
          },
        },
      },
    )

    await waitFor(async () => (runs.length >= 3 ? runs.length : undefined), {
      timeoutMs: 20_000,
      description: 'the schedule to produce repeated jobs',
    })

    // Distinct job ids: three iterations, not one job retried three times.
    expect(new Set(runs).size).toBeGreaterThanOrEqual(3)

    await queue.removeJobScheduler('fast-cleanup')
  })
})
