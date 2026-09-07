import type { JobSchedulerJson } from 'bullmq'

import {
  MAINTENANCE_CLEANUP_LOOKBACK_MS,
  MAINTENANCE_SCHEDULER_ID,
  QUEUE_NAMES,
} from '../config/defaults.js'
import type { Env } from '../config/env.js'
import { cleanupJob } from '../jobs/definitions/cleanup.job.js'
import type { JobEnvelope } from '../jobs/types.js'
import type { AppLogger } from '../observability/logger.js'
import type { QueueRegistry } from './registry.js'

/**
 * Recurring work, via BullMQ v6 Job Schedulers.
 *
 * BullMQ v6 removed the legacy repeatable-job API, so `queue.add(name, data,
 * { repeat: { cron } })` no longer exists. A Job Scheduler is a named factory
 * that produces one delayed job per iteration, which fixes the old failure mode
 * where a changed cron expression left an orphaned repeat key behind.
 */

export interface ScheduleSpec {
  /**
   * Stable, human-chosen id. `upsertJobScheduler` keys off this, so redeploying
   * updates the existing schedule instead of adding another one, and every
   * replica calling it on boot converges on the same single schedule.
   */
  id: string
  queue: string
  jobName: string
  /** Cron expression evaluated by BullMQ. */
  pattern: string
  /** IANA timezone. Unset means the worker's local time, which is UTC in the image. */
  tz?: string
  /**
   * Builds the payload stored in the scheduler's template.
   *
   * Called once per upsert, not once per iteration: BullMQ stores one template
   * and reuses it for every job the schedule produces. A time-dependent field
   * is therefore fixed at upsert time and only refreshed when a worker boots
   * and re-upserts. If you need a genuinely rolling window, express it
   * relatively in the payload and resolve it inside the processor.
   */
  buildPayload: () => unknown
}

export function buildDefaultSchedules(env: Env): readonly ScheduleSpec[] {
  return [
    {
      id: MAINTENANCE_SCHEDULER_ID,
      queue: QUEUE_NAMES.maintenance,
      jobName: cleanupJob.name,
      pattern: env.MAINTENANCE_CLEANUP_CRON,
      buildPayload: () => ({
        before: new Date(Date.now() - MAINTENANCE_CLEANUP_LOOKBACK_MS).toISOString(),
        dryRun: false,
      }),
    },
  ]
}

export interface UpsertSchedulesOptions {
  queues: QueueRegistry
  schedules: readonly ScheduleSpec[]
  logger: AppLogger
}

/**
 * Creates or updates every schedule. Safe to call from every worker replica on
 * every boot: the operation is an upsert against a deterministic id.
 */
export async function upsertSchedules(options: UpsertSchedulesOptions): Promise<void> {
  const { queues, schedules, logger } = options

  for (const spec of schedules) {
    const queue = queues.getOrThrow(spec.queue)

    // Scheduler templates are stored once, so every iteration shares this
    // correlation id. It identifies the schedule rather than a single run;
    // `jobId` is what distinguishes iterations in logs.
    const envelope: JobEnvelope<unknown> = {
      payload: spec.buildPayload(),
      meta: {
        correlationId: `scheduler:${spec.id}`,
        enqueuedAt: new Date().toISOString(),
      },
    }

    await queue.upsertJobScheduler(
      spec.id,
      {
        pattern: spec.pattern,
        ...(spec.tz !== undefined ? { tz: spec.tz } : {}),
      },
      {
        name: spec.jobName,
        data: envelope,
      },
    )

    logger.info(
      { schedulerId: spec.id, queue: spec.queue, jobName: spec.jobName, pattern: spec.pattern },
      'job scheduler upserted',
    )
  }
}

/** Every schedule currently registered on a queue. */
export async function listSchedules(
  queues: QueueRegistry,
  queueName: string,
): Promise<JobSchedulerJson[]> {
  return queues.getOrThrow(queueName).getJobSchedulers()
}

/**
 * Removes a schedule. Already-produced jobs are unaffected: they are ordinary
 * delayed jobs at this point and will still run.
 */
export async function removeSchedule(
  queues: QueueRegistry,
  queueName: string,
  schedulerId: string,
): Promise<boolean> {
  return queues.getOrThrow(queueName).removeJobScheduler(schedulerId)
}
