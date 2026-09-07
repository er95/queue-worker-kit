import { z } from 'zod'

import { QUEUE_NAMES } from '../../config/defaults.js'
import { sleep } from '../../utils/sleep.js'
import { defineJob, type JobProcessor } from '../types.js'

/**
 * The recurring job, driven by a BullMQ Job Scheduler.
 *
 * Written to be safely re-runnable: the work is derived entirely from `before`,
 * so running it twice for the same cutoff is indistinguishable from running it
 * once. That property is what makes at-least-once delivery survivable.
 */

export const cleanupPayloadSchema = z.object({
  /** ISO-8601 cutoff; records older than this are in scope. */
  before: z.iso.datetime({ error: 'must be an ISO-8601 date-time string' }),
  /** Report what would be removed without removing it. */
  dryRun: z.boolean(),
})

export type CleanupPayload = z.output<typeof cleanupPayloadSchema>

export interface CleanupResult {
  before: string
  dryRun: boolean
  scanned: number
  removed: number
}

export const cleanupJob = defineJob<
  'maintenance.cleanup',
  typeof cleanupPayloadSchema,
  CleanupResult
>({
  name: 'maintenance.cleanup',
  queue: QUEUE_NAMES.maintenance,
  schema: cleanupPayloadSchema,
  defaultJobOptions: {
    // The next scheduled run will sweep anything this one misses, so there is
    // little value in retrying for long.
    attempts: 2,
  },
})

/**
 * Stands in for "delete rows older than the cutoff". It deliberately touches
 * nothing: no files, no external state. The count is a pure function of the
 * cutoff, which keeps the job idempotent and its tests deterministic.
 */
function countStaleRecords(before: Date): number {
  const days = Math.max(0, Math.floor((Date.now() - before.getTime()) / 86_400_000))
  return days * 17
}

export function createCleanupProcessor(): JobProcessor<CleanupPayload, CleanupResult> {
  return async (context) => {
    const { data, logger, signal } = context

    const before = new Date(data.before)
    const scanned = countStaleRecords(before)

    await sleep(20, signal)

    const removed = data.dryRun ? 0 : scanned

    logger.info({ before: data.before, dryRun: data.dryRun, scanned, removed }, 'cleanup finished')

    return { before: data.before, dryRun: data.dryRun, scanned, removed }
  }
}
