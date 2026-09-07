import { z } from 'zod'

import { QUEUE_NAMES } from '../../config/defaults.js'
import { sleep, throwIfAborted } from '../../utils/sleep.js'
import { defineJob, type JobProcessor } from '../types.js'

/**
 * A longer-running job that reports progress, so the job-status endpoint and
 * Bull Board have something meaningful to show while work is in flight.
 */

export const generateReportPayloadSchema = z.object({
  reportId: z.string().min(1).max(64),
  requestedBy: z.string().min(1).max(128),
  format: z.enum(['csv', 'json']),
})

export type GenerateReportPayload = z.output<typeof generateReportPayloadSchema>

export interface GenerateReportResult {
  reportId: string
  format: 'csv' | 'json'
  generatedRows: number
  bytes: number
}

export const generateReportJob = defineJob<
  'report.generate',
  typeof generateReportPayloadSchema,
  GenerateReportResult
>({
  name: 'report.generate',
  queue: QUEUE_NAMES.reports,
  schema: generateReportPayloadSchema,
  defaultJobOptions: {
    // Report generation is expensive and rarely fixed by a fourth attempt.
    attempts: 3,
  },
})

/** Structured progress: BullMQ accepts any JSON value, not just a number. */
export interface ReportProgress {
  percent: number
  stage: 'query' | 'transform' | 'serialize' | 'done'
}

const STAGES: readonly { stage: ReportProgress['stage']; percent: number; workMs: number }[] = [
  { stage: 'query', percent: 10, workMs: 40 },
  { stage: 'transform', percent: 40, workMs: 40 },
  { stage: 'serialize', percent: 70, workMs: 40 },
  { stage: 'done', percent: 100, workMs: 0 },
]

const ROWS_PER_REPORT = 2_500

export function createGenerateReportProcessor(): JobProcessor<
  GenerateReportPayload,
  GenerateReportResult
> {
  return async (context) => {
    const { job, data, logger, signal } = context

    for (const { stage, percent, workMs } of STAGES) {
      // Cooperative cancellation between stages: a job that has lost its lock or
      // blown its deadline should stop doing work rather than finish and write a
      // result no one will trust.
      throwIfAborted(signal)

      if (workMs > 0) {
        await sleep(workMs, signal)
      }

      const progress: ReportProgress = { percent, stage }
      await job.updateProgress(progress)
      logger.debug({ stage, percent }, 'report progress')
    }

    const bytes = data.format === 'csv' ? ROWS_PER_REPORT * 42 : ROWS_PER_REPORT * 96

    logger.info({ reportId: data.reportId, format: data.format }, 'report generated')

    return {
      reportId: data.reportId,
      format: data.format,
      generatedRows: ROWS_PER_REPORT,
      bytes,
    }
  }
}
