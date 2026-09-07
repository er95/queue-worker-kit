import type { FastifyInstance, FastifyPluginAsync } from 'fastify'
import { z } from 'zod'

import { MAX_ENQUEUE_DELAY_MS } from '../../config/defaults.js'
import { cleanupPayloadSchema } from '../../jobs/definitions/cleanup.job.js'
import { generateReportPayloadSchema } from '../../jobs/definitions/generate-report.job.js'
import { sendEmailPayloadSchema } from '../../jobs/definitions/send-email.job.js'
import type { Enqueuer, EnqueueOptions, EnqueueResult } from '../../queue/enqueue.js'
import type { QueueRegistry } from '../../queue/registry.js'
import { notFound } from '../errors.js'

/**
 * Enqueue and inspection endpoints.
 *
 * Two constraints shape this file. A caller can only ever name a job that is
 * registered at build time, and only ever read from a queue in the registry, so
 * no HTTP input reaches `new Queue(...)` or selects which code runs. And the
 * job-status response is an explicit projection rather than a serialized
 * BullMQ job, so internal fields cannot leak by accident later.
 */

/** Per-call options every enqueue endpoint accepts, alongside its own payload. */
const enqueueOptionsSchema = z.object({
  correlationId: z.string().min(1).max(200).optional(),
  delay: z.number().int().min(0).max(MAX_ENQUEUE_DELAY_MS).optional(),
  idempotencyKey: z.string().min(1).max(200).optional(),
})

const emailRequestSchema = sendEmailPayloadSchema.extend(enqueueOptionsSchema.shape).strict()
const reportRequestSchema = generateReportPayloadSchema.extend(enqueueOptionsSchema.shape).strict()
const cleanupRequestSchema = cleanupPayloadSchema.extend(enqueueOptionsSchema.shape).strict()

const jobParamsSchema = z.object({
  queue: z.string().min(1).max(64),
  id: z.string().min(1).max(128),
})

type EmailRequest = z.output<typeof emailRequestSchema>
type ReportRequest = z.output<typeof reportRequestSchema>
type CleanupRequest = z.output<typeof cleanupRequestSchema>
type JobParams = z.output<typeof jobParamsSchema>

/** Splits transport options off a request body, leaving the job payload. */
function splitEnqueueOptions<T extends z.output<typeof enqueueOptionsSchema>>(
  body: T,
): { options: EnqueueOptions; payload: Omit<T, keyof z.output<typeof enqueueOptionsSchema>> } {
  const { correlationId, delay, idempotencyKey, ...payload } = body
  return {
    options: {
      ...(correlationId !== undefined ? { correlationId } : {}),
      ...(delay !== undefined ? { delay } : {}),
      ...(idempotencyKey !== undefined ? { idempotencyKey } : {}),
    },
    payload,
  }
}

/** Job state as the API reports it. */
export interface JobStatusResponse {
  id: string
  name: string
  queue: string
  state: string
  progress: unknown
  attemptsMade: number
  correlationId: string | null
  createdAt: string | null
  processedAt: string | null
  finishedAt: string | null
  failedReason: string | null
  result: unknown
}

export interface JobsRoutesOptions {
  enqueuer: Enqueuer
  queues: QueueRegistry
  /** Queues readable over HTTP. Excludes nothing today, but is explicit. */
  readableQueues: readonly string[]
}

export function createJobsRoutes(options: JobsRoutesOptions): FastifyPluginAsync {
  const { enqueuer, queues, readableQueues } = options
  const readable = new Set(readableQueues)

  return async function jobsRoutes(app: FastifyInstance): Promise<void> {
    app.post<{ Body: EmailRequest; Reply: { data: EnqueueResult } }>(
      '/jobs/email',
      { schema: { body: emailRequestSchema } },
      async (request, reply) => {
        const { options: enqueueOptions, payload } = splitEnqueueOptions(request.body)
        const data = await enqueuer.enqueue('email.send', payload, enqueueOptions)
        return reply.status(202).send({ data })
      },
    )

    app.post<{ Body: ReportRequest; Reply: { data: EnqueueResult } }>(
      '/jobs/report',
      { schema: { body: reportRequestSchema } },
      async (request, reply) => {
        const { options: enqueueOptions, payload } = splitEnqueueOptions(request.body)
        const data = await enqueuer.enqueue('report.generate', payload, enqueueOptions)
        return reply.status(202).send({ data })
      },
    )

    app.post<{ Body: CleanupRequest; Reply: { data: EnqueueResult } }>(
      '/jobs/cleanup',
      { schema: { body: cleanupRequestSchema } },
      async (request, reply) => {
        const { options: enqueueOptions, payload } = splitEnqueueOptions(request.body)
        const data = await enqueuer.enqueue('maintenance.cleanup', payload, enqueueOptions)
        return reply.status(202).send({ data })
      },
    )

    app.get<{ Params: JobParams; Reply: { data: JobStatusResponse } }>(
      '/jobs/:queue/:id',
      { schema: { params: jobParamsSchema } },
      async (request, reply) => {
        const { queue: queueName, id } = request.params

        // A registry lookup, not a constructor: an unknown name is a 404, not a
        // newly created queue in a caller-chosen keyspace.
        const queue = readable.has(queueName) ? queues.get(queueName) : undefined
        if (queue === undefined) {
          throw notFound(`Unknown queue "${queueName}"`)
        }

        let job = await queue.getJob(id)
        if (job === undefined) {
          // Also the answer for a job that has aged out of its retention window.
          throw notFound(`Job "${id}" was not found in queue "${queueName}"`)
        }

        const state = await job.getState()

        /**
         * `getJob` and `getState` are two round trips, so a job that settles
         * between them yields a snapshot older than the state: the response
         * would claim `completed` while still reporting `result: null`.
         * `finishedOn` is written atomically with the state transition, so its
         * absence is the tell, and one re-read makes the two agree.
         */
        if ((state === 'completed' || state === 'failed') && job.finishedOn === undefined) {
          job = (await queue.getJob(id)) ?? job
        }
        const meta = (job.data as { meta?: { correlationId?: unknown } } | null | undefined)?.meta
        const correlationId = typeof meta?.correlationId === 'string' ? meta.correlationId : null

        // BullMQ types `failedReason` as `string`, but a job that has never
        // failed simply has no such field, so treat it as optional.
        const failedReason = job.failedReason as string | undefined

        const data: JobStatusResponse = {
          id: job.id ?? id,
          name: job.name,
          queue: queueName,
          state,
          progress: job.progress,
          attemptsMade: job.attemptsMade,
          correlationId,
          createdAt: job.timestamp > 0 ? new Date(job.timestamp).toISOString() : null,
          processedAt:
            job.processedOn === undefined ? null : new Date(job.processedOn).toISOString(),
          finishedAt: job.finishedOn === undefined ? null : new Date(job.finishedOn).toISOString(),
          // `failedReason` originates in a processor and may quote an upstream
          // error, so it is reported only while the job is actually failed.
          failedReason: state === 'failed' ? (failedReason ?? null) : null,
          result: state === 'completed' ? job.returnvalue : null,
        }

        return reply.status(200).send({ data })
      },
    )
  }
}
