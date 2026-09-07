import type { Job, JobsOptions } from 'bullmq'
import { z } from 'zod'

import type { QueueName } from '../config/defaults.js'
import type { AppLogger } from '../observability/logger.js'

/**
 * A job definition is the single source of truth for one kind of work: its
 * canonical name, the queue it lands on, and the schema its payload must
 * satisfy. Producers and consumers both read from it, so a payload cannot drift
 * between the side that writes it and the side that runs it.
 */

/** Options a definition is allowed to pin. Excludes anything per-call (`delay`, `jobId`). */
export type JobDefaultOptions = Pick<
  JobsOptions,
  'attempts' | 'backoff' | 'priority' | 'removeOnComplete' | 'removeOnFail'
>

export interface JobDefinition<TName extends string, TSchema extends z.ZodType, TResult> {
  readonly name: TName
  readonly queue: QueueName
  /**
   * Generic over the schema rather than over the payload. Zod 4's `ZodType` is
   * effectively invariant in its output, so a `ZodObject` is not assignable to
   * `ZodType<ThatObject>`; carrying the schema type lets `z.output<>` do the
   * work at every use site instead.
   */
  readonly schema: TSchema
  /** Merged over the queue-wide defaults from `queue/defaults.ts`. */
  readonly defaultJobOptions?: JobDefaultOptions
  /** Overrides `JOB_TIMEOUT_MS` for this job only. */
  readonly timeoutMs?: number
  /**
   * Phantom field. Carries the result type so `ResultOf<'email.send'>` works
   * without every call site restating it; it is never read at runtime.
   */
  readonly __result?: TResult
}

/**
 * Identity helper, for inference only: it pins `TName` as a literal and lets
 * the payload type be read off the schema instead of restated.
 */
export function defineJob<TName extends string, TSchema extends z.ZodType, TResult>(
  definition: JobDefinition<TName, TSchema, TResult>,
): JobDefinition<TName, TSchema, TResult> {
  return definition
}

/** Payload type of a definition. */
export type PayloadOfDefinition<TDefinition> =
  TDefinition extends JobDefinition<string, infer TSchema, unknown> ? z.output<TSchema> : never

/**
 * What actually travels through Redis.
 *
 * Payload and transport metadata are kept apart so a correlation id never has
 * to be smuggled into a business payload, and so metadata can gain fields
 * without touching any job's schema.
 */
export const jobMetaSchema = z.object({
  correlationId: z.string().min(1).max(200),
  enqueuedAt: z.iso.datetime(),
  idempotencyKey: z.string().min(1).max(200).optional(),
})

export type JobMeta = z.output<typeof jobMetaSchema>

export interface JobEnvelope<TPayload> {
  payload: TPayload
  meta: JobMeta
}

/**
 * Envelope schema for a given payload schema, used for worker-side validation.
 *
 * The return type is inferred rather than annotated as
 * `ZodType<JobEnvelope<...>>`, which would need an unsound cast to satisfy: the
 * inferred `ZodObject` output is structurally a `JobEnvelope` already.
 */
export function envelopeSchema<TSchema extends z.ZodType>(payload: TSchema) {
  return z.object({
    payload,
    meta: jobMetaSchema,
  })
}

/**
 * What a processor is handed. The logger already carries queue, jobId, jobName,
 * correlationId and attempt, so handlers log a message and nothing else.
 */
export interface ProcessorContext<TPayload> {
  readonly job: Job<JobEnvelope<TPayload>, unknown, string>
  readonly data: TPayload
  readonly logger: AppLogger
  /**
   * Aborts on the job timeout, on worker shutdown past its deadline, and when
   * BullMQ loses the job's lock. Cooperative: pass it to `fetch` and to any
   * sleep, and check it between stages.
   */
  readonly signal: AbortSignal
  readonly correlationId: string
  /** 1 on the first run. */
  readonly attempt: number
}

export type JobProcessor<TPayload, TResult> = (
  context: ProcessorContext<TPayload>,
) => Promise<TResult>
