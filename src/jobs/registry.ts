import type { Job } from 'bullmq'
import { z } from 'zod'

import type { QueueName } from '../config/defaults.js'
import { PayloadValidationError } from '../errors/errors.js'
import type { AppLogger } from '../observability/logger.js'
import { cleanupJob, createCleanupProcessor } from './definitions/cleanup.job.js'
import {
  createGenerateReportProcessor,
  generateReportJob,
} from './definitions/generate-report.job.js'
import {
  createDemoEmailProvider,
  createSendEmailProcessor,
  sendEmailJob,
  type EmailProvider,
} from './definitions/send-email.job.js'
import {
  jobMetaSchema,
  type JobDefinition,
  type JobEnvelope,
  type JobMeta,
  type JobProcessor,
} from './types.js'

/**
 * The closed set of jobs this application knows how to run.
 *
 * A plain object rather than a mutable global registry buys two things: job
 * names become a union type instead of `string`, and nothing can register a job
 * as an import side effect.
 */
export const jobDefinitions = {
  [sendEmailJob.name]: sendEmailJob,
  [generateReportJob.name]: generateReportJob,
  [cleanupJob.name]: cleanupJob,
} as const

export type JobName = keyof typeof jobDefinitions

/** Payload type for a job name: `PayloadOf<'email.send'>` is `SendEmailPayload`. */
export type PayloadOf<TName extends JobName> = z.output<(typeof jobDefinitions)[TName]['schema']>

/** Result type for a job name, carried by the definition's phantom `__result`. */
export type ResultOf<TName extends JobName> = NonNullable<
  (typeof jobDefinitions)[TName]['__result']
>

export const JOB_NAMES = Object.keys(jobDefinitions) as JobName[]

export function getJobDefinition<TName extends JobName>(
  name: TName,
): (typeof jobDefinitions)[TName] {
  return jobDefinitions[name]
}

export function isJobName(name: string): name is JobName {
  return Object.hasOwn(jobDefinitions, name)
}

/** Queue a given job lands on. */
export function queueForJob(name: JobName): QueueName {
  return jobDefinitions[name].queue
}

/**
 * Definition metadata with its type parameters erased. Deliberately omits
 * `schema`, which is the only field that would drag the payload type back in.
 */
export type AnyJobDefinition = Pick<
  JobDefinition<string, z.ZodType, unknown>,
  'name' | 'queue' | 'defaultJobOptions' | 'timeoutMs'
>

/** Everything the worker must supply to run a job, once its payload has parsed. */
export interface JobInvocationContext {
  readonly logger: AppLogger
  readonly signal: AbortSignal
  readonly attempt: number
}

export interface JobInvocationResult {
  readonly meta: JobMeta
  readonly result: unknown
}

/**
 * A definition bound to the function that runs it.
 *
 * Validation and execution sit behind a single `invoke` so the payload type
 * stays inside the closure that created it. A registry of
 * `RegisteredJob<TPayload>` values cannot be stored in one collection without
 * an unsound cast, because a processor accepting `TPayload` is not a processor
 * accepting `unknown`; hiding the type parameter avoids needing one.
 */
export interface RegisteredJob {
  readonly definition: AnyJobDefinition
  /**
   * Validates `job.data` as an envelope, then runs the processor.
   *
   * `onValidated` is called between those two steps, with the metadata the
   * caller needs in order to build a correlation-scoped logger before any of
   * the handler's own logs are emitted.
   *
   * @throws PayloadValidationError when the stored payload does not match the
   * current schema. Non-retryable: another four attempts will not fix it.
   */
  invoke(
    job: Job<unknown, unknown, string>,
    onValidated: (meta: JobMeta) => JobInvocationContext,
  ): Promise<JobInvocationResult>
}

export interface JobRegistry {
  /** `undefined` when a job name found in Redis has no definition in this build. */
  get(name: string): RegisteredJob | undefined
  names(): readonly string[]
  /** Job names handled by a given queue. */
  namesForQueue(queue: string): readonly string[]
}

export interface CreateJobRegistryOptions {
  /** Swappable so tests, and real deployments, can supply their own provider. */
  emailProvider?: EmailProvider
}

/**
 * Outer shape of a job envelope, with the payload left opaque.
 *
 * Validation is two-step, and deliberately so: `z.object({ payload:
 * definition.schema, meta })` cannot be resolved while `TSchema` is still
 * generic, because zod decides whether a key is optional from the schema's
 * output type. Splitting it keeps the payload strongly typed with no cast, and
 * yields better messages, since a bad envelope and a bad payload are reported
 * as different problems.
 */
const envelopeShapeSchema = z.object({
  payload: z.unknown(),
  meta: jobMetaSchema,
})

function issuesOf(error: z.ZodError, prefix = ''): string[] {
  return error.issues.map((issue) => {
    const path = issue.path.join('.')
    const full = prefix.length > 0 && path.length > 0 ? `${prefix}.${path}` : prefix + path
    return `${full.length > 0 ? full : '(root)'}: ${issue.message}`
  })
}

function register<TName extends string, TSchema extends z.ZodType, TResult>(
  definition: JobDefinition<TName, TSchema, TResult>,
  process: JobProcessor<z.output<TSchema>, TResult>,
): RegisteredJob {
  return {
    definition,

    async invoke(job, onValidated) {
      const envelope = envelopeShapeSchema.safeParse(job.data)
      if (!envelope.success) {
        throw new PayloadValidationError(
          `Invalid job envelope for "${definition.name}"`,
          issuesOf(envelope.error),
        )
      }

      const parsedPayload = definition.schema.safeParse(envelope.data.payload)
      if (!parsedPayload.success) {
        throw new PayloadValidationError(
          `Invalid payload for job "${definition.name}"`,
          issuesOf(parsedPayload.error, 'payload'),
        )
      }

      const payload: z.output<TSchema> = parsedPayload.data
      const meta: JobMeta = envelope.data.meta

      const { logger, signal, attempt } = onValidated(meta)

      const result = await process({
        // The only cast in the hot path. `job` arrives from BullMQ typed as
        // `Job<unknown>` because Redis hands back untyped JSON; the two parses
        // above are what actually establish the shape, at runtime.
        job: job as Job<JobEnvelope<z.output<TSchema>>, unknown, string>,
        data: payload,
        logger,
        signal,
        correlationId: meta.correlationId,
        attempt,
      })

      return { meta, result }
    },
  }
}

export function createJobRegistry(options: CreateJobRegistryOptions = {}): JobRegistry {
  const emailProvider = options.emailProvider ?? createDemoEmailProvider()

  const entries: readonly RegisteredJob[] = [
    register(sendEmailJob, createSendEmailProcessor(emailProvider)),
    register(generateReportJob, createGenerateReportProcessor()),
    register(cleanupJob, createCleanupProcessor()),
  ]

  const byName = new Map<string, RegisteredJob>(
    entries.map((entry) => [entry.definition.name, entry]),
  )

  return {
    get(name) {
      return byName.get(name)
    },
    names() {
      return [...byName.keys()]
    },
    namesForQueue(queue) {
      return entries
        .filter((entry) => entry.definition.queue === queue)
        .map((entry) => entry.definition.name)
    },
  }
}
