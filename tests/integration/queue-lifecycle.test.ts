import type { Job } from 'bullmq'
import { Worker } from 'bullmq'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { QUEUE_NAMES } from '../../src/config/defaults.js'
import { PayloadValidationError } from '../../src/errors/errors.js'
import { createJobRegistry } from '../../src/jobs/registry.js'
import { createMetrics, type Metrics } from '../../src/observability/metrics.js'
import { buildWorkerOptions } from '../../src/queue/defaults.js'
import { createEnqueuer, type Enqueuer } from '../../src/queue/enqueue.js'
import { createQueueRegistry, type QueueRegistry } from '../../src/queue/registry.js'
import { createWorker } from '../../src/worker/create-worker.js'
import { createDeadLetterWriter, type DeadLetterWriter } from '../../src/worker/dead-letter.js'
import { createJobProcessor } from '../../src/worker/processor.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { captureError, waitFor, waitForCondition, waitForFinishedJob } from '../helpers/wait.js'

/**
 * End-to-end behaviour against a real Redis.
 *
 * Redis is not mocked here on purpose: every property under test (backoff
 * timing, deduplication TTLs, delayed-set promotion, terminal-failure
 * bookkeeping) lives in BullMQ's Lua scripts, and a mock would only assert that
 * the mock behaves as assumed.
 */

interface Harness {
  context: TestContext
  queues: QueueRegistry
  enqueuer: Enqueuer
  metrics: Metrics
  deadLetter: DeadLetterWriter
}

function setup(overrides: Record<string, string | undefined> = {}): Harness {
  const context = createTestContext('lifecycle', overrides)
  const metrics = createMetrics()

  const queues = createQueueRegistry({
    env: context.env,
    connection: context.redis,
    logger: context.logger,
    names: [
      QUEUE_NAMES.email,
      QUEUE_NAMES.reports,
      QUEUE_NAMES.maintenance,
      QUEUE_NAMES.deadLetter,
    ],
  })
  context.track(queues)

  const deadLetter = createDeadLetterWriter({
    queue: queues.getOrThrow(QUEUE_NAMES.deadLetter),
    logger: context.logger,
    metrics,
    maxEntries: context.env.DEAD_LETTER_MAX_ENTRIES,
  })

  const enqueuer = createEnqueuer({ queues, logger: context.logger, metrics })

  return { context, queues, enqueuer, metrics, deadLetter }
}

/**
 * A worker running a test-specific processor.
 *
 * Failure behaviour is injected here rather than through a `failTimes` field on
 * a production payload: the demo jobs stay realistic, and the retry mechanics
 * are still tested against the real BullMQ machinery.
 */
function startTestWorker(
  harness: Harness,
  queueName: string,
  processor: (job: Job<unknown, unknown, string>) => Promise<unknown>,
): Worker<unknown, unknown, string> {
  const worker = new Worker<unknown, unknown, string>(queueName, async (job) => processor(job), {
    ...buildWorkerOptions(harness.context.env),
    connection: harness.context.redis,
  })
  harness.context.track(worker)
  return worker
}

describe('queue lifecycle', () => {
  let harness: Harness

  beforeEach(() => {
    harness = setup()
  })

  afterEach(async () => {
    await harness.context.cleanup()
  })

  // Test 1: enqueue and process.
  describe('enqueue and process', () => {
    it('runs a job through the real worker and exposes its result', async () => {
      const registry = createJobRegistry()
      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      const enqueued = await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_123',
      })

      expect(enqueued).toMatchObject({ name: 'email.send', queue: QUEUE_NAMES.email })
      expect(enqueued.id).toBeTruthy()

      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      const job = await waitForFinishedJob(() => queue.getJob(enqueued.id), {
        description: 'the email job to complete',
      })

      expect(job.returnvalue).toMatchObject({ delivered: true, template: 'welcome' })
      expect(job.returnvalue).toHaveProperty('providerMessageId')
    })

    it('stamps a correlation id onto the job and propagates an upstream one', async () => {
      const enqueued = await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_1' },
        { correlationId: 'trace-from-upstream' },
      )

      const job = await harness.queues.getOrThrow(QUEUE_NAMES.email).getJob(enqueued.id)
      const data = job?.data as { meta: { correlationId: string; enqueuedAt: string } }

      expect(data.meta.correlationId).toBe('trace-from-upstream')
      expect(Date.parse(data.meta.enqueuedAt)).not.toBeNaN()
    })

    it('generates a correlation id when the caller supplies none', async () => {
      const enqueued = await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_1',
      })

      const job = await harness.queues.getOrThrow(QUEUE_NAMES.email).getJob(enqueued.id)
      const data = job?.data as { meta: { correlationId: string } }

      expect(data.meta.correlationId).toHaveLength(36)
    })

    it('applies the configured retry defaults to every job', async () => {
      const enqueued = await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_1',
      })

      const job = await harness.queues.getOrThrow(QUEUE_NAMES.email).getJob(enqueued.id)

      expect(job?.opts.attempts).toBe(5)
      expect(job?.opts.backoff).toMatchObject({ type: 'exponential', delay: 2000 })
    })

    it('lets a job definition override the shared attempt count', async () => {
      const enqueued = await harness.enqueuer.enqueue('report.generate', {
        reportId: 'r-1',
        requestedBy: 'ops',
        format: 'csv',
      })

      const job = await harness.queues.getOrThrow(QUEUE_NAMES.reports).getJob(enqueued.id)

      expect(job?.opts.attempts).toBe(3)
    })

    it('reports structured progress from a long-running job', async () => {
      const registry = createJobRegistry()
      const worker = createWorker({
        queueName: QUEUE_NAMES.reports,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      const enqueued = await harness.enqueuer.enqueue('report.generate', {
        reportId: 'r-progress',
        requestedBy: 'ops',
        format: 'json',
      })

      const queue = harness.queues.getOrThrow(QUEUE_NAMES.reports)
      const job = await waitForFinishedJob(() => queue.getJob(enqueued.id), {
        description: 'the report job to complete',
      })

      expect(job.progress).toMatchObject({ percent: 100, stage: 'done' })
      expect(job.returnvalue).toMatchObject({ reportId: 'r-progress', generatedRows: 2500 })
    })
  })

  // Test 2: validation happens before Redis.
  describe('validation', () => {
    it('rejects an invalid payload before it reaches Redis', async () => {
      await expect(
        harness.enqueuer.enqueue('email.send', {
          to: 'not-an-email',
          template: 'welcome',
          userId: 'usr_1',
        }),
      ).rejects.toThrow(PayloadValidationError)

      // The point of validating first: nothing was written, so nothing can
      // fail five times and land in the dead-letter queue.
      expect(await harness.queues.getOrThrow(QUEUE_NAMES.email).getJobCounts('waiting')).toEqual({
        waiting: 0,
      })
    })

    it('surfaces the offending field', async () => {
      const error = await captureError<PayloadValidationError>(
        harness.enqueuer.enqueue('email.send', {
          to: 'nope',
          template: 'welcome',
          userId: 'usr_1',
        }),
      )

      expect(error.issues.join(' ')).toContain('to')
    })

    it('rejects an out-of-range delay', async () => {
      await expect(
        harness.enqueuer.enqueue(
          'email.send',
          { to: 'user@example.com', template: 'welcome', userId: 'usr_1' },
          { delay: 999_999_999_999 },
        ),
      ).rejects.toThrow(/delay must be an integer/)
    })

    /**
     * Worker-side validation is the second line of defence: a payload written
     * by an older build can already be sitting in Redis, and must fail
     * permanently rather than being retried five times.
     */
    it('fails a malformed job already in Redis without retrying it', async () => {
      const registry = createJobRegistry()
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      // Bypasses `enqueue`, exactly as a stale producer would.
      const job = await queue.add('email.send', { payload: { to: 'garbage' }, meta: {} })

      const failed = await waitForFinishedJob(() => queue.getJob(job.id!), {
        description: 'the malformed job to fail',
      })

      expect(failed.failedReason).toMatch(/Invalid job envelope|Invalid payload/)
      // One attempt, not five: the error is non-retryable.
      expect(failed.attemptsMade).toBe(1)
    })
  })

  // Test 3: retries with backoff, then success.
  describe('retries', () => {
    it('retries a transient failure and succeeds on a later attempt', async () => {
      const attempts: number[] = []
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      startTestWorker(harness, QUEUE_NAMES.email, async (job) => {
        attempts.push(job.attemptsMade + 1)
        if (job.attemptsMade < 2) {
          throw new Error('upstream 502')
        }
        return { ok: true, onAttempt: job.attemptsMade + 1 }
      })

      const enqueued = await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_retry' },
        // A short backoff keeps the test quick while still exercising the real
        // exponential-backoff path rather than an immediate re-fetch.
        { attempts: 5 },
      )

      const job = await waitForFinishedJob(() => queue.getJob(enqueued.id), {
        timeoutMs: 30_000,
        description: 'the job to succeed after retrying',
      })

      expect(attempts).toEqual([1, 2, 3])
      expect(job.attemptsMade).toBe(3)
      expect(job.returnvalue).toMatchObject({ ok: true, onAttempt: 3 })
    })

    it('waits longer between successive attempts', async () => {
      const timestamps: number[] = []
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      startTestWorker(harness, QUEUE_NAMES.email, async () => {
        timestamps.push(Date.now())
        throw new Error('always fails')
      })

      const enqueued = await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_backoff' },
        { attempts: 3 },
      )

      await waitForCondition(async () => timestamps.length >= 3, {
        timeoutMs: 30_000,
        description: 'three attempts',
      })

      const firstGap = (timestamps[1] ?? 0) - (timestamps[0] ?? 0)
      const secondGap = (timestamps[2] ?? 0) - (timestamps[1] ?? 0)

      // Exponential, so 2s then 4s. Compared loosely: the assertion is about
      // the shape of the curve, not the exact millisecond.
      expect(firstGap).toBeGreaterThan(1_000)
      expect(secondGap).toBeGreaterThan(firstGap)

      await queue.getJob(enqueued.id)
    })

    it('does not retry a non-retryable error', async () => {
      const runs = vi.fn()
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      const registry = createJobRegistry()
      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      worker.on('failed', () => runs())
      harness.context.track(worker)

      // An unknown template is permanent, so BullMQ must stop after one attempt
      // even though four remain.
      const enqueued = await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'no-such-template',
        userId: 'usr_1',
      })

      const failed = await waitForFinishedJob(() => queue.getJob(enqueued.id), {
        description: 'the job to fail permanently',
      })

      expect(failed.attemptsMade).toBe(1)
      expect(failed.opts.attempts).toBe(5)
      expect(failed.failedReason).toMatch(/Unknown email template/)
    })
  })

  // Test 4: terminal failure reaches the dead-letter queue.
  describe('dead-letter queue', () => {
    it('records a terminal failure with everything needed to investigate it', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)

      const registry = createJobRegistry()
      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      const enqueued = await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'no-such-template', userId: 'usr_dlq' },
        { correlationId: 'trace-dlq' },
      )

      const records = await waitFor(
        async () => {
          const waiting = await dlq.getWaiting(0, 10)
          return waiting.length > 0 ? waiting : undefined
        },
        { description: 'a dead-letter record' },
      )

      expect(records).toHaveLength(1)

      const record = records[0]?.data as Record<string, unknown>

      expect(record).toMatchObject({
        sourceQueue: QUEUE_NAMES.email,
        sourceJobId: enqueued.id,
        sourceJobName: 'email.send',
        correlationId: 'trace-dlq',
        attemptsMade: 1,
      })
      expect(record.failedReason).toMatch(/Unknown email template/)
      expect(record.payload).toMatchObject({
        payload: { to: 'user@example.com', userId: 'usr_dlq' },
      })
      expect(Date.parse(String(record.failedAt))).not.toBeNaN()
      expect((record.error as { stack?: string }).stack).toBeTruthy()

      await queue.getJob(enqueued.id)
    })

    it('dead-letters only after every attempt is exhausted', async () => {
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)
      const failures: number[] = []

      const worker = startTestWorker(harness, QUEUE_NAMES.email, async () => {
        throw new Error('always fails')
      })
      worker.on('failed', (job) => {
        if (job !== undefined) failures.push(job.attemptsMade)
      })

      // The writer is wired to the worker's `failed` event by `createWorker`,
      // so a hand-built worker needs it connected explicitly.
      worker.on('failed', (job, error) => {
        if (job !== undefined && job.attemptsMade >= (job.opts.attempts ?? 1)) {
          void harness.deadLetter.record({
            sourceQueue: QUEUE_NAMES.email,
            job,
            error,
            correlationId: 'trace-exhaust',
            attemptsMade: job.attemptsMade,
          })
        }
      })

      await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_exhaust' },
        { attempts: 2 },
      )

      await waitForCondition(async () => (await dlq.getWaitingCount()) > 0, {
        timeoutMs: 30_000,
        description: 'the dead-letter record',
      })

      // Two failures, one record: intermediate failures are retried silently.
      expect(failures).toEqual([1, 2])
      expect(await dlq.getWaitingCount()).toBe(1)
    })

    /**
     * Terminal detection asks BullMQ (via `finishedOn`) rather than
     * recomputing its retry rule, so a job failed early by an
     * `UnrecoverableError` is dead-lettered even though four attempts remain,
     * and the intermediate failures of a retrying job are not.
     */
    it('records exactly one entry for a retrying job that finally fails', async () => {
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)
      const attempts: number[] = []

      const registry = createJobRegistry({
        emailProvider: {
          send: () => {
            attempts.push(attempts.length + 1)
            return Promise.reject(new Error('provider is down'))
          },
        },
      })

      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_terminal' },
        { attempts: 3 },
      )

      await waitForCondition(async () => (await dlq.getWaitingCount()) > 0, {
        timeoutMs: 30_000,
        description: 'the dead-letter record',
      })

      // Three attempts, one record: the first two failures were retried and
      // must not have produced entries of their own.
      expect(attempts).toHaveLength(3)
      expect(await dlq.getWaitingCount()).toBe(1)

      const [record] = await dlq.getWaiting(0, 5)
      expect(record?.data).toMatchObject({ attemptsMade: 3, sourceJobName: 'email.send' })
    })

    /**
     * The structural guarantee against a dead-letter loop. Nothing consumes the
     * dead-letter queue, and the writer refuses a job that came from it, so
     * there is no path back into itself.
     */
    it('refuses to dead-letter a job originating in the dead-letter queue', async () => {
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)

      await harness.deadLetter.record({
        sourceQueue: QUEUE_NAMES.deadLetter,
        job: {
          id: 'dlq-1',
          name: 'dead-letter.record',
          data: {},
          attemptsMade: 1,
          opts: {},
        } as Job<unknown, unknown, string>,
        error: new Error('nested failure'),
        correlationId: 'trace-loop',
        attemptsMade: 1,
      })

      expect(await dlq.getWaitingCount()).toBe(0)
    })

    it('collapses duplicate writes for the same terminal failure', async () => {
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)

      const job = {
        id: '42',
        name: 'email.send',
        data: { payload: {}, meta: {} },
        attemptsMade: 5,
        opts: { attempts: 5 },
        finishedOn: Date.now(),
      } as Job<unknown, unknown, string>

      const input = {
        sourceQueue: QUEUE_NAMES.email,
        job,
        error: new Error('boom'),
        correlationId: 'trace-dup',
        attemptsMade: 5,
      }

      await harness.deadLetter.record(input)
      await harness.deadLetter.record(input)
      await harness.deadLetter.record(input)

      // The record's job id is derived from the failure, so Redis rejects the
      // repeats rather than storing three copies.
      expect(await dlq.getWaitingCount()).toBe(1)
    })

    it('keeps the queue bounded by evicting the oldest records', async () => {
      const bounded = setup({ DEAD_LETTER_MAX_ENTRIES: '3' })
      const dlq = bounded.queues.getOrThrow(QUEUE_NAMES.deadLetter)

      try {
        for (let i = 0; i < 6; i += 1) {
          await bounded.deadLetter.record({
            sourceQueue: QUEUE_NAMES.email,
            job: {
              id: String(i),
              name: 'email.send',
              data: { payload: { seq: i }, meta: {} },
              attemptsMade: 1,
              opts: {},
              finishedOn: Date.now(),
            } as Job<unknown, unknown, string>,
            error: new Error(`failure ${i}`),
            correlationId: `trace-${i}`,
            attemptsMade: 1,
          })
        }

        // Nothing consumes this queue, so `removeOnFail` retention never
        // applies and it would otherwise be the one unbounded structure.
        expect(await dlq.getWaitingCount()).toBe(3)

        const remaining = await dlq.getWaiting(0, 10)
        const sequences = remaining.map(
          (entry) => (entry.data as { payload: { payload: { seq: number } } }).payload.payload.seq,
        )
        expect(sequences.sort((a, b) => a - b)).toEqual([3, 4, 5])
      } finally {
        await bounded.context.cleanup()
      }
    })

    // The original job has already failed; failing the writer too would only
    // take the worker down with it.
    it('never throws when the write itself fails', async () => {
      const broken = createDeadLetterWriter({
        queue: {
          add: () => Promise.reject(new Error('redis is gone')),
          getWaitingCount: () => Promise.resolve(0),
          getWaiting: () => Promise.resolve([]),
        } as never,
        logger: harness.context.logger,
        metrics: harness.metrics,
        maxEntries: 10,
      })

      await expect(
        broken.record({
          sourceQueue: QUEUE_NAMES.email,
          job: { id: '1', name: 'email.send', data: {}, attemptsMade: 1, opts: {} } as Job<
            unknown,
            unknown,
            string
          >,
          error: new Error('original failure'),
          correlationId: 'trace-x',
          attemptsMade: 1,
        }),
      ).resolves.toBeUndefined()

      // Swallowed, but counted, so a broken dead-letter path is alertable.
      const [sample] = (await harness.metrics.deadLetterWriteFailures.get()).values
      expect(sample?.value).toBe(1)
    })
  })

  // Test 5: delayed jobs.
  describe('delayed jobs', () => {
    it('holds a delayed job back, then runs it', async () => {
      const processed: number[] = []
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      startTestWorker(harness, QUEUE_NAMES.email, async () => {
        processed.push(Date.now())
        return { ok: true }
      })

      const enqueuedAt = Date.now()
      const enqueued = await harness.enqueuer.enqueue(
        'email.send',
        { to: 'user@example.com', template: 'welcome', userId: 'usr_delay' },
        { delay: 1_500 },
      )

      const job = await queue.getJob(enqueued.id)
      expect(await job?.getState()).toBe('delayed')
      expect(job?.opts.delay).toBe(1_500)

      await waitForCondition(async () => processed.length > 0, {
        timeoutMs: 20_000,
        description: 'the delayed job to run',
      })

      expect((processed[0] ?? 0) - enqueuedAt).toBeGreaterThanOrEqual(1_400)
    })

    it('runs an undelayed job immediately', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)

      const enqueued = await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_now',
      })

      const job = await queue.getJob(enqueued.id)
      expect(await job?.getState()).toBe('waiting')
    })
  })

  // Test 6: idempotency.
  describe('idempotency', () => {
    /**
     * The documented semantics: while the deduplication key is live, a repeat
     * enqueue creates no new job and returns the existing job's id.
     */
    it('collapses repeat enqueues sharing an idempotency key', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)
      const payload = { to: 'user@example.com', template: 'welcome', userId: 'usr_idem' } as const

      const first = await harness.enqueuer.enqueue('email.send', payload, {
        idempotencyKey: 'welcome:usr_idem',
      })
      const second = await harness.enqueuer.enqueue('email.send', payload, {
        idempotencyKey: 'welcome:usr_idem',
      })

      expect(second.id).toBe(first.id)
      expect(second.deduplicationId).toBe('welcome:usr_idem')
      expect(await queue.getWaitingCount()).toBe(1)
    })

    it('runs the handler once for a deduplicated request', async () => {
      const runs: string[] = []
      const payload = { to: 'user@example.com', template: 'welcome', userId: 'usr_once' } as const

      startTestWorker(harness, QUEUE_NAMES.email, async (job) => {
        runs.push(job.id ?? 'unknown')
        return { ok: true }
      })

      for (let i = 0; i < 4; i += 1) {
        await harness.enqueuer.enqueue('email.send', payload, {
          idempotencyKey: 'welcome:usr_once',
          idempotencyTtlMs: 10_000,
        })
      }

      await waitForCondition(async () => runs.length > 0, {
        description: 'the deduplicated job to run',
      })

      // Give any duplicate a chance to appear before asserting it did not.
      await waitForCondition(
        async () => (await harness.queues.getOrThrow(QUEUE_NAMES.email).getWaitingCount()) === 0,
        { description: 'the queue to drain' },
      )

      expect(runs).toHaveLength(1)
    })

    it('treats different keys as different work', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)
      const payload = { to: 'user@example.com', template: 'welcome', userId: 'usr_multi' } as const

      const a = await harness.enqueuer.enqueue('email.send', payload, { idempotencyKey: 'key-a' })
      const b = await harness.enqueuer.enqueue('email.send', payload, { idempotencyKey: 'key-b' })

      expect(a.id).not.toBe(b.id)
      expect(await queue.getWaitingCount()).toBe(2)
    })

    it('does not deduplicate when no key is supplied', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)
      const payload = { to: 'user@example.com', template: 'welcome', userId: 'usr_nokey' } as const

      await harness.enqueuer.enqueue('email.send', payload)
      await harness.enqueuer.enqueue('email.send', payload)

      expect(await queue.getWaitingCount()).toBe(2)
    })

    it('allows the same key again once its TTL has expired', async () => {
      const queue = harness.queues.getOrThrow(QUEUE_NAMES.email)
      const payload = { to: 'user@example.com', template: 'welcome', userId: 'usr_ttl' } as const

      const first = await harness.enqueuer.enqueue('email.send', payload, {
        idempotencyKey: 'short-lived',
        idempotencyTtlMs: 300,
      })

      await waitForCondition(
        async () => (await queue.getDeduplicationJobId('short-lived')) === null,
        { description: 'the deduplication key to expire' },
      )

      const second = await harness.enqueuer.enqueue('email.send', payload, {
        idempotencyKey: 'short-lived',
        idempotencyTtlMs: 300,
      })

      expect(second.id).not.toBe(first.id)
    })
  })

  // Test 7: shutdown and resource cleanup.
  describe('graceful shutdown', () => {
    /**
     * The property that makes a rolling deploy safe. If `close()` did not wait,
     * the job would be abandoned mid-flight and re-run by another replica once
     * its lock expired.
     */
    it('waits for an active job to finish before closing', async () => {
      let finished = false
      let started = false

      const worker = new Worker<unknown, unknown, string>(
        QUEUE_NAMES.email,
        async () => {
          started = true
          await new Promise((resolve) => setTimeout(resolve, 600))
          finished = true
          return { ok: true }
        },
        { ...buildWorkerOptions(harness.context.env), connection: harness.context.redis },
      )

      await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_shutdown',
      })

      await waitForCondition(() => started, { description: 'the job to start' })

      await worker.close()

      expect(finished).toBe(true)
    })

    it('stops fetching new jobs once closed', async () => {
      const runs: string[] = []

      const worker = new Worker<unknown, unknown, string>(
        QUEUE_NAMES.email,
        async (job) => {
          runs.push(job.id ?? '')
          return { ok: true }
        },
        { ...buildWorkerOptions(harness.context.env), connection: harness.context.redis },
      )

      await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_first',
      })
      await waitForCondition(() => runs.length === 1, { description: 'the first job' })

      await worker.close()

      await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_second',
      })
      await new Promise((resolve) => setTimeout(resolve, 500))

      expect(runs).toHaveLength(1)
      expect(await harness.queues.getOrThrow(QUEUE_NAMES.email).getWaitingCount()).toBe(1)
    })

    it('closes repeatedly without error', async () => {
      const queues = createQueueRegistry({
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        names: [QUEUE_NAMES.email],
      })

      await queues.close()
      await expect(queues.close()).resolves.toBeUndefined()
    })

    it('drains in-flight dead-letter writes', async () => {
      const dlq = harness.queues.getOrThrow(QUEUE_NAMES.deadLetter)

      // Fired and not awaited, exactly as the worker's `failed` handler does.
      void harness.deadLetter.record({
        sourceQueue: QUEUE_NAMES.email,
        job: {
          id: 'drain-1',
          name: 'email.send',
          data: { payload: {}, meta: {} },
          attemptsMade: 1,
          opts: {},
          finishedOn: Date.now(),
        } as Job<unknown, unknown, string>,
        error: new Error('boom'),
        correlationId: 'trace-drain',
        attemptsMade: 1,
      })

      await harness.deadLetter.drain(5_000)

      // Without the drain step, shutdown could close the queue mid-write and
      // lose the record.
      expect(harness.deadLetter.pending()).toBe(0)
      expect(await dlq.getWaitingCount()).toBe(1)
    })
  })

  describe('job timeouts', () => {
    /**
     * A cooperative deadline. Node cannot terminate arbitrary async work, so
     * the guarantee is that the *attempt* fails and the concurrency slot is
     * released, not that the underlying operation stops.
     */
    it('fails an attempt that outruns its deadline', async () => {
      const timeoutHarness = setup({ JOB_TIMEOUT_MS: '400' })
      const queue = timeoutHarness.queues.getOrThrow(QUEUE_NAMES.reports)

      try {
        const registry = createJobRegistry()
        const worker = new Worker<unknown, unknown, string>(
          QUEUE_NAMES.reports,
          createJobProcessor({
            queueName: QUEUE_NAMES.reports,
            registry: {
              get: () => ({
                definition: { name: 'report.generate', queue: QUEUE_NAMES.reports },
                invoke: async (_job, onValidated) => {
                  const ctx = onValidated({
                    correlationId: 'trace-timeout',
                    enqueuedAt: new Date().toISOString(),
                  })
                  // Ignores the signal on purpose: this is the pessimistic case.
                  await new Promise((resolve) => setTimeout(resolve, 5_000))
                  return { meta: { correlationId: 'trace-timeout', enqueuedAt: '' }, result: ctx }
                },
              }),
              names: () => registry.names(),
              namesForQueue: () => [],
            },
            env: timeoutHarness.context.env,
            logger: timeoutHarness.context.logger,
            metrics: timeoutHarness.metrics,
          }),
          {
            ...buildWorkerOptions(timeoutHarness.context.env),
            connection: timeoutHarness.context.redis,
          },
        )
        timeoutHarness.context.track(worker)

        const enqueued = await timeoutHarness.enqueuer.enqueue(
          'report.generate',
          { reportId: 'r-timeout', requestedBy: 'ops', format: 'csv' },
          { attempts: 1 },
        )

        const failed = await waitForFinishedJob(() => queue.getJob(enqueued.id), {
          description: 'the job to time out',
        })

        expect(failed.failedReason).toMatch(/timeout/i)
      } finally {
        await timeoutHarness.context.cleanup()
      }
    })

    it('aborts the signal so cooperative work unwinds', async () => {
      const timeoutHarness = setup({ JOB_TIMEOUT_MS: '300' })
      const queue = timeoutHarness.queues.getOrThrow(QUEUE_NAMES.reports)
      let sawAbort = false

      try {
        const registry = createJobRegistry()
        const worker = new Worker<unknown, unknown, string>(
          QUEUE_NAMES.reports,
          createJobProcessor({
            queueName: QUEUE_NAMES.reports,
            registry: {
              get: () => ({
                definition: { name: 'report.generate', queue: QUEUE_NAMES.reports },
                invoke: async (_job, onValidated) => {
                  const { signal } = onValidated({
                    correlationId: 'trace-abort',
                    enqueuedAt: new Date().toISOString(),
                  })
                  await new Promise<void>((resolve) => {
                    signal.addEventListener(
                      'abort',
                      () => {
                        sawAbort = true
                        resolve()
                      },
                      { once: true },
                    )
                  })
                  throw new Error('aborted by signal')
                },
              }),
              names: () => registry.names(),
              namesForQueue: () => [],
            },
            env: timeoutHarness.context.env,
            logger: timeoutHarness.context.logger,
            metrics: timeoutHarness.metrics,
          }),
          {
            ...buildWorkerOptions(timeoutHarness.context.env),
            connection: timeoutHarness.context.redis,
          },
        )
        timeoutHarness.context.track(worker)

        const enqueued = await timeoutHarness.enqueuer.enqueue(
          'report.generate',
          { reportId: 'r-abort', requestedBy: 'ops', format: 'csv' },
          { attempts: 1 },
        )

        await waitForFinishedJob(() => queue.getJob(enqueued.id), {
          description: 'the aborted job to fail',
        })

        expect(sawAbort).toBe(true)
      } finally {
        await timeoutHarness.context.cleanup()
      }
    })
  })

  describe('metrics', () => {
    it('counts a job through its whole lifecycle', async () => {
      const registry = createJobRegistry()
      const worker = createWorker({
        queueName: QUEUE_NAMES.email,
        env: harness.context.env,
        connection: harness.context.redis,
        logger: harness.context.logger,
        metrics: harness.metrics,
        registry,
        deadLetter: harness.deadLetter,
      })
      harness.context.track(worker)

      await harness.enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_metrics',
      })

      await waitForCondition(
        async () => (await harness.metrics.jobsCompleted.get()).values.length > 0,
        { description: 'the completion metric' },
      )

      const labels = { queue: QUEUE_NAMES.email, job_name: 'email.send' }

      const value = async (counter: { get: () => Promise<{ values: { value: number }[] }> }) =>
        (await counter.get()).values[0]?.value

      expect(await value(harness.metrics.jobsEnqueued)).toBe(1)
      expect(await value(harness.metrics.jobsStarted)).toBe(1)
      expect(await value(harness.metrics.jobsCompleted)).toBe(1)

      const duration = (await harness.metrics.jobDuration.get()).values.find(
        (sample) => sample.metricName?.endsWith('_count') === true,
      )
      expect(duration?.value).toBe(1)
      expect(labels.queue).toBe(QUEUE_NAMES.email)
    })
  })
})
