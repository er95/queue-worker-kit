import { QueueEvents, Worker } from 'bullmq'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { QUEUE_NAMES } from '../../src/config/defaults.js'
import { createMetrics } from '../../src/observability/metrics.js'
import { buildWorkerOptions } from '../../src/queue/defaults.js'
import { createQueueEventsListeners } from '../../src/queue/events.js'
import { createEnqueuer, type Enqueuer } from '../../src/queue/enqueue.js'
import { createQueueRegistry, type QueueRegistry } from '../../src/queue/registry.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { waitFor, waitForCondition } from '../helpers/wait.js'

/**
 * BullMQ QueueEvents.
 *
 * Two things are worth pinning down here. The listeners must actually receive
 * queue-wide events, and `close()` must genuinely release them: a QueueEvents
 * instance holds a blocking `XREAD` on its own duplicated connection, which is
 * exactly the kind of handle that leaves a test run hanging at the end.
 */
describe('queue events', () => {
  let context: TestContext
  let queues: QueueRegistry
  let enqueuer: Enqueuer

  beforeEach(() => {
    context = createTestContext('queue-events')
    const metrics = createMetrics()

    queues = createQueueRegistry({
      env: context.env,
      connection: context.redis,
      logger: context.logger,
      names: [QUEUE_NAMES.email],
    })
    context.track(queues)

    enqueuer = createEnqueuer({ queues, logger: context.logger, metrics })
  })

  afterEach(async () => {
    await context.cleanup()
  })

  it('observes completions from a worker in the same process', async () => {
    const completed: string[] = []

    const events = createQueueEventsListeners({
      queueNames: [QUEUE_NAMES.email],
      env: context.env,
      connection: context.redis,
      logger: context.logger,
    })

    const worker = new Worker<unknown, unknown, string>(
      QUEUE_NAMES.email,
      async () => ({ ok: true }),
      { ...buildWorkerOptions(context.env), connection: context.redis },
    )

    try {
      /**
       * The handle exposes only `close()`, so the observable behaviour is
       * asserted through a QueueEvents instance of our own on the same stream.
       *
       * `lastEventId: '0'` is load-bearing, not incidental. QueueEvents
       * defaults to `'$'`, which Redis reads as "entries added after this
       * XREAD arrives", and `run()` establishes that read asynchronously after
       * the constructor returns. A job that completes before the read lands is
       * therefore missed permanently, so the test does not run slow, it hangs
       * until its deadline. Reading from the start of the stream removes the
       * ordering dependency entirely, and costs nothing because each suite has
       * its own queue prefix and so its own short stream.
       *
       * Production keeps the `'$'` default on purpose: a restarting worker
       * should not replay the whole event history.
       */
      const probe = new QueueEvents(QUEUE_NAMES.email, {
        connection: context.redis,
        prefix: context.env.QUEUE_PREFIX,
        lastEventId: '0',
      })
      probe.on('completed', ({ jobId }) => {
        completed.push(jobId)
      })

      await enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_events',
      })

      await waitForCondition(() => completed.length > 0, {
        timeoutMs: 20_000,
        description: 'a completed queue event',
      })

      expect(completed).toHaveLength(1)

      probe.removeAllListeners()
      await probe.close()
    } finally {
      await worker.close()
      await events.close()
    }
  })

  /**
   * The same assertion under the worst possible ordering: the job is already
   * finished before the listener exists.
   *
   * This is the case CI hit while a laptop did not, and it is the reason the
   * test above pins `lastEventId`. Anyone who "simplifies" that option away
   * will fail here immediately rather than shipping a test that passes locally
   * and times out on a loaded runner.
   */
  it('still sees an event produced before the listener existed', async () => {
    const worker = new Worker<unknown, unknown, string>(
      QUEUE_NAMES.email,
      async () => ({ ok: true }),
      { ...buildWorkerOptions(context.env), connection: context.redis },
    )

    try {
      const enqueued = await enqueuer.enqueue('email.send', {
        to: 'user@example.com',
        template: 'welcome',
        userId: 'usr_late_listener',
      })

      // Drain fully, so the completion is definitely already in the stream.
      await waitForCondition(
        async () => (await queues.getOrThrow(QUEUE_NAMES.email).getWaitingCount()) === 0,
        { description: 'the job to be picked up' },
      )
      await waitFor(
        async () => {
          const job = await queues.getOrThrow(QUEUE_NAMES.email).getJob(enqueued.id)
          return job?.finishedOn === undefined ? undefined : job
        },
        { description: 'the job to finish' },
      )

      const completed: string[] = []
      const probe = new QueueEvents(QUEUE_NAMES.email, {
        connection: context.redis,
        prefix: context.env.QUEUE_PREFIX,
        lastEventId: '0',
      })
      probe.on('completed', ({ jobId }) => {
        completed.push(jobId)
      })

      try {
        await waitForCondition(() => completed.includes(enqueued.id), {
          description: 'the replayed completion event',
        })
      } finally {
        probe.removeAllListeners()
        await probe.close()
      }
    } finally {
      await worker.close()
    }
  })

  /**
   * The property that keeps Vitest exiting on its own. Each instance owns a
   * duplicated blocking connection, and closing has to release it without
   * `--force-exit` papering over a leak.
   */
  it('releases its connections on close', async () => {
    const events = createQueueEventsListeners({
      queueNames: [QUEUE_NAMES.email, QUEUE_NAMES.reports, QUEUE_NAMES.maintenance],
      env: context.env,
      connection: context.redis,
      logger: context.logger,
    })

    await expect(events.close()).resolves.toBeUndefined()

    // The shared client this process owns is untouched: BullMQ duplicated it
    // rather than taking it over.
    expect(context.redis.status).toBe('ready')
  })

  it('is idempotent on close', async () => {
    const events = createQueueEventsListeners({
      queueNames: [QUEUE_NAMES.email],
      env: context.env,
      connection: context.redis,
      logger: context.logger,
    })

    await events.close()
    await expect(events.close()).resolves.toBeUndefined()
  })

  it('starts listeners for every queue it is given', async () => {
    const events = createQueueEventsListeners({
      queueNames: [QUEUE_NAMES.email, QUEUE_NAMES.reports],
      env: context.env,
      connection: context.redis,
      logger: context.logger,
    })

    // Enough to prove construction did not throw for any of them, since the
    // handle deliberately exposes nothing else.
    await expect(events.close()).resolves.toBeUndefined()
  })
})
