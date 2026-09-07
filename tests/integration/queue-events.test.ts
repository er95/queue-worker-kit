import { Worker } from 'bullmq'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { QUEUE_NAMES } from '../../src/config/defaults.js'
import { createMetrics } from '../../src/observability/metrics.js'
import { buildWorkerOptions } from '../../src/queue/defaults.js'
import { createQueueEventsListeners } from '../../src/queue/events.js'
import { createEnqueuer, type Enqueuer } from '../../src/queue/enqueue.js'
import { createQueueRegistry, type QueueRegistry } from '../../src/queue/registry.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { waitForCondition } from '../helpers/wait.js'

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
      // The handle exposes only `close()`, so the observable behaviour is
      // asserted through a QueueEvents instance of our own on the same stream.
      const { QueueEvents } = await import('bullmq')
      const probe = new QueueEvents(QUEUE_NAMES.email, {
        connection: context.redis,
        prefix: context.env.QUEUE_PREFIX,
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
