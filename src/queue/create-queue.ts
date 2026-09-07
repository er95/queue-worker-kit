import { Queue, type QueueOptions } from 'bullmq'
import type { Redis } from 'ioredis'

import type { Env } from '../config/env.js'
import { QUEUE_STREAM_OPTIONS, buildDefaultJobOptions } from './defaults.js'

export interface CreateQueueOptions {
  name: string
  env: Env
  /**
   * A client this process owns. Handing BullMQ an instance keeps the number of
   * Redis connections proportional to processes rather than to queues.
   */
  connection: Redis
}

export function createQueue(options: CreateQueueOptions): Queue {
  const { name, env, connection } = options

  const queueOptions: QueueOptions = {
    connection,
    prefix: env.QUEUE_PREFIX,
    defaultJobOptions: buildDefaultJobOptions(env),
    streams: { events: { maxLen: QUEUE_STREAM_OPTIONS.events.maxLen } },
  }

  return new Queue(name, queueOptions)
}
