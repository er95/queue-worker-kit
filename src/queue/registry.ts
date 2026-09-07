import type { Queue } from 'bullmq'
import type { Redis } from 'ioredis'

import type { Env } from '../config/env.js'
import type { AppLogger } from '../observability/logger.js'
import { serializeError } from '../utils/serialize-error.js'
import { createQueue } from './create-queue.js'

/**
 * The set of queues a process can talk to, fixed at bootstrap.
 *
 * This exists mainly so untrusted input can never name a queue. `new
 * Queue(request.params.queue)` would let a caller create arbitrary Redis key
 * namespaces and a connection per request; a lookup against a known set cannot.
 */
export interface QueueRegistry {
  /** `undefined` for an unknown name. Callers turn that into a 404. */
  get(name: string): Queue | undefined
  /** For internal callers that hold a name from `defaults.ts`. */
  getOrThrow(name: string): Queue
  names(): readonly string[]
  all(): readonly Queue[]
  /** Idempotent; closes every queue created here. */
  close(): Promise<void>
}

export interface CreateQueueRegistryOptions {
  env: Env
  connection: Redis
  logger: AppLogger
  names: readonly string[]
}

export function createQueueRegistry(options: CreateQueueRegistryOptions): QueueRegistry {
  const { env, connection, logger, names } = options

  const queues = new Map<string, Queue>()
  for (const name of names) {
    queues.set(name, createQueue({ name, env, connection }))
  }

  // A Queue emits `error` for backend failures. Without a listener, an
  // EventEmitter 'error' event becomes an unhandled exception and takes the
  // process down over a transient Redis blip.
  for (const [name, queue] of queues) {
    queue.on('error', (error: Error) => {
      // Projected, never raw: these errors come from the Redis client and can
      // carry the failed command's arguments, including AUTH credentials.
      logger.warn({ component: 'queue', queue: name, err: serializeError(error) }, 'queue error')
    })
  }

  let closing: Promise<void> | undefined

  return {
    get(name) {
      return queues.get(name)
    },
    getOrThrow(name) {
      const queue = queues.get(name)
      if (queue === undefined) {
        throw new Error(`Queue "${name}" is not registered in this process`)
      }
      return queue
    },
    names() {
      return [...queues.keys()]
    },
    all() {
      return [...queues.values()]
    },
    close() {
      closing ??= (async () => {
        // The shared client is closed by whoever created it, not here.
        await Promise.all([...queues.values()].map((queue) => queue.close()))
      })()
      return closing
    },
  }
}
