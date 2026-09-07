import type { DefaultJobOptions, RateLimiterOptions, WorkerOptions } from 'bullmq'

import {
  JOB_PAYLOAD_SIZE_LIMIT_BYTES,
  QUEUE_EVENTS_STREAM_MAX_LEN,
  WORKER_LOCK_DURATION_MS,
  WORKER_MAX_STALLED_COUNT,
  WORKER_STALLED_INTERVAL_MS,
} from '../config/defaults.js'
import type { Env } from '../config/env.js'

/**
 * Every queue and worker in the kit is built from these two functions, so the
 * retry policy and retention policy are decided exactly once.
 */

export function buildDefaultJobOptions(env: Env): DefaultJobOptions {
  return {
    attempts: env.JOB_ATTEMPTS,
    // BullMQ's native exponential backoff: delay * 2^(attemptsMade - 1).
    // With the default 2000ms that is 2s, 4s, 8s, 16s between five attempts.
    backoff: { type: 'exponential', delay: env.JOB_BACKOFF_DELAY_MS },
    // Retention is a Redis memory decision. Unbounded completed/failed sets are
    // the most common way a BullMQ deployment runs its Redis out of memory.
    removeOnComplete: { count: env.JOB_REMOVE_ON_COMPLETE_COUNT },
    removeOnFail: { count: env.JOB_REMOVE_ON_FAIL_COUNT },
    sizeLimit: JOB_PAYLOAD_SIZE_LIMIT_BYTES,
  }
}

export const QUEUE_STREAM_OPTIONS = {
  events: { maxLen: QUEUE_EVENTS_STREAM_MAX_LEN },
} as const

export function buildRateLimiterOptions(env: Env): RateLimiterOptions {
  return {
    max: env.WORKER_RATE_LIMIT_MAX,
    duration: env.WORKER_RATE_LIMIT_DURATION_MS,
  }
}

/**
 * Note that `concurrency` and `limiter` are both *per worker process*. Three
 * replicas with concurrency 10 will run up to 30 jobs at once, and each gets
 * its own rate-limit budget. Cluster-wide ceilings need
 * `queue.setGlobalConcurrency()` / `queue.setGlobalRateLimit()`.
 */
export function buildWorkerOptions(env: Env): Omit<WorkerOptions, 'connection'> {
  return {
    prefix: env.QUEUE_PREFIX,
    concurrency: env.WORKER_CONCURRENCY,
    limiter: buildRateLimiterOptions(env),
    lockDuration: WORKER_LOCK_DURATION_MS,
    stalledInterval: WORKER_STALLED_INTERVAL_MS,
    maxStalledCount: WORKER_MAX_STALLED_COUNT,
    removeOnComplete: { count: env.JOB_REMOVE_ON_COMPLETE_COUNT },
    removeOnFail: { count: env.JOB_REMOVE_ON_FAIL_COUNT },
  }
}
