import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client'

import { JOB_DURATION_BUCKETS_SECONDS, METRIC_PREFIX } from '../config/defaults.js'

/**
 * Label sets are intentionally tiny.
 *
 * `queue` and `job_name` come from a closed registry of definitions, so their
 * cardinality is bounded by the codebase. Job ids, correlation ids and user
 * ids are deliberately absent: each would mint a new time series per job and
 * eventually take Prometheus down with it.
 */
export type JobLabels = 'queue' | 'job_name'

export interface Metrics {
  readonly registry: Registry
  readonly jobsEnqueued: Counter<JobLabels>
  readonly jobsStarted: Counter<JobLabels>
  readonly jobsCompleted: Counter<JobLabels>
  readonly jobsFailed: Counter<JobLabels>
  readonly jobsRetrying: Counter<JobLabels>
  readonly jobsDeadLettered: Counter<JobLabels>
  readonly jobsStalled: Counter<'queue'>
  readonly deadLetterWriteFailures: Counter<'queue'>
  readonly jobDuration: Histogram<JobLabels>
  /** Sampled on scrape via the collector below. */
  readonly queueDepth: Gauge<'queue' | 'state'>
  /** Registers a scrape-time collector for queue depth. */
  readonly registerQueueDepthCollector: (collect: QueueDepthCollector) => void
}

export type QueueDepthCollector = (report: QueueDepthReporter) => Promise<void>

export interface QueueDepthReporter {
  set(labels: { queue: string; state: string }, value: number): void
}

export interface CreateMetricsOptions {
  /**
   * Prometheus' default registry is process-global, which makes it a liability
   * in tests and under hot reload: registering the same metric twice throws.
   * Every call here gets a fresh registry unless one is supplied explicitly.
   */
  registry?: Registry
  /** Node process/GC metrics. Off by default so tests stay cheap. */
  collectDefaults?: boolean
}

export function createMetrics(options: CreateMetricsOptions = {}): Metrics {
  const registry = options.registry ?? new Registry()

  if (options.collectDefaults ?? false) {
    collectDefaultMetrics({ register: registry })
  }

  const jobLabelNames = ['queue', 'job_name'] as const

  const counter = <T extends string>(name: string, help: string, labelNames: readonly T[]) =>
    new Counter<T>({
      name: `${METRIC_PREFIX}${name}`,
      help,
      labelNames: [...labelNames],
      registers: [registry],
    })

  // Set by `registerQueueDepthCollector`. The gauge's `collect` hook has to be
  // supplied at construction time, so it delegates through this instead.
  let queueDepthCollector: QueueDepthCollector | undefined

  const queueDepth = new Gauge<'queue' | 'state'>({
    name: `${METRIC_PREFIX}queue_jobs`,
    help: 'Jobs currently in a given queue, by state, sampled at scrape time.',
    labelNames: ['queue', 'state'],
    registers: [registry],
    // prom-client invokes this on every scrape, which is the only safe place to
    // reach out to Redis for a gauge like this. Resetting first drops label
    // combinations that no longer exist.
    collect: async function collectQueueDepth() {
      if (queueDepthCollector === undefined) return
      this.reset()
      await queueDepthCollector({
        set: (labels, value) => {
          this.set(labels, value)
        },
      })
    },
  })

  return {
    registry,

    jobsEnqueued: counter(
      'jobs_enqueued_total',
      'Jobs accepted and written to a queue.',
      jobLabelNames,
    ),
    jobsStarted: counter('jobs_started_total', 'Job processing attempts started.', jobLabelNames),
    jobsCompleted: counter(
      'jobs_completed_total',
      'Job processing attempts that succeeded.',
      jobLabelNames,
    ),
    jobsFailed: counter('jobs_failed_total', 'Job processing attempts that threw.', jobLabelNames),
    jobsRetrying: counter(
      'jobs_retrying_total',
      'Failed attempts that will be retried (attempts remain and the error is retryable).',
      jobLabelNames,
    ),
    jobsDeadLettered: counter(
      'jobs_dead_lettered_total',
      'Jobs whose retries were exhausted and were recorded in the dead-letter queue.',
      jobLabelNames,
    ),
    jobsStalled: counter('jobs_stalled_total', 'Jobs observed as stalled by this worker.', [
      'queue',
    ] as const),
    deadLetterWriteFailures: counter(
      'dead_letter_write_failures_total',
      'Dead-letter records that could not be written. Alert on any increase.',
      ['queue'] as const,
    ),

    jobDuration: new Histogram<JobLabels>({
      name: `${METRIC_PREFIX}job_duration_seconds`,
      help: 'Wall-clock duration of a single job processing attempt.',
      labelNames: [...jobLabelNames],
      buckets: [...JOB_DURATION_BUCKETS_SECONDS],
      registers: [registry],
    }),

    queueDepth,

    registerQueueDepthCollector(collect) {
      queueDepthCollector = collect
    },
  }
}
