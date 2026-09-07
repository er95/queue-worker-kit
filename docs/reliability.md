# Reliability

What this system actually guarantees, and what it does not. Read the second half of each section: the limitations matter more than the features.

## At-least-once delivery

**BullMQ is at-least-once. Your handlers will sometimes run more than once.**

This is not a defect to be configured away. Consider a worker that has just sent an email and is about to report success:

```
worker fetches job -> sends email -> [process killed] -> lock expires
                                                      -> stalled checker returns job to wait
                                                      -> another replica sends the email again
```

There is no way to make "perform side effect" and "record that it was performed" a single atomic operation across two systems. Every queue that survives worker crashes has this property; the ones claiming exactly-once are either lying or have moved the deduplication somewhere you cannot see.

Causes of a second run: process kill (SIGKILL, OOM, node eviction), a lock expiring because the event loop was blocked, a network partition between worker and Redis, or an ordinary retry after a failure that happened _after_ the side effect.

**Never advertise exactly-once. Design handlers that can run twice.**

## Idempotency

Two separate layers, and only one of them is the queue's job.

### Queue level: collapsing submissions

```ts
await enqueuer.enqueue('email.send', payload, {
  idempotencyKey: `welcome:${userId}`,
  idempotencyTtlMs: 24 * 60 * 60 * 1000, // the default
})
```

This maps onto BullMQ's deduplication. While the key is live, a second `enqueue` with the same key creates no new job and returns the existing job's id.

What it covers: a double-clicked button, a webhook delivered twice, a producer retrying a failed HTTP call.

What it does **not** cover:

- **A retried job.** Deduplication is evaluated at `add` time. Once a job exists, its retries are the same job and run the handler again.
- **Anything past the TTL.** After 24h the key is gone and the same logical request enqueues again.
- **A concurrent duplicate at a different layer.** Two producers computing different keys for the same intent are two jobs.

Detecting it from the caller's side: if two calls return the same `id`, the second created no work.

```ts
const first = await enqueuer.enqueue('email.send', payload, { idempotencyKey: key })
const second = await enqueuer.enqueue('email.send', payload, { idempotencyKey: key })
first.id === second.id // true: no new job
```

### Business level: making the side effect safe

**This is the layer that actually protects you.** A queue cannot un-send an email that a previous attempt already sent.

```ts
// Fragile: a second run sends a second email.
await emailProvider.send({ to, template })

// Better: the provider deduplicates on a key you supply.
await emailProvider.send({ to, template, idempotencyKey: `welcome:${userId}` })

// Better still: your own store is the source of truth.
const claimed = await db.emailLog.insertIfAbsent({ key: `welcome:${userId}` })
if (!claimed) return { skipped: true }
await emailProvider.send({ to, template })
```

Patterns that work: a unique constraint on a natural key, a conditional write (`INSERT ... ON CONFLICT DO NOTHING`, compare-and-swap), an upstream idempotency key (Stripe, SendGrid and most payment APIs accept one), or making the operation naturally idempotent (`SET status = 'done'` rather than `INCREMENT attempts`).

`maintenance.cleanup` is the demo of the last one: its result is a pure function of the `before` cutoff, so running it twice for the same cutoff is indistinguishable from running it once.

## Retries

Five attempts by default, with BullMQ's native exponential backoff: **2s, 4s, 8s, 16s**. Configure with `JOB_ATTEMPTS` and `JOB_BACKOFF_DELAY_MS`; a job definition or an individual call can override.

### Classification

Retrying is only useful when the failure might not recur.

| Retryable                            | Non-retryable                           |
| ------------------------------------ | --------------------------------------- |
| Timeout, `ECONNRESET`, `ETIMEDOUT`   | Invalid payload (schema violation)      |
| HTTP 502/503/504, rate limit         | HTTP 400/404/422 for a permanent reason |
| Redis blip, network partition        | Unknown job name                        |
| Deadlock, optimistic-lock conflict   | Unsupported operation                   |
| Unexpected errors (the safe default) | A resource known to be permanently gone |

```ts
import { NonRetryableError, TransientError } from 'queue-worker-kit'

throw new NonRetryableError(`Unknown template "${t}"`) // fails immediately
throw new TransientError('provider returned 502') // retried with backoff
throw new Error('something unexpected') // retried (safe default)
```

`NonRetryableError` extends BullMQ's `UnrecoverableError`, so this uses BullMQ's own mechanism rather than a parallel retry policy layered on top. Payload validation failures are non-retryable automatically, at both the producer and the worker.

The cost of getting it wrong: classify a permanent failure as retryable and it burns five attempts plus 30 seconds of backoff to fail anyway, holding a concurrency slot each time. Classify a transient failure as permanent and you discard recoverable work.

### What is not retried

An error thrown _before_ the handler runs (an unknown job name, a malformed envelope) is non-retryable by construction. An error thrown _after_ a side effect completed is retried, which is the whole reason the idempotency section above exists.

## Dead-letter queue

A job that exhausts its attempts stays in BullMQ's `failed` set, which `JOB_REMOVE_ON_FAIL_COUNT` trims. That is correct for the queue and useless for an incident: the record you need at 3am rolled off yesterday. So terminal failures are copied into a dedicated `dead-letter` queue as a self-contained record (source queue, job id, job name, full payload, failure reason, stack, attempt count, correlation id, timestamp).

Four design properties:

**No recursion.** Nothing consumes the dead-letter queue, so there is no processor that can fail and re-dead-letter. The writer additionally refuses any job whose source queue is the dead-letter queue itself. Both would have to be removed for a loop to be possible.

**No duplicates.** The record's job id is derived from the failure: `dlq-<queue>-<jobId>-<attemptsMade>`. `Queue.add` ignores a job whose id already exists, so a repeated write is a Redis no-op rather than a second entry. Including `attemptsMade` still allows a genuinely retried-then-failed-again job to record a new one.

**No cascade.** A failed dead-letter write is logged and counted (`qwk_dead_letter_write_failures_total`), never rethrown. The original job has already failed; failing the writer as well would take the worker down over a bookkeeping error. **Alert on any increase in that counter**: it means failures are happening and not being recorded.

**Bounded.** Records sit in `waiting` forever because nothing consumes them, which means `removeOnFail` retention never applies. Without intervention this is the one unbounded structure in the system, so past `DEAD_LETTER_MAX_ENTRIES` (5000) the oldest records are evicted. Export to durable storage if you need full history.

Shutdown waits for in-flight dead-letter writes. They are fired from an event handler rather than awaited inline, so without that drain step a terminating worker could close the queue mid-write and lose the record.

## Stalled jobs

A worker holds a lock on its job and renews it every `lockDuration / 2` (15s by default). If it stops renewing, another worker's stalled checker returns the job to `wait`.

Causes, in order of how often they actually happen:

1. **A blocked event loop.** Synchronous CPU work (JSON parsing a huge payload, crypto, image processing, a tight loop) prevents the renewal timer from firing. The job is still running; Redis just cannot tell.
2. **The process died.** Genuine, and exactly what the mechanism is for.
3. **A network partition** between worker and Redis.
4. **`lockDuration` shorter than the real work.** Only if lock renewal is also disabled.

Case 1 is the dangerous one: the job is running _and_ has been handed to another worker, so it now runs twice concurrently. That is at-least-once delivery in its most surprising form.

`WORKER_MAX_STALLED_COUNT` is 1: a job recovered from stalled once may run again, but the second time it is failed outright. Without that limit, a job that reliably kills its worker cycles through the entire fleet.

Do not build your own stalled detector. BullMQ's is correct and the failure mode above is a symptom to fix in your handler, not in the detector.

Watch `qwk_jobs_stalled_total`. Anything above a trickle means blocked event loops or dying workers.

## Timeouts

`JOB_TIMEOUT_MS` (30s) is a **cooperative** deadline. What it does:

- Aborts the job's `AbortSignal`
- Fails the attempt with `JobTimeoutError`, which is retryable
- Releases the concurrency slot

What it cannot do: **stop the work.** Node has no way to terminate an arbitrary async operation. A handler that ignores its signal keeps running to completion in the background, holding whatever resources it holds, after the job has already been marked failed. The slot is freed, so throughput recovers; the work does not stop.

Write handlers that cooperate:

```ts
return async ({ data, signal }) => {
  const response = await fetch(url, { signal }) // aborts with the job
  throwIfAborted(signal) // between stages
  await sleep(1000, signal) // abort-aware
}
```

The signal also fires when BullMQ loses the job's lock, and when a worker cancels active jobs during a forced shutdown. So respecting it is not only about timeouts: it is how a handler learns that its work is no longer wanted.

For genuinely unbounded work, a deadline in the _dependency_ (an HTTP client timeout, a statement timeout) is the real fix. This timeout is a backstop.

## CPU-heavy jobs

`WORKER_CONCURRENCY` is concurrency, not parallelism. Ten CPU-bound jobs on one Node process do not run ten times faster; they interleave on one thread and block each other, and each blocked stretch risks the lock renewal described above.

Options, roughly in order of effort:

| Approach                                                              | When                                              |
| --------------------------------------------------------------------- | ------------------------------------------------- |
| **Sandboxed processors** (BullMQ runs the handler in a child process) | Moderate CPU work, minimal code change            |
| **Worker threads**                                                    | CPU work that shares memory with the parent       |
| **A separate service**                                                | Sustained heavy work with its own scaling profile |
| **A different compute model** (batch, GPU, native)                    | Video, ML, large data                             |

Whichever you choose, keep CPU-bound jobs on a **different queue** with `concurrency: 1` or 2, so they cannot starve I/O-bound jobs of event loop time. This kit does not implement any of these; the point is to know which one you need.

## Redis failures

| Situation                 | Behaviour                                                                           |
| ------------------------- | ----------------------------------------------------------------------------------- |
| Redis down at API boot    | The API starts anyway. `/health/live` is 200, `/health/ready` is 503                |
| Redis down at worker boot | The worker starts, logs a warning, and consumes as soon as ioredis connects         |
| Redis dies while running  | Enqueue **fails fast** with a 5xx (`enableOfflineQueue: false`); readiness goes 503 |
| Redis dies mid-job        | The attempt fails and is retried once the connection returns                        |
| Redis returns             | ioredis reconnects with bounded exponential backoff plus jitter; workers resume     |

Failing fast rather than buffering is deliberate. A caller would rather get a 503 it can retry than have its request hang until a socket timeout. Workers use the opposite setting, buffering across a blip instead of erroring, because there is no impatient caller waiting.

Neither process crash-loops on a Redis outage. Restarting a container because a dependency is briefly unavailable just moves the outage, whereas an API that starts and reports 503 gives the orchestrator exactly the signal it needs.

**Redis eviction is the one that silently loses data.** A queue is durable state, not a cache. Under any `allkeys-*` maxmemory policy, Redis is free to delete job keys to make room, and queued work vanishes with no error anywhere. Use `noeviction` so Redis rejects writes instead, turning data loss into backpressure you can see. See BullMQ's production recommendations.

## What this kit does not guarantee

Stated plainly, so nothing here is oversold:

- **Not exactly-once.** At-least-once, as above.
- **No ordering guarantee.** Concurrency, retries and backoff all reorder jobs. Use a queue with `concurrency: 1`, or BullMQ flows, if order matters.
- **No cross-job transactions.** Two jobs succeeding or failing together is not something a queue provides.
- **Timeouts do not stop work.** Cooperative only.
- **No cluster-wide concurrency limit by default.** Per-process, unless you call `setGlobalConcurrency()`.
- **No measured throughput figures.** None are published here because none were benchmarked. Yours depend on your jobs, your Redis and your network.
