# Operations

Running this in production: deploys, scaling, monitoring, and what to do when jobs fail.

Everything here uses the endpoints, metrics and BullMQ APIs the kit actually ships. Where an action needs a script or a console rather than a feature that exists, it says so.

## Starting and stopping

```bash
# Local
pnpm dev:api
pnpm dev:worker

# Built
pnpm build
pnpm start:api
pnpm start:worker

# Docker
docker compose up -d --build
docker compose down
```

**Stopping cleanly means sending SIGTERM and waiting.** Both processes run an ordered shutdown with a hard `SHUTDOWN_TIMEOUT_MS` deadline (30s). The worker's sequence is: mark draining, stop fetching and **wait for active jobs**, drain in-flight dead-letter writes, close the metrics server, close event listeners, close queues, close Redis.

Give the orchestrator a grace period above that deadline, or SIGKILL arrives mid-job:

| Platform       | Setting                         | Suggested   |
| -------------- | ------------------------------- | ----------- |
| Docker Compose | `stop_grace_period`             | `40s` (set) |
| Kubernetes     | `terminationGracePeriodSeconds` | `40`        |
| systemd        | `TimeoutStopSec`                | `40s`       |
| ECS            | `stopTimeout`                   | `40`        |

If jobs regularly take longer than 30s, raise `SHUTDOWN_TIMEOUT_MS` _and_ the platform grace period together.

## Deploying a new version

A rolling deploy is safe because `worker.close()` waits for active jobs. The sequence per replica:

```
SIGTERM -> stop fetching new jobs -> finish active jobs -> exit -> new replica starts
```

Nothing is lost as long as the grace period exceeds your longest job. If a container is SIGKILLed mid-job, the job is not lost either: its lock expires and another worker picks it up (which is a second run, so see [reliability.md](reliability.md) on idempotency).

**Deploy order.** When a release adds a new job type, deploy **workers first**, then the API. A producer that enqueues `image.resize` before any worker understands it produces jobs that fail as non-retryable `UnknownJobError`. In the other order there is simply an idle handler for a few minutes.

**Payload changes are a compatibility problem.** Jobs enqueued by the old version are still in Redis when the new version starts consuming. Make schema changes additive: add optional fields, and only make them required a release later, once no old jobs remain. A tightened schema fails every in-flight job as a non-retryable validation error.

**Schedules** are re-upserted on every worker boot against a deterministic id, so a changed `MAINTENANCE_CLEANUP_CRON` takes effect on deploy with no orphaned schedule left behind.

## Scaling workers

```bash
docker compose up -d --scale worker=3
kubectl scale deployment/worker --replicas=3
```

Remember that **concurrency and rate limits are per process**:

```
3 replicas x WORKER_CONCURRENCY=10  = up to 30 concurrent jobs
3 replicas x WORKER_RATE_LIMIT_MAX=100 per second = ~300/s total
```

Scale replicas for throughput and fault tolerance. Raise concurrency for I/O-bound work that spends its time waiting. Do neither for CPU-bound work: add replicas or move it off the Node event loop entirely.

For a genuine cluster-wide ceiling, for instance when a third-party API allows 50 requests/second no matter how many workers you run:

```ts
await queue.setGlobalConcurrency(50)
await queue.setGlobalRateLimit(50, 1000)
```

Those are cluster-wide because they live in Redis. Neither is set by this kit; add them to a bootstrap or run them once from a console.

**Sizing signal:** if `qwk_queue_jobs{state="waiting"}` grows steadily while `qwk_job_duration_seconds` stays flat, you need more capacity. If duration is climbing instead, the bottleneck is downstream and more workers will make it worse.

## Monitoring

Scrape **both** processes. Job execution counters exist only in the worker, and queue depth only in the API.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: qwk-api
    static_configs:
      - targets: ['api:3000']
  - job_name: qwk-worker
    static_configs:
      - targets: ['worker:9090']
```

### What to alert on

| Signal                     | Query sketch                                                          | Why                                                         |
| -------------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------- |
| Dead-letter writes failing | `increase(qwk_dead_letter_write_failures_total[15m]) > 0`             | Failures are happening and not being recorded. Page on this |
| Jobs dead-lettering        | `increase(qwk_jobs_dead_lettered_total[15m]) > 0`                     | Work is being permanently dropped                           |
| Queue backing up           | `qwk_queue_jobs{state="waiting"}` rising over 30m                     | Under-capacity, or a stuck downstream                       |
| Failure ratio              | `rate(qwk_jobs_failed_total[5m]) / rate(qwk_jobs_started_total[5m])`  | A bad deploy or a failing dependency                        |
| Stalls                     | `increase(qwk_jobs_stalled_total[15m])`                               | Blocked event loops or dying workers                        |
| Latency                    | `histogram_quantile(0.95, rate(qwk_job_duration_seconds_bucket[5m]))` | Slow dependency before it becomes a timeout                 |
| Readiness                  | `/health/ready` returning 503                                         | Redis unreachable from the API                              |

Useful dashboard panels: waiting/active/delayed/failed per queue, p50/p95/p99 duration by `job_name`, enqueued vs completed rate (the gap is your backlog), and the dead-letter queue depth.

### Reading the logs

Every job log line carries `queue`, `jobId`, `jobName`, `correlationId` and `attempt`. Trace one request end to end with the correlation id it was given:

```bash
docker compose logs worker | grep '"correlationId":"8af799ba-'
```

The client can choose it, which is what makes cross-service tracing work:

```bash
curl -X POST http://localhost:3000/jobs/email \
  -H 'x-correlation-id: order-4711-notify' \
  -H 'content-type: application/json' \
  -d '{"to":"user@example.com","template":"receipt","userId":"usr_9"}'
```

Messages worth grepping: `job moved to dead-letter`, `job failed, retrying`, `job stalled and was returned to wait`, `redis reconnecting`, `shutdown deadline exceeded`.

## Investigating a failed job

**1. Ask the API.**

```bash
curl http://localhost:3000/jobs/email/42
```

```json
{
  "data": {
    "id": "42",
    "state": "failed",
    "attemptsMade": 5,
    "correlationId": "8af799ba-...",
    "failedReason": "TransientError: provider returned 502",
    "processedAt": "...",
    "finishedAt": "..."
  }
}
```

**2. Trace it in the logs** with the correlation id, to see every attempt.

**3. Look at the dead-letter record**, which survives after the failed job is trimmed. Its id is derived from the failure, so you can fetch it directly:

```bash
# dlq-<sourceQueue>-<sourceJobId>-<attemptsMade>
curl http://localhost:3000/jobs/dead-letter/dlq-email-42-5
```

Or browse the queue in Bull Board.

## Inspecting the dead-letter queue

Bull Board at `/admin/queues` is the intended tool: pick `dead-letter`, look at `waiting`, and each record contains the original payload, the failure reason and the stack.

Programmatically, from a maintenance script:

```ts
const dlq = queues.getOrThrow('dead-letter')

console.log(await dlq.getWaitingCount())

for (const entry of await dlq.getWaiting(0, 49)) {
  const r = entry.data
  console.log(r.failedAt, r.sourceQueue, r.sourceJobName, r.failedReason)
}
```

**Replaying** a record means enqueueing its payload again, after fixing whatever caused the failure. There is deliberately no HTTP endpoint for this, since a blind bulk replay of poison messages is how a bad afternoon starts:

```ts
const record = entry.data
await enqueuer.enqueue(record.sourceJobName, record.payload.payload, {
  correlationId: record.correlationId, // keeps the original trace
  idempotencyKey: `replay:${record.sourceJobId}`,
})
await entry.remove()
```

Check first that the handler is idempotent, or that the side effect did not already happen on the failed attempt.

**Retention.** Oldest records are evicted past `DEAD_LETTER_MAX_ENTRIES` (5000). If you need full history, export them somewhere durable rather than raising that number indefinitely: it is Redis memory.

## Pausing a queue

For a downstream outage, pause instead of scaling to zero. Pausing keeps jobs accumulating in Redis and lets active jobs finish; scaling to zero mid-job relies on the grace period.

```ts
await queue.pause() // stops delivery queue-wide; active jobs finish
await queue.resume()
await queue.isPaused()
```

Pausing is queue-wide and persists in Redis, so it survives worker restarts. **Remember to resume**, and note that a paused queue still accepts new jobs, so watch memory during a long pause.

`worker.pause()` affects only that one process, which is occasionally what you want when draining a single replica.

None of these are HTTP routes. Run them from a console, a maintenance script, or Bull Board. Exposing pause/drain/obliterate as public endpoints would make an outage one unauthenticated request away.

## Using Bull Board

```env
ENABLE_BULL_BOARD=true
BULL_BOARD_USERNAME=ops
BULL_BOARD_PASSWORD=<at least 16 chars, not a known example>
```

Available at `/admin/queues`, behind basic auth with constant-time credential comparison. Config validation refuses to boot in production with a missing, short or well-known password.

Good for: inspecting the dead-letter queue, reading a payload and stack, retrying one job by hand, watching queue depth during an incident.

**Treat it as a privileged control surface.** It can retry, remove and drain queues. Basic auth transmits the password in cleartext on every request, so terminate TLS in front of it, prefer an authenticated reverse proxy (SSO, mTLS, VPN) for anything shared, and never expose it to the public internet.

## Redis memory and retention

Queue data is durable state. When Redis fills up:

```env
JOB_REMOVE_ON_COMPLETE_COUNT=1000    # per queue
JOB_REMOVE_ON_FAIL_COUNT=5000        # per queue
DEAD_LETTER_MAX_ENTRIES=5000
```

Unbounded retention is the most common way a BullMQ deployment runs Redis out of memory. Two things to keep in mind:

- **Retention is per queue**, so four queues at 1000 completed each is 4000 stored jobs, and each holds its full payload. `JOB_PAYLOAD_SIZE_LIMIT_BYTES` (16KB) caps the worst case per job.
- **Eviction is best-effort.** BullMQ trims when a job finishes; it runs no background timer. A quiet queue keeps its finished jobs past their nominal limit until the next job completes.

Checking usage:

```bash
docker compose exec redis redis-cli INFO memory | grep used_memory_human
docker compose exec redis redis-cli --scan --pattern 'qwk:*' --count 100 | head
docker compose exec redis redis-cli INFO stats | grep evicted_keys    # must stay 0
```

`evicted_keys` above zero means Redis is deleting your job data. Fix the maxmemory policy immediately:

```bash
docker compose exec redis redis-cli CONFIG GET maxmemory-policy   # want: noeviction
```

Manual cleanup, for when retention was not enough:

```ts
await queue.clean(24 * 60 * 60 * 1000, 1000, 'completed')   # older than 24h, max 1000
await queue.clean(7 * 24 * 60 * 60 * 1000, 1000, 'failed')
```

`queue.obliterate()` deletes a queue and everything in it, including waiting jobs. There is no endpoint for it, and there should not be.

## Runbook: common situations

**Queue depth climbing.** Check `qwk_job_duration_seconds` first. Flat duration means add capacity; rising duration means a slow dependency, so fix that before adding workers. Also confirm the queue is not paused (`queue.isPaused()`) and that workers are actually connected (`qwk_jobs_started_total` still increasing).

**Everything failing after a deploy.** Look at `failedReason` on a recent job. A validation error means a payload schema was tightened against jobs already in Redis: roll back, and make the change additive. An `UnknownJobError` means the API was deployed ahead of the workers.

**A job runs twice.** Expected under at-least-once. If it is frequent, check `qwk_jobs_stalled_total`: a blocked event loop is the usual cause. Add business-level idempotency regardless.

**Readiness 503 but Redis is up.** Check credentials and TLS in `REDIS_URL`, connection limits on a managed Redis, and network policy. The API's `redis error` log lines carry a redacted URL so you can confirm which host it is dialling.

**`shutdown deadline exceeded` in the logs.** A job outlived `SHUTDOWN_TIMEOUT_MS`. Either raise it and the platform grace period, or find out why the job takes that long. The process still exits, with code 1.
