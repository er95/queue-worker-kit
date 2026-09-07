import { serializeError } from '../../src/utils/serialize-error.js'

/**
 * Polling helpers with explicit deadlines.
 *
 * Deliberately not `await sleep(2000)`. A fixed sleep is either slower than it
 * needs to be or, on a loaded CI runner, too short, which is the single most
 * common source of flaky queue tests. These wait for the condition and fail
 * with a useful message when it never arrives.
 */

export interface WaitOptions {
  timeoutMs?: number
  intervalMs?: number
  /** Included in the failure message. */
  description?: string
}

export async function waitFor<T>(
  probe: () => Promise<T | undefined> | T | undefined,
  options: WaitOptions = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 15_000
  const intervalMs = options.intervalMs ?? 25
  const deadline = Date.now() + timeoutMs

  let lastError: unknown

  while (Date.now() < deadline) {
    try {
      const value = await probe()
      if (value !== undefined && value !== false) {
        return value
      }
    } catch (error) {
      // A probe may legitimately throw while the state it reads is still
      // settling; only the final failure is reported.
      lastError = error
    }
    await delay(intervalMs)
  }

  const description = options.description ?? 'condition'
  const suffix = lastError === undefined ? '' : ` Last error: ${serializeError(lastError).message}`
  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${description}.${suffix}`)
}

export async function waitForCondition(
  probe: () => Promise<boolean> | boolean,
  options: WaitOptions = {},
): Promise<void> {
  await waitFor(async () => ((await probe()) ? true : undefined), options)
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * A deferred value, for asserting on something an event handler produces.
 */
export interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (error: unknown) => void
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/** Collects values a listener reports, for order-sensitive assertions. */
export function collector<T>(): { values: T[]; push: (value: T) => void } {
  const values: T[] = []
  return { values, push: (value: T) => void values.push(value) }
}

/**
 * Captures a rejection so the error can be asserted on with its real type.
 *
 * `promise.catch((e) => e as T)` widens to `T | ResolvedValue`, which then
 * needs a second cast at every property access.
 */
export async function captureError<T = Error>(promise: Promise<unknown>): Promise<T> {
  const outcome = await promise.then(
    () => ({ rejected: false }) as const,
    (error: unknown) => ({ rejected: true, error }) as const,
  )

  if (!outcome.rejected) {
    throw new Error('Expected the promise to reject, but it resolved')
  }
  return outcome.error as T
}

/**
 * Waits until a job has reached a terminal state, returning a snapshot whose
 * fields agree with that state.
 *
 * Not `getJob()` then `getState()`: those are two round trips, so a job that
 * settles between them yields a snapshot older than the state just observed.
 * Asserting on it then reads a stale `attemptsMade` or a null `returnvalue`,
 * which is a genuine flake under CPU contention rather than a timing quirk.
 *
 * `finishedOn` is the fix: BullMQ writes it atomically with the transition into
 * `completed`/`failed` (and only when the job is not being retried), so a
 * snapshot that has it is internally consistent in a single read.
 */
export async function waitForFinishedJob<T extends { finishedOn?: number | undefined }>(
  getJob: () => Promise<T | undefined>,
  options: WaitOptions = {},
): Promise<T> {
  return waitFor(
    async () => {
      const job = await getJob()
      return job?.finishedOn === undefined ? undefined : job
    },
    { description: 'the job to finish', ...options },
  )
}
