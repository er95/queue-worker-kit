import { JobCancelledError } from '../errors/errors.js'

/**
 * `setTimeout` that honours an `AbortSignal`.
 *
 * Demo processors use this instead of a bare timer so that a job timeout or a
 * worker shutdown actually interrupts simulated work, rather than being noticed
 * only once the timer happens to fire.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted === true) {
      reject(abortReason(signal))
      return
    }

    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)

    function onAbort(): void {
      clearTimeout(timer)
      reject(abortReason(signal))
    }

    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

function abortReason(signal: AbortSignal | undefined): Error {
  const reason: unknown = signal?.reason
  if (reason instanceof Error) return reason
  return new JobCancelledError(typeof reason === 'string' ? reason : 'aborted')
}

/** Throws if the job has been cancelled. Call between stages of long work. */
export function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw abortReason(signal)
  }
}
