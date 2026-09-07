import { describe, expect, it, vi } from 'vitest'

import { JobCancelledError } from '../../src/errors/errors.js'
import { sleep, throwIfAborted } from '../../src/utils/sleep.js'

/**
 * The abort-aware timer demo processors use.
 *
 * It matters more than it looks: a bare `setTimeout` would mean a timed-out or
 * cancelled job keeps sleeping to completion, so the concurrency slot is only
 * released when the timer happens to fire.
 *
 * Rejection semantics, which the cases below pin down: an `Error` abort reason
 * is propagated unchanged (so a lock-lost or timeout error keeps its identity),
 * a string reason is wrapped in `JobCancelledError`, and a bare `abort()`
 * surfaces Node's own `AbortError`. All three are ordinary errors, so BullMQ
 * treats an interrupted job as retryable.
 */
describe('sleep', () => {
  it('resolves after the delay', async () => {
    const startedAt = Date.now()
    await sleep(40)
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(30)
  })

  it('resolves without a signal', async () => {
    await expect(sleep(1)).resolves.toBeUndefined()
  })

  it('rejects as soon as the signal aborts, not when the timer fires', async () => {
    const controller = new AbortController()
    const startedAt = Date.now()

    const pending = sleep(10_000, controller.signal)
    setTimeout(() => controller.abort(), 20)

    await expect(pending).rejects.toThrow(/aborted/i)
    // The 10s timer never fired.
    expect(Date.now() - startedAt).toBeLessThan(1_000)
  })

  it('rejects immediately when the signal is already aborted', async () => {
    const controller = new AbortController()
    controller.abort()

    // A bare `abort()` sets `signal.reason` to Node's AbortError, which is an
    // Error already, so it is propagated rather than rewrapped.
    await expect(sleep(10_000, controller.signal)).rejects.toThrow(/aborted/i)
  })

  it('propagates an Error abort reason unchanged', async () => {
    const controller = new AbortController()
    const reason = new Error('lock lost')
    controller.abort(reason)

    await expect(sleep(1_000, controller.signal)).rejects.toThrow(reason)
  })

  it('wraps a string abort reason', async () => {
    const controller = new AbortController()
    controller.abort('job timeout')

    await expect(sleep(1_000, controller.signal)).rejects.toThrow(/job timeout/)
  })

  // A long-lived worker running thousands of jobs against one shutdown signal
  // would otherwise accumulate a listener per sleep.
  it('removes its abort listener once the timer resolves', async () => {
    const controller = new AbortController()
    const removeSpy = vi.spyOn(controller.signal, 'removeEventListener')

    await sleep(5, controller.signal)

    expect(removeSpy).toHaveBeenCalledWith('abort', expect.any(Function))
  })
})

describe('throwIfAborted', () => {
  it('does nothing while the signal is live', () => {
    expect(() => throwIfAborted(new AbortController().signal)).not.toThrow()
  })

  it('throws once aborted', () => {
    const controller = new AbortController()
    controller.abort()
    expect(() => throwIfAborted(controller.signal)).toThrow(/aborted/i)
  })

  it('wraps a string abort reason', () => {
    const controller = new AbortController()
    controller.abort('job timeout')
    expect(() => throwIfAborted(controller.signal)).toThrow(JobCancelledError)
  })

  it('throws the original Error reason', () => {
    const controller = new AbortController()
    const reason = new Error('shutting down')
    controller.abort(reason)
    expect(() => throwIfAborted(controller.signal)).toThrow(reason)
  })
})
