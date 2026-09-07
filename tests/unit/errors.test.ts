import { UnrecoverableError } from 'bullmq'
import { describe, expect, it } from 'vitest'

import {
  JobCancelledError,
  JobTimeoutError,
  NonRetryableError,
  PayloadValidationError,
  TransientError,
  UnknownJobError,
  classifyError,
  isNonRetryableError,
} from '../../src/errors/errors.js'

/**
 * Retry classification is the highest-stakes logic in the kit: get it wrong in
 * one direction and a permanently broken job burns five attempts and pollutes
 * the dead-letter queue; get it wrong in the other and a transient blip
 * discards real work.
 */
describe('error classification', () => {
  describe('non-retryable', () => {
    // Extending BullMQ's own class is what makes the native "stop retrying"
    // path apply, instead of us reimplementing retry policy alongside it.
    it('inherits from BullMQ UnrecoverableError so BullMQ stops retrying', () => {
      expect(new NonRetryableError('permanent')).toBeInstanceOf(UnrecoverableError)
      expect(new PayloadValidationError('bad', [])).toBeInstanceOf(UnrecoverableError)
      expect(new UnknownJobError('nope.job')).toBeInstanceOf(UnrecoverableError)
    })

    it.each([
      new NonRetryableError('permanent'),
      new PayloadValidationError('bad payload', ['to: required']),
      new UnknownJobError('unknown.job'),
      new UnrecoverableError('from bullmq'),
    ])('classifies %s as non-retryable', (error) => {
      expect(isNonRetryableError(error)).toBe(true)
      expect(classifyError(error)).toBe('non_retryable')
    })

    /**
     * BullMQ itself falls back to a name check, because an error that crossed a
     * module boundary (two copies of a package, a sandboxed processor) fails
     * `instanceof` while still being the same logical error.
     */
    it('recognises a non-retryable error by name when instanceof fails', () => {
      const foreign = Object.assign(new Error('from another realm'), {
        name: 'UnrecoverableError',
      })
      expect(isNonRetryableError(foreign)).toBe(true)
    })

    it('recognises the kit-specific names by name too', () => {
      for (const name of ['NonRetryableError', 'PayloadValidationError', 'UnknownJobError']) {
        expect(isNonRetryableError(Object.assign(new Error('x'), { name }))).toBe(true)
      }
    })
  })

  describe('retryable', () => {
    it.each([
      new Error('plain failure'),
      new TransientError('upstream 502'),
      new JobTimeoutError(30_000),
      new JobCancelledError('shutting down'),
      new TypeError('unexpected shape'),
    ])('classifies %s as retryable', (error) => {
      expect(isNonRetryableError(error)).toBe(false)
      expect(classifyError(error)).toBe('retryable')
    })

    /**
     * A timeout usually means a slow dependency, which is exactly the kind of
     * thing that succeeds on the next attempt. Treating it as permanent would
     * discard recoverable work.
     */
    it('treats a timeout as retryable and records its budget', () => {
      const error = new JobTimeoutError(5_000)
      expect(error.timeoutMs).toBe(5_000)
      expect(error.message).toContain('5000ms')
      expect(classifyError(error)).toBe('retryable')
    })

    it.each([undefined, null, 'a string', 42, {}])(
      'treats the non-Error value %s as retryable',
      (value) => {
        expect(isNonRetryableError(value)).toBe(false)
      },
    )
  })

  describe('error shape', () => {
    it('carries validation issues for the API to surface', () => {
      const error = new PayloadValidationError('Invalid payload', [
        'to: must be a valid email address',
      ])
      expect(error.issues).toEqual(['to: must be a valid email address'])
      expect(error.name).toBe('PayloadValidationError')
    })

    it('preserves a cause', () => {
      const root = new Error('root')
      expect(new NonRetryableError('wrapper', { cause: root }).cause).toBe(root)
      expect(new TransientError('wrapper', { cause: root }).cause).toBe(root)
    })

    it('leaves cause absent when none is given', () => {
      expect('cause' in new NonRetryableError('no cause')).toBe(false)
    })

    it('names the job that has no definition', () => {
      expect(new UnknownJobError('image.resize').message).toContain('image.resize')
    })
  })
})
