import { describe, expect, it } from 'vitest'

import { ERROR_STACK_MAX_LINES } from '../../src/config/defaults.js'
import { formatErrorReason, serializeError } from '../../src/utils/serialize-error.js'

/**
 * `JSON.stringify(new Error('boom'))` returns `{}`, because `message` and
 * `stack` are not enumerable. Every one of these cases has produced an
 * unhelpful `{}` in someone's production logs.
 */
describe('serializeError', () => {
  it('extracts the fields an Error does not expose to JSON', () => {
    const serialized = serializeError(new TypeError('bad input'))

    expect(serialized.name).toBe('TypeError')
    expect(serialized.message).toBe('bad input')
    expect(serialized.stack).toContain('TypeError: bad input')
  })

  it('keeps a scalar `code`, the de facto standard on Node errors', () => {
    const error = Object.assign(new Error('connection refused'), { code: 'ECONNREFUSED' })
    expect(serializeError(error).code).toBe('ECONNREFUSED')
  })

  it('stringifies a numeric code', () => {
    const error = Object.assign(new Error('http'), { code: 502 })
    expect(serializeError(error).code).toBe('502')
  })

  it('walks the cause chain', () => {
    const root = new Error('socket closed')
    const middle = new Error('query failed', { cause: root })
    const top = new Error('job failed', { cause: middle })

    const serialized = serializeError(top)

    expect(serialized.cause?.message).toBe('query failed')
    expect(serialized.cause?.cause?.message).toBe('socket closed')
  })

  // A self-referencing cause is rare but real, and must not hang a log call.
  it('does not recurse forever on a circular cause', () => {
    const error = new Error('loops')
    ;(error as { cause?: unknown }).cause = error

    const serialized = serializeError(error)

    let depth = 0
    let node = serialized.cause
    while (node !== undefined) {
      depth += 1
      node = node.cause
      expect(depth).toBeLessThan(10)
    }
    expect(depth).toBeGreaterThan(0)
  })

  it('truncates a long stack so one error cannot dominate a log line', () => {
    const error = new Error('deep')
    error.stack = [
      'Error: deep',
      ...Array.from({ length: 200 }, (_v, i) => `    at frame${i}`),
    ].join('\n')

    const lines = serializeError(error).stack?.split('\n') ?? []

    expect(lines).toHaveLength(ERROR_STACK_MAX_LINES + 1)
    expect(lines.at(-1)).toContain('truncated')
  })

  it('leaves a short stack untouched', () => {
    const error = new Error('shallow')
    error.stack = 'Error: shallow\n    at one\n    at two'
    expect(serializeError(error).stack).toBe(error.stack)
  })

  // JavaScript permits throwing anything at all.
  describe('non-Error values', () => {
    it('handles a thrown string', () => {
      expect(serializeError('something broke')).toEqual({
        name: 'Error',
        message: 'something broke',
      })
    })

    it.each([
      [null, 'null'],
      [undefined, 'undefined'],
    ])('handles %s', (value, expected) => {
      expect(serializeError(value).message).toBe(expected)
    })

    it('handles a number and a boolean', () => {
      expect(serializeError(42).message).toBe('42')
      expect(serializeError(false).message).toBe('false')
    })

    it('handles a symbol, which JSON.stringify cannot represent', () => {
      expect(serializeError(Symbol('token')).message).toBe('Symbol(token)')
    })

    it('handles a bigint, which JSON.stringify throws on', () => {
      expect(serializeError(10n).message).toBe('10')
    })

    it('reads name and message off an error-shaped object', () => {
      const serialized = serializeError({ name: 'UpstreamError', message: 'gateway timeout' })
      expect(serialized).toEqual({ name: 'UpstreamError', message: 'gateway timeout' })
    })

    it('stringifies a plain object with no message', () => {
      expect(serializeError({ status: 503 }).message).toBe('{"status":503}')
    })

    it('survives a circular object', () => {
      const circular: Record<string, unknown> = { a: 1 }
      circular.self = circular
      expect(serializeError(circular).message).toBe('[unserializable value]')
    })

    it('survives a throwing getter', () => {
      const hostile = {
        get message(): string {
          throw new Error('nope')
        },
      }
      expect(() => serializeError(hostile)).not.toThrow()
    })
  })
})

describe('formatErrorReason', () => {
  it('produces a one-line name and message summary', () => {
    expect(formatErrorReason(new TypeError('bad'), 100)).toBe('TypeError: bad')
  })

  it('truncates past the limit', () => {
    const reason = formatErrorReason(new Error('x'.repeat(500)), 50)
    expect(reason).toHaveLength(50)
    expect(reason.endsWith('...')).toBe(true)
  })

  it('falls back to the name when there is no message', () => {
    expect(formatErrorReason(new Error(''), 100)).toBe('Error')
  })
})
