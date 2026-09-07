import { describe, expect, it } from 'vitest'

import {
  getCorrelationId,
  newCorrelationId,
  resolveCorrelationId,
  runWithCorrelationId,
} from '../../src/observability/context.js'

describe('correlation ids', () => {
  it('generates distinct v4 UUIDs', () => {
    const a = newCorrelationId()
    const b = newCorrelationId()

    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
    expect(a).not.toBe(b)
  })

  it('has no ambient id outside a context', () => {
    expect(getCorrelationId()).toBeUndefined()
  })

  it('exposes the id inside its context', () => {
    runWithCorrelationId('trace-1', () => {
      expect(getCorrelationId()).toBe('trace-1')
    })
    expect(getCorrelationId()).toBeUndefined()
  })

  // The property that makes this usable from a Fastify hook: the id has to
  // survive the await boundaries between the hook and `enqueue`.
  it('survives async boundaries', async () => {
    await runWithCorrelationId('trace-async', async () => {
      await Promise.resolve()
      await new Promise((resolve) => setTimeout(resolve, 1))
      expect(getCorrelationId()).toBe('trace-async')
    })
  })

  it('keeps concurrent contexts separate', async () => {
    const observe = (id: string) =>
      runWithCorrelationId(id, async () => {
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 5))
        return getCorrelationId()
      })

    expect(await Promise.all([observe('a'), observe('b'), observe('c')])).toEqual(['a', 'b', 'c'])
  })

  it('nests', () => {
    runWithCorrelationId('outer', () => {
      runWithCorrelationId('inner', () => {
        expect(getCorrelationId()).toBe('inner')
      })
      expect(getCorrelationId()).toBe('outer')
    })
  })

  describe('resolveCorrelationId', () => {
    // Precedence exists so an upstream trace id is never discarded.
    it('prefers an explicit id over the ambient one', () => {
      runWithCorrelationId('ambient', () => {
        expect(resolveCorrelationId('explicit')).toBe('explicit')
      })
    })

    it('falls back to the ambient id', () => {
      runWithCorrelationId('ambient', () => {
        expect(resolveCorrelationId()).toBe('ambient')
      })
    })

    it('mints a fresh id when there is nothing to inherit', () => {
      const resolved = resolveCorrelationId()
      expect(resolved).toHaveLength(36)
    })

    it('treats undefined as absent', () => {
      runWithCorrelationId('ambient', () => {
        expect(resolveCorrelationId(undefined)).toBe('ambient')
      })
    })
  })
})
