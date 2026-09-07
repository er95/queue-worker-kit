import type { Job } from 'bullmq'
import { describe, expect, it, vi } from 'vitest'

import { QUEUE_NAMES } from '../../src/config/defaults.js'
import { PayloadValidationError } from '../../src/errors/errors.js'
import {
  JOB_NAMES,
  createJobRegistry,
  getJobDefinition,
  isJobName,
  queueForJob,
} from '../../src/jobs/registry.js'
import type { JobMeta } from '../../src/jobs/types.js'
import { createLogger } from '../../src/observability/logger.js'
import { captureError } from '../helpers/wait.js'

const silentLogger = createLogger({ level: 'silent', name: 'test', pretty: false })

function fakeJob(name: string, data: unknown): Job<unknown, unknown, string> {
  return { id: '1', name, data, attemptsMade: 0, opts: {} } as Job<unknown, unknown, string>
}

const validMeta: JobMeta = {
  correlationId: 'corr-1',
  enqueuedAt: '2026-01-01T00:00:00.000Z',
}

const invocationContext = () => ({
  logger: silentLogger,
  signal: new AbortController().signal,
  attempt: 1,
})

describe('job definitions', () => {
  it('exposes the three demo jobs under their canonical names', () => {
    expect(JOB_NAMES).toEqual(['email.send', 'report.generate', 'maintenance.cleanup'])
  })

  it('maps each job to its queue', () => {
    expect(queueForJob('email.send')).toBe(QUEUE_NAMES.email)
    expect(queueForJob('report.generate')).toBe(QUEUE_NAMES.reports)
    expect(queueForJob('maintenance.cleanup')).toBe(QUEUE_NAMES.maintenance)
  })

  // The guard that keeps HTTP input from ever selecting which code runs.
  it('recognises only registered job names', () => {
    expect(isJobName('email.send')).toBe(true)
    expect(isJobName('email.destroy')).toBe(false)
    expect(isJobName('__proto__')).toBe(false)
    expect(isJobName('constructor')).toBe(false)
  })

  it('never routes a job to the dead-letter queue', () => {
    for (const name of JOB_NAMES) {
      expect(getJobDefinition(name).queue).not.toBe(QUEUE_NAMES.deadLetter)
    }
  })
})

describe('payload schemas', () => {
  describe('email.send', () => {
    const { schema } = getJobDefinition('email.send')

    it('accepts a well-formed payload', () => {
      expect(
        schema.safeParse({ to: 'user@example.com', template: 'welcome', userId: 'usr_123' })
          .success,
      ).toBe(true)
    })

    it.each(['not-an-email', 'user@', '@example.com', 'user example.com', ''])(
      'rejects %s as a recipient',
      (to) => {
        expect(schema.safeParse({ to, template: 'welcome', userId: 'usr_1' }).success).toBe(false)
      },
    )

    /**
     * A template is an identifier, not a path. Constraining its shape keeps
     * traversal-flavoured values out of the payload entirely.
     */
    it.each(['../../etc/passwd', 'Welcome', 'welcome!', 'a'.repeat(65), ''])(
      'rejects %s as a template',
      (template) => {
        expect(
          schema.safeParse({ to: 'user@example.com', template, userId: 'usr_1' }).success,
        ).toBe(false)
      },
    )

    it('rejects a missing field', () => {
      expect(schema.safeParse({ to: 'user@example.com', template: 'welcome' }).success).toBe(false)
    })
  })

  describe('report.generate', () => {
    const { schema } = getJobDefinition('report.generate')

    it('accepts both supported formats', () => {
      for (const format of ['csv', 'json']) {
        expect(schema.safeParse({ reportId: 'r1', requestedBy: 'ops', format }).success).toBe(true)
      }
    })

    it('rejects an unsupported format', () => {
      expect(schema.safeParse({ reportId: 'r1', requestedBy: 'ops', format: 'pdf' }).success).toBe(
        false,
      )
    })
  })

  describe('maintenance.cleanup', () => {
    const { schema } = getJobDefinition('maintenance.cleanup')

    it('accepts an ISO-8601 cutoff', () => {
      expect(schema.safeParse({ before: '2026-01-01T00:00:00.000Z', dryRun: true }).success).toBe(
        true,
      )
    })

    it.each(['2026-01-01', 'yesterday', '01/01/2026', ''])('rejects %s as a cutoff', (before) => {
      expect(schema.safeParse({ before, dryRun: true }).success).toBe(false)
    })

    // A string "false" silently becoming `true` would turn a dry run into a
    // real one, so the boolean is not coerced.
    it('does not coerce dryRun from a string', () => {
      expect(
        schema.safeParse({ before: '2026-01-01T00:00:00.000Z', dryRun: 'false' }).success,
      ).toBe(false)
    })
  })
})

describe('createJobRegistry', () => {
  it('resolves registered jobs and nothing else', () => {
    const registry = createJobRegistry()

    expect(registry.get('email.send')).toBeDefined()
    expect(registry.get('nope.job')).toBeUndefined()
  })

  it('groups job names by queue', () => {
    const registry = createJobRegistry()

    expect(registry.namesForQueue(QUEUE_NAMES.email)).toEqual(['email.send'])
    expect(registry.namesForQueue(QUEUE_NAMES.deadLetter)).toEqual([])
  })

  describe('invoke', () => {
    it('validates, then runs the processor with the parsed payload', async () => {
      const send = vi.fn().mockResolvedValue({ providerMessageId: 'demo_1' })
      const registry = createJobRegistry({ emailProvider: { send } })

      const entry = registry.get('email.send')
      const { result, meta } = await entry!.invoke(
        fakeJob('email.send', {
          payload: { to: 'user@example.com', template: 'welcome', userId: 'usr_1' },
          meta: validMeta,
        }),
        invocationContext,
      )

      expect(meta.correlationId).toBe('corr-1')
      expect(result).toMatchObject({ delivered: true, providerMessageId: 'demo_1' })
      expect(send).toHaveBeenCalledOnce()
    })

    /**
     * Worker-side validation is defensive: a payload already in Redis may have
     * been written by an older build. It must fail permanently and legibly
     * rather than mysteriously, and never reach the processor.
     */
    it('rejects a payload already in Redis that no longer matches the schema', async () => {
      const send = vi.fn()
      const registry = createJobRegistry({ emailProvider: { send } })

      await expect(
        registry.get('email.send')!.invoke(
          fakeJob('email.send', {
            payload: { to: 'not-an-email', template: 'welcome', userId: 'usr_1' },
            meta: validMeta,
          }),
          invocationContext,
        ),
      ).rejects.toThrow(PayloadValidationError)

      expect(send).not.toHaveBeenCalled()
    })

    it('prefixes payload issues so the failing field is obvious', async () => {
      const registry = createJobRegistry({ emailProvider: { send: vi.fn() } })

      const error = await captureError<PayloadValidationError>(
        registry.get('email.send')!.invoke(
          fakeJob('email.send', {
            payload: { to: 'nope', template: 'welcome', userId: 'usr_1' },
            meta: validMeta,
          }),
          invocationContext,
        ),
      )

      expect(error.issues.join(' ')).toContain('payload.to')
    })

    it.each([
      ['a missing envelope', { to: 'user@example.com' }],
      ['missing meta', { payload: { to: 'user@example.com', template: 'welcome', userId: 'u' } }],
      [
        'a missing correlation id',
        {
          payload: { to: 'user@example.com', template: 'welcome', userId: 'u' },
          meta: { enqueuedAt: '2026-01-01T00:00:00.000Z' },
        },
      ],
      ['null data', null],
    ])('rejects %s', async (_label, data) => {
      const registry = createJobRegistry({ emailProvider: { send: vi.fn() } })

      await expect(
        registry.get('email.send')!.invoke(fakeJob('email.send', data), invocationContext),
      ).rejects.toThrow(PayloadValidationError)
    })

    // The callback is what builds the correlation-scoped logger, so it has to
    // fire before any handler code runs, and never for an invalid payload.
    it('calls back only after validation succeeds', async () => {
      const registry = createJobRegistry({ emailProvider: { send: vi.fn() } })
      const onValidated = vi.fn(invocationContext)

      await registry
        .get('email.send')!
        .invoke(fakeJob('email.send', { payload: {}, meta: validMeta }), onValidated)
        .catch(() => undefined)

      expect(onValidated).not.toHaveBeenCalled()
    })

    it('reports the correlation id to the callback', async () => {
      const registry = createJobRegistry({ emailProvider: { send: vi.fn() } })
      const onValidated = vi.fn(invocationContext)

      await registry.get('maintenance.cleanup')!.invoke(
        fakeJob('maintenance.cleanup', {
          payload: { before: '2026-01-01T00:00:00.000Z', dryRun: true },
          meta: { correlationId: 'trace-abc', enqueuedAt: '2026-01-01T00:00:00.000Z' },
        }),
        onValidated,
      )

      expect(onValidated).toHaveBeenCalledWith(
        expect.objectContaining({ correlationId: 'trace-abc' }),
      )
    })
  })
})

describe('demo processors', () => {
  it('reports a dry-run cleanup as removing nothing', async () => {
    const registry = createJobRegistry()

    const { result } = await registry.get('maintenance.cleanup')!.invoke(
      fakeJob('maintenance.cleanup', {
        payload: { before: '2026-01-01T00:00:00.000Z', dryRun: true },
        meta: validMeta,
      }),
      invocationContext,
    )

    expect(result).toMatchObject({ dryRun: true, removed: 0 })
  })

  it('reports progress through every stage of a report', async () => {
    const registry = createJobRegistry()
    const updateProgress = vi.fn().mockResolvedValue(undefined)

    const job = {
      ...fakeJob('report.generate', {
        payload: { reportId: 'r1', requestedBy: 'ops', format: 'csv' },
        meta: validMeta,
      }),
      updateProgress,
    } as unknown as Job<unknown, unknown, string>

    const { result } = await registry.get('report.generate')!.invoke(job, invocationContext)

    expect(updateProgress.mock.calls.map(([p]) => (p as { percent: number }).percent)).toEqual([
      10, 40, 70, 100,
    ])
    expect(result).toMatchObject({ reportId: 'r1', format: 'csv' })
  })

  // Retrying cannot make a template exist, so this must be permanent.
  it('treats an unknown email template as permanently failed', async () => {
    const registry = createJobRegistry()

    await expect(
      registry.get('email.send')!.invoke(
        fakeJob('email.send', {
          payload: { to: 'user@example.com', template: 'no-such-template', userId: 'usr_1' },
          meta: validMeta,
        }),
        invocationContext,
      ),
    ).rejects.toThrow(/Unknown email template/)
  })
})
