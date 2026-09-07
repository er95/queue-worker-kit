import { Writable } from 'node:stream'

import { pino } from 'pino'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { serializeError } from '../../src/utils/serialize-error.js'
import { createTestContext, type TestContext } from '../helpers/redis.js'
import { captureError } from '../helpers/wait.js'

/**
 * Regression test for a credential leak.
 *
 * ioredis attaches the failed command to every error it raises, as
 * `err.command = { name, args }` (see `DataHandler.returnError`). pino's
 * default `err` serializer then copies every own enumerable property onto the
 * log record. Since `args` holds *all* of the command's arguments, a failed
 * `AUTH` or `HELLO` handshake puts the plaintext password from `REDIS_URL` into
 * the log stream, where read access is usually far broader than secret access.
 *
 * The guard is that every call site logs `serializeError(error)`, which
 * projects to name/message/stack/code only.
 *
 * The error below is provoked with a deliberate syntax error rather than a bad
 * password, because Redis's `default` user has `nopass` and therefore accepts
 * any credentials. The mechanism under test is identical: whatever arguments
 * were sent are attached to the error.
 */

/** Stands in for a credential. Distinctive so it is easy to search for. */
const SECRET = 'S3cretRedisPassw0rd-do-not-log'

function capturingStream(lines: string[]): Writable {
  return new Writable({
    write(chunk, _encoding, callback) {
      lines.push(String(chunk))
      callback()
    },
  })
}

let context: TestContext

/** A command error whose `args` contain the secret, from the real server. */
function provokeCommandError(): Promise<unknown> {
  return context.redis.call('set', `${context.prefix}:leak-probe`, SECRET, 'NOT_AN_OPTION')
}

beforeAll(() => {
  context = createTestContext('credential-leak')
})

afterAll(async () => {
  await context.cleanup()
})

describe('Redis credential leakage', () => {
  /**
   * Establishes the premise against a real server. If this stops holding,
   * ioredis has changed and the guard below is no longer load-bearing.
   */
  it('confirms ioredis attaches the arguments it sent to the error', async () => {
    const error = await captureError<Error & { command?: { args?: unknown[] } }>(
      provokeCommandError(),
    )

    expect(error.command?.args).toContain(SECRET)
    // Which is precisely why a raw driver error must never reach a logger.
    expect(JSON.stringify({ ...error })).toContain(SECRET)
  })

  it('strips the arguments from a serialized error', async () => {
    const serialized = serializeError(await captureError(provokeCommandError()))

    expect(JSON.stringify(serialized)).not.toContain(SECRET)
    // The diagnostic value survives.
    expect(serialized.name).toBeTruthy()
    expect(serialized.message).toBeTruthy()
  })

  // The end-to-end property: a real error carrying a real secret, logged the
  // way the application logs it, must not put the secret in the stream.
  it('never writes the arguments to the log stream', async () => {
    const lines: string[] = []
    const logger = pino({ level: 'trace' }, capturingStream(lines))

    const error = await captureError(provokeCommandError())

    // Exactly what src/queue/connection.ts and src/queue/registry.ts do.
    logger.warn({ err: serializeError(error) }, 'redis error')

    const output = lines.join('\n')

    expect(output).toContain('redis error')
    expect(output).not.toContain(SECRET)
    expect(output).not.toContain('"args"')
  })

  /**
   * Defence in depth. Should a future call site log a raw driver error, the
   * redaction paths configured in `createLogger` still have to catch it.
   */
  it('redacts command arguments even when a raw error is logged', async () => {
    const { REDACT_PATHS } = await import('../../src/observability/logger.js')

    const lines: string[] = []
    const logger = pino(
      { level: 'trace', redact: { paths: [...REDACT_PATHS], censor: '[redacted]' } },
      capturingStream(lines),
    )

    const error = await captureError(provokeCommandError())
    logger.warn({ err: error }, 'redis error')

    expect(lines.join('\n')).not.toContain(SECRET)
  })
})
