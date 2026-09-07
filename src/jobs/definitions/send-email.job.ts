import { z } from 'zod'

import { QUEUE_NAMES } from '../../config/defaults.js'
import { NonRetryableError, TransientError } from '../../errors/errors.js'
import { sleep } from '../../utils/sleep.js'
import { defineJob, type JobProcessor } from '../types.js'

/**
 * A representative I/O-bound job: call a third-party API, retry the blips,
 * give up permanently on the things retrying cannot fix.
 */

export const sendEmailPayloadSchema = z.object({
  /** Zod's email check, not a hand-rolled regex. */
  to: z.email({ error: 'must be a valid email address' }),
  /**
   * A template identifier, not a file path or a function name. The pattern
   * keeps traversal and injection-shaped values out of the payload; whether the
   * template exists is the provider's business.
   */
  template: z
    .string()
    .min(1)
    .max(64)
    .regex(/^[a-z0-9][a-z0-9-]*$/, 'must be a lowercase slug'),
  userId: z.string().min(1).max(64),
})

export type SendEmailPayload = z.output<typeof sendEmailPayloadSchema>

export interface SendEmailResult {
  delivered: true
  providerMessageId: string
  template: string
}

export const sendEmailJob = defineJob<'email.send', typeof sendEmailPayloadSchema, SendEmailResult>(
  {
    name: 'email.send',
    queue: QUEUE_NAMES.email,
    schema: sendEmailPayloadSchema,
  },
)

/**
 * The seam a real deployment replaces with Postmark/SES/Resend. Kept as an
 * interface so the kit ships no provider SDK and no outbound network calls.
 */
export interface EmailProvider {
  send(message: {
    to: string
    template: string
    userId: string
    correlationId: string
    signal: AbortSignal
  }): Promise<{ providerMessageId: string }>
}

/** Templates the demo provider knows about. An unknown one is a permanent error. */
const KNOWN_TEMPLATES = new Set(['welcome', 'password-reset', 'receipt', 'invite'])

/**
 * In-memory stand-in for a real provider. Deterministic on purpose: a starter
 * that randomly fails makes its own tests flaky. Retry behaviour is exercised
 * by dedicated processors in the integration tests instead.
 */
export function createDemoEmailProvider(options: { latencyMs?: number } = {}): EmailProvider {
  const latencyMs = options.latencyMs ?? 25

  return {
    async send(message) {
      if (!KNOWN_TEMPLATES.has(message.template)) {
        // Retrying cannot conjure a template into existence, so do not burn
        // four more attempts discovering that.
        throw new NonRetryableError(`Unknown email template "${message.template}"`)
      }

      await sleep(latencyMs, message.signal)

      if (message.to.endsWith('@example.invalid')) {
        // Shaped like a provider 502: worth retrying.
        throw new TransientError('Email provider temporarily rejected the request')
      }

      return { providerMessageId: `demo_${message.userId}_${Date.now().toString(36)}` }
    },
  }
}

export function createSendEmailProcessor(
  provider: EmailProvider,
): JobProcessor<SendEmailPayload, SendEmailResult> {
  return async (context) => {
    const { data, logger, signal, correlationId } = context

    logger.debug({ template: data.template }, 'sending email')

    const { providerMessageId } = await provider.send({
      to: data.to,
      template: data.template,
      userId: data.userId,
      correlationId,
      signal,
    })

    // Note the absence of `data.to` here: recipient addresses are personal data
    // and do not belong in an info-level log line.
    logger.info({ template: data.template, providerMessageId }, 'email delivered')

    return { delivered: true, providerMessageId, template: data.template }
  }
}
