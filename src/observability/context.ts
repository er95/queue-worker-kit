import { AsyncLocalStorage } from 'node:async_hooks'
import { randomUUID } from 'node:crypto'

/**
 * Ambient correlation id for the current async operation.
 *
 * Used so an HTTP handler does not have to thread a correlation id through
 * every call just to have `enqueue` stamp it onto a job. Anything on a hot
 * path still takes the id explicitly; this is a fallback, not the mechanism.
 */
export interface CorrelationContext {
  readonly correlationId: string
}

const storage = new AsyncLocalStorage<CorrelationContext>()

/** `crypto.randomUUID()`: no dependency needed for this. */
export function newCorrelationId(): string {
  return randomUUID()
}

export function runWithCorrelationId<T>(correlationId: string, fn: () => T): T {
  return storage.run({ correlationId }, fn)
}

export function getCorrelationId(): string | undefined {
  return storage.getStore()?.correlationId
}

/**
 * Resolves the correlation id to stamp on a job: an explicitly supplied id
 * wins, then the ambient one, and only then is a fresh id minted.
 */
export function resolveCorrelationId(explicit?: string): string {
  return explicit ?? getCorrelationId() ?? newCorrelationId()
}

/** Header used to accept and echo a caller-supplied correlation id. */
export const CORRELATION_ID_HEADER = 'x-correlation-id'
