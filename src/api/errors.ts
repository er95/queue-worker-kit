import type { FastifyReply, FastifyRequest } from 'fastify'
import { z } from 'zod'

import { NonRetryableError, PayloadValidationError, UnknownJobError } from '../errors/errors.js'
import { serializeError } from '../utils/serialize-error.js'

/**
 * One JSON error shape for the whole API, and one rule about its contents:
 * production responses carry a stable code and a safe message, never a stack
 * trace and never an internal error string. Redis connection errors in
 * particular can echo host names and credentials.
 */

export interface ApiErrorBody {
  error: {
    code: ApiErrorCode
    message: string
    /** Only ever field-level validation detail, and only when it is safe. */
    details?: readonly string[]
  }
}

export type ApiErrorCode =
  | 'VALIDATION_ERROR'
  | 'NOT_FOUND'
  | 'UNKNOWN_JOB'
  | 'UNAUTHORIZED'
  | 'PAYLOAD_TOO_LARGE'
  | 'RATE_LIMITED'
  | 'SERVICE_UNAVAILABLE'
  | 'INTERNAL_ERROR'

export class HttpError extends Error {
  readonly statusCode: number
  readonly code: ApiErrorCode
  readonly details?: readonly string[]

  constructor(
    statusCode: number,
    code: ApiErrorCode,
    message: string,
    details?: readonly string[],
  ) {
    super(message)
    this.name = 'HttpError'
    this.statusCode = statusCode
    this.code = code
    if (details !== undefined) {
      this.details = details
    }
  }
}

export function notFound(message: string): HttpError {
  return new HttpError(404, 'NOT_FOUND', message)
}

/** Marker Fastify recognises so a zod failure becomes a 400 with field detail. */
interface FastifyValidationError extends Error {
  validation: { instancePath: string; message: string }[]
  validationContext?: string
}

function isFastifyValidationError(error: unknown): error is FastifyValidationError {
  return error instanceof Error && Array.isArray((error as { validation?: unknown }).validation)
}

/** Formats zod issues as `path: message`, with no input values. */
export function formatZodIssues(error: z.ZodError): string[] {
  return error.issues.map((issue) => {
    const path = issue.path.join('.')
    return path.length > 0 ? `${path}: ${issue.message}` : issue.message
  })
}

export function toValidationError(error: z.ZodError): FastifyValidationError {
  const validationError = Object.assign(new Error('Request validation failed'), {
    validation: error.issues.map((issue) => ({
      instancePath: `/${issue.path.join('/')}`,
      message: issue.message,
    })),
  })
  return validationError
}

function readStatusCode(error: unknown): number | undefined {
  const status = (error as { statusCode?: unknown } | null)?.statusCode
  return typeof status === 'number' && Number.isInteger(status) ? status : undefined
}

function codeForStatus(status: number): ApiErrorCode {
  switch (status) {
    case 401:
    case 403:
      return 'UNAUTHORIZED'
    case 404:
      return 'NOT_FOUND'
    case 413:
      return 'PAYLOAD_TOO_LARGE'
    case 429:
      return 'RATE_LIMITED'
    default:
      return 'VALIDATION_ERROR'
  }
}

function messageForStatus(status: number): string {
  switch (status) {
    case 401:
    case 403:
      return 'Authentication required'
    case 404:
      return 'Resource not found'
    case 413:
      return 'Request body is too large'
    case 429:
      return 'Too many requests'
    default:
      return 'The request could not be processed'
  }
}

export interface ErrorHandlerOptions {
  /** When true, unexpected errors include their message. Never in production. */
  exposeInternals: boolean
}

/**
 * Maps every error the API can produce onto a status code and a body.
 *
 * Domain errors are translated here rather than at each call site so a route
 * handler can simply let `enqueue` throw.
 */
export function buildErrorResponse(
  error: unknown,
  options: ErrorHandlerOptions,
): { statusCode: number; body: ApiErrorBody } {
  if (error instanceof HttpError) {
    return {
      statusCode: error.statusCode,
      body: {
        error: {
          code: error.code,
          message: error.message,
          ...(error.details !== undefined ? { details: error.details } : {}),
        },
      },
    }
  }

  if (error instanceof PayloadValidationError) {
    return {
      statusCode: 400,
      body: {
        error: { code: 'VALIDATION_ERROR', message: 'Invalid job payload', details: error.issues },
      },
    }
  }

  if (error instanceof UnknownJobError) {
    return {
      statusCode: 400,
      body: { error: { code: 'UNKNOWN_JOB', message: error.message } },
    }
  }

  if (error instanceof z.ZodError) {
    return {
      statusCode: 400,
      body: {
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Invalid request body',
          details: formatZodIssues(error),
        },
      },
    }
  }

  if (isFastifyValidationError(error)) {
    return {
      statusCode: 400,
      body: {
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Invalid request',
          // Fastify's own messages describe the schema, not the input.
          details: error.validation.map((issue) =>
            issue.instancePath.length > 1
              ? `${issue.instancePath.slice(1).replaceAll('/', '.')}: ${issue.message}`
              : issue.message,
          ),
        },
      },
    }
  }

  // A rejected enqueue caused by bad option values, e.g. an out-of-range delay.
  if (error instanceof NonRetryableError) {
    return {
      statusCode: 400,
      body: { error: { code: 'VALIDATION_ERROR', message: error.message } },
    }
  }

  const fastifyCode = (error as { code?: unknown } | null)?.code
  if (fastifyCode === 'FST_ERR_CTP_BODY_TOO_LARGE') {
    return {
      statusCode: 413,
      body: { error: { code: 'PAYLOAD_TOO_LARGE', message: 'Request body is too large' } },
    }
  }

  /**
   * Fastify raises plenty of errors that already carry the right status: a
   * malformed JSON body, an unsupported content type, an unparseable header.
   * Those are the caller's problem, and reporting them as 500s would bury real
   * server faults in noise. The status is honoured; the message is not, since
   * these strings come from library internals.
   */
  const statusCode = readStatusCode(error)
  if (statusCode !== undefined && statusCode >= 400 && statusCode < 500) {
    return {
      statusCode,
      body: { error: { code: codeForStatus(statusCode), message: messageForStatus(statusCode) } },
    }
  }

  // Everything else is an unexpected failure. The message is withheld in
  // production because it may quote a Redis URL or an internal host name.
  return {
    statusCode: 500,
    body: {
      error: {
        code: 'INTERNAL_ERROR',
        message: options.exposeInternals
          ? `${serializeError(error).name}: ${serializeError(error).message}`
          : 'An unexpected error occurred',
      },
    },
  }
}

/** Fastify error handler. Logs the full error, returns the safe projection. */
export function createErrorHandler(options: ErrorHandlerOptions) {
  return function handleError(
    error: unknown,
    request: FastifyRequest,
    reply: FastifyReply,
  ): FastifyReply {
    const { statusCode, body } = buildErrorResponse(error, options)

    // 4xx is the caller's problem and stays at warn; 5xx is ours.
    const log = statusCode >= 500 ? request.log.error : request.log.warn
    log.call(
      request.log,
      { err: serializeError(error), statusCode, code: body.error.code },
      'request failed',
    )

    return reply.status(statusCode).send(body)
  }
}
