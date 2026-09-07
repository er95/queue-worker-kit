import { ERROR_CAUSE_MAX_DEPTH, ERROR_STACK_MAX_LINES } from '../config/defaults.js'

/**
 * Turns an unknown thrown value into something safe to log and to store in a
 * dead-letter record.
 *
 * JavaScript lets you throw anything, and `JSON.stringify(new Error(...))`
 * yields `{}` because `message` and `stack` are not enumerable. So this walks
 * the value explicitly, bounds the stack and the `cause` chain, and never
 * assumes it was handed an `Error`.
 */
export interface SerializedError {
  name: string
  message: string
  stack?: string
  code?: string
  cause?: SerializedError
}

function truncateStack(stack: string): string {
  const lines = stack.split('\n')
  if (lines.length <= ERROR_STACK_MAX_LINES) return stack
  return [...lines.slice(0, ERROR_STACK_MAX_LINES), '    ... stack truncated'].join('\n')
}

/**
 * Property access that cannot throw.
 *
 * A getter is allowed to throw, and an object built to be hostile (or just a
 * proxy) will. This utility exists to make error reporting reliable, so it must
 * not itself become the thing that fails while reporting an error.
 */
function readProperty(value: object, key: string): unknown {
  try {
    return (value as Record<string, unknown>)[key]
  } catch {
    return undefined
  }
}

/** `code` is a de facto standard on Node and driver errors; keep it when it is a scalar. */
function readCode(value: object): string | undefined {
  const code = readProperty(value, 'code')
  if (typeof code === 'string') return code
  if (typeof code === 'number') return String(code)
  return undefined
}

export function serializeError(value: unknown, depth = 0): SerializedError {
  if (value instanceof Error) {
    const name = readProperty(value, 'name')
    const message = readProperty(value, 'message')

    const serialized: SerializedError = {
      name: typeof name === 'string' && name.length > 0 ? name : 'Error',
      message: typeof message === 'string' ? message : safeStringify(message),
    }

    const stack = readProperty(value, 'stack')
    if (typeof stack === 'string' && stack.length > 0) {
      serialized.stack = truncateStack(stack)
    }

    const code = readCode(value)
    if (code !== undefined) {
      serialized.code = code
    }

    // Bounded: `cause` chains can be circular, and a self-referencing error
    // must not turn a log line into an infinite loop.
    const cause = readProperty(value, 'cause')
    if (cause !== undefined && depth < ERROR_CAUSE_MAX_DEPTH) {
      serialized.cause = serializeError(cause, depth + 1)
    }

    return serialized
  }

  if (typeof value === 'string') {
    return { name: 'Error', message: value }
  }

  if (value === null || value === undefined) {
    return { name: 'Error', message: String(value) }
  }

  if (typeof value === 'object') {
    const message = readProperty(value, 'message')
    const name = readProperty(value, 'name')

    const serialized: SerializedError = {
      name: typeof name === 'string' && name.length > 0 ? name : 'Error',
      message: typeof message === 'string' ? message : safeStringify(value),
    }

    const code = readCode(value)
    if (code !== undefined) {
      serialized.code = code
    }

    return serialized
  }

  // Numbers, booleans, symbols, bigints, functions.
  return { name: 'Error', message: safeStringify(value) }
}

function safeStringify(value: unknown): string {
  // The values `JSON.stringify` cannot represent, handled before it is asked:
  // it returns `undefined` for these, despite its `string` return type.
  if (
    value === undefined ||
    typeof value === 'symbol' ||
    typeof value === 'function' ||
    typeof value === 'bigint'
  ) {
    return String(value)
  }

  try {
    return JSON.stringify(value)
  } catch {
    // Circular structures, throwing getters, a BigInt nested in an object.
    return '[unserializable value]'
  }
}

/** One-line summary suitable for a `failedReason` field. */
export function formatErrorReason(value: unknown, maxChars: number): string {
  const { name, message } = serializeError(value)
  const reason = message.length > 0 ? `${name}: ${message}` : name
  return reason.length > maxChars ? `${reason.slice(0, maxChars - 3)}...` : reason
}
