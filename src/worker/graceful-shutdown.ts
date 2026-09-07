import { SHUTDOWN_EXIT_GRACE_MS } from '../config/defaults.js'
import type { AppLogger } from '../observability/logger.js'
import { serializeError } from '../utils/serialize-error.js'

/**
 * Shutdown, shared by the API and the worker.
 *
 * Two things make container shutdown go wrong in practice: handlers that run
 * twice because the orchestrator sends SIGTERM and then SIGINT, and shutdowns
 * that hang forever on a step that never resolves. This handles both. Steps run
 * in declared order rather than in parallel, because "stop accepting work" has
 * to finish before "close the connection that work travels over".
 */

export interface ShutdownStep {
  name: string
  run: () => Promise<void>
}

export interface ShutdownController {
  /** Idempotent: repeat calls return the first shutdown's promise. */
  shutdown(reason: string): Promise<void>
  isShuttingDown(): boolean
}

export interface CreateShutdownControllerOptions {
  steps: readonly ShutdownStep[]
  logger: AppLogger
  /** Hard deadline for all steps combined. */
  timeoutMs: number
  /** Injectable so tests can assert on the exit code without exiting. */
  exit?: (code: number) => void
}

export function createShutdownController(
  options: CreateShutdownControllerOptions,
): ShutdownController {
  const { steps, logger, timeoutMs } = options
  const exit = options.exit ?? ((code: number) => process.exit(code))

  let running: Promise<void> | undefined

  async function runSteps(): Promise<void> {
    for (const step of steps) {
      const startedAt = Date.now()
      try {
        await step.run()
        logger.info(
          { step: step.name, durationMs: Date.now() - startedAt },
          'shutdown step complete',
        )
      } catch (error) {
        // One failed step must not strand the rest. A worker that cannot close
        // cleanly should still get its Redis connection released.
        logger.error(
          { step: step.name, err: serializeError(error), durationMs: Date.now() - startedAt },
          'shutdown step failed',
        )
      }
    }
  }

  return {
    isShuttingDown() {
      return running !== undefined
    },

    shutdown(reason: string): Promise<void> {
      if (running !== undefined) {
        logger.debug({ reason }, 'shutdown already in progress')
        return running
      }

      logger.info({ reason, timeoutMs }, 'shutdown started')

      running = (async () => {
        let timer: NodeJS.Timeout | undefined

        const deadline = new Promise<'timeout'>((resolve) => {
          timer = setTimeout(() => {
            resolve('timeout')
          }, timeoutMs)
        })

        // The race resolves to a value rather than flipping a flag inside the
        // timer callback, which the compiler cannot observe being reassigned.
        let outcome: 'complete' | 'timeout'
        try {
          outcome = await Promise.race([runSteps().then((): 'complete' => 'complete'), deadline])
        } finally {
          if (timer !== undefined) clearTimeout(timer)
        }

        const timedOut = outcome === 'timeout'

        if (timedOut) {
          logger.error({ reason, timeoutMs }, 'shutdown deadline exceeded, forcing exit')
        } else {
          logger.info({ reason }, 'shutdown complete')
        }

        // A short grace period lets pino's transport flush. `process.exit` is
        // used only here, at the very end, never as the first response to a
        // signal: that would abandon in-flight jobs to the stalled checker.
        setTimeout(() => {
          exit(timedOut ? 1 : 0)
        }, SHUTDOWN_EXIT_GRACE_MS).unref()
      })()

      return running
    },
  }
}

export type Signal = 'SIGTERM' | 'SIGINT'

const DEFAULT_SIGNALS: readonly Signal[] = ['SIGTERM', 'SIGINT']

export interface InstallSignalHandlersOptions {
  controller: ShutdownController
  logger: AppLogger
  signals?: readonly Signal[]
}

/**
 * Wires signals and last-resort process events to the controller.
 *
 * @returns a function removing every listener it added, so tests and repeated
 * bootstraps do not stack handlers on the process object.
 */
export function installSignalHandlers(options: InstallSignalHandlersOptions): () => void {
  const { controller, logger } = options
  const signals = options.signals ?? DEFAULT_SIGNALS

  const removeListeners: (() => void)[] = []

  for (const signal of signals) {
    const handler = (): void => {
      logger.info({ signal }, 'signal received')
      void controller.shutdown(signal)
    }
    process.on(signal, handler)
    removeListeners.push(() => {
      process.off(signal, handler)
    })
  }

  const onUnhandledRejection = (reason: unknown): void => {
    logger.error({ err: serializeError(reason) }, 'unhandled promise rejection')
    void controller.shutdown('unhandledRejection')
  }
  process.on('unhandledRejection', onUnhandledRejection)
  removeListeners.push(() => {
    process.off('unhandledRejection', onUnhandledRejection)
  })

  const onUncaughtException = (error: Error): void => {
    // Process state is unknowable after this, so shut down rather than pretend
    // the worker is still healthy.
    logger.fatal({ err: serializeError(error) }, 'uncaught exception')
    void controller.shutdown('uncaughtException')
  }
  process.on('uncaughtException', onUncaughtException)
  removeListeners.push(() => {
    process.off('uncaughtException', onUncaughtException)
  })

  return () => {
    for (const remove of removeListeners) {
      remove()
    }
  }
}
