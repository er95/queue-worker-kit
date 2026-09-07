import { describe, expect, it, vi } from 'vitest'

import { createLogger } from '../../src/observability/logger.js'
import {
  createShutdownController,
  installSignalHandlers,
  type ShutdownStep,
} from '../../src/worker/graceful-shutdown.js'

const logger = createLogger({ level: 'silent', name: 'test', pretty: false })

const step = (name: string, run: () => Promise<void>): ShutdownStep => ({ name, run })

describe('createShutdownController', () => {
  it('runs steps in declared order', async () => {
    const order: string[] = []
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit: () => undefined,
      steps: [
        step('first', async () => {
          await new Promise((resolve) => setTimeout(resolve, 10))
          order.push('first')
        }),
        step('second', async () => {
          order.push('second')
        }),
      ],
    })

    await controller.shutdown('test')

    // Sequential, not parallel: closing the connection that work travels over
    // must not race the step that stops accepting work.
    expect(order).toEqual(['first', 'second'])
  })

  /**
   * Orchestrators routinely send SIGTERM and then SIGINT. Running the sequence
   * twice would double-close workers and queues, which surfaces as spurious
   * errors during every deploy.
   */
  it('is idempotent across repeat calls', async () => {
    const run = vi.fn().mockResolvedValue(undefined)
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit: () => undefined,
      steps: [step('once', run)],
    })

    await Promise.all([
      controller.shutdown('SIGTERM'),
      controller.shutdown('SIGINT'),
      controller.shutdown('SIGTERM'),
    ])

    expect(run).toHaveBeenCalledOnce()
  })

  it('returns the same promise for concurrent calls', () => {
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit: () => undefined,
      steps: [step('noop', async () => undefined)],
    })

    expect(controller.shutdown('a')).toBe(controller.shutdown('b'))
  })

  it('reports its state', async () => {
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit: () => undefined,
      steps: [step('noop', async () => undefined)],
    })

    expect(controller.isShuttingDown()).toBe(false)
    await controller.shutdown('test')
    expect(controller.isShuttingDown()).toBe(true)
  })

  // A worker that cannot close cleanly should still release its Redis handle.
  it('continues past a failing step', async () => {
    const after = vi.fn().mockResolvedValue(undefined)
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit: () => undefined,
      steps: [
        step('broken', () => Promise.reject(new Error('close failed'))),
        step('after', after),
      ],
    })

    await expect(controller.shutdown('test')).resolves.toBeUndefined()
    expect(after).toHaveBeenCalledOnce()
  })

  it('exits 0 on a clean shutdown', async () => {
    const exit = vi.fn()
    const controller = createShutdownController({
      logger,
      timeoutMs: 5_000,
      exit,
      steps: [step('noop', async () => undefined)],
    })

    await controller.shutdown('test')
    await vi.waitFor(() => {
      expect(exit).toHaveBeenCalledWith(0)
    })
  })

  /**
   * The property that matters most: a step that never resolves must not hang
   * the process forever. Without a deadline, a wedged Redis connection means a
   * container that only ever leaves via SIGKILL.
   */
  it('stops waiting on a hanging step and exits non-zero', async () => {
    const exit = vi.fn()
    const controller = createShutdownController({
      logger,
      timeoutMs: 50,
      exit,
      steps: [step('hangs', () => new Promise<void>(() => undefined))],
    })

    await controller.shutdown('test')

    await vi.waitFor(() => {
      expect(exit).toHaveBeenCalledWith(1)
    })
  })
})

describe('installSignalHandlers', () => {
  it('shuts down on a signal and removes its listeners afterwards', async () => {
    const run = vi.fn().mockResolvedValue(undefined)
    const controller = createShutdownController({
      logger,
      timeoutMs: 1_000,
      exit: () => undefined,
      steps: [step('noop', run)],
    })

    const before = process.listenerCount('SIGTERM')
    const uninstall = installSignalHandlers({ controller, logger, signals: ['SIGTERM'] })

    expect(process.listenerCount('SIGTERM')).toBe(before + 1)

    process.emit('SIGTERM')
    await vi.waitFor(() => {
      expect(run).toHaveBeenCalledOnce()
    })

    // Leaked process listeners are exactly what makes a test suite fail on the
    // fifth file with a max-listeners warning.
    uninstall()
    expect(process.listenerCount('SIGTERM')).toBe(before)
  })

  it('removes every listener it added', () => {
    const controller = createShutdownController({
      logger,
      timeoutMs: 1_000,
      exit: () => undefined,
      steps: [],
    })

    const counts = {
      sigterm: process.listenerCount('SIGTERM'),
      sigint: process.listenerCount('SIGINT'),
      rejection: process.listenerCount('unhandledRejection'),
      exception: process.listenerCount('uncaughtException'),
    }

    installSignalHandlers({ controller, logger })()

    expect(process.listenerCount('SIGTERM')).toBe(counts.sigterm)
    expect(process.listenerCount('SIGINT')).toBe(counts.sigint)
    expect(process.listenerCount('unhandledRejection')).toBe(counts.rejection)
    expect(process.listenerCount('uncaughtException')).toBe(counts.exception)
  })
})
