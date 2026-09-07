import { createHash, timingSafeEqual } from 'node:crypto'

import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter'
import { FastifyAdapter } from '@bull-board/fastify'
import basicAuth from '@fastify/basic-auth'
import type { FastifyInstance } from 'fastify'

import type { QueueRegistry } from '../queue/registry.js'

/**
 * Bull Board, the operator dashboard.
 *
 * It is genuinely useful for inspecting the dead-letter queue and retrying a
 * job by hand, and it is also a fully privileged control surface: it can
 * retry, remove and drain queues. So it is opt-in, always behind
 * authentication, and never anonymous.
 *
 * Basic auth over plain HTTP still sends the password in the clear on every
 * request. It is adequate on a laptop and behind a TLS-terminating proxy; it is
 * not a substitute for real access control. See `docs/operations.md`.
 */

export const BULL_BOARD_BASE_PATH = '/admin/queues'

export interface RegisterBullBoardOptions {
  app: FastifyInstance
  queues: QueueRegistry
  username: string
  password: string
}

/**
 * Compares credentials without leaking timing information.
 *
 * `timingSafeEqual` throws on length mismatch, which would itself leak the
 * expected length, so both sides are hashed to a fixed 32 bytes first and the
 * digests are compared instead.
 */
function safeEqual(candidate: string, expected: string): boolean {
  const a = createHash('sha256').update(candidate).digest()
  const b = createHash('sha256').update(expected).digest()
  return timingSafeEqual(a, b)
}

export async function registerBullBoard(options: RegisterBullBoardOptions): Promise<void> {
  const { app, queues, username, password } = options

  // Registered inside an encapsulated plugin so the auth hook and Bull Board's
  // own view/static plugins apply to this prefix only, and cannot alter how the
  // rest of the API parses or renders responses.
  await app.register(
    async (scope) => {
      await scope.register(basicAuth, {
        authenticate: { realm: 'queue-worker-kit' },
        validate: (candidateUser, candidatePassword, _request, _reply, done) => {
          // Both comparisons always run: short-circuiting on the username would
          // make it possible to probe for a valid one.
          const userOk = safeEqual(candidateUser, username)
          const passwordOk = safeEqual(candidatePassword, password)

          if (userOk && passwordOk) {
            done()
            return
          }
          done(new Error('Invalid credentials'))
        },
      })

      // `onRequest` so authentication happens before any body is parsed or any
      // dashboard route is matched.
      scope.addHook('onRequest', scope.basicAuth)

      const serverAdapter = new FastifyAdapter()
      serverAdapter.setBasePath(BULL_BOARD_BASE_PATH)

      // Only registry queues are exposed, so the dashboard cannot be pointed at
      // an arbitrary keyspace.
      createBullBoard({
        queues: queues.all().map((queue) => new BullMQAdapter(queue)),
        serverAdapter,
      })

      // No prefix here: the enclosing plugin is already mounted at
      // BULL_BOARD_BASE_PATH, and setBasePath above tells the UI where it lives.
      await scope.register(serverAdapter.registerPlugin())
    },
    { prefix: BULL_BOARD_BASE_PATH },
  )
}
