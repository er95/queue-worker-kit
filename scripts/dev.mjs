#!/usr/bin/env node
/**
 * Runs the API and the worker together for local development.
 *
 * Why this exists rather than `pnpm run --parallel "/^dev:/"`: a nested `pnpm`
 * call resolves to whichever pnpm is first on PATH, which is not necessarily
 * the Corepack-managed version pinned in `packageManager`. When the two differ,
 * pnpm refuses to run and `pnpm dev` fails with a confusing version error.
 *
 * And rather than `tsx a & tsx b`: `&` is not a shell operator on Windows.
 *
 * So: plain Node, no dependencies, works the same on macOS, Linux and Windows.
 * `tsx` is invoked through `process.execPath` rather than the `.bin` shim, which
 * avoids the shim's platform differences entirely.
 */
import { spawn } from 'node:child_process'
import { createRequire } from 'node:module'
import process from 'node:process'

const require = createRequire(import.meta.url)
const tsxCli = require.resolve('tsx/cli')

const targets = [
  { label: 'api', entry: 'src/api/server.ts' },
  { label: 'worker', entry: 'src/worker/server.ts' },
]

const PREFIX_WIDTH = Math.max(...targets.map((t) => t.label.length))

/** Prefixes each line so two interleaved log streams stay readable. */
function forward(stream, label, target) {
  let buffered = ''
  stream.setEncoding('utf8')
  stream.on('data', (chunk) => {
    buffered += chunk
    const lines = buffered.split('\n')
    // The last element is an incomplete line; hold it until its newline lands.
    buffered = lines.pop() ?? ''
    for (const line of lines) {
      target.write(`[${label.padEnd(PREFIX_WIDTH)}] ${line}\n`)
    }
  })
  stream.on('end', () => {
    if (buffered.length > 0) {
      target.write(`[${label.padEnd(PREFIX_WIDTH)}] ${buffered}\n`)
    }
  })
}

const children = targets.map(({ label, entry }) => {
  const child = spawn(process.execPath, [tsxCli, 'watch', entry], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  })

  forward(child.stdout, label, process.stdout)
  forward(child.stderr, label, process.stderr)

  return { label, child }
})

let shuttingDown = false

function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true

  // Both children get the signal; each runs its own graceful shutdown, which is
  // the same path production takes.
  for (const { child } of children) {
    child.kill(signal)
  }
}

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    shutdown(signal)
  })
}

// If one process dies, take the other down too: a half-running stack silently
// enqueueing jobs nothing consumes is worse than a clean stop.
for (const { label, child } of children) {
  child.on('exit', (code, signal) => {
    if (!shuttingDown) {
      process.stderr.write(
        `[dev] ${label} exited (code ${code ?? 'null'}, signal ${signal ?? 'none'})\n`,
      )
      process.exitCode = code ?? 1
      shutdown('SIGTERM')
    }
  })

  child.on('error', (error) => {
    process.stderr.write(`[dev] failed to start ${label}: ${error.message}\n`)
    process.exitCode = 1
    shutdown('SIGTERM')
  })
}
