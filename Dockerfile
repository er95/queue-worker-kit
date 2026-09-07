# syntax=docker/dockerfile:1

# One image, two entrypoints. The API and the worker run the same build with
# different commands, so there is no chance of the two drifting apart between
# deploys.
#
# Alpine is safe here specifically because nothing in the dependency tree needs
# to compile: `pnpm-workspace.yaml` disables every install script, and BullMQ's
# only native dependency (msgpackr-extract) is an optional accelerator with a
# pure-JS fallback. Reach for `node:24-slim` if you add a package that does
# build against glibc.
FROM node:24-alpine AS base
ENV PNPM_HOME=/pnpm \
    PATH=/pnpm:$PATH \
    # Corepack must not stop and ask whether it may download pnpm.
    COREPACK_ENABLE_DOWNLOAD_PROMPT=0
# The pnpm version comes from `packageManager` in package.json, so the build
# uses exactly the version the lockfile was written by.
RUN corepack enable
WORKDIR /app


# --- Build dependencies -----------------------------------------------------
# Separate from the source copy so a code change does not re-resolve the tree.
FROM base AS deps
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --frozen-lockfile


# --- Compile ----------------------------------------------------------------
FROM base AS build
COPY --from=deps /app/node_modules ./node_modules
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN pnpm run build


# --- Runtime dependencies ---------------------------------------------------
# Production only: no TypeScript, no Vitest, no ESLint in the shipped image.
#
# `node-linker=hoisted` produces a plain node_modules tree instead of pnpm's
# symlink farm, which copies between build stages far more predictably.
FROM base AS prod-deps
ENV NPM_CONFIG_NODE_LINKER=hoisted
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --prod --frozen-lockfile --ignore-scripts


# --- Runtime ----------------------------------------------------------------
FROM node:24-alpine AS runner

ENV NODE_ENV=production \
    # Bind all interfaces: the container's own network namespace is the boundary.
    HOST=0.0.0.0 \
    PORT=3000

WORKDIR /app

# `node` (uid 1000) ships with the image. Ownership is set on copy rather than
# with a later `chown -R`, which would duplicate every layer it touched.
COPY --from=prod-deps --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node package.json ./

# Nothing below this line runs as root.
USER node

EXPOSE 3000

# Node 24 has a global `fetch`, so the check needs no curl or wget in the image.
# Compose disables this for the worker, which serves no HTTP.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

# The API by default; compose overrides this for the worker. Signals reach Node
# directly because there is no shell in the exec form, which is what lets the
# graceful-shutdown handler run.
CMD ["node", "dist/api/server.js"]
