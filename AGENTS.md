# Repository Guidelines

## Project Structure & Module Organization
The backend lives entirely under `src/`. `server.js` loads `app.js`, which composes Express middlewares, `routes/` and `controllers/`. Business logic is separated into `services/` (event builder, log writer, pixel responder) and helpers in `utils/`. HTTP hardening resides in `middlewares/`. Production process files live at the repo root (`Dockerfile`, `ecosystem.config.js`, `scripts/deploy-prod.sh`). Keep new assets in dedicated folders (e.g., `/src/assets` if binary payloads are needed) and keep routing-only code in `/src/routes`.

## Build, Test, and Development Commands
Run `npm install` once to pull the minimal dependencies listed in `package.json`. Start the server locally with `PORT=8080 node src/server.js`; Express is configured for GET-only CORS so no extra proxying is needed. `npm test` is currently a placeholder; replace it with your preferred runner before committing tests. For container smoke checks, `docker build -t pixel-server .` mirrors the production workflow, while `./scripts/deploy-prod.sh` performs an end-to-end build, replace-run, and health check on a host.

## Coding Style & Naming Conventions
Stick to CommonJS modules, 4-space indentation, and terminating semicolons like the existing files. Use double quotes for strings, camelCase for variables/functions (`trackPixel`, `buildEvent`), and keep filenames kebab-case (`pixel.route.js`). Group middleware, controllers, and services exactly where existing peers sit so imports stay shallow. Log output is structured JSON (see `log.service.js`), so emit objects rather than ad-hoc strings.

## Testing Guidelines
Add HTTP-level tests that exercise `/health` and `/p.gif` flows, ensuring rate limits still return a pixel. Prefer Jest + Supertest (or an equivalent) and name specs `*.spec.js` under `tests/` mirroring `src/`. Capture both happy-path and noisy query-parameter cases for `buildEvent`. Update `npm test` to run the suite and keep coverage high for controllers and services; document any intentionally skipped files in the PR.

## Commit & Pull Request Guidelines
History favors short, imperative messages (`change public port`), so keep subjects under ~60 characters and describe the behavior, not the implementation. Reference issues with `Closes #123` when applicable and mention whether `.env` or deployment scripts need operator action. PRs should summarize the observable change, list manual/automated test evidence, and include screenshots or curl transcripts when altering responses. Tag reviewers who own the affected route, middleware, or infra script to keep review latency low.

## Security & Configuration Tips
Configuration comes from `src/config/env.js`, so rely on `.env` (never committed) for `PORT`, `NODE_ENV`, and future secrets. Ensure new middlewares are idempotent and do not block the pixel response path. When touching `scripts/deploy-prod.sh` or the Dockerfile, keep the health check (`/health`) in sync so operators can reuse the published curl commands.
