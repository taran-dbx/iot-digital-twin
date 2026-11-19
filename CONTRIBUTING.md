# Contributing

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
make install        # Python dev deps, client deps and git hooks
```

## Workflow

1. Branch from `feat/industry-first-ui` (the default branch). Use `feat/`, `fix/`,
   `chore/`, `refactor/` or `docs/` prefixes.
2. Keep changes focused; one concern per pull request.
3. Run `make check` before pushing. It runs the same gates as CI.
4. Open a pull request with a summary and a test plan. A reviewer other than the author
   approves and merges.

## Commit messages

Conventional Commits: `<type>: <description>` with an optional body.
Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `ci`, `build`, `style`.

## Standards

- **Python 3.11+**, formatted and linted with ruff; mypy covers `core` and `models`.
- **Immutability first:** build new objects instead of mutating inputs.
- **Small files:** aim for 200 to 400 lines, 800 at most; split large modules by area.
- **Validate at boundaries:** check all external input; handle errors explicitly and never
  swallow them silently.
- **Tests:** add or update tests with every change; keep coverage at or above 80%. Tests
  that need a live workspace skip themselves when it is unreachable.
- **Layout:** application code lives under `src/iot_twin/`. Routes stay thin; business
  logic belongs in `services/`.
- **Frontend:** `client/` uses ESLint and `tsc`. After a frontend change, run
  `make frontend-build` and commit the refreshed `static/dist`; CI fails if it is stale.

## Secrets and workspace identifiers

Never commit secrets, tokens, workspace hosts, IDs, e-mail addresses or broker lists.
Use placeholders in git and real values in gitignored files or bundle variable overrides.
`pre-commit` runs gitleaks and `scripts/check_identifiers.py`; CI runs both.

## Deployment changes

Changes to `databricks.yml`, `resources/` or `app.yaml` must keep `make bundle-validate`
green and update [docs/deployment.md](docs/deployment.md). Never deploy a bundle over an
existing app without binding it first.
