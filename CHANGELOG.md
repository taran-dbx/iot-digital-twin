# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Declarative Automation Bundle (`databricks.yml`, `resources/app.yml`) with `dev` and
  `prod` targets and variable-driven configuration.
- `client/` frontend source is tracked, so `static/dist` is reproducible.
- Engineering standards: `pyproject.toml`, ruff, mypy, ESLint, pre-commit, gitleaks,
  GitHub Actions CI, Dependabot, Makefile and `.editorconfig`.
- Identifier check (`scripts/check_identifiers.py`) that blocks workspace-specific values.
- Documentation: README, architecture, deployment and contributing guides.

### Changed
- Python code moved to the `src/iot_twin/` package; `services/` grouped into subpackages.
- App start command is `uvicorn --app-dir src iot_twin.app:app`.
- Workspace profiles are templates (`*.profile.example`); real profiles are gitignored.
- `LAKETS_SQL_PATH` is optional and falls back to the packaged `lakets.sql`.
- Dev scripts require an explicit CLI profile.
- Repository layout tidied with no behaviour change: `design-ref/` -> `docs/design-ref/`,
  `config/workspaces/` and `variable-overrides.json.example` -> `deploy/`, `qa/playwright/` ->
  `tests/e2e/playwright/`, packaged `src/iot_twin/resources/` -> `src/iot_twin/assets/`, and
  `requirements-dev.txt` folded into the `dev` extra in `pyproject.toml`
  (`pip install -r requirements.txt -e ".[dev]"`).

### Removed
- Legacy static pages and `/legacy` routes, unused modules, the duplicate simulator wheel,
  `.databricksignore` (replaced by bundle `sync` rules) and committed agent/IDE files.
