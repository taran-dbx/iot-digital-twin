# Playwright UI QA

This suite tests the committed production SPA in `static/dist`. It serves the
bundle with SPA fallback behavior and mocks only the APIs required by each
scenario.

```bash
cd qa/playwright
npm install
npx playwright install chromium
npm test
```

Set `PLAYWRIGHT_BASE_URL` to run the same tests against a deployed app instead
of the local static server.

Reports are written to `qa-reports/`:

- `results/playwright.xml` — JUnit for CI/TestQuality ingestion
- `results/playwright.json` — input for `qa-shadow-report`
- `playwright-report/` — HTML report
- `playwright-artifacts/` — screenshots, traces, and videos

The project-scoped Playwright browser connector lives in `.cursor/mcp.json`.
Reload Cursor after changing that file so its browser tools are rediscovered.
