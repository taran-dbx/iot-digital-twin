import { mockModeDashboardApis } from "../fixtures/dashboard-apis";
import {
  WRAPPED_PROMPT,
  mockPipelineGenie,
  setGeniePollHooks,
  starterQuestions,
  type PipelineGenieMockOptions,
} from "../fixtures/pipeline-genie";
import { expect as baseExpect, test } from "../fixtures/test";
import { ModeDashboardPage } from "../pages/mode-dashboard.page";
import type { Page } from "@playwright/test";

// Polling + result fetches race the WebGL twin for the main thread; allow slack.
const expect = baseExpect.configure({ timeout: 45_000 });

type PollHooks = { pollIntervalMs?: number; pollDeadlineMs?: number };

async function openCell(page: Page, options: PipelineGenieMockOptions = {}, hooks: PollHooks = { pollIntervalMs: 200 }) {
  await setGeniePollHooks(page, hooks);
  await mockModeDashboardApis(page);
  const mock = await mockPipelineGenie(page, options);
  const dashboard = new ModeDashboardPage(page);
  await dashboard.openManufacturingCell();
  await dashboard.ensureGenieOpen();
  return { dashboard, mock };
}

test.describe("[Windsor] Per-pipeline Genie on the manufacturing cell", () => {
  // The cell page mounts the WebGL twin, which is slow under headless Chromium;
  // run these serially so parallel twins do not starve each other.
  test.describe.configure({ mode: "serial", timeout: 300_000 });

  test("gates on space status and explains why it is not provisioned [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      available: false,
      reason: "Space is provisioned after the first deploy.",
    });

    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await expect(dashboard.genieDisabled).toContainText("Space is provisioned after the first deploy.");
    await expect(dashboard.genieInput).toBeDisabled();
    await expect(page.getByTestId("genie-starters")).toHaveCount(0);
  });

  test("distinguishes an existing but unusable space [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      available: false,
      spaceId: "space-1",
      reason: "Genie space tables do not match the pipeline allowlist.",
    });

    await expect(dashboard.genieStatus).toHaveText("Unavailable");
    await expect(dashboard.genieDisabled).toContainText("Genie space unavailable");
    await expect(dashboard.genieDisabled).not.toContainText("not provisioned");
    await expect(dashboard.genieDisabled).toContainText("allowlist");
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("treats a 404 from /status as not provisioned [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { statusNotFound: true });

    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await expect(dashboard.genieDisabled).toContainText("No Genie space exists for this pipeline yet");
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("polls through pending statuses; starter → line, follow-up → bar + table [smoke]", async ({ page }) => {
    const { dashboard, mock } = await openCell(page);
    await expect(dashboard.genieStatus).toHaveText("Ready");

    const starters = page.getByRole("group", { name: "Starter questions" });
    await expect(starters.getByRole("button")).toHaveCount(starterQuestions.length);
    await starters.getByRole("button", { name: starterQuestions[0] }).click();

    const first = dashboard.genieTranscript.getByTestId("genie-answer").first();
    await expect(first).toContainText("Spindle temperature climbed");
    expect(mock.polledStatuses.slice(0, 3)).toEqual(["SUBMITTED", "ASKING_AI", "COMPLETED"]);
    await expect(dashboard.genieTranscript).not.toContainText(WRAPPED_PROMPT);

    const lineResult = first.getByTestId("genie-result");
    await expect(lineResult).toHaveAttribute("data-chart-kind", "line");
    await expect(lineResult.getByTestId("genie-result-line")).toBeVisible();
    await expect(lineResult.getByTestId("genie-result-truncated")).toHaveText(/truncated to 3 rows/);
    await expect(first).toContainText("Average spindle temperature per 5 minutes");

    const sql = first.getByTestId("genie-sql");
    await sql.getByText("Show SQL").click();
    await expect(sql.locator("code")).toContainText("SELECT window_start");

    await lineResult.getByTestId("genie-result-view-table").click();
    await expect(lineResult.getByRole("region", { name: "Query result table" })).toContainText("avg_spindle_temp");
    await expect(lineResult.getByTestId("genie-result-view-table")).toHaveAttribute("aria-pressed", "true");

    const followUps = page.getByRole("group", { name: "Suggested follow-up questions" });
    await followUps.getByRole("button", { name: "Which station raised the most alerts?" }).click();

    const second = dashboard.genieTranscript.getByTestId("genie-answer").nth(1);
    const bar = second.getByTestId("genie-result").nth(0);
    const table = second.getByTestId("genie-result").nth(1);
    await expect(bar).toHaveAttribute("data-chart-kind", "bar");
    await expect(bar.getByTestId("genie-result-bar")).toBeVisible();
    await expect(table).toHaveAttribute("data-chart-kind", "table");
    await expect(table.getByTestId("genie-result-table")).toContainText("M-SPINDLE-HOT");
    await expect(table.getByTestId("genie-result-view-chart")).toHaveCount(0);
  });

  test("recovers from an expired conversation (404) by asking again [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { expireFirstMessage: true });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    await expect(page.getByTestId("genie-error")).toHaveText("This conversation expired - ask again.");
    await expect(dashboard.genieInput).toBeEnabled();

    await dashboard.genieInput.fill("Which station raised the most alerts?");
    await dashboard.genieSend.click();
    await expect(dashboard.genieTranscript.getByTestId("genie-answer").first().getByTestId("genie-result").first()).toHaveAttribute(
      "data-chart-kind",
      "bar",
    );
    await expect(page.getByTestId("genie-error")).toHaveCount(0);
  });

  test("stops polling at the deadline with an explicit timeout [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { neverFinish: true }, { pollIntervalMs: 200, pollDeadlineMs: 1500 });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    await expect(page.getByTestId("genie-error")).toContainText("taking too long");
    await expect(dashboard.genieStatus).toHaveText("Ready");
    await expect(dashboard.genieInput).toBeEnabled();
  });

  test("shows an explicit, retryable error when a result fails to load [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { failResults: true });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    const error = dashboard.genieTranscript.getByTestId("genie-result-error");
    await expect(error).toContainText("warehouse unavailable");
    await expect(error.getByRole("button", { name: "Retry" })).toBeVisible();
  });
});
