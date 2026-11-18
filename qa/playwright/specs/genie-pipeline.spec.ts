import { mockModeDashboardApis } from "../fixtures/dashboard-apis";
import { mockPipelineGenie, starterQuestions } from "../fixtures/pipeline-genie";
import { expect as baseExpect, test } from "../fixtures/test";
import { ModeDashboardPage } from "../pages/mode-dashboard.page";

// Polling + result fetches race the WebGL twin for the main thread; allow slack.
const expect = baseExpect.configure({ timeout: 20_000 });

test.describe("[Windsor] Per-pipeline Genie on the manufacturing cell", () => {
  // The cell page mounts the WebGL twin, which is slow under headless Chromium;
  // run these serially so parallel twins do not starve each other.
  test.describe.configure({ mode: "serial", timeout: 180_000 });

  test("gates on space status and explains why it is unavailable [defect]", async ({ page }) => {
    await mockModeDashboardApis(page);
    await mockPipelineGenie(page, { available: false, reason: "Space is provisioned after the first deploy." });
    const dashboard = new ModeDashboardPage(page);
    await dashboard.openManufacturingCell();
    await dashboard.ensureGenieOpen();

    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await expect(dashboard.genieDisabled).toContainText("Space is provisioned after the first deploy.");
    await expect(dashboard.genieInput).toBeDisabled();
    await expect(page.getByTestId("genie-starters")).toHaveCount(0);
  });

  test("starter chip → line chart, SQL, truncation; follow-up chip → bar + table [smoke]", async ({ page }) => {
    await mockModeDashboardApis(page);
    await mockPipelineGenie(page);
    const dashboard = new ModeDashboardPage(page);
    await dashboard.openManufacturingCell();
    await dashboard.ensureGenieOpen();
    await expect(dashboard.genieStatus).toHaveText("Ready");

    const starters = page.getByRole("group", { name: "Starter questions" });
    await expect(starters.getByRole("button")).toHaveCount(starterQuestions.length);
    await starters.getByRole("button", { name: starterQuestions[0] }).click();

    const first = dashboard.genieTranscript.getByTestId("genie-answer").first();
    await expect(first).toContainText("Spindle temperature climbed");
    const lineResult = first.getByTestId("genie-result");
    await expect(lineResult).toHaveAttribute("data-chart-kind", "line");
    await expect(lineResult.getByTestId("genie-result-line")).toBeVisible();
    await expect(lineResult.getByTestId("genie-result-truncated")).toHaveText(/truncated to 500 rows/);
    await expect(first).toContainText("Average spindle temperature per 5 minutes");

    const sql = first.getByTestId("genie-sql");
    await sql.getByText("Show SQL").click();
    await expect(sql.locator("code")).toContainText("SELECT window_start");

    await lineResult.getByTestId("genie-result-view-table").click();
    await expect(lineResult.getByTestId("genie-result-table")).toContainText("avg_spindle_temp");
    await expect(lineResult.getByTestId("genie-result-view-table")).toHaveAttribute("aria-pressed", "true");

    const followUps = page.getByRole("group", { name: "Suggested follow-up questions" });
    await followUps.getByRole("button", { name: "Which station raised the most alerts?" }).click();

    const second = dashboard.genieTranscript.getByTestId("genie-answer").nth(1);
    const [bar, table] = [second.getByTestId("genie-result").nth(0), second.getByTestId("genie-result").nth(1)];
    await expect(bar).toHaveAttribute("data-chart-kind", "bar");
    await expect(bar.getByTestId("genie-result-bar")).toBeVisible();
    await expect(table).toHaveAttribute("data-chart-kind", "table");
    await expect(table.getByTestId("genie-result-table")).toContainText("M-SPINDLE-HOT");
    await expect(table.getByTestId("genie-result-view-chart")).toHaveCount(0);
  });

  test("shows an explicit, retryable error when a result fails to load [defect]", async ({ page }) => {
    await mockModeDashboardApis(page);
    await mockPipelineGenie(page, { failResults: true });
    const dashboard = new ModeDashboardPage(page);
    await dashboard.openManufacturingCell();
    await dashboard.ensureGenieOpen();

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    const error = dashboard.genieTranscript.getByTestId("genie-result-error");
    await expect(error).toContainText("warehouse unavailable");
    await expect(error.getByRole("button", { name: "Retry" })).toBeVisible();
  });
});
