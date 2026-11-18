import type { Page } from "@playwright/test";

import { manufacturingCatalog } from "./dashboard-apis";

export const PIPELINE_SETUP_ID = "setup-mfg-1";
export const PIPELINE_DEPLOYMENT_ID = "deploy-mfg-rt";
const GENIE_BASE = `/api/genie/pipelines/${PIPELINE_SETUP_ID}/realtime`;

/** Catalog whose realtime mode is a running industry-first deployment. */
export const deployedManufacturingCatalog = {
  ...manufacturingCatalog,
  industries: manufacturingCatalog.industries.map((industry) => ({
    ...industry,
    modes: industry.modes.map((mode) => ({
      ...mode,
      status: "running",
      deployment_id: PIPELINE_DEPLOYMENT_ID,
      setup_id: PIPELINE_SETUP_ID,
    })),
  })),
};

export const starterQuestions = [
  "How has spindle temperature trended this shift?",
  "Which station raised the most alerts?",
];

const lineResult = {
  columns: [
    { name: "window_start", type: "TIMESTAMP" },
    { name: "avg_spindle_temp", type: "DOUBLE" },
  ],
  rows: [
    ["2026-11-18T08:00:00Z", "61.2"],
    ["2026-11-18T08:05:00Z", "63.9"],
    ["2026-11-18T08:10:00Z", "67.4"],
  ],
  truncated: true,
};

const barResult = {
  columns: [
    { name: "station", type: "STRING" },
    { name: "alert_count", type: "BIGINT" },
  ],
  rows: [
    ["MILL", "7"],
    ["INSPECT", "2"],
    ["PACK", "1"],
  ],
  truncated: false,
};

const tableResult = {
  columns: [
    { name: "station", type: "STRING" },
    { name: "fault_code", type: "STRING" },
    { name: "operator", type: "STRING" },
  ],
  rows: [["MILL", "M-SPINDLE-HOT", "shift-a"]],
  truncated: false,
};

const messages: Record<string, Record<string, unknown>> = {
  "msg-1": {
    conversation_id: "conv-1",
    message_id: "msg-1",
    status: "COMPLETED",
    content: starterQuestions[0],
    error: null,
    attachments: [
      { attachment_id: "att-text-1", text: "Spindle temperature climbed 6°C over the window." },
      {
        attachment_id: "att-line",
        query: { sql: "SELECT window_start, avg(temp) FROM spindle GROUP BY 1", description: "Average spindle temperature per 5 minutes" },
      },
      { attachment_id: "att-suggest", suggested_questions: ["Which station raised the most alerts?"] },
    ],
  },
  "msg-2": {
    conversation_id: "conv-1",
    message_id: "msg-2",
    status: "COMPLETED",
    content: "Which station raised the most alerts?",
    error: null,
    attachments: [
      { attachment_id: "att-bar", query: { sql: "SELECT station, count(*) FROM alerts GROUP BY 1", description: null } },
      { attachment_id: "att-table", query: { sql: "SELECT station, fault_code, operator FROM alerts", description: "Raw alerts" } },
    ],
  },
};

const results: Record<string, unknown> = {
  "att-line": lineResult,
  "att-bar": barResult,
  "att-table": tableResult,
};

export async function mockPipelineGenie(
  page: Page,
  options: { available?: boolean; reason?: string; failResults?: boolean } = {},
) {
  const available = options.available ?? true;
  let nextMessage = 1;

  await page.route("**/api/industries/catalog", async (route) => {
    await route.fulfill({ json: deployedManufacturingCatalog });
  });

  await page.route(`**${GENIE_BASE}/**`, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname.slice(GENIE_BASE.length);

    if (path === "/status") {
      await route.fulfill({
        json: { available, space_id: available ? "space-1" : null, reason: options.reason },
      });
      return;
    }
    if (path === "/starters") {
      await route.fulfill({ json: { questions: starterQuestions } });
      return;
    }
    if (request.method() === "POST" && /^\/conversations(\/conv-1\/messages)?$/.test(path)) {
      const messageId = `msg-${nextMessage}`;
      nextMessage += 1;
      await route.fulfill({ json: { conversation_id: "conv-1", message_id: messageId } });
      return;
    }
    const resultMatch = path.match(/^\/conversations\/conv-1\/messages\/(msg-\d)\/attachments\/([^/]+)\/result$/);
    if (resultMatch) {
      if (options.failResults) {
        await route.fulfill({ status: 502, json: { detail: "warehouse unavailable" } });
        return;
      }
      await route.fulfill({ json: results[resultMatch[2]] });
      return;
    }
    const messageMatch = path.match(/^\/conversations\/conv-1\/messages\/(msg-\d)$/);
    if (messageMatch && messages[messageMatch[1]]) {
      await route.fulfill({ json: messages[messageMatch[1]] });
      return;
    }
    await route.fulfill({ status: 404, json: { detail: `Unexpected Genie request: ${path}` } });
  });
}
