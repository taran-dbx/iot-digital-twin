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

/** What the backend echoes as `content`: the trusted prompt wrapper. Must never render. */
export const WRAPPED_PROMPT = "[TRUSTED CONTEXT] pipeline=setup-mfg-1 [UNTRUSTED QUESTION] …";

const messages: Record<string, Record<string, unknown>> = {
  "msg-1": {
    conversation_id: "conv-1",
    message_id: "msg-1",
    status: "COMPLETED",
    content: WRAPPED_PROMPT,
    error: null,
    attachments: [
      {
        attachment_id: "att-text-1",
        text: "Spindle temperature climbed 6°C over the window.",
        query: null,
        suggested_questions: [],
      },
      {
        attachment_id: "att-line",
        text: null,
        query: { sql: "SELECT window_start, avg(temp) FROM spindle GROUP BY 1", description: "Average spindle temperature per 5 minutes" },
        suggested_questions: [],
      },
      {
        attachment_id: "att-suggest",
        text: null,
        query: null,
        suggested_questions: ["Which station raised the most alerts?"],
      },
    ],
  },
  "msg-2": {
    conversation_id: "conv-1",
    message_id: "msg-2",
    status: "COMPLETED",
    content: WRAPPED_PROMPT,
    error: null,
    attachments: [
      {
        attachment_id: "att-bar",
        text: null,
        query: { sql: "SELECT station, count(*) FROM alerts GROUP BY 1", description: null },
        suggested_questions: [],
      },
      {
        attachment_id: "att-table",
        text: null,
        query: { sql: "SELECT station, fault_code, operator FROM alerts", description: "Raw alerts" },
        suggested_questions: [],
      },
    ],
  },
};

const results: Record<string, unknown> = {
  "att-line": lineResult,
  "att-bar": barResult,
  "att-table": tableResult,
};

export interface PipelineGenieMockOptions {
  available?: boolean;
  spaceId?: string | null;
  reason?: string;
  /** /status answers 404 (unknown setup / non-UUID id). */
  statusNotFound?: boolean;
  failResults?: boolean;
  /** Polls for the first message answer 404 (conversation lost after an app restart). */
  expireFirstMessage?: boolean;
  /** Polls never reach a terminal status. */
  neverFinish?: boolean;
}

/** Non-terminal statuses served before COMPLETED on every poll sequence. */
export const PENDING_SEQUENCE = ["SUBMITTED", "ASKING_AI"] as const;

/** Speed up polling and shorten the 3-minute deadline (read by lib/genie-poll.ts). */
export async function setGeniePollHooks(page: Page, hooks: { pollIntervalMs?: number; pollDeadlineMs?: number }) {
  await page.addInitScript((value) => {
    (window as unknown as { __genieTestHooks?: unknown }).__genieTestHooks = value;
  }, hooks);
}

export async function mockPipelineGenie(page: Page, options: PipelineGenieMockOptions = {}) {
  const available = options.available ?? true;
  const spaceId = options.spaceId !== undefined ? options.spaceId : available ? "space-1" : null;
  let nextMessage = 1;
  const pollCounts = new Map<string, number>();
  const polledStatuses: string[] = [];

  await page.route("**/api/industries/catalog", async (route) => {
    await route.fulfill({ json: deployedManufacturingCatalog });
  });

  await page.route(`**${GENIE_BASE}/**`, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname.slice(GENIE_BASE.length);

    if (path === "/status") {
      if (options.statusNotFound) {
        await route.fulfill({ status: 404, json: { detail: "unknown setup" } });
        return;
      }
      await route.fulfill({ json: { available, space_id: spaceId, reason: options.reason } });
      return;
    }
    if (options.expireFirstMessage && path === "/conversations/conv-1/messages/msg-1") {
      await route.fulfill({ status: 404, json: { detail: "unknown conversation" } });
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
      const id = messageMatch[1];
      const count = pollCounts.get(id) ?? 0;
      pollCounts.set(id, count + 1);
      const status = options.neverFinish ? "ASKING_AI" : (PENDING_SEQUENCE[count] ?? "COMPLETED");
      polledStatuses.push(status);
      const body = status === "COMPLETED" ? messages[id] : { ...messages[id], status, attachments: [] };
      await route.fulfill({ json: body });
      return;
    }
    await route.fulfill({ status: 404, json: { detail: `Unexpected Genie request: ${path}` } });
  });
  return { polledStatuses };
}
