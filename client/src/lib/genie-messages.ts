/**
 * Shared Genie message helpers: exhaustive status handling for polling and
 * defensive normalisation of per-pipeline attachments (external data).
 */
import type {
  GenieAttachmentQuery,
  GenieMessageStatus,
  GeniePipelineAttachment,
} from "@/lib/types";

export const GENIE_POLL_INTERVAL_MS = 1500;

export function classifyStatus(status: string | undefined): GenieMessageStatus {
  switch (status) {
    case "IN_PROGRESS":
    case "EXECUTING_QUERY":
    case "COMPLETED":
    case "FAILED":
    case "CANCELLED":
    case "UNKNOWN":
      return status;
    default:
      return "UNKNOWN";
  }
}

export function isPending(status: GenieMessageStatus): boolean {
  switch (status) {
    case "IN_PROGRESS":
    case "EXECUTING_QUERY":
      return true;
    case "COMPLETED":
    case "FAILED":
    case "CANCELLED":
    case "UNKNOWN":
      return false;
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

export function statusLabel(status: GenieMessageStatus): string {
  switch (status) {
    case "IN_PROGRESS":
      return "Working";
    case "EXECUTING_QUERY":
      return "Querying data";
    case "COMPLETED":
      return "Complete";
    case "FAILED":
      return "Failed";
    case "CANCELLED":
      return "Cancelled";
    case "UNKNOWN":
      return "Unknown";
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringList(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const items = value.filter((v): v is string => typeof v === "string" && v.trim() !== "");
  return items.length > 0 ? items : null;
}

function parseQuery(value: unknown): GenieAttachmentQuery | null {
  if (!isRecord(value) || typeof value.sql !== "string" || value.sql.trim() === "") return null;
  return {
    sql: value.sql,
    description: typeof value.description === "string" ? value.description : null,
  };
}

/** Drop malformed attachments; never trust the wire shape. */
export function normalizeAttachments(raw: unknown): GeniePipelineAttachment[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((item, index): GeniePipelineAttachment[] => {
    if (!isRecord(item)) return [];
    const id = typeof item.attachment_id === "string" && item.attachment_id ? item.attachment_id : `att-${index}`;
    const attachment: GeniePipelineAttachment = {
      attachment_id: id,
      text: typeof item.text === "string" && item.text.trim() !== "" ? item.text : null,
      query: parseQuery(item.query),
      suggested_questions: stringList(item.suggested_questions),
    };
    const empty = !attachment.text && !attachment.query && !attachment.suggested_questions;
    return empty ? [] : [attachment];
  });
}

/** Suggested follow-ups from a message's attachments, de-duplicated, in order. */
export function suggestedQuestions(attachments: readonly GeniePipelineAttachment[]): string[] {
  const all = attachments.flatMap((att) => att.suggested_questions ?? []);
  return [...new Set(all)];
}
