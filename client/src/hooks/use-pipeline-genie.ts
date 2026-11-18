/**
 * State + polling for the per-pipeline Genie space
 * (`/api/genie/pipelines/{setup_id}/{mode}/…`). All updates are immutable;
 * mount with a `key` of setupId+mode to reset on pipeline change.
 */
import { useCallback, useEffect, useState } from "react";

import { api } from "@/lib/api";
import {
  GENIE_POLL_INTERVAL_MS,
  classifyStatus,
  isPending,
  normalizeAttachments,
} from "@/lib/genie-messages";
import type { PipelineMode } from "@/lib/industries-api";
import type {
  GenieMessageStatus,
  GeniePipelineAttachment,
  GeniePipelineMessageResponse,
  GeniePipelineStatus,
} from "@/lib/types";

export interface PipelineTranscriptEntry {
  id: string;
  role: "user" | "assistant";
  text: string | null;
  status?: GenieMessageStatus;
  conversationId?: string;
  messageId?: string;
  attachments: GeniePipelineAttachment[];
}

interface Starters {
  questions: string[];
  error: string | null;
}

function errorText(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

/** Assistant text: error first, else content only when Genie sent no attachments. */
function assistantText(message: GeniePipelineMessageResponse, attachments: GeniePipelineAttachment[], status: GenieMessageStatus): string | null {
  if (message.error) return message.error;
  if (status === "FAILED") return "Genie could not complete this investigation.";
  if (attachments.length > 0) return null;
  return message.content || "No response";
}

function usePipelineStatus(setupId: string, mode: PipelineMode) {
  const [status, setStatus] = useState<GeniePipelineStatus | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [starters, setStarters] = useState<Starters>({ questions: [], error: null });

  useEffect(() => {
    let cancelled = false;
    api
      .getPipelineGenieStatus(setupId, mode)
      .then((body) => {
        if (cancelled) return;
        setStatus(body);
        setStatusError(null);
        if (!body.available) return;
        api
          .getPipelineGenieStarters(setupId, mode)
          .then((s) => {
            if (!cancelled) setStarters({ questions: Array.isArray(s.questions) ? s.questions : [], error: null });
          })
          .catch((err: unknown) => {
            if (!cancelled) setStarters({ questions: [], error: errorText(err, "Starter questions unavailable") });
          });
      })
      .catch((err: unknown) => {
        if (!cancelled) setStatusError(errorText(err, "Genie status unavailable"));
      });
    return () => {
      cancelled = true;
    };
  }, [setupId, mode]);

  return { status, statusError, starters };
}

export function usePipelineGenie(setupId: string, mode: PipelineMode) {
  const { status, statusError, starters } = usePipelineStatus(setupId, mode);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [pendingMessageId, setPendingMessageId] = useState<string | null>(null);
  const [pollStatus, setPollStatus] = useState<GenieMessageStatus>("UNKNOWN");
  const [transcript, setTranscript] = useState<PipelineTranscriptEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const available = status?.available === true;
  const pending = pendingMessageId != null;

  const send = useCallback(
    async (text: string): Promise<boolean> => {
      const trimmed = text.trim();
      if (!trimmed || !available || busy || pending) return false;
      setBusy(true);
      setError(null);
      setTranscript((prev) => [
        ...prev,
        { id: `u-${Date.now()}`, role: "user", text: trimmed, attachments: [] },
      ]);
      try {
        const body = { question: trimmed };
        const ids = conversationId
          ? await api.addPipelineGenieMessage(setupId, mode, conversationId, body)
          : await api.startPipelineGenieConversation(setupId, mode, body);
        setConversationId(ids.conversation_id);
        setPendingMessageId(ids.message_id);
        setPollStatus("IN_PROGRESS");
        return true;
      } catch (err: unknown) {
        setError(errorText(err, "Genie request failed"));
        return false;
      } finally {
        setBusy(false);
      }
    },
    [available, busy, pending, conversationId, setupId, mode],
  );

  useEffect(() => {
    if (!conversationId || !pendingMessageId) return undefined;
    let cancelled = false;
    const tick = () => {
      api
        .getPipelineGenieMessage(setupId, mode, conversationId, pendingMessageId)
        .then((message) => {
          if (cancelled) return;
          const next = classifyStatus(String(message.status));
          setPollStatus(next);
          if (isPending(next)) return;
          cancelled = true;
          setPendingMessageId(null);
          const attachments = normalizeAttachments(message.attachments);
          setTranscript((prev) => [
            ...prev,
            {
              id: message.message_id || pendingMessageId,
              role: "assistant",
              text: assistantText(message, attachments, next),
              status: next,
              conversationId,
              messageId: message.message_id || pendingMessageId,
              attachments,
            },
          ]);
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          setError(errorText(err, "Genie poll failed"));
          setPendingMessageId(null);
        });
    };
    tick();
    const handle = window.setInterval(tick, GENIE_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(handle);
    };
  }, [setupId, mode, conversationId, pendingMessageId]);

  return {
    status,
    statusError,
    starters,
    available,
    transcript,
    pollStatus,
    pending,
    busy,
    error,
    send,
  };
}
