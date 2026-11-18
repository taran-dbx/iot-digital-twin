/**
 * State + polling for the per-pipeline Genie space
 * (`/api/genie/pipelines/{setup_id}/{mode}/…`). All updates are immutable;
 * mount with a `key` of setupId+mode to reset on pipeline change.
 */
import { useCallback, useEffect, useState } from "react";

import { api, isApiStatus } from "@/lib/api";
import { answerText, normalizeAttachments, type GenieAttachmentView } from "@/lib/genie-messages";
import { GENIE_EXPIRED_MESSAGE, GENIE_TIMEOUT_MESSAGE, startGeniePoll } from "@/lib/genie-poll";
import type { PipelineMode } from "@/lib/industries-api";
import type { GenieMessageStatus, GeniePipelineStatus } from "@/lib/types";

export const NOT_PROVISIONED_REASON =
  "No Genie space exists for this pipeline yet. Deploy the pipeline to create one.";

/** loading → ready | not_provisioned (no space) | unavailable (space exists, unusable) | error. */
export type SpaceState = "loading" | "ready" | "not_provisioned" | "unavailable" | "error";

export interface PipelineTranscriptEntry {
  id: string;
  role: "user" | "assistant";
  text: string | null;
  status?: GenieMessageStatus;
  conversationId?: string;
  messageId?: string;
  attachments: GenieAttachmentView[];
}

interface Starters {
  questions: string[];
  error: string | null;
}

function errorText(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

export function spaceStateOf(status: GeniePipelineStatus | null, statusError: string | null): SpaceState {
  if (statusError) return "error";
  if (!status) return "loading";
  if (status.available) return "ready";
  return status.space_id ? "unavailable" : "not_provisioned";
}

function usePipelineStatus(setupId: string, mode: PipelineMode) {
  const [status, setStatus] = useState<GeniePipelineStatus | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [starters, setStarters] = useState<Starters>({ questions: [], error: null });

  useEffect(() => {
    let cancelled = false;
    const loadStarters = () =>
      api
        .getPipelineGenieStarters(setupId, mode)
        .then((s) => {
          const questions = Array.isArray(s.questions) ? s.questions.filter((q) => typeof q === "string" && q) : [];
          if (!cancelled) setStarters({ questions, error: null });
        })
        .catch((err: unknown) => {
          if (!cancelled) setStarters({ questions: [], error: errorText(err, "Starter questions unavailable") });
        });

    api
      .getPipelineGenieStatus(setupId, mode)
      .then((body) => {
        if (cancelled) return;
        setStatus(body);
        setStatusError(null);
        if (body.available) void loadStarters();
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (isApiStatus(err, 404)) {
          setStatus({ available: false, space_id: null, reason: NOT_PROVISIONED_REASON });
        } else {
          setStatusError(errorText(err, "Genie status unavailable"));
        }
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
  const [pollStatus, setPollStatus] = useState<GenieMessageStatus>("SUBMITTED");
  const [transcript, setTranscript] = useState<PipelineTranscriptEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const spaceState = spaceStateOf(status, statusError);
  const available = spaceState === "ready";
  const pending = pendingMessageId != null;

  const expire = useCallback(() => {
    setConversationId(null);
    setPendingMessageId(null);
    setError(GENIE_EXPIRED_MESSAGE);
  }, []);

  const send = useCallback(
    async (text: string): Promise<boolean> => {
      const trimmed = text.trim();
      if (!trimmed || !available || busy || pending) return false;
      setBusy(true);
      setError(null);
      setTranscript((prev) => [...prev, { id: `u-${Date.now()}`, role: "user", text: trimmed, attachments: [] }]);
      try {
        const body = { question: trimmed };
        const ids = conversationId
          ? await api.addPipelineGenieMessage(setupId, mode, conversationId, body)
          : await api.startPipelineGenieConversation(setupId, mode, body);
        setConversationId(ids.conversation_id);
        setPendingMessageId(ids.message_id);
        setPollStatus("SUBMITTED");
        return true;
      } catch (err: unknown) {
        if (conversationId && isApiStatus(err, 404)) expire();
        else setError(errorText(err, "Genie request failed"));
        return false;
      } finally {
        setBusy(false);
      }
    },
    [available, busy, pending, conversationId, setupId, mode, expire],
  );

  useEffect(() => {
    if (!conversationId || !pendingMessageId) return undefined;
    return startGeniePoll(
      () => api.getPipelineGenieMessage(setupId, mode, conversationId, pendingMessageId),
      setPollStatus,
      (outcome) => {
        switch (outcome.kind) {
          case "done": {
            setPendingMessageId(null);
            const attachments = normalizeAttachments(outcome.message.attachments);
            const messageId = outcome.message.message_id || pendingMessageId;
            setTranscript((prev) => [
              ...prev,
              {
                id: messageId,
                role: "assistant",
                text: answerText(outcome.status, outcome.message.error, attachments),
                status: outcome.status,
                conversationId,
                messageId,
                attachments,
              },
            ]);
            return;
          }
          case "expired":
            expire();
            return;
          case "timeout":
            setPendingMessageId(null);
            setError(GENIE_TIMEOUT_MESSAGE);
            return;
          case "error":
            setPendingMessageId(null);
            setError(outcome.message);
            return;
          default: {
            const exhaustive: never = outcome;
            return exhaustive;
          }
        }
      },
    );
  }, [setupId, mode, conversationId, pendingMessageId, expire]);

  return { status, spaceState, starters, available, transcript, pollStatus, pending, busy, error, statusError, send };
}
