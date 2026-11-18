import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { PanelRight, Send } from "lucide-react";

import { api } from "@/lib/api";
import type {
  AlertRecord,
  GenieAllowlistStatus,
  GenieAskRequest,
  GenieMessageResponse,
  GenieMessageStatus,
  GenieStatusResponse,
} from "@/lib/types";
import type { IndustryAssets, PipelineMode } from "@/lib/industries-api";

interface TranscriptEntry {
  id: string;
  role: "user" | "assistant" | "system";
  text: string;
  status?: string;
}

interface GenieCopilotProps {
  industry: string;
  mode: PipelineMode;
  assets: IndustryAssets | null;
  alert: AlertRecord | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function classifyStatus(status: string | undefined): GenieMessageStatus {
  switch (status) {
    case "IN_PROGRESS":
    case "EXECUTING_QUERY":
    case "COMPLETED":
    case "FAILED":
    case "CANCELLED":
    case "UNKNOWN":
      return status;
    case undefined:
    case "":
      return "UNKNOWN";
    default: {
      return "UNKNOWN";
    }
  }
}

function isPending(status: GenieMessageStatus): boolean {
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

function statusLabel(status: GenieMessageStatus): string {
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

function operatorFacingReason(raw: string): string {
  if (
    raw.includes("GENIE_SPACE_ID") ||
    raw.includes("genie-space") ||
    raw.includes("valueFrom")
  ) {
    return "Genie isn’t connected in this workspace. Ask an admin to attach a Genie space to the app.";
  }
  return raw;
}

function disabledReason(config: GenieStatusResponse | null, configError: string | null): string {
  return operatorFacingReason(
    config?.reason ||
      config?.table_allowlist?.requirement ||
      configError ||
      "Genie is not configured for this workspace.",
  );
}

function disabledStatusLabel(status: GenieAllowlistStatus | undefined, hasSpaceReason: boolean): string {
  if (hasSpaceReason) {
    return "Disabled";
  }
  switch (status) {
    case "mismatch":
      return "Allowlist mismatch";
    case "manual_required":
      return "Allowlist unverified";
    case "matched":
    case undefined:
      return "Disabled";
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

function defaultAssetId(assets: IndustryAssets | null): string | undefined {
  return assets?.sites[0]?.assets[0]?.asset_id;
}

function defaultComponentId(assets: IndustryAssets | null, assetId?: string): string | undefined {
  if (!assets || !assetId) return undefined;
  for (const site of assets.sites) {
    const asset = site.assets.find((item) => item.asset_id === assetId);
    if (asset) return asset.components[0]?.component_id;
  }
  return undefined;
}

export function GenieCopilot({
  industry,
  mode,
  assets,
  alert,
  open,
  onOpenChange,
}: GenieCopilotProps) {
  const [config, setConfig] = useState<GenieStatusResponse | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [question, setQuestion] = useState("");
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [pendingMessageId, setPendingMessageId] = useState<string | null>(null);
  const [pollStatus, setPollStatus] = useState<GenieMessageStatus>("UNKNOWN");
  const [transcript, setTranscript] = useState<TranscriptEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const assetId = defaultAssetId(assets);
  const componentId = defaultComponentId(assets, assetId);
  const disabled = config != null && config.enabled === false;

  useEffect(() => {
    let cancelled = false;
    api
      .getGenieStatus()
      .then((body) => {
        if (!cancelled) {
          setConfig(body);
          setConfigError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setConfigError(err instanceof Error ? err.message : "Genie status unavailable");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [industry, mode]);

  const prompts = useMemo(() => {
    const items: Array<{ id: string; label: string; question: string }> = [];
    switch (mode) {
      case "realtime":
        items.push({
          id: "live",
          label: "Live stream",
          question: "What does the live SSE stream imply for this asset right now?",
        });
        break;
      case "microbatch":
        items.push({
          id: "batch",
          label: "Micro-batch window",
          question: "How should I read the latest micro-batch Delta window for this asset?",
        });
        break;
      case "zerobus":
        items.push({
          id: "ingest",
          label: "Zerobus ingest",
          question: "What should I inspect in Zerobus ingest for this asset?",
        });
        break;
      default: {
        const exhaustive: never = mode;
        return exhaustive;
      }
    }
    if (assetId) {
      items.push({
        id: "maint",
        label: "Maintenance",
        question: "Summarize maintenance history and likely next inspection for this asset.",
      });
    }
    if (alert) {
      items.push({
        id: "alert",
        label: "Explain this alert",
        question: `Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action. Do not change alert state.`,
      });
    }
    return items;
  }, [mode, assetId, alert]);

  const askBody = useCallback(
    (text: string): GenieAskRequest => ({
      industry,
      mode,
      question: text,
      asset_id: assetId,
      component_id: componentId,
      alert_id: alert?.id,
    }),
    [industry, mode, assetId, componentId, alert],
  );

  const send = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed || disabled || busy) return;
      setBusy(true);
      setError(null);
      setTranscript((prev) => [
        ...prev,
        { id: `u-${Date.now()}`, role: "user", text: trimmed },
      ]);
      try {
        const ids = conversationId
          ? await api.addGenieMessage(conversationId, askBody(trimmed))
          : await api.startGenieConversation(askBody(trimmed));
        setConversationId(ids.conversation_id);
        setPendingMessageId(ids.message_id);
        setPollStatus("IN_PROGRESS");
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : "Genie request failed");
      } finally {
        setBusy(false);
        setQuestion("");
      }
    },
    [askBody, busy, conversationId, disabled],
  );

  useEffect(() => {
    if (!conversationId || !pendingMessageId) return undefined;
    let cancelled = false;
    const tick = () => {
      api
        .getGenieMessage(conversationId, pendingMessageId)
        .then((message: GenieMessageResponse) => {
          if (cancelled) return;
          const status = classifyStatus(String(message.status));
          setPollStatus(status);
          if (!isPending(status)) {
            setPendingMessageId(null);
            const text =
              message.content ||
              message.error ||
              (status === "FAILED" ? "Genie could not complete this investigation." : "No response");
            setTranscript((prev) => [
              ...prev,
              {
                id: message.message_id,
                role: "assistant",
                text,
                status,
              },
            ]);
          }
        })
        .catch((err: unknown) => {
          if (!cancelled) {
            setError(err instanceof Error ? err.message : "Genie poll failed");
            setPendingMessageId(null);
          }
        });
    };
    tick();
    const handle = window.setInterval(tick, 1500);
    return () => {
      cancelled = true;
      window.clearInterval(handle);
    };
  }, [conversationId, pendingMessageId]);

  useEffect(() => {
    if (alert && open) {
      setQuestion(
        `Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action.`,
      );
    }
  }, [alert, open]);

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    void send(question);
  }

  const loading = config == null && configError == null;
  const pending = pendingMessageId != null;

  return (
    <aside
      className={`genie-drawer${open ? " is-open" : ""}`}
      data-testid="genie-copilot"
      aria-label="Genie investigation"
    >
      <button
        type="button"
        className="genie-drawer__toggle"
        data-testid="genie-copilot-toggle"
        aria-expanded={open}
        onClick={() => onOpenChange(!open)}
      >
        <PanelRight size={16} aria-hidden="true" />
        <span>Investigate</span>
      </button>
      <div
        className="genie-drawer__panel"
        data-testid="genie-copilot-drawer"
        hidden={!open}
        aria-hidden={!open}
      >
        <header className="genie-drawer__head">
          <div>
            <h2>Genie investigation</h2>
            <p title={`${industry} · ${mode}${assetId ? ` · ${assetId}` : ""}`}>
              {industry} · {mode}
              {assetId ? ` · ${assetId}` : ""}
            </p>
          </div>
          <span
            className="genie-drawer__status"
            data-testid="genie-status"
            aria-live="polite"
          >
            {loading
              ? "Checking…"
              : disabled
                ? disabledStatusLabel(
                    config?.table_allowlist?.status,
                    Boolean(config?.reason?.includes("GENIE_SPACE_ID")),
                  )
                : pending
                  ? statusLabel(pollStatus)
                  : "Ready"}
          </span>
        </header>

        {loading && (
          <p className="genie-drawer__hint" aria-busy="true">
            Loading Genie configuration…
          </p>
        )}
        {(disabled || configError) && (
          <p className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
            {disabledReason(config, configError)}
          </p>
        )}
        {error && (
          <p className="genie-drawer__error" role="alert">
            {error}
          </p>
        )}

        <div className="genie-drawer__prompts">
          {prompts.map((item) => (
            <button
              type="button"
              key={item.id}
              className="btn sm"
              disabled={disabled || busy}
              onClick={() => void send(item.question)}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div
          className="genie-drawer__transcript"
          data-testid="genie-copilot-transcript"
          aria-live="polite"
        >
          {transcript.length === 0 ? (
            <p className="genie-drawer__hint">
              Ask about this asset, mode, or alert. Genie can explain and recommend; it cannot change alerts.
            </p>
          ) : (
            transcript.map((entry) => (
              <article key={entry.id} className={`genie-msg genie-msg--${entry.role}`}>
                <span>{entry.role === "user" ? "You" : "Genie"}</span>
                <p>{entry.text}</p>
              </article>
            ))
          )}
          {pending && (
            <p className="genie-drawer__hint" aria-busy="true">
              {statusLabel(pollStatus)}…
            </p>
          )}
        </div>

        <form className="genie-drawer__composer" onSubmit={onSubmit}>
          <label className="sr-only" htmlFor="genie-question">
            Investigation question
          </label>
          <textarea
            id="genie-question"
            data-testid="genie-copilot-input"
            rows={3}
            value={question}
            disabled={disabled || busy}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="Investigate this twin…"
          />
          <button
            type="submit"
            className="btn primary sm"
            data-testid="genie-copilot-send"
            disabled={disabled || busy || !question.trim()}
          >
            <Send size={14} aria-hidden="true" />
            Send
          </button>
        </form>
      </div>
    </aside>
  );
}
