/**
 * Per-pipeline Genie copilot: talks to the Genie space provisioned for one
 * pipeline setup × mode. Gated on `/status`; shows starter chips in an empty
 * conversation, follow-up chips from Genie's suggested questions, and a
 * chart/table + SQL block per query attachment.
 */
import { useEffect, useMemo, useState } from "react";

import { GenieComposer, GenieDrawer } from "@/components/dashboard/genie-drawer";
import { GenieQueryAttachment } from "@/components/dashboard/genie-query-attachment";
import { usePipelineGenie, type PipelineTranscriptEntry } from "@/hooks/use-pipeline-genie";
import { statusLabel, suggestedQuestions } from "@/lib/genie-messages";
import type { IndustryAssets, PipelineMode } from "@/lib/industries-api";
import type { AlertRecord } from "@/lib/types";

interface GeniePipelineCopilotProps {
  industry: string;
  mode: PipelineMode;
  setupId: string;
  assets: IndustryAssets | null;
  alert: AlertRecord | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function QuestionChips({
  questions,
  label,
  testId,
  disabled,
  onPick,
}: {
  questions: readonly string[];
  label: string;
  testId: string;
  disabled: boolean;
  onPick: (question: string) => void;
}) {
  if (questions.length === 0) return null;
  return (
    <div className="genie-drawer__prompts" role="group" aria-label={label} data-testid={testId}>
      {questions.map((q) => (
        <button
          type="button"
          key={q}
          className="btn sm genie-chip"
          disabled={disabled}
          title={q}
          onClick={() => onPick(q)}
        >
          {q}
        </button>
      ))}
    </div>
  );
}

function AssistantEntry({
  entry,
  setupId,
  mode,
}: {
  entry: PipelineTranscriptEntry;
  setupId: string;
  mode: PipelineMode;
}) {
  return (
    <article className="genie-msg genie-msg--assistant" data-testid="genie-answer">
      <span>Genie</span>
      {entry.text && <p>{entry.text}</p>}
      {entry.attachments.map((att) => (
        <div key={att.attachment_id} className="genie-msg__attachment">
          {att.text && <p>{att.text}</p>}
          {att.query && entry.conversationId && entry.messageId && (
            <GenieQueryAttachment
              setupId={setupId}
              mode={mode}
              conversationId={entry.conversationId}
              messageId={entry.messageId}
              attachmentId={att.attachment_id}
              query={att.query}
            />
          )}
        </div>
      ))}
    </article>
  );
}

export function GeniePipelineCopilot({
  industry,
  mode,
  setupId,
  assets,
  alert,
  open,
  onOpenChange,
}: GeniePipelineCopilotProps) {
  const genie = usePipelineGenie(setupId, mode);
  const [question, setQuestion] = useState("");
  const assetId = assets?.sites[0]?.assets[0]?.asset_id;

  useEffect(() => {
    if (alert && open) {
      setQuestion(`Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action.`);
    }
  }, [alert, open]);

  const followUps = useMemo(() => {
    const last = [...genie.transcript].reverse().find((e) => e.role === "assistant");
    return last ? suggestedQuestions(last.attachments) : [];
  }, [genie.transcript]);

  const loading = genie.status == null && genie.statusError == null;
  const unavailable = genie.status != null && !genie.available;
  const locked = !genie.available || genie.busy || genie.pending;

  const ask = (text: string) => {
    void genie.send(text).then((sent) => {
      if (sent) setQuestion("");
    });
  };

  const statusText = loading
    ? "Checking…"
    : genie.statusError
      ? "Unavailable"
      : unavailable
        ? "Not provisioned"
        : genie.pending
          ? statusLabel(genie.pollStatus)
          : "Ready";

  const notices = (
    <>
      {loading && (
        <p className="genie-drawer__hint" aria-busy="true">
          Loading Genie space…
        </p>
      )}
      {unavailable && (
        <div className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
          <strong>Genie space not provisioned yet</strong>
          {genie.status?.reason && <p>{genie.status.reason}</p>}
        </div>
      )}
      {genie.statusError && (
        <p className="genie-drawer__error" data-testid="genie-disabled" role="alert">
          Genie space status unavailable: {genie.statusError}
        </p>
      )}
      {genie.error && (
        <p className="genie-drawer__error" role="alert">
          {genie.error}
        </p>
      )}
    </>
  );

  return (
    <GenieDrawer
      open={open}
      onOpenChange={onOpenChange}
      subtitle={`${industry} · ${mode}${assetId ? ` · ${assetId}` : ""} · pipeline space`}
      statusText={statusText}
      notices={notices}
      composer={
        <GenieComposer value={question} onChange={setQuestion} onSubmit={ask} disabled={locked} />
      }
    >
      {genie.transcript.length === 0 ? (
        <>
          <p className="genie-drawer__hint">
            Ask about this pipeline’s data. Genie can explain and recommend; it cannot change alerts.
          </p>
          <QuestionChips
            questions={genie.starters.questions}
            label="Starter questions"
            testId="genie-starters"
            disabled={locked}
            onPick={ask}
          />
          {genie.starters.error && <p className="genie-drawer__hint">Starter questions unavailable.</p>}
        </>
      ) : (
        genie.transcript.map((entry) =>
          entry.role === "user" ? (
            <article key={entry.id} className="genie-msg genie-msg--user">
              <span>You</span>
              <p>{entry.text}</p>
            </article>
          ) : (
            <AssistantEntry key={entry.id} entry={entry} setupId={setupId} mode={mode} />
          ),
        )
      )}
      {genie.pending && (
        <p className="genie-drawer__hint" aria-busy="true">
          {statusLabel(genie.pollStatus)}…
        </p>
      )}
      {!genie.pending && (
        <QuestionChips
          questions={followUps}
          label="Suggested follow-up questions"
          testId="genie-followups"
          disabled={locked}
          onPick={ask}
        />
      )}
    </GenieDrawer>
  );
}
