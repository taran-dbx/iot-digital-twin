/**
 * One Genie query attachment: description, a collapsible "Show SQL" block,
 * and the fetched result rendered by {@link GenieResultChart}.
 */
import { useCallback, useEffect, useState } from "react";

import { GenieResultChart } from "@/components/dashboard/genie-result-chart";
import { api } from "@/lib/api";
import type { PipelineMode } from "@/lib/industries-api";
import type { GenieAttachmentQuery, GenieQueryResult } from "@/lib/types";

interface GenieQueryAttachmentProps {
  setupId: string;
  mode: PipelineMode;
  conversationId: string;
  messageId: string;
  attachmentId: string;
  query: GenieAttachmentQuery;
}

interface ResultState {
  result: GenieQueryResult | null;
  loading: boolean;
  error: string | null;
}

const INITIAL: ResultState = { result: null, loading: true, error: null };

export function GenieQueryAttachment({
  setupId,
  mode,
  conversationId,
  messageId,
  attachmentId,
  query,
}: GenieQueryAttachmentProps) {
  const [state, setState] = useState<ResultState>(INITIAL);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setState(INITIAL);
    api
      .getPipelineGenieAttachmentResult(setupId, mode, conversationId, messageId, attachmentId)
      .then((result) => {
        if (!cancelled) setState({ result, loading: false, error: null });
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setState({
            result: null,
            loading: false,
            error: err instanceof Error ? err.message : "Result unavailable",
          });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [setupId, mode, conversationId, messageId, attachmentId, attempt]);

  const retry = useCallback(() => setAttempt((n) => n + 1), []);

  return (
    <section className="genie-attachment" data-testid="genie-query-attachment" aria-label="Genie query result">
      {query.description && <p className="genie-attachment__desc">{query.description}</p>}
      <details className="genie-attachment__sql" data-testid="genie-sql">
        <summary>Show SQL</summary>
        <pre>
          <code>{query.sql}</code>
        </pre>
      </details>
      <GenieResultChart
        result={state.result}
        loading={state.loading}
        error={state.error}
        onRetry={retry}
      />
    </section>
  );
}
