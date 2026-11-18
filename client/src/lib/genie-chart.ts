/**
 * Pure chart-type selection for Genie query results.
 *
 * Rules (first match wins):
 *   - no rows                                   → "empty"
 *   - a time-like column + ≥1 numeric column     → "line" (x = time, y = numerics)
 *   - exactly one categorical + one numeric col  → "bar"
 *   - anything else                              → "table"
 *
 * No React, no DOM — safe to unit test in isolation.
 */
import type { GenieQueryResult, GenieResultColumn } from "@/lib/types";

/** Maximum line series plotted at once; the table keeps every column. */
export const MAX_LINE_SERIES = 6;
/** Row cap enforced by the backend before it sets `truncated`. */
export const GENIE_RESULT_ROW_CAP = 500;

export type GenieChartSpec =
  | { kind: "empty" }
  | { kind: "table" }
  | { kind: "line"; x: string; y: string[] }
  | { kind: "bar"; x: string; y: string };

export type GenieChartRow = Record<string, string | number | null>;

const NUMERIC_TYPE = /^(tinyint|smallint|int|integer|bigint|long|short|byte|float|double|real|decimal|numeric)\b/;
const TIME_TYPE = /^(timestamp|timestamp_ntz|timestamp_ltz|date|datetime)\b/;
const STRING_TYPE = /^(string|varchar|char|text)\b/;
const ISO_DATE_PREFIX = /^\d{4}-\d{2}-\d{2}/;

function normType(type: string): string {
  return type.trim().toLowerCase();
}

export function isNumericType(type: string): boolean {
  return NUMERIC_TYPE.test(normType(type));
}

export function isTimeType(type: string): boolean {
  return TIME_TYPE.test(normType(type));
}

function columnValues(rows: readonly unknown[][], index: number): unknown[] {
  return rows.map((row) => row[index]).filter((v) => v !== null && v !== undefined && v !== "");
}

/** ISO-looking string column whose every non-null value parses as a date. */
function looksLikeTimeStrings(values: readonly unknown[]): boolean {
  return (
    values.length > 0 &&
    values.every(
      (v) => typeof v === "string" && ISO_DATE_PREFIX.test(v) && !Number.isNaN(Date.parse(v)),
    )
  );
}

export function isTimeColumn(column: GenieResultColumn, rows: readonly unknown[][], index: number): boolean {
  if (isTimeType(column.type)) return true;
  if (!STRING_TYPE.test(normType(column.type))) return false;
  return looksLikeTimeStrings(columnValues(rows, index));
}

export function selectChartSpec(result: GenieQueryResult): GenieChartSpec {
  const { columns, rows } = result;
  if (rows.length === 0 || columns.length === 0) return { kind: "empty" };

  const timeIndex = columns.findIndex((col, i) => isTimeColumn(col, rows, i));
  const numeric = columns.filter((col, i) => i !== timeIndex && isNumericType(col.type));

  if (timeIndex >= 0 && numeric.length >= 1) {
    return {
      kind: "line",
      x: columns[timeIndex].name,
      y: numeric.slice(0, MAX_LINE_SERIES).map((col) => col.name),
    };
  }

  if (columns.length === 2 && numeric.length === 1) {
    const categorical = columns.find((col) => col.name !== numeric[0].name);
    if (categorical && !isNumericType(categorical.type)) {
      return { kind: "bar", x: categorical.name, y: numeric[0].name };
    }
  }

  return { kind: "table" };
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

function toEpochMs(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const ms = Date.parse(String(value));
  return Number.isNaN(ms) ? null : ms;
}

/**
 * Project raw rows into recharts-ready records for a chart spec. Line data is
 * keyed by epoch-ms on `x` and sorted ascending; rows with an unparseable x
 * are dropped. Returns a new array — the input result is never mutated.
 */
export function toChartRows(result: GenieQueryResult, spec: GenieChartSpec): GenieChartRow[] {
  if (spec.kind !== "line" && spec.kind !== "bar") return [];
  const indexOf = (name: string) => result.columns.findIndex((col) => col.name === name);
  const xIndex = indexOf(spec.x);
  const yNames = spec.kind === "line" ? spec.y : [spec.y];
  const yIndexes = yNames.map(indexOf);

  const projected = result.rows.map((row): GenieChartRow | null => {
    const x = spec.kind === "line" ? toEpochMs(row[xIndex]) : String(row[xIndex] ?? "—");
    if (x === null) return null;
    const entries = yNames.map((name, i) => [name, toNumber(row[yIndexes[i]])] as const);
    return { [spec.x]: x, ...Object.fromEntries(entries) };
  });

  const rows = projected.filter((row): row is GenieChartRow => row !== null);
  return spec.kind === "line"
    ? [...rows].sort((a, b) => Number(a[spec.x]) - Number(b[spec.x]))
    : rows;
}

/** Stringify a raw cell for the table view. */
export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
