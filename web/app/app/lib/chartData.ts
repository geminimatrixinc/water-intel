import type { AnomalyRecord } from "./types";

export type AnomalyTimelinePoint = {
  timestamp: string;
  anomalyScore: number;
  parameter: string;
  dateLabel: string;
  dateTimeLabel: string;
};

export type ParameterBreakdownSlice = {
  id: string;
  label: string;
  value: number;
  color: string;
};

const DONUT_COLORS = [
  "#38bdf8",
  "#818cf8",
  "#34d399",
  "#fbbf24",
  "#f87171",
  "#c084fc",
];

function formatDateLabel(value: string) {
  return new Date(value).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function formatDateTimeLabel(value: string) {
  return new Date(value).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function buildAnomalyTimeline(anomalies: AnomalyRecord[]): AnomalyTimelinePoint[] {
  return [...anomalies]
    .sort((left, right) => left.timestamp.localeCompare(right.timestamp))
    .map((anomaly) => ({
      timestamp: anomaly.timestamp,
      anomalyScore: anomaly.anomaly_score,
      parameter: anomaly.parameter,
      dateLabel: formatDateLabel(anomaly.timestamp),
      dateTimeLabel: formatDateTimeLabel(anomaly.timestamp),
    }));
}

export function buildRecentTrendPoints(
  anomalies: AnomalyRecord[],
  maxPoints = 12,
): AnomalyTimelinePoint[] {
  return buildAnomalyTimeline(anomalies).slice(-maxPoints);
}

export function buildParameterBreakdown(
  anomalies: AnomalyRecord[],
  maxSlices = 5,
): ParameterBreakdownSlice[] {
  const counts = new Map<string, number>();

  for (const anomaly of anomalies) {
    const key = anomaly.parameter || "Unknown";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  const entries = [...counts.entries()]
    .sort((left, right) => right[1] - left[1])
    .map(([label, value]) => ({ label, value }));

  const visibleEntries =
    entries.length > maxSlices
      ? [
          ...entries.slice(0, maxSlices - 1),
          {
            label: "Other",
            value: entries.slice(maxSlices - 1).reduce((sum, entry) => sum + entry.value, 0),
          },
        ]
      : entries;

  return visibleEntries.map((entry, index) => ({
    id: entry.label,
    label: entry.label,
    value: entry.value,
    color: DONUT_COLORS[index % DONUT_COLORS.length],
  }));
}
