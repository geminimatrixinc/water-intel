"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { AnomalyRecord } from "../../../lib/types";

type AnomalyTimelineProps = {
  anomalies: AnomalyRecord[];
};

function formatTick(value: string) {
  return new Date(value).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

export default function AnomalyTimeline({ anomalies }: AnomalyTimelineProps) {
  const data = [...anomalies]
    .sort((left, right) => left.timestamp.localeCompare(right.timestamp))
    .map((anomaly) => ({
      timestamp: anomaly.timestamp,
      anomaly_score: anomaly.anomaly_score,
      parameter: anomaly.parameter,
    }));

  return (
    <div
      style={{
        marginTop: 18,
        border: "1px solid #e5e5e5",
        borderRadius: 16,
        padding: 16,
        background: "#fff",
        color: "#111111",
      }}
    >
      <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 12, color: "#111111" }}>
        Anomaly score timeline
      </div>
      <div style={{ width: "100%", height: 260, minWidth: 0, minHeight: 260 }}>
        <ResponsiveContainer width="100%" height="100%" minWidth={0}>
          <LineChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
            <CartesianGrid stroke="#f0f0f0" strokeDasharray="4 4" />
            <XAxis dataKey="timestamp" tickFormatter={formatTick} minTickGap={24} tick={{ fill: "#111111" }} />
            <YAxis domain={[0, "dataMax"]} tick={{ fill: "#111111" }} />
            <Tooltip
              labelFormatter={(value) => new Date(value).toLocaleString()}
              contentStyle={{ color: "#111111", borderRadius: 12, border: "1px solid #e5e5e5" }}
              formatter={(value, name, item) => {
                const numericValue = typeof value === "number" ? value : Number(value ?? 0);
                return [
                  numericValue.toFixed(3),
                  `${String(name)} (${item.payload.parameter})`,
                ];
              }}
            />
            <Line
              type="monotone"
              dataKey="anomaly_score"
              stroke="#0f766e"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
