"use client";

import { SparkLineChart } from "@mui/x-charts/SparkLineChart";

import type { AnomalyTimelinePoint } from "../lib/chartData";
import type { RiskLabel } from "../lib/types";

type SiteTrendSparklineProps = {
  points: AnomalyTimelinePoint[];
  riskLabel: RiskLabel;
};

function trendColor(riskLabel: RiskLabel) {
  if (riskLabel === "Safe") return "#34d399";
  if (riskLabel === "Watch") return "#fbbf24";
  return "#f87171";
}

export default function SiteTrendSparkline({
  points,
  riskLabel,
}: SiteTrendSparklineProps) {
  const color = trendColor(riskLabel);
  const values = points.map((point) => point.anomalyScore);

  return (
    <div
      style={{
        marginTop: 14,
        minWidth: 180,
        maxWidth: 220,
        width: "100%",
      }}
    >
      <div
        style={{
          borderRadius: 12,
          border: `1px solid ${color}33`,
          background: "#09090b",
          padding: "8px 10px",
        }}
      >
        {values.length > 0 ? (
          <>
            <div style={{ fontSize: 12, color: "#d4d4d8" }}>
              Latest score{" "}
              <span style={{ color, fontWeight: 700 }}>
                {values[values.length - 1].toFixed(3)}
              </span>
            </div>
            <SparkLineChart
              data={values}
              height={48}
              area
              curve="monotoneX"
              color={color}
              showTooltip={false}
              showHighlight={false}
              sx={{
                "& .MuiAreaElement-root": {
                  opacity: 0.16,
                },
                "& .MuiLineElement-root": {
                  strokeWidth: 2.5,
                },
              }}
            />
            <div style={{ marginTop: 4, fontSize: 11, color: "#71717a" }}>
              Last {values.length} anomaly points
            </div>
          </>
        ) : (
          <div style={{ fontSize: 12, color: "#71717a" }}>
            No recent anomaly trend available
          </div>
        )}
      </div>
    </div>
  );
}
