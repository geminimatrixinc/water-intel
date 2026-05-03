"use client";

import { LineChart } from "@mui/x-charts/LineChart";

import { buildAnomalyTimeline } from "../../../lib/chartData";
import { anomalyTimelineTitle } from "../../../lib/interpretation";
import type { AnomalyRecord, RiskLabel } from "../../../lib/types";

type AnomalyTimelineProps = {
  anomalies: AnomalyRecord[];
  riskLabel: RiskLabel;
};

function timelineColors(riskLabel: RiskLabel) {
  if (riskLabel === "Safe") {
    return {
      stroke: "#34d399",
      glow: "rgba(52, 211, 153, 0.28)",
      areaTop: "rgba(52, 211, 153, 0.28)",
      areaBottom: "rgba(52, 211, 153, 0.02)",
      highlight: "rgba(52, 211, 153, 0.45)",
    };
  }

  if (riskLabel === "Watch") {
    return {
      stroke: "#fbbf24",
      glow: "rgba(251, 191, 36, 0.28)",
      areaTop: "rgba(251, 191, 36, 0.28)",
      areaBottom: "rgba(251, 191, 36, 0.02)",
      highlight: "rgba(251, 191, 36, 0.45)",
    };
  }

  return {
    stroke: "#f87171",
    glow: "rgba(248, 113, 113, 0.28)",
    areaTop: "rgba(248, 113, 113, 0.28)",
    areaBottom: "rgba(248, 113, 113, 0.02)",
    highlight: "rgba(248, 113, 113, 0.45)",
  };
}

export default function AnomalyTimeline({
  anomalies,
  riskLabel,
}: AnomalyTimelineProps) {
  const data = buildAnomalyTimeline(anomalies);
  const colors = timelineColors(riskLabel);

  return (
    <div
      style={{
        marginTop: 18,
        border: "1px solid rgba(255, 255, 255, 0.1)",
        borderRadius: 16,
        padding: 18,
        background: "#18181b",
        color: "#f5f5f5",
        boxShadow: "0 18px 40px rgba(0, 0, 0, 0.18)",
      }}
    >
      <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 14, color: "#ffffff" }}>
        {anomalyTimelineTitle()}
      </div>
      <div style={{ width: "100%", minWidth: 0 }}>
        <LineChart
          dataset={data}
          height={300}
          margin={{ top: 20, right: 20, bottom: 40, left: 52 }}
          grid={{ horizontal: true }}
          xAxis={[
            {
              dataKey: "timestamp",
              scaleType: "point",
              hideTooltip: true,
              tickLabelMinGap: 24,
              valueFormatter: (value: string, context?: { location?: string }) =>
                context?.location === "tick"
                  ? data.find((point) => point.timestamp === value)?.dateLabel ?? value
                  : data.find((point) => point.timestamp === value)?.dateTimeLabel ?? value,
              tickLabelStyle: {
                fill: "#a1a1aa",
                fontSize: 12,
              },
            },
          ]}
          yAxis={[
            {
              min: 0,
              tickLabelStyle: {
                fill: "#d4d4d8",
                fontSize: 12,
              },
              labelStyle: {
                fill: "#d4d4d8",
              },
            },
          ]}
          series={[
            {
              id: "anomaly-score",
              dataKey: "anomalyScore",
              label: "Anomaly score",
              color: colors.stroke,
              curve: "monotoneX",
              area: true,
              showMark: false,
            },
          ]}
          axisHighlight={{ x: "none", y: "none" }}
          skipAnimation
          slotProps={{ tooltip: { trigger: "none" } }}
          sx={{
            "& .MuiChartsAxis-line, & .MuiChartsAxis-tick": {
              stroke: "rgba(255,255,255,0.18)",
            },
            "& .MuiChartsAxis-tickLabel": {
              fill: "#a1a1aa",
            },
            "& .MuiChartsLegend-label, & .MuiChartsLegend-series text": {
              fill: "#ffffff",
              color: "#ffffff",
            },
            "& .MuiChartsGrid-line": {
              stroke: "rgba(255,255,255,0.08)",
              strokeDasharray: "4 6",
            },
            "& .MuiLineElement-root": {
              strokeWidth: 3,
              filter: `drop-shadow(0 0 10px ${colors.glow})`,
            },
            "& .MuiMarkElement-root": {
              fill: colors.stroke,
              stroke: "#ffffff",
              strokeWidth: 2,
            },
            "& .MuiAreaElement-root": {
              fill: "url(#anomalyTimelineGradient)",
            },
            "& .MuiChartsAxisHighlight-root": {
              stroke: colors.highlight,
              strokeDasharray: "4 4",
            },
          }}
        >
          <defs>
            <linearGradient id="anomalyTimelineGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={colors.areaTop} />
              <stop offset="100%" stopColor={colors.areaBottom} />
            </linearGradient>
          </defs>
        </LineChart>
      </div>
    </div>
  );
}
