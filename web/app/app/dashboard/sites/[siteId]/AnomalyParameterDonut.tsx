"use client";

import { PieChart } from "@mui/x-charts/PieChart";

import { buildParameterBreakdown } from "../../../lib/chartData";
import type { AnomalyRecord } from "../../../lib/types";

type AnomalyParameterDonutProps = {
  anomalies: AnomalyRecord[];
};

export default function AnomalyParameterDonut({
  anomalies,
}: AnomalyParameterDonutProps) {
  const slices = buildParameterBreakdown(anomalies);

  if (slices.length === 0) {
    return (
      <section
        style={{
          border: "1px solid rgba(255, 255, 255, 0.1)",
          borderRadius: 16,
          padding: 18,
          background: "#18181b",
          color: "#f5f5f5",
          boxShadow: "0 18px 40px rgba(0, 0, 0, 0.18)",
        }}
      >
        <div style={{ fontSize: 13, color: "#d4d4d8" }}>Anomaly parameter mix</div>
        <div style={{ marginTop: 6, fontWeight: 700, color: "#ffffff" }}>
          Which source-water parameters drive this station&apos;s flagged readings
        </div>
        <div style={{ marginTop: 10, fontSize: 13, color: "#a1a1aa" }}>
          No anomaly breakdown available for this station.
        </div>
      </section>
    );
  }

  return (
    <section
      style={{
        border: "1px solid rgba(255, 255, 255, 0.1)",
        borderRadius: 16,
        padding: 18,
        background: "#18181b",
        color: "#f5f5f5",
        boxShadow: "0 18px 40px rgba(0, 0, 0, 0.18)",
      }}
    >
      <div style={{ fontSize: 13, color: "#d4d4d8" }}>Anomaly parameter mix</div>
      <div style={{ marginTop: 6, fontWeight: 700, color: "#ffffff" }}>
        Which source-water parameters drive this station&apos;s flagged readings
      </div>

      <div
        style={{
          marginTop: 14,
          display: "grid",
          gridTemplateColumns: "minmax(0, 1fr) minmax(0, 180px) minmax(0, 1fr)",
          gap: 14,
          alignItems: "center",
        }}
      >
        <div
          style={{
            display: "grid",
            alignContent: "start",
            gap: 10,
          }}
        >
          <div style={{ fontSize: 13, color: "#d4d4d8" }}>
            Total flagged readings
          </div>
          <div style={{ fontSize: 34, fontWeight: 800, lineHeight: 1, color: "#ffffff" }}>
            {slices.reduce((sum, slice) => sum + slice.value, 0)}
          </div>
          <div style={{ fontSize: 13, lineHeight: 1.6, color: "#a1a1aa" }}>
            Use this breakdown to see whether one parameter is dominating the site&apos;s
            anomaly history or whether the signal is spread across multiple source-water
            indicators.
          </div>
        </div>

        <div style={{ display: "flex", justifyContent: "center" }}>
          <PieChart
            width={180}
            height={180}
            hideLegend
            skipAnimation
            slotProps={{
              tooltip: {
                trigger: "item",
                sx: {
                  "& .MuiChartsTooltip-paper": {
                    backgroundColor: "#09090b",
                    color: "#f5f5f5",
                    border: "1px solid rgba(255, 255, 255, 0.12)",
                    borderRadius: "14px",
                    boxShadow: "0 18px 40px rgba(0, 0, 0, 0.35)",
                  },
                  "& .MuiChartsTooltip-labelCell, & .MuiChartsTooltip-valueCell": {
                    color: "#ffffff",
                  },
                  "& .MuiChartsTooltip-mark": {
                    border: "2px solid rgba(255,255,255,0.9)",
                  },
                },
              },
            }}
            series={[
              {
                data: slices,
                innerRadius: 42,
                outerRadius: 68,
                paddingAngle: 3,
                cornerRadius: 4,
                cx: 90,
                cy: 90,
                arcLabel: (item) => (item.value >= 4 ? String(item.value) : ""),
                arcLabelMinAngle: 22,
                highlightScope: { fade: "global", highlight: "item" },
                faded: {
                  additionalRadius: -4,
                  color: "rgba(255,255,255,0.2)",
                },
                valueFormatter: (value) => `${value.value} anomalies`,
              },
            ]}
            sx={{
              "& .MuiPieArcLabel-root": {
                fill: "#ffffff",
                fontSize: 11,
                fontWeight: 700,
              },
            }}
          />
        </div>

        <div style={{ display: "grid", gap: 10 }}>
          {slices.map((slice) => (
            <div
              key={slice.id}
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 12,
                borderBottom: "1px solid rgba(255,255,255,0.06)",
                paddingBottom: 8,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span
                  aria-hidden="true"
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: 999,
                    background: slice.color,
                    boxShadow: `0 0 12px ${slice.color}55`,
                  }}
                />
                <span style={{ fontSize: 13, color: "#e4e4e7" }}>{slice.label}</span>
              </div>
              <span style={{ fontSize: 13, fontWeight: 700, color: "#ffffff" }}>
                {slice.value}
              </span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
