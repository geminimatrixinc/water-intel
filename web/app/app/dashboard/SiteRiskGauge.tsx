"use client";

import {
  GaugeContainer,
  GaugeReferenceArc,
  GaugeValueArc,
  gaugeClasses,
  useGaugeState,
} from "@mui/x-charts/Gauge";

import type { RiskLabel } from "../lib/types";

type SiteRiskGaugeProps = {
  riskLabel: RiskLabel;
  riskScore: number;
};

function gaugeColor(riskLabel: RiskLabel) {
  if (riskLabel === "Safe") return "#34d399";
  if (riskLabel === "Watch") return "#fbbf24";
  return "#f87171";
}

function GaugePointer({ color }: { color: string }) {
  const { valueAngle, outerRadius, cx, cy } = useGaugeState();

  if (valueAngle === null) {
    return null;
  }

  const pointerLength = outerRadius * 0.82;
  const target = {
    x: cx + pointerLength * Math.sin(valueAngle),
    y: cy - pointerLength * Math.cos(valueAngle),
  };

  return (
    <g>
      <circle cx={cx} cy={cy} r={4} fill={color} />
      <path d={`M ${cx} ${cy} L ${target.x} ${target.y}`} stroke={color} strokeWidth={3} />
    </g>
  );
}

export default function SiteRiskGauge({
  riskLabel,
  riskScore,
}: SiteRiskGaugeProps) {
  const color = gaugeColor(riskLabel);

  return (
    <div
      style={{
        marginTop: 14,
        minWidth: 120,
        width: 120,
      }}
    >
      <div
        style={{
          fontSize: 11,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          color: "#a1a1aa",
          textAlign: "center",
          marginBottom: 2,
        }}
      >
        Risk score
      </div>
      <div
        style={{
          borderRadius: 12,
          background: "transparent",
          padding: 0,
        }}
      >
        <div style={{ display: "flex", justifyContent: "center" }}>
          <GaugeContainer
            width={96}
            height={92}
            value={riskScore}
            valueMin={0}
            valueMax={100}
            startAngle={-110}
            endAngle={110}
            innerRadius="80%"
            outerRadius="100%"
            sx={{
              [`& .${gaugeClasses.referenceArc}`]: {
                fill: "rgba(255,255,255,0.12)",
              },
              [`& .${gaugeClasses.valueArc}`]: {
                fill: color,
              },
            }}
          >
            <GaugeReferenceArc />
            <GaugeValueArc />
            <GaugePointer color={color} />
          </GaugeContainer>
        </div>
      </div>
    </div>
  );
}
