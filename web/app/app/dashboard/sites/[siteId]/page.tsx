import Link from "next/link";
import { notFound } from "next/navigation";
import { Alert } from "@heroui/react";

import { getAppUrl } from "../../../lib/api";
import { formatDisplayDate } from "../../../lib/dates";
import {
  anomalyDefinition,
  anomalyHistoryTitle,
  anomalyTimelineCaption,
  historicalRecordLabel,
  humanizeDriverHints,
  recommendedAction,
  riskLegend,
  roleBasedUsage,
  scadaRequirementNote,
  siteSummaryInterpretation,
  sourceMonitoringLabel,
  sourceWaterBanner,
  trendAnomalyLabel,
  upstreamRiskLabel,
  whyThisMattersTitle,
} from "../../../lib/interpretation";
import { getStationMetadata } from "../../../lib/stations";
import type { SiteDetailResponse } from "../../../lib/types";

import AnomalyParameterDonut from "./AnomalyParameterDonut";
import AnomalyTimeline from "./AnomalyTimeline";

async function getSiteDetail(siteId: string): Promise<SiteDetailResponse> {
  const res = await fetch(getAppUrl(`/api/sites/${siteId}`), {
    cache: "no-store",
  });

  if (res.status === 404) {
    notFound();
  }

  if (!res.ok) {
    throw new Error("Failed to load site detail");
  }

  return (await res.json()) as SiteDetailResponse;
}

function riskAccentColor(label: SiteDetailResponse["site"]["risk_label"]) {
  if (label === "Safe") {
    return "#34d399";
  }

  if (label === "Watch") {
    return "#fbbf24";
  }

  return "#f87171";
}

export default async function SiteDetailPage({
  params,
}: {
  params: Promise<{ siteId: string }>;
}) {
  const { siteId } = await params;
  let detail: SiteDetailResponse;

  try {
    detail = await getSiteDetail(siteId);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load site detail";

    return (
      <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
        <Link href="/dashboard" style={{ textDecoration: "none" }}>
          ← Back
        </Link>
        <Alert
          status="danger"
          title={message}
          style={{
            marginTop: 18,
            borderRadius: 12,
            background: "#18181b",
            border: "1px solid rgba(248, 113, 113, 0.35)",
            color: "#f5f5f5",
            boxShadow: "0 18px 40px rgba(0, 0, 0, 0.22)",
          }}
        />
      </main>
    );
  }

  const { site, anomalies } = detail;
  const riskColor = riskAccentColor(site.risk_label);
  const metadata = getStationMetadata(site.station_id);
  const usage = roleBasedUsage();

  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <Link href="/dashboard" style={{ textDecoration: "none" }}>
        ← Back
      </Link>

      <h1 style={{ fontSize: 26, fontWeight: 750, marginTop: 14 }}>
        {metadata.name}
      </h1>
      <div style={{ marginTop: 6, color: "#111111" }}>{metadata.location}</div>
      <div
        style={{
          marginTop: 8,
          color: "#666666",
          fontSize: 13,
        }}
      >
        {sourceMonitoringLabel()} <span style={{ color: "#ffffff", fontWeight: 700 }}>{site.station_id}</span>
      </div>
      <div style={{ marginTop: 10 }}>
        <Alert
          status="default"
          className="rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
        >
          <Alert.Indicator className="text-zinc-300" />
          <Alert.Content className="gap-1">
            <Alert.Title className="font-semibold text-white">
              {sourceWaterBanner()}
            </Alert.Title>
            <Alert.Description className="text-sm text-zinc-300">
              {scadaRequirementNote()}
            </Alert.Description>
          </Alert.Content>
        </Alert>
      </div>

      <div
        style={{
          marginTop: 18,
          display: "grid",
          gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
          gap: 12,
          alignItems: "stretch",
        }}
      >
        <section
          style={{
            borderRadius: 16,
            padding: 18,
            border: `1px solid ${riskColor}33`,
            background: "#18181b",
            color: "#f5f5f5",
            boxShadow: "0 18px 40px rgba(0, 0, 0, 0.18)",
          }}
        >
          <div style={{ fontSize: 13, color: "#d4d4d8" }}>{upstreamRiskLabel()}</div>
          <div
            style={{
              fontSize: 40,
              fontWeight: 800,
              lineHeight: 1.1,
              marginTop: 6,
              color: riskColor,
            }}
          >
            {site.risk_score}
          </div>
          <div style={{ marginTop: 8, fontWeight: 800, color: riskColor }}>{site.risk_label}</div>
          <div style={{ marginTop: 8, fontSize: 13, color: riskColor, fontWeight: 700 }}>
            {riskLegend(site.risk_label)}
          </div>
          <div style={{ marginTop: 8, fontSize: 13, color: "#a1a1aa" }}>
            Last updated {formatDisplayDate(site.last_reading_date)}
          </div>
        </section>

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
          <div style={{ fontSize: 13, color: "#d4d4d8" }}>{historicalRecordLabel()}</div>
          <div style={{ marginTop: 6, fontWeight: 700 }}>
            {formatDisplayDate(site.date_range_start)} to {formatDisplayDate(site.date_range_end)}
          </div>
          <div style={{ marginTop: 12, fontSize: 13, color: "#d4d4d8" }}>
            {trendAnomalyLabel()} {site.anomaly_count} · 30d rolling {site.rolling_30d_anomaly_count}
          </div>
          <div style={{ marginTop: 8, fontSize: 13, color: "#d4d4d8" }}>
            Top anomaly parameter {site.top_anomaly_parameter ?? "Unavailable"}
          </div>
        </section>

      </div>

      <div style={{ marginTop: 12 }}>
        <AnomalyParameterDonut anomalies={anomalies} />
      </div>

      <div style={{ marginTop: 16 }}>
        <Alert
          status={site.risk_label === "Safe" ? "success" : site.risk_label === "Watch" ? "warning" : "danger"}
          className="rounded-2xl bg-zinc-900 shadow-sm"
          style={{ border: `1px solid ${riskColor}33` }}
        >
          <Alert.Indicator />
          <Alert.Content className="gap-1">
            <Alert.Title className="font-bold">{whyThisMattersTitle()}</Alert.Title>
            <Alert.Description className="text-zinc-200">
              <div style={{ fontSize: 14, fontWeight: 700, color: riskColor }}>
                {siteSummaryInterpretation(site)}
              </div>
              <div style={{ marginTop: 10, fontSize: 14, color: "#e4e4e7" }}>
                <span style={{ fontWeight: 700, color: riskColor }}>Recommended action:</span>{" "}
                {recommendedAction(site)}
              </div>
            </Alert.Description>
          </Alert.Content>
        </Alert>
      </div>

      <div style={{ marginTop: 16 }}>
        <Alert
          status="accent"
          className="rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
          style={{ border: "1px solid rgba(56, 189, 248, 0.35)" }}
        >
          <Alert.Indicator className="text-sky-400" />
          <Alert.Content className="gap-1">
            <Alert.Title className="font-bold text-sky-400">
              How this complements SCADA
            </Alert.Title>
            <Alert.Description className="text-zinc-200">
              <div style={{ marginTop: 2, fontSize: 14, color: "#e4e4e7" }}>
                <strong>For operators:</strong> {usage.operators}
              </div>
              <div style={{ marginTop: 10, fontSize: 14, color: "#e4e4e7" }}>
                <strong>For council / leadership:</strong> {usage.leadership}
              </div>
            </Alert.Description>
          </Alert.Content>
        </Alert>
      </div>

      {anomalies.length > 0 ? (
        <AnomalyTimeline anomalies={anomalies} riskLabel={site.risk_label} />
      ) : null}
      {anomalies.length > 0 ? (
        <p style={{ marginTop: 8, color: "#d4d4d8", fontSize: 13 }}>
          {anomalyTimelineCaption()}
        </p>
      ) : null}

      <h2 style={{ fontSize: 18, fontWeight: 700, marginTop: 22 }}>
        {anomalyHistoryTitle()}
      </h2>
      <p style={{ marginTop: 8, color: "#ffffff", fontSize: 13 }}>
        {anomalyDefinition()} Driver hints explain the source-water pattern behind each anomaly in plain language rather than model terms.
      </p>

      {anomalies.length === 0 ? (
        <p style={{ marginTop: 12, opacity: 0.75 }}>
          No anomaly records were found for this station in the current output set.
        </p>
      ) : (
        <div
          style={{
            overflowX: "auto",
            marginTop: 10,
            border: "1px solid rgba(255, 255, 255, 0.1)",
            borderRadius: 12,
            background: "#18181b",
          }}
        >
          <table
            style={{
              width: "100%",
              minWidth: 760,
              borderCollapse: "collapse",
              background: "#18181b",
            }}
          >
            <thead>
              <tr>
                <th
                  style={{
                    background: "#18181b",
                    borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
                    padding: "12px 14px",
                    textAlign: "left",
                    fontSize: 12,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#d4d4d8",
                  }}
                >
                  Timestamp
                </th>
                <th
                  style={{
                    background: "#18181b",
                    borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
                    padding: "12px 14px",
                    textAlign: "left",
                    fontSize: 12,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#d4d4d8",
                  }}
                >
                  Parameter
                </th>
                <th
                  style={{
                    background: "#18181b",
                    borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
                    padding: "12px 14px",
                    textAlign: "left",
                    fontSize: 12,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#d4d4d8",
                  }}
                >
                  Value
                </th>
                <th
                  style={{
                    background: "#18181b",
                    borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
                    padding: "12px 14px",
                    textAlign: "left",
                    fontSize: 12,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#d4d4d8",
                  }}
                >
                  Score
                </th>
                <th
                  style={{
                    background: "#18181b",
                    borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
                    padding: "12px 14px",
                    textAlign: "left",
                    fontSize: 12,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#d4d4d8",
                  }}
                >
                  Driver hints
                </th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((anomaly) => (
                <tr key={`${anomaly.timestamp}-${anomaly.parameter}`}>
                  <td
                    style={{
                      borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                      padding: "12px 14px",
                      verticalAlign: "top",
                      fontSize: 14,
                      color: "#e4e4e7",
                    }}
                  >
                    {new Date(anomaly.timestamp).toLocaleString()}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                      padding: "12px 14px",
                      verticalAlign: "top",
                      fontSize: 14,
                      color: "#e4e4e7",
                    }}
                  >
                    {anomaly.parameter}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                      padding: "12px 14px",
                      verticalAlign: "top",
                      fontSize: 14,
                      color: "#e4e4e7",
                    }}
                  >
                    {anomaly.value ?? "—"} {anomaly.unit ?? ""}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                      padding: "12px 14px",
                      verticalAlign: "top",
                      fontSize: 14,
                      color: "#e4e4e7",
                    }}
                  >
                    {anomaly.anomaly_score.toFixed(3)}
                  </td>
                  <td
                    style={{
                      borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                      padding: "12px 14px",
                      verticalAlign: "top",
                      fontSize: 14,
                      color: "#e4e4e7",
                    }}
                  >
                    {humanizeDriverHints(anomaly.top_features)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}
