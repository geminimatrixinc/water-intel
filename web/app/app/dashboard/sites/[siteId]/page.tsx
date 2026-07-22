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

function confidenceBadge(confidence: string | null | undefined) {
  if (confidence === "High") {
    return {
      label: "High correlation",
      color: "#86efac",
      border: "rgba(134, 239, 172, 0.35)",
      background: "rgba(22, 101, 52, 0.35)",
    };
  }

  if (confidence === "Possible") {
    return {
      label: "Possible correlation",
      color: "#fcd34d",
      border: "rgba(252, 211, 77, 0.35)",
      background: "rgba(120, 53, 15, 0.35)",
    };
  }

  if (confidence === "None") {
    return {
      label: "Checked: no public match",
      color: "#d4d4d8",
      border: "rgba(212, 212, 216, 0.3)",
      background: "rgba(63, 63, 70, 0.35)",
    };
  }

  return {
    label: "Not yet cross-referenced",
    color: "#a1a1aa",
    border: "rgba(161, 161, 170, 0.25)",
    background: "rgba(39, 39, 42, 0.45)",
  };
}

function eventTypeLabel(eventType: string | null | undefined) {
  if (!eventType) {
    return "No event context";
  }

  if (eventType === "data_quality") {
    return "Data quality";
  }

  if (eventType === "flow_spike") {
    return "Flow spike";
  }

  if (eventType === "spill") {
    return "Spill";
  }

  if (eventType === "spill_and_flow_spike") {
    return "Spill + flow spike";
  }

  if (eventType === "none") {
    return "No matching event";
  }

  return eventType.replace(/_/g, " ");
}

function formatMatchLag(
  anomalyTimestamp: string,
  matchedEventDate: string | null | undefined,
) {
  if (!matchedEventDate) {
    return null;
  }

  const anomalyDate = new Date(anomalyTimestamp);
  const eventDateMatch = matchedEventDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  const eventDate = eventDateMatch
    ? new Date(
        Number(eventDateMatch[1]),
        Number(eventDateMatch[2]) - 1,
        Number(eventDateMatch[3]),
      )
    : new Date(matchedEventDate);
  if (Number.isNaN(anomalyDate.getTime()) || Number.isNaN(eventDate.getTime())) {
    return null;
  }

  const anomalyDateOnly = new Date(
    anomalyDate.getFullYear(),
    anomalyDate.getMonth(),
    anomalyDate.getDate(),
  );
  const eventDateOnly = new Date(
    eventDate.getFullYear(),
    eventDate.getMonth(),
    eventDate.getDate(),
  );

  const diffMs = eventDateOnly.getTime() - anomalyDateOnly.getTime();
  const diffDays = Math.round(diffMs / (1000 * 60 * 60 * 24));

  if (diffDays === 0) {
    return "observed on the same day";
  }

  if (diffDays > 0) {
    return `observed ${diffDays}d later`;
  }

  return `observed ${Math.abs(diffDays)}d earlier`;
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
  const crossReferenced = anomalies.filter(
    (anomaly) => anomaly.event_confidence !== null && anomaly.event_confidence !== undefined,
  );
  const confidenceCounts = crossReferenced.reduce(
    (acc, anomaly) => {
      if (anomaly.event_confidence === "High") {
        acc.High += 1;
      } else if (anomaly.event_confidence === "Possible") {
        acc.Possible += 1;
      } else if (anomaly.event_confidence === "None") {
        acc.None += 1;
      }
      return acc;
    },
    { High: 0, Possible: 0, None: 0 },
  );
  const notCrossReferencedCount = anomalies.length - crossReferenced.length;
  const corroboratedEvents = crossReferenced.filter(
    (anomaly) =>
      anomaly.event_confidence === "High" ||
      anomaly.event_confidence === "Possible",
  );

  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
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

      {anomalies.length > 0 ? (
        <section
          style={{
            marginTop: 16,
            border: "1px solid rgba(56, 189, 248, 0.28)",
            borderRadius: 16,
            padding: 18,
            background: "#18181b",
            color: "#f5f5f5",
            boxShadow: "0 18px 40px rgba(0, 0, 0, 0.18)",
          }}
        >
          <div style={{ fontSize: 16, fontWeight: 750, color: "#ffffff" }}>
            Corroborated event context
          </div>
          <div style={{ marginTop: 6, fontSize: 14, lineHeight: 1.6, color: "#d4d4d8" }}>
            Public event records provided context for <strong>{corroboratedEvents.length}</strong>{" "}
            of the anomalies cross-referenced at this site. These are temporal correlations,
            not proof that an external event caused the reading.
          </div>

          {corroboratedEvents.length > 0 ? (
            <div style={{ marginTop: 14, display: "grid", gap: 10 }}>
              {corroboratedEvents.map((anomaly) => {
                const badge = confidenceBadge(anomaly.event_confidence);
                const lag = formatMatchLag(anomaly.timestamp, anomaly.matched_event_date);

                return (
                  <div
                    key={`event-${anomaly.timestamp}-${anomaly.parameter}`}
                    style={{
                      border: `1px solid ${badge.border}`,
                      borderLeft: `4px solid ${badge.color}`,
                      borderRadius: 8,
                      padding: "12px 14px",
                      background: badge.background,
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        flexWrap: "wrap",
                        alignItems: "center",
                        gap: 8,
                      }}
                    >
                      <span style={{ color: badge.color, fontSize: 12, fontWeight: 800 }}>
                        {badge.label}
                      </span>
                      <span style={{ color: "#ffffff", fontSize: 14, fontWeight: 700 }}>
                        {eventTypeLabel(anomaly.matched_event_type)}
                      </span>
                      <span style={{ color: "#a1a1aa", fontSize: 12 }}>
                        {anomaly.parameter} · {formatDisplayDate(anomaly.timestamp)}
                      </span>
                    </div>
                    <div style={{ marginTop: 6, color: "#e4e4e7", fontSize: 13, lineHeight: 1.55 }}>
                      {anomaly.matched_event_description}
                    </div>
                    {anomaly.matched_event_date ? (
                      <div style={{ marginTop: 5, color: "#a1a1aa", fontSize: 12 }}>
                        Public record: {formatDisplayDate(anomaly.matched_event_date)}
                        {lag ? ` · ${lag}` : ""}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          ) : (
            <div style={{ marginTop: 12, color: "#a1a1aa", fontSize: 13 }}>
              No High or Possible public-record correlations were found for this site in the
              current annex.
            </div>
          )}

          <div
            style={{
              marginTop: 14,
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
            }}
          >
            <span
              style={{
                border: "1px solid rgba(134, 239, 172, 0.35)",
                background: "rgba(22, 101, 52, 0.35)",
                color: "#86efac",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 700,
                padding: "4px 10px",
              }}
            >
              High correlation {confidenceCounts.High}
            </span>
            <span
              style={{
                border: "1px solid rgba(252, 211, 77, 0.35)",
                background: "rgba(120, 53, 15, 0.35)",
                color: "#fcd34d",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 700,
                padding: "4px 10px",
              }}
            >
              Possible correlation {confidenceCounts.Possible}
            </span>
            <span
              style={{
                border: "1px solid rgba(212, 212, 216, 0.3)",
                background: "rgba(63, 63, 70, 0.35)",
                color: "#d4d4d8",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 700,
                padding: "4px 10px",
              }}
            >
              Checked: no public match {confidenceCounts.None}
            </span>
            <span
              style={{
                border: "1px solid rgba(161, 161, 170, 0.25)",
                background: "rgba(39, 39, 42, 0.45)",
                color: "#a1a1aa",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 700,
                padding: "4px 10px",
              }}
            >
              Not yet cross-referenced {notCrossReferencedCount}
            </span>
          </div>
          <div style={{ marginTop: 10, color: "#a1a1aa", fontSize: 12, lineHeight: 1.5 }}>
            Scope: the current annex covers the top 25 anomalies across the monitoring network,
            using public spill and hydrometric flow records.
          </div>
        </section>
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
              minWidth: 980,
              tableLayout: "auto",
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
                    minWidth: 150,
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
                    minWidth: 380,
                  }}
                >
                  Reading
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
                    minWidth: 80,
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
                    minWidth: 300,
                  }}
                >
                  Event intelligence
                </th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((anomaly) => {
                const evidenceBadge = confidenceBadge(anomaly.event_confidence);
                const eventLabel = eventTypeLabel(anomaly.matched_event_type);
                const isCorroborated =
                  anomaly.event_confidence === "High" ||
                  anomaly.event_confidence === "Possible";
                const lagLabel = formatMatchLag(
                  anomaly.timestamp,
                  anomaly.matched_event_date,
                );
                const driverHints = humanizeDriverHints(anomaly.top_features)
                  .split(",")
                  .map((hint) => hint.trim())
                  .filter(Boolean);

                return (
                  <tr
                    key={`${anomaly.timestamp}-${anomaly.parameter}`}
                    style={{
                      background:
                        anomaly.event_confidence === "High"
                          ? "rgba(22, 101, 52, 0.12)"
                          : anomaly.event_confidence === "Possible"
                            ? "rgba(120, 53, 15, 0.12)"
                            : "transparent",
                      boxShadow: isCorroborated
                        ? `inset 3px 0 0 ${evidenceBadge.color}`
                        : "none",
                    }}
                  >
                    <td
                      style={{
                        borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                        padding: "12px 14px",
                        verticalAlign: "top",
                        fontSize: 14,
                        color: "#e4e4e7",
                        minWidth: 150,
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
                        minWidth: 380,
                      }}
                    >
                      <div style={{ fontWeight: 700, lineHeight: 1.4 }}>
                        {anomaly.parameter}
                      </div>
                      <div style={{ marginTop: 5, color: "#a1a1aa", fontSize: 13 }}>
                        {anomaly.value ?? "—"} {anomaly.unit ?? ""}
                      </div>
                      <div
                        style={{
                          marginTop: 10,
                          display: "flex",
                          flexWrap: "wrap",
                          alignItems: "center",
                          gap: 6,
                        }}
                      >
                        <span style={{ color: "#71717a", fontSize: 11, fontWeight: 700 }}>
                          Drivers
                        </span>
                        {driverHints.map((hint) => (
                          <span
                            key={hint}
                            style={{
                              border: "1px solid rgba(148, 163, 184, 0.22)",
                              background: "rgba(39, 39, 42, 0.55)",
                              borderRadius: 999,
                              color: "#d4d4d8",
                              fontSize: 11,
                              lineHeight: 1.35,
                              padding: "3px 8px",
                              whiteSpace: "normal",
                            }}
                          >
                            {hint}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td
                      style={{
                        borderBottom: "1px solid rgba(255, 255, 255, 0.05)",
                        padding: "12px 14px",
                        verticalAlign: "top",
                        fontSize: 14,
                        color: "#e4e4e7",
                        minWidth: 80,
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
                        minWidth: 300,
                      }}
                    >
                      <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
                        <span
                          style={{
                            display: "inline-flex",
                            alignItems: "center",
                            width: "fit-content",
                            borderRadius: 999,
                            border: `1px solid ${evidenceBadge.border}`,
                            background: evidenceBadge.background,
                            color: evidenceBadge.color,
                            fontSize: 12,
                            fontWeight: 700,
                            padding: "4px 10px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {evidenceBadge.label}
                        </span>
                        {anomaly.event_confidence ? (
                          <>
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              width: "fit-content",
                              borderRadius: 999,
                              border: "1px solid rgba(148, 163, 184, 0.35)",
                              background: "rgba(30, 41, 59, 0.35)",
                              color: "#bae6fd",
                              fontSize: 12,
                              fontWeight: 700,
                              padding: "3px 9px",
                            }}
                          >
                            {eventLabel}
                          </span>
                          {anomaly.matched_event_date ? (
                            <span style={{ color: "#a1a1aa" }}>
                              {formatDisplayDate(anomaly.matched_event_date)}
                              {lagLabel ? ` · ${lagLabel}` : ""}
                            </span>
                          ) : null}
                            <span style={{ color: "#d4d4d8", fontSize: 13, lineHeight: 1.5 }}>
                              {anomaly.matched_event_description}
                            </span>
                          </>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}
