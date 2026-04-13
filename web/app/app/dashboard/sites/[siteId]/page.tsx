import Link from "next/link";
import { notFound } from "next/navigation";

import { getAppUrl } from "../../../lib/api";
import { formatDisplayDate } from "../../../lib/dates";
import {
  anomalyTimelineCaption,
  humanizeDriverHints,
  recommendedAction,
  riskLegend,
  siteSummaryInterpretation,
} from "../../../lib/interpretation";
import { getStationMetadata } from "../../../lib/stations";
import type { SiteDetailResponse } from "../../../lib/types";

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

function riskCardTone(label: SiteDetailResponse["site"]["risk_label"]) {
  if (label === "Safe") {
    return {
      border: "1px solid #86efac",
      background: "#f0fdf4",
      color: "#166534",
    };
  }

  if (label === "Watch") {
    return {
      border: "1px solid #fde68a",
      background: "#fffbeb",
      color: "#92400e",
    };
  }

  return {
    border: "1px solid #fca5a5",
    background: "#fef2f2",
    color: "#991b1b",
  };
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
        <div
          style={{
            marginTop: 18,
            padding: 14,
            borderRadius: 12,
            border: "1px solid #fecaca",
            background: "#fef2f2",
            color: "#991b1b",
          }}
        >
          {message}
        </div>
      </main>
    );
  }

  const { site, anomalies } = detail;
  const riskTone = riskCardTone(site.risk_label);
  const metadata = getStationMetadata(site.station_id);

  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <Link href="/dashboard" style={{ textDecoration: "none" }}>
        ← Back
      </Link>

      <h1 style={{ fontSize: 26, fontWeight: 750, marginTop: 14 }}>
        {metadata.name}
      </h1>
      <div style={{ marginTop: 6, color: "#111111" }}>{metadata.location}</div>
      <div style={{ marginTop: 4, color: "#666666", fontSize: 13 }}>
        Monitoring station {site.station_id}
      </div>
      <div
        style={{
          marginTop: 10,
          padding: "12px 14px",
          borderRadius: 12,
          background: "#ecfeff",
          border: "1px solid #a5f3fc",
          color: "#155e75",
        }}
      >
        Historical Analysis using public PWQMN data (2019–2024) · Real-time
        monitoring requires Phase 2 SCADA or community sensor access
      </div>

      <div
        style={{
          marginTop: 18,
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: 12,
        }}
      >
        <section
          style={{
            ...riskTone,
            borderRadius: 16,
            padding: 18,
          }}
        >
          <div style={{ fontSize: 13, opacity: 0.85 }}>Current risk score</div>
          <div style={{ fontSize: 40, fontWeight: 800, lineHeight: 1.1, marginTop: 6 }}>
            {site.risk_score}
          </div>
          <div style={{ marginTop: 8, fontWeight: 700 }}>{site.risk_label}</div>
          <div style={{ marginTop: 8, fontSize: 13, opacity: 0.9 }}>
            {riskLegend(site.risk_label)}
          </div>
          <div style={{ marginTop: 8, fontSize: 13, opacity: 0.85 }}>
            Last updated {formatDisplayDate(site.last_reading_date)}
          </div>
        </section>

        <section
          style={{
            border: "1px solid #e5e5e5",
            borderRadius: 16,
            padding: 18,
            background: "#fff",
            color: "#111111",
          }}
        >
          <div style={{ fontSize: 13, color: "#111111" }}>Historical coverage</div>
          <div style={{ marginTop: 6, fontWeight: 700 }}>
            {formatDisplayDate(site.date_range_start)} to {formatDisplayDate(site.date_range_end)}
          </div>
          <div style={{ marginTop: 12, fontSize: 13, color: "#111111" }}>
            Total anomalies {site.anomaly_count} · 30d rolling {site.rolling_30d_anomaly_count}
          </div>
          <div style={{ marginTop: 8, fontSize: 13, color: "#111111" }}>
            Top anomaly parameter {site.top_anomaly_parameter ?? "Unavailable"}
          </div>
        </section>
      </div>

      <section
        style={{
          marginTop: 16,
          border: "1px solid #e5e5e5",
          borderRadius: 16,
          padding: 16,
          background: "#fff",
          color: "#111111",
        }}
      >
        <div style={{ fontSize: 16, fontWeight: 700 }}>What this means</div>
        <div style={{ marginTop: 8, fontSize: 14 }}>
          {siteSummaryInterpretation(site)}
        </div>
        <div style={{ marginTop: 10, fontSize: 14 }}>
          Recommended action: {recommendedAction(site)}
        </div>
      </section>

      {anomalies.length > 0 ? <AnomalyTimeline anomalies={anomalies} /> : null}
      {anomalies.length > 0 ? (
        <p style={{ marginTop: 8, color: "#111111", fontSize: 13 }}>
          {anomalyTimelineCaption()}
        </p>
      ) : null}

      <h2 style={{ fontSize: 18, fontWeight: 700, marginTop: 22 }}>
        Anomaly history
      </h2>
      <p style={{ marginTop: 8, color: "#111111", fontSize: 13 }}>
        Driver hints describe the pattern behind the anomaly in plain language rather
        than model terms.
      </p>

      {anomalies.length === 0 ? (
        <p style={{ marginTop: 12, opacity: 0.75 }}>
          No anomaly records were found for this station in the current output set.
        </p>
      ) : (
        <div style={{ overflowX: "auto", marginTop: 10 }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              border: "1px solid #e5e5e5",
              borderRadius: 12,
            }}
          >
            <thead>
              <tr style={{ textAlign: "left" }}>
                <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                  Timestamp
                </th>
                <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                  Parameter
                </th>
                <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                  Value
                </th>
                <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                  Score
                </th>
                <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                  Driver hints
                </th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((anomaly) => (
                <tr key={`${anomaly.timestamp}-${anomaly.parameter}`}>
                  <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                    {new Date(anomaly.timestamp).toLocaleString()}
                  </td>
                  <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                    {anomaly.parameter}
                  </td>
                  <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                    {anomaly.value ?? "—"} {anomaly.unit ?? ""}
                  </td>
                  <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                    {anomaly.anomaly_score.toFixed(3)}
                  </td>
                  <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
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
