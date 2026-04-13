import Link from "next/link";

import { getAppUrl } from "../lib/api";
import { formatDisplayDate } from "../lib/dates";
import { recommendedAction, riskLegend, siteSummaryInterpretation } from "../lib/interpretation";
import { getStationMetadata } from "../lib/stations";
import type { RiskLabel, SiteSummary } from "../lib/types";

async function getSites(): Promise<SiteSummary[]> {
  const res = await fetch(getAppUrl("/api/sites"), { cache: "no-store" });

  if (!res.ok) throw new Error("Failed to load sites");
  return (await res.json()) as SiteSummary[];
}

function statusPill(status: RiskLabel) {
  const base =
    "inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium";
  if (status === "Safe") return `${base} border-green-300`;
  if (status === "Watch") return `${base} border-yellow-300`;
  return `${base} border-red-300`;
}

export default async function DashboardPage() {
  let sites: SiteSummary[] = [];
  let errorMessage: string | null = null;

  try {
    sites = await getSites();
  } catch (error) {
    errorMessage =
      error instanceof Error ? error.message : "Failed to load dashboard data";
  }

  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, fontWeight: 700 }}>Water Intel Dashboard</h1>
      <div
        style={{
          marginTop: 12,
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
          marginTop: 12,
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: 10,
        }}
      >
        {(["Safe", "Watch", "Concern"] as RiskLabel[]).map((label) => (
          <div
            key={label}
            style={{
              border: "1px solid #e5e5e5",
              borderRadius: 12,
              padding: 12,
              background: "#fff",
              color: "#111111",
            }}
          >
            <div className={statusPill(label)}>{label}</div>
            <div style={{ marginTop: 8, fontSize: 13, color: "#111111" }}>
              {riskLegend(label)}
            </div>
          </div>
        ))}
      </div>

      {errorMessage ? (
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
          {errorMessage}
        </div>
      ) : null}

      <div style={{ marginTop: 20, display: "grid", gap: 12 }}>
        {sites.map((site) => (
          (() => {
            const metadata = getStationMetadata(site.station_id);
            return (
          <Link
            key={site.station_id}
            href={`/dashboard/sites/${site.station_id}`}
            style={{
              display: "flex",
              justifyContent: "space-between",
              gap: 12,
              padding: 14,
              border: "1px solid #e5e5e5",
              borderRadius: 12,
              textDecoration: "none",
              color: "inherit",
            }}
          >
            <div>
              <div style={{ fontWeight: 650 }}>{metadata.name}</div>
              <div style={{ fontSize: 13, opacity: 0.75, marginTop: 4 }}>
                {metadata.location}
              </div>
              <div style={{ fontSize: 13, opacity: 0.75, marginTop: 4 }}>
                Station {site.station_id} • Risk score {site.risk_score} • Last updated{" "}
                {formatDisplayDate(site.last_reading_date)}
              </div>
              <div style={{ fontSize: 13, opacity: 0.75, marginTop: 4 }}>
                {site.anomaly_count} anomalies • Top parameter{" "}
                {site.top_anomaly_parameter ?? "Unavailable"}
              </div>
              <div style={{ fontSize: 13, color: "#111111", marginTop: 6 }}>
                {siteSummaryInterpretation(site)}
              </div>
              <div style={{ fontSize: 13, color: "#111111", marginTop: 4 }}>
                Recommended action: {recommendedAction(site)}
              </div>
            </div>

            <div className={statusPill(site.risk_label)}>{site.risk_label}</div>
          </Link>
            );
          })()
        ))}
      </div>
    </main>
  );
}
