import Link from "next/link";
import { Alert } from "@heroui/react";

import { getAppUrl } from "../lib/api";
import { buildRecentTrendPoints } from "../lib/chartData";
import { formatDisplayDate } from "../lib/dates";
import {
  dashboardTitle,
  recommendedAction,
  riskLegend,
  scadaRequirementNote,
  siteSummaryInterpretation,
  sourceWaterBanner,
  sourceWaterComparison,
} from "../lib/interpretation";
import { getStationMetadata } from "../lib/stations";
import type { RiskLabel, SiteDetailResponse, SiteSummary } from "../lib/types";

import SiteTrendSparkline from "./SiteTrendSparkline";

async function getSites(): Promise<SiteSummary[]> {
  const res = await fetch(getAppUrl("/api/sites"), { cache: "no-store" });

  if (!res.ok) throw new Error("Failed to load sites");
  return (await res.json()) as SiteSummary[];
}

async function getSiteTrendMap(siteIds: string[]) {
  const entries = await Promise.all(
    siteIds.map(async (siteId) => {
      const response = await fetch(getAppUrl(`/api/sites/${siteId}`), {
        cache: "no-store",
      });

      if (!response.ok) {
        throw new Error(`Failed to load trend data for ${siteId}`);
      }

      const detail = (await response.json()) as SiteDetailResponse;
      return [siteId, buildRecentTrendPoints(detail.anomalies)] as const;
    }),
  );

  return Object.fromEntries(entries);
}

function legendAlertStatus(status: RiskLabel): "success" | "warning" | "danger" {
  if (status === "Safe") return "success";
  if (status === "Watch") return "warning";
  return "danger";
}

function legendTitleClass(status: RiskLabel) {
  if (status === "Safe") return "text-sm font-semibold text-emerald-400";
  if (status === "Watch") return "text-sm font-semibold text-amber-300";
  return "text-sm font-semibold text-red-400";
}

function siteInsightColor(status: RiskLabel) {
  if (status === "Safe") return "#34d399";
  if (status === "Watch") return "#fbbf24";
  return "#f87171";
}

function siteStatusBadgeStyle(status: RiskLabel) {
  if (status === "Safe") {
    return {
      border: "1px solid rgba(52, 211, 153, 0.35)",
      background: "rgba(16, 185, 129, 0.12)",
      color: "#6ee7b7",
    };
  }
  if (status === "Watch") {
    return {
      border: "1px solid rgba(251, 191, 36, 0.35)",
      background: "rgba(245, 158, 11, 0.12)",
      color: "#fde68a",
    };
  }
  return {
    border: "1px solid rgba(248, 113, 113, 0.35)",
    background: "rgba(239, 68, 68, 0.12)",
    color: "#fca5a5",
  };
}

function siteStatusIcon(status: RiskLabel) {
  if (status === "Safe") return "check";
  if (status === "Watch") return "warning";
  return "danger";
}

export default async function DashboardPage() {
  let sites: SiteSummary[] = [];
  let siteTrendMap: Awaited<ReturnType<typeof getSiteTrendMap>> = {};
  let errorMessage: string | null = null;
  const comparison = sourceWaterComparison();

  try {
    sites = await getSites();
    siteTrendMap = await getSiteTrendMap(sites.map((site) => site.station_id));
  } catch (error) {
    errorMessage =
      error instanceof Error ? error.message : "Failed to load dashboard data";
  }

  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <header
        style={{
          display: "grid",
          gap: 8,
          justifyItems: "center",
          textAlign: "center",
          marginBottom: 4,
        }}
      >
        <h1
          style={{
            fontSize: 34,
            fontWeight: 800,
            letterSpacing: "-0.03em",
            lineHeight: 1.1,
            color: "#ffffff",
            margin: 0,
          }}
        >
          {dashboardTitle()}
        </h1>
        <p
          style={{
            margin: 0,
            maxWidth: "100%",
            fontSize: 13,
            lineHeight: 1.6,
            color: "#a1a1aa",
            whiteSpace: "nowrap",
          }}
        >
          Historical source-water intelligence for anomaly interpretation, site
          prioritization, and pilot-ready watershed decision support.
        </p>
      </header>
      <div style={{ marginTop: 12 }}>
        <Alert
          status="default"
          className="rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
        >
          <Alert.Indicator className="text-zinc-300" />
          <Alert.Content className="gap-1">
            <Alert.Title className="font-semibold text-white">
              {sourceWaterBanner()}
            </Alert.Title>
            <Alert.Description className="text-sm text-zinc-400">
              {scadaRequirementNote()}
            </Alert.Description>
          </Alert.Content>
        </Alert>
      </div>

      <div
        style={{
          marginTop: 12,
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: 10,
        }}
      >
        <Alert
          status="accent"
          className="h-full rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
        >
          <Alert.Indicator className="text-sky-400" />
          <Alert.Content className="gap-1">
            <Alert.Title className="text-sm font-semibold text-sky-400">
              {comparison.waterIntelTitle}
            </Alert.Title>
            <Alert.Description className="text-sm text-zinc-300">
              {comparison.waterIntelBody}
            </Alert.Description>
          </Alert.Content>
        </Alert>
        <Alert
          status="accent"
          className="h-full rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
        >
          <Alert.Indicator className="text-sky-400" />
          <Alert.Content className="gap-1">
            <Alert.Title className="text-sm font-semibold text-sky-400">
              {comparison.scadaTitle}
            </Alert.Title>
            <Alert.Description className="text-sm text-zinc-300">
              {comparison.scadaBody}
            </Alert.Description>
          </Alert.Content>
        </Alert>
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
          <Alert
            key={label}
            status={legendAlertStatus(label)}
            className="h-full rounded-2xl border border-white/10 bg-zinc-900 shadow-sm"
          >
            <Alert.Indicator />
            <Alert.Content className="gap-1">
              <Alert.Title className={legendTitleClass(label)}>{label}</Alert.Title>
              <Alert.Description className="text-sm text-zinc-300">
                {riskLegend(label)}
              </Alert.Description>
            </Alert.Content>
          </Alert>
        ))}
      </div>

      {errorMessage ? (
        <div style={{ marginTop: 18 }}>
          <Alert
            status="danger"
            className="border border-red-200 bg-red-50 text-red-800"
          >
            <Alert.Indicator />
            <Alert.Content>
              <Alert.Title>Dashboard data unavailable</Alert.Title>
              <Alert.Description>{errorMessage}</Alert.Description>
            </Alert.Content>
          </Alert>
        </div>
      ) : null}

      <div style={{ marginTop: 20, display: "grid", gap: 12 }}>
        {sites.map((site) => (
          (() => {
            const metadata = getStationMetadata(site.station_id);
            const emphasisColor = siteInsightColor(site.risk_label);
            const badgeStyle = siteStatusBadgeStyle(site.risk_label);
            const statusIcon = siteStatusIcon(site.risk_label);
            return (
          <Link
            key={site.station_id}
            href={`/dashboard/sites/${site.station_id}`}
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "stretch",
              gap: 12,
              padding: 14,
              border: "1px solid rgba(255,255,255,0.1)",
              borderRadius: 12,
              textDecoration: "none",
              color: "#f4f4f5",
              background: "#18181b",
              boxShadow: "0 1px 2px rgba(0,0,0,0.12)",
            }}
            >
            <div style={{ flex: "1 1 auto" }}>
              <div
                style={{
                  fontSize: 18,
                  fontWeight: 800,
                  letterSpacing: "-0.02em",
                  lineHeight: 1.15,
                  color: "#ffffff",
                }}
              >
                {metadata.name}
              </div>
              <div
                style={{
                  fontSize: 13,
                  color: "#a1a1aa",
                  marginTop: 12,
                  marginBottom: 12,
                }}
              >
                {metadata.location}
              </div>
              <div style={{ fontSize: 13, color: "#a1a1aa" }}>
                Station <span style={{ color: "#ffffff", fontWeight: 700 }}>{site.station_id}</span>
              </div>
              <div style={{ fontSize: 13, color: "#a1a1aa", marginTop: 4 }}>
                Source-water risk {site.risk_score} • Last updated{" "}
                {formatDisplayDate(site.last_reading_date)}
              </div>
              <div style={{ fontSize: 13, color: "#a1a1aa", marginTop: 4 }}>
                {site.anomaly_count} anomalies • Top parameter{" "}
                {site.top_anomaly_parameter ?? "Unavailable"}
              </div>
              <div
                style={{
                  fontSize: 13,
                  color: emphasisColor,
                  marginTop: 14,
                  fontWeight: 700,
                }}
              >
                {siteSummaryInterpretation(site)}
              </div>
              <div style={{ fontSize: 13, color: "#d4d4d8", marginTop: 6 }}>
                <span style={{ color: emphasisColor, fontWeight: 700 }}>
                  Recommended action:
                </span>{" "}
                {recommendedAction(site)}
              </div>
            </div>

            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "space-between", flex: "0 0 200px", maxWidth: 220 }}>
              <div
                style={{
                  minWidth: 104,
                  borderRadius: 999,
                  padding: "8px 10px",
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 8,
                  justifyContent: "center",
                  ...badgeStyle,
                }}
              >
                <span aria-hidden="true" style={{ display: "inline-flex", width: 16, height: 16 }}>
                  <svg viewBox="0 0 16 16" width="16" height="16" fill="none">
                    {statusIcon === "check" ? (
                      <path
                        d="M13.5 8A5.5 5.5 0 1 1 2.5 8a5.5 5.5 0 0 1 11 0M15 8A7 7 0 1 1 1 8a7 7 0 0 1 14 0m-3.9-1.55a.75.75 0 1 0-1.2-.9L7.419 8.858L6.03 7.47a.75.75 0 0 0-1.06 1.06l2 2a.75.75 0 0 0 1.13-.08z"
                        fill="currentColor"
                        fillRule="evenodd"
                        clipRule="evenodd"
                      />
                    ) : statusIcon === "warning" ? (
                      <path
                        d="M7.134 2.994L2.217 11.5a1 1 0 0 0 .866 1.5h9.834a1 1 0 0 0 .866-1.5L8.866 2.993a1 1 0 0 0-1.732 0m3.03-.75c-.962-1.665-3.366-1.665-4.329 0L.918 10.749c-.963 1.666.24 3.751 2.165 3.751h9.834c1.925 0 3.128-2.085 2.164-3.751zM8 5a.75.75 0 0 1 .75.75v2a.75.75 0 0 1-1.5 0v-2A.75.75 0 0 1 8 5m1 5.75a1 1 0 1 1-2 0a1 1 0 0 1 2 0"
                        fill="currentColor"
                        fillRule="evenodd"
                        clipRule="evenodd"
                      />
                    ) : (
                      <path
                        d="M8 13.5a5.5 5.5 0 1 0 0-11a5.5 5.5 0 0 0 0 11M8 15A7 7 0 1 0 8 1a7 7 0 0 0 0 14m1-4.5a1 1 0 1 1-2 0a1 1 0 0 1 2 0M8.75 5a.75.75 0 0 0-1.5 0v2.5a.75.75 0 0 0 1.5 0z"
                        fill="currentColor"
                        fillRule="evenodd"
                        clipRule="evenodd"
                      />
                    )}
                  </svg>
                </span>
                <span style={{ fontSize: 13, fontWeight: 800, lineHeight: 1 }}>
                  {site.risk_label}
                </span>
              </div>

              <SiteTrendSparkline
                points={siteTrendMap[site.station_id] ?? []}
                riskLabel={site.risk_label}
              />
            </div>
          </Link>
            );
          })()
        ))}
      </div>
    </main>
  );
}
