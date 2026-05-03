import type { RiskLabel, SiteSummary } from "./types";

const DRIVER_LABELS: Record<string, string> = {
  rolling_mean_30: "sustained level shift",
  rolling_mean_14: "two-week level shift",
  rolling_mean_7: "short-term level shift",
  rolling_std_30: "higher long-term volatility",
  rolling_std_14: "higher two-week volatility",
  rolling_std_7: "higher short-term volatility",
  rate_of_change: "rapid change",
  delta: "large jump from previous reading",
  time_gap_days: "long gap between readings",
  zscore: "far from normal range",
  is_gap: "sampling gap",
};

export function humanizeDriverHints(topFeatures: string | null): string {
  if (!topFeatures) {
    return "No driver hints available";
  }

  return topFeatures
    .split(",")
    .map((feature) => feature.trim())
    .filter(Boolean)
    .map((feature) => DRIVER_LABELS[feature] ?? feature.replaceAll("_", " "))
    .join(", ");
}

export function riskLegend(label: RiskLabel): string {
  if (label === "Safe") {
    return "0–30: Stable historical pattern";
  }

  if (label === "Watch") {
    return "31–60: Repeated anomalies worth monitoring";
  }

  return "61–100: Persistent or high-severity anomalies";
}

export function dashboardTitle(): string {
  return "Water-Intel: Source Water Intelligence";
}

export function sourceWaterBanner(): string {
  return "Historical analysis using public PWQMN data (2019–2024). Designed to turn watershed and source-water signals into anomaly interpretation, site-level risk context, and decision support alongside existing monitoring portals and plant SCADA.";
}

export function scadaRequirementNote(): string {
  return "Real-time monitoring requires Phase 2 SCADA or community sensor access.";
}

export function sourceWaterComparison() {
  return {
    waterIntelTitle: "Water-Intel",
    waterIntelBody:
      "Interprets source-water trends into anomaly summaries, site prioritization, and pilot-ready watershed risk context.",
    scadaTitle: "Plant SCADA",
    scadaBody:
      "Watches live plant telemetry, alarms, controls, and compliance operations inside the treatment system.",
  };
}

export function sourceMonitoringLabel(): string {
  return "Source monitoring station";
}

export function upstreamRiskLabel(): string {
  return "Upstream risk score";
}

export function historicalRecordLabel(): string {
  return "Historical monitoring record";
}

export function trendAnomalyLabel(): string {
  return "Trend anomalies";
}

export function whyThisMattersTitle(): string {
  return "Why this matters";
}

export function roleBasedUsage() {
  return {
    operators:
      "Use this view to spot unusual source-water patterns that deserve review alongside plant alarms, operator logs, and lab checks.",
    leadership:
      "Use this view to understand relative watershed risk, recurring problem areas, and where follow-up or investment may be needed.",
  };
}

export function anomalyDefinition(): string {
  return "Anomaly scores reflect readings that deviate from this station's historical pattern. They help turn raw watershed signals into explainable site-level context that chart portals and plant-only dashboards may not surface on their own.";
}

export function anomalyTimelineTitle(): string {
  return "Source-water anomaly timeline";
}

export function anomalyHistoryTitle(): string {
  return "Source-water anomaly history";
}

export function recommendedAction(site: SiteSummary): string {
  if (site.risk_label === "Concern") {
    return `Review recent ${site.top_anomaly_parameter?.toLowerCase() ?? "water quality"} events and escalate for operator review.`;
  }

  if (site.risk_label === "Watch") {
    return `Monitor the next set of readings and compare against the recent ${site.top_anomaly_parameter?.toLowerCase() ?? "anomaly"} pattern.`;
  }

  return "Continue standard monitoring; this station has a comparatively stable historical profile.";
}

export function siteSummaryInterpretation(site: SiteSummary): string {
  if (site.risk_label === "Concern") {
    return `Elevated historical risk driven by repeated ${site.top_anomaly_parameter?.toLowerCase() ?? "parameter"} anomalies.`;
  }

  if (site.risk_label === "Watch") {
    return `Intermittent elevated readings suggest this station deserves closer monitoring.`;
  }

  return "Mostly stable historical profile with fewer flagged anomalies than higher-risk stations.";
}

export function anomalyTimelineCaption(): string {
  return "Higher values mean the reading was more unusual relative to this station's historical pattern and may help explain source-water shifts before plant dashboards show the same context.";
}