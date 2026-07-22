export type RiskLabel = "Safe" | "Watch" | "Concern";

export type SiteSummary = {
  station_id: string;
  total_readings: number;
  anomaly_count: number;
  anomaly_rate: number;
  last_reading_date: string;
  last_anomaly_date: string | null;
  avg_anomaly_score: number;
  parameter_count: number;
  date_range_start: string;
  date_range_end: string;
  top_anomaly_parameter: string | null;
  rolling_30d_anomaly_count: number;
  risk_score: number;
  risk_label: RiskLabel;
  corroborated_event_count?: number | null;
  has_data_quality_flag?: boolean | null;
};

export type AnomalyRecord = {
  station_id: string;
  timestamp: string;
  parameter: string;
  value: number | null;
  unit: string | null;
  anomaly_score: number;
  is_anomaly: number;
  top_features: string | null;
  top_feature_values: string | null;
  event_confidence?: "High" | "Possible" | "None" | null;
  matched_event_type?: string | null;
  matched_event_date?: string | null;
  matched_event_description?: string | null;
};

export type SiteDetailResponse = {
  site: SiteSummary;
  anomalies: AnomalyRecord[];
};
