export type WaterSite = {
  id: string;
  name: string;
  region: string;
  status: "Safe" | "Watch" | "Unsafe";
  lastUpdated: string; // ISO string
};

export type WaterReading = {
  timestamp: string; // ISO string
  turbidity: number; // NTU
  ph: number;
  chlorine: number; // mg/L
};

export const sites: WaterSite[] = [
  {
    id: "okanagan-001",
    name: "Okanagan Lake Intake",
    region: "BC",
    status: "Safe",
    lastUpdated: new Date(Date.now() - 1000 * 60 * 25).toISOString(),
  },
  {
    id: "grand-river-014",
    name: "Grand River Station 014",
    region: "ON",
    status: "Watch",
    lastUpdated: new Date(Date.now() - 1000 * 60 * 58).toISOString(),
  },
  {
    id: "red-river-002",
    name: "Red River Station 002",
    region: "MB",
    status: "Unsafe",
    lastUpdated: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
  },
];

export function getSiteById(siteId: string): WaterSite | undefined {
  return sites.find((s) => s.id === siteId);
}

export function getMockReadings(siteId: string, n = 24): WaterReading[] {
  // deterministic-ish seed from siteId length
  const base = siteId.length * 0.3;

  const now = Date.now();
  return Array.from({ length: n }, (_, i) => {
    const minutesAgo = (n - 1 - i) * 60;
    const timestamp = new Date(now - minutesAgo * 60 * 1000).toISOString();

    // Simple fluctuations
    const turbidity = Number((0.7 + base + Math.sin(i / 3) * 0.2).toFixed(2));
    const ph = Number((7.2 + Math.cos(i / 4) * 0.15).toFixed(2));
    const chlorine = Number((0.8 + Math.sin(i / 5) * 0.1).toFixed(2));

    return { timestamp, turbidity, ph, chlorine };
  });
}
