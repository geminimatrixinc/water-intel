import Link from "next/link";

type WaterSite = {
  id: string;
  name: string;
  region: string;
  status: "Safe" | "Watch" | "Unsafe";
  lastUpdated: string;
};

type WaterReading = {
  timestamp: string;
  turbidity: number;
  ph: number;
  chlorine: number;
};

async function getSiteDetail(siteId: string): Promise<{
  site: WaterSite;
  readings: WaterReading[];
}> {
  const res = await fetch(`http://localhost:3000/api/sites/${siteId}`, {
    cache: "no-store",
  });

  if (!res.ok) throw new Error("Failed to load site detail");
  return res.json();
}

export default async function SiteDetailPage({
  params,
}: {
  params: Promise<{ siteId: string }>;
}) {
  const { siteId } = await params;
  const { site, readings } = await getSiteDetail(siteId);

  return (
    <main style={{ padding: 24, maxWidth: 900, margin: "0 auto" }}>
      <Link href="/dashboard" style={{ textDecoration: "none" }}>
        ← Back
      </Link>

      <h1 style={{ fontSize: 26, fontWeight: 750, marginTop: 14 }}>
        {site.name}
      </h1>
      <div style={{ marginTop: 6, opacity: 0.8 }}>
        {site.region} • Status: <strong>{site.status}</strong> • Last updated{" "}
        {new Date(site.lastUpdated).toLocaleString()}
      </div>

      <h2 style={{ fontSize: 18, fontWeight: 700, marginTop: 22 }}>
        Recent readings (last 24 hours)
      </h2>

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
                Time
              </th>
              <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                Turbidity (NTU)
              </th>
              <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                pH
              </th>
              <th style={{ padding: 10, borderBottom: "1px solid #e5e5e5" }}>
                Chlorine (mg/L)
              </th>
            </tr>
          </thead>
          <tbody>
            {readings.map((r) => (
              <tr key={r.timestamp}>
                <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                  {new Date(r.timestamp).toLocaleString()}
                </td>
                <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                  {r.turbidity}
                </td>
                <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                  {r.ph}
                </td>
                <td style={{ padding: 10, borderBottom: "1px solid #f0f0f0" }}>
                  {r.chlorine}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p style={{ marginTop: 14, opacity: 0.75 }}>
        Chart coming next (we’ll plot turbidity/pH/chlorine over time).
      </p>
    </main>
  );
}
