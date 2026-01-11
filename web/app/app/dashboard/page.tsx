import Link from "next/link";

type WaterSite = {
  id: string;
  name: string;
  region: string;
  status: "Safe" | "Watch" | "Unsafe";
  lastUpdated: string;
};

async function getSites(): Promise<WaterSite[]> {
  const res = await fetch("http://localhost:3000/api/sites", {
    cache: "no-store",
  });

  if (!res.ok) throw new Error("Failed to load sites");
  const data = await res.json();
  return data.sites as WaterSite[];
}

function statusPill(status: WaterSite["status"]) {
  const base =
    "inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium";
  if (status === "Safe") return `${base} border-green-300`;
  if (status === "Watch") return `${base} border-yellow-300`;
  return `${base} border-red-300`;
}

export default async function DashboardPage() {
  const sites = await getSites();

  return (
    <main style={{ padding: 24, maxWidth: 900, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, fontWeight: 700 }}>Water Intel Dashboard</h1>
      <p style={{ marginTop: 8, opacity: 0.8 }}>
        Temporary data (mock). Click a site to view readings.
      </p>

      <div style={{ marginTop: 20, display: "grid", gap: 12 }}>
        {sites.map((site) => (
          <Link
            key={site.id}
            href={`/dashboard/sites/${site.id}`}
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
              <div style={{ fontWeight: 650 }}>{site.name}</div>
              <div style={{ fontSize: 13, opacity: 0.75, marginTop: 4 }}>
                {site.region} • Last updated{" "}
                {new Date(site.lastUpdated).toLocaleString()}
              </div>
            </div>

            <div className={statusPill(site.status)}>{site.status}</div>
          </Link>
        ))}
      </div>
    </main>
  );
}
