import { NextResponse } from "next/server";
import { getMockReadings, getSiteById } from "../../../lib/mockData";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  const { siteId } = await params;
  const site = getSiteById(siteId);

  if (!site) {
    return NextResponse.json({ error: "Site not found" }, { status: 404 });
  }

  const readings = getMockReadings(site.id, 24);

  return NextResponse.json({
    site,
    readings,
  });
}
