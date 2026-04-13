import { NextResponse } from "next/server";

import { getBackendUrl } from "../../lib/api";

export async function GET() {
  const response = await fetch(getBackendUrl("/sites"), {
    cache: "no-store",
  });

  if (!response.ok) {
    return NextResponse.json(
      { error: "Failed to load sites from backend" },
      { status: response.status }
    );
  }

  const sites = await response.json();
  return NextResponse.json(sites);
}
