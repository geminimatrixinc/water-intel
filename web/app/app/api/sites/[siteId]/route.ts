import { NextResponse } from "next/server";

import { getBackendUrl } from "../../../lib/api";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  const { siteId } = await params;
  const response = await fetch(getBackendUrl(`/sites/${siteId}`), {
    cache: "no-store",
  });

  if (!response.ok) {
    return NextResponse.json(
      {
        error:
          response.status === 404
            ? "Site not found"
            : "Failed to load site detail from backend",
      },
      { status: response.status }
    );
  }

  const detail = await response.json();
  return NextResponse.json(detail);
}
