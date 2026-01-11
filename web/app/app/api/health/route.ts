import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    ok: true,
    service: "water-intel",
    timestamp: new Date().toISOString(),
  });
}
