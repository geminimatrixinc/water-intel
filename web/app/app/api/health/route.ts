import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    ok: true,
    service: "water-intel",
    version: process.env.NEXT_PUBLIC_APP_VERSION ?? "unknown",
    build: process.env.NEXT_PUBLIC_BUILD_ID || null,
    timestamp: new Date().toISOString(),
  });
}
