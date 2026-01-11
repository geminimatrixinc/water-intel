import { NextResponse } from "next/server";
import { sites } from "../../lib/mockData";

export function GET() {
  return NextResponse.json({ sites });
}
