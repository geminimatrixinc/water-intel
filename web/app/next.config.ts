import type { NextConfig } from "next";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const appVersion = readFileSync(resolve(process.cwd(), "../../VERSION"), "utf8").trim();
const buildId =
  process.env.WATER_INTEL_BUILD_ID ??
  process.env.VERCEL_GIT_COMMIT_SHA ??
  process.env.GITHUB_SHA ??
  "";

const nextConfig: NextConfig = {
  env: {
    NEXT_PUBLIC_APP_VERSION: appVersion,
    NEXT_PUBLIC_BUILD_ID: buildId.slice(0, 7),
  },
};

export default nextConfig;
