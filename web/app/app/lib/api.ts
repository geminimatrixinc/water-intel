const BACKEND_API_URL = process.env.WATER_INTEL_API_URL ?? "http://127.0.0.1:8000";
const APP_API_URL = process.env.NEXT_PUBLIC_APP_URL ?? "http://localhost:3000";

export function getBackendUrl(path: string): string {
  return `${BACKEND_API_URL}${path}`;
}

export function getAppUrl(path: string): string {
  return `${APP_API_URL}${path}`;
}
