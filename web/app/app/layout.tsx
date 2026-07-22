import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Water-Intel",
  description: "Historical source-water intelligence dashboard",
};

const appVersion = process.env.NEXT_PUBLIC_APP_VERSION ?? "unknown";
const buildId = process.env.NEXT_PUBLIC_BUILD_ID;

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
          <div style={{ flex: "1 0 auto" }}>{children}</div>
          <footer
            aria-label="Application version"
            style={{
              padding: "8px 16px 12px",
              color: "#71717a",
              fontFamily: "var(--font-geist-mono)",
              fontSize: 10,
              lineHeight: 1.4,
              textAlign: "right",
            }}
          >
            Water-Intel v{appVersion}
            {buildId ? ` · ${buildId}` : ""}
          </footer>
        </div>
      </body>
    </html>
  );
}
