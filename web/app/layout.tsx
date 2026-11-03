import type { Metadata, Viewport } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "tau · mesh console",
  description:
    "Live view of a tau peer-to-peer node on the Reticulum Network Stack",
};

export const viewport: Viewport = {
  themeColor: "#05080a",
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
