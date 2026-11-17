import type { Metadata } from "next";
import "./globals.css";
import Shell from "@/components/chrome/Shell";

export const metadata: Metadata = {
  title: "τ — Mesh Console",
  description: "Web console for a tau node on the Reticulum Network Stack.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className="dark">
      <body className="bg-zinc-950 text-zinc-100 antialiased">
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
