import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Static export: the tau bridge serves web/out from the same origin.
  output: "export",
  // No server components that need runtime data; keep the export portable.
  trailingSlash: true,
};

export default nextConfig;
