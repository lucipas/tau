import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The Python bridge serves the exported files from web/out, so the console
  // is same-origin in production and needs no Node server at runtime.
  output: "export",
  trailingSlash: true,
  images: { unoptimized: true },
};

export default nextConfig;
