import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const root = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    // Mirror the tsconfig "@/*" paths so component tests can import lib
    // the same way the app does.
    alias: { "@": root },
  },
  test: {
    environment: "jsdom",
    environmentOptions: {
      // A non-localhost origin, so API_BASE resolves to "" (same-origin)
      // and the api tests assert plain "/api/..." paths.
      jsdom: { url: "http://example.com/" },
    },
    globals: true,
    include: ["lib/**/*.test.{ts,tsx}", "components/**/*.test.{ts,tsx}", "app/**/*.test.{ts,tsx}"],
    setupFiles: [],
  },
});
