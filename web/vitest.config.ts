import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "jsdom",
    environmentOptions: {
      // A non-localhost origin, so API_BASE resolves to "" (same-origin)
      // and the api tests assert plain "/api/..." paths.
      jsdom: { url: "http://example.com/" },
    },
    globals: true,
    include: ["lib/**/*.test.{ts,tsx}"],
    setupFiles: [],
  },
});
