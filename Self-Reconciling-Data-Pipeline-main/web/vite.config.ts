import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, the console calls the local API (uvicorn on :8000). In the cloud, the API serves the
// built console itself, so both share one origin.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: { "/api": "http://localhost:8000" } },
  test: { environment: "jsdom", globals: true },
});
