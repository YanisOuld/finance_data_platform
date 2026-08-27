import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base: "/app/" -- the built assets are served from FastAPI's StaticFiles
// mount at /app (see src/main.py), not from the site root.
export default defineConfig({
  plugins: [react()],
  base: "/app/",
  server: {
    // `npm run dev` runs against Vite's own dev server (hot reload) on a
    // different port than uvicorn; proxy API calls through so the app can
    // be developed without rebuilding on every change.
    proxy: {
      "/instruments": "http://127.0.0.1:8000",
      "/prices": "http://127.0.0.1:8000",
      "/fundamentals": "http://127.0.0.1:8000",
      "/macro": "http://127.0.0.1:8000",
      "/health": "http://127.0.0.1:8000",
    },
  },
});
