import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'node:fs';

// The console is released with the workspace, so its version is the workspace's.
// The Docker build of the web app has only console/web, so a missing file must not stop the build.
let cargo = '';
try {
  cargo = readFileSync(new URL('../../Cargo.toml', import.meta.url), 'utf8');
} catch {
  /* no workspace around: the footer shows no version */
}
const version = /^\[workspace\.package\][^[]*?^version\s*=\s*"([^"]+)"/ms.exec(cargo)?.[1] ?? /^version\s*=\s*"([^"]+)"/m.exec(cargo)?.[1] ?? '';

// `npm run dev` serves the app on :5173 and forwards the API and the
// WebSocket to a lotse-console running on :8080.
export default defineConfig({
  plugins: [react()],
  define: { __APP_VERSION__: JSON.stringify(version) },
  server: {
    proxy: {
      '/api': { target: 'http://127.0.0.1:8080', ws: true, changeOrigin: false },
    },
  },
  build: { outDir: 'dist', sourcemap: true },
});
