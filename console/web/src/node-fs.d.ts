/** Just what vite.config.ts reads; the app itself has no Node.js types. */
declare module 'node:fs' {
  export function readFileSync(path: URL, encoding: 'utf8'): string;
}
