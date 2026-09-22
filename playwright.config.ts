import { defineConfig } from '@playwright/test';
import path from 'node:path';

// Evidence lands next to the other task-18 artifacts; override with
// PANEL_EVIDENCE_DIR when running from a different checkout layout.
const evidenceDir =
  process.env.PANEL_EVIDENCE_DIR ??
  path.resolve(process.cwd(), '..', '.omo', 'evidence', 'devin2api-rust-parity', 'task-18');

export default defineConfig({
  testDir: './e2e',
  globalSetup: './e2e/global-setup.ts',
  // One mock server per run; tests mutate shared panel state (debug toggle,
  // abort), so parallel workers would race each other.
  workers: 1,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [
    ['list'],
    ['html', { outputFolder: path.join(evidenceDir, 'report'), open: 'never' }],
  ],
  use: {
    viewport: { width: 1440, height: 900 },
    actionTimeout: 10_000,
  },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],
});
