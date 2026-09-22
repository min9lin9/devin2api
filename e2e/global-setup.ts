// Global setup for the panel e2e: build and spawn the disposable
// mock-backed server (examples/panel_mock.rs), wait for its stdout
// readiness line, and hand the bound port to workers via process.env.
// Teardown kills the server and removes its state directory.

import { spawn, execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

export default async function globalSetup() {
  const root = process.cwd();
  execFileSync('cargo', ['build', '--locked', '--example', 'panel_mock'], {
    cwd: root,
    stdio: 'inherit',
    env: { ...process.env, CARGO_BUILD_JOBS: '2' },
  });
  const stateDir = mkdtempSync(path.join(tmpdir(), 'devin2api-panel-e2e-'));
  const child = spawn(path.join(root, 'target', 'debug', 'examples', 'panel_mock'), [], {
    env: { ...process.env, PANEL_STATE_DIR: stateDir },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const port = await new Promise<string>((resolve, reject) => {
    let buf = '';
    const timer = setTimeout(() => reject(new Error('panel_mock readiness timeout')), 120_000);
    child.stdout.on('data', (chunk: Buffer) => {
      buf += chunk.toString();
      const m = /^PANEL_READY (\d+)$/m.exec(buf);
      if (m) {
        clearTimeout(timer);
        resolve(m[1]);
      }
    });
    child.on('exit', code => {
      clearTimeout(timer);
      reject(new Error(`panel_mock exited before readiness: ${code}\n${buf}`));
    });
  });
  process.env.PANEL_BASE_URL = `http://127.0.0.1:${port}`;
  process.env.PANEL_STATE_DIR = stateDir;

  return async () => {
    child.kill('SIGKILL');
    await new Promise<void>(resolve => child.once('exit', () => resolve()));
    rmSync(stateDir, { recursive: true, force: true });
  };
}
