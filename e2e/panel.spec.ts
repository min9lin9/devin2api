// Panel e2e against the disposable mock-backed server (examples/panel_mock.rs,
// spawned by global-setup.ts). PANEL_BASE_URL is injected by global setup.

import { expect, test, type Page } from '@playwright/test';
import path from 'node:path';

const base = () => process.env.PANEL_BASE_URL!;
const evidenceDir =
  process.env.PANEL_EVIDENCE_DIR ??
  path.resolve(process.cwd(), '..', '.omo', 'evidence', 'devin2api-rust-parity', 'task-18');

async function login(page: Page) {
  await page.goto(base() + '/panel');
  await expect(page.locator('#loginForm')).toBeVisible();
  await page.fill('#password', 'pw');
  await page.click('#loginForm button[type=submit]');
  await expect(page.locator('#topNav')).toBeVisible();
}

async function collectPageErrors(page: Page) {
  const errors: string[] = [];
  page.on('pageerror', err => errors.push(String(err)));
  return errors;
}

// echarts canvases animate in with a left-to-right wipe; screenshots taken
// earlier capture a half-drawn chart. Settled = the real completion signals,
// never a timeout: at least one 'finished' render event must be observed
// AND the canvas bitmap must be identical across consecutive animation
// frames (a running wipe/transition repaints every frame). If neither state
// is reached, waitForFunction's timeout fails the test.
async function chartSettled(page: Page, id: string) {
  await page.waitForFunction(
    chartId =>
      new Promise<boolean>(resolve => {
        const el = document.getElementById(chartId);
        const inst = el && (window as any).echarts?.getInstanceByDom(el);
        const canvas = el?.querySelector('canvas');
        if (!inst || !canvas) return resolve(false);
        let finishedSeen = false;
        inst.on('finished', () => {
          finishedSeen = true;
        });
        let last = canvas.toDataURL('image/png');
        let stableFrames = 0;
        const tick = () => {
          const cur = canvas.toDataURL('image/png');
          stableFrames = cur === last ? stableFrames + 1 : 0;
          last = cur;
          if ((finishedSeen && stableFrames >= 3) || stableFrames >= 10) {
            return resolve(true);
          }
          requestAnimationFrame(tick);
        };
        requestAnimationFrame(tick);
      }),
    id,
    { timeout: 15_000 },
  );
}

test('login: wrong password shows the server error, correct password enters the panel', async ({
  page,
}) => {
  const errors = await collectPageErrors(page);
  await page.goto(base() + '/panel');
  // Unauthenticated visitors get the login template, styled by the
  // unauthenticated panel.css exception.
  await expect(page.locator('.login-box')).toBeVisible();
  await expect(page.locator('#versionTag')).toHaveCount(0);
  const bg = await page.locator('body').evaluate(el => getComputedStyle(el).backgroundColor);
  expect(bg).not.toBe('rgba(0, 0, 0, 0)');

  await page.fill('#password', 'wrong');
  await page.click('#loginForm button[type=submit]');
  await expect(page.locator('#err')).toHaveText('密码错误');

  await page.fill('#password', 'pw');
  await page.click('#loginForm button[type=submit]');
  await expect(page.locator('#topNav')).toBeVisible();
  await expect(page.locator('#page-overview')).toBeVisible();
  await expect(page.locator('#versionTag')).toHaveText('qa-panel-mock · rust');
  expect(errors).toEqual([]);
});

test('navigation: every tab renders its data panels and charts', async ({ page }) => {
  const errors = await collectPageErrors(page);
  await login(page);

  // Overview: KPIs, quota bars, health matrix, trend chart.
  await expect(page.locator('#ovKpis .kpi').first()).toBeVisible();
  await expect(page.locator('#ovHealth .mx-cells i').first()).toBeAttached();
  await expect(page.locator('#ovTrendChart canvas')).toBeVisible();
  await expect(page.locator('#ovQuotaPanel .qbar').first()).toBeVisible();

  await page.click('#topNav a[data-tab=requests]');
  await expect(page.locator('#reqBody tr[data-dir]').first()).toBeVisible();

  await page.click('#topNav a[data-tab=usage]');
  await expect(page.locator('#usageRangeChips .chip').first()).toBeVisible();
  await expect(page.locator('#usageBody canvas').first()).toBeVisible();

  await page.click('#topNav a[data-tab=quota]');
  await expect(page.locator('#quotaCurve canvas')).toBeVisible();
  await expect(page.locator('#accountPanel')).toContainText('Teams Mock');

  await page.click('#topNav a[data-tab=models]');
  await expect(page.locator('#modelTable tbody tr')).toHaveCount(4);

  await page.click('#topNav a[data-tab=system]');
  await expect(page.locator('#sysKpis .kpi')).toHaveCount(8);

  // Hash navigation round-trips (browser back / direct links).
  await page.goto(base() + '/panel#quota');
  await expect(page.locator('#page-quota')).toBeVisible();
  expect(errors).toEqual([]);
});

test('models: search and tag chips filter the catalog', async ({ page }) => {
  await login(page);
  await page.click('#topNav a[data-tab=models]');
  await expect(page.locator('#modelTable tbody tr')).toHaveCount(4);

  await page.fill('#search', 'lite');
  await expect(page.locator('#modelTable tbody tr')).toHaveCount(1);
  await expect(page.locator('#modelTable')).toContainText('mock-lite-2');

  await page.fill('#search', '');
  await page.click('#chips .chip[data-tag=free]');
  await expect(page.locator('#modelTable tbody tr')).toHaveCount(1);
  await expect(page.locator('#modelTable')).toContainText('mock-free-0');
  await page.click('#chips .chip[data-tag=free]');
  await expect(page.locator('#modelTable tbody tr')).toHaveCount(4);
});

test('requests: detail expand, merged view, abort and export', async ({ page }) => {
  const errors = await collectPageErrors(page);
  await login(page);
  await page.click('#topNav a[data-tab=requests]');

  // The seeded in-flight request renders a pending row with an abort action.
  const pending = page.locator('#reqBody tr.pending-row');
  await expect(pending).toHaveCount(1);

  // Expand the newest completed request and open the merged response view.
  const completed = page.locator('#reqBody tr[data-dir]:not(.pending-row)').first();
  await completed.click();
  const detail = page.locator('tr.detail-row');
  await expect(detail).toBeVisible();
  await expect(detail).toContainText('meta.json');
  await detail.locator('[data-merged]').click();
  await expect(detail).toContainText('Hello from the mock');

  // Abort the in-flight request through the confirm dialog.
  await page.locator('#reqBody [data-abort]').first().click();
  await page.locator('.dlg [data-a=yes]').click();
  // The list pauses auto-refresh while a file view is open; a tab round-trip
  // forces a reload, showing the abort outcome.
  await page.click('#topNav a[data-tab=overview]');
  await page.click('#topNav a[data-tab=requests]');
  await expect(page.locator('#reqBody tr.pending-row')).toHaveCount(0);
  await expect(page.locator('#reqBody')).toContainText('已中断');

  // CSV export downloads with the Go header row.
  const downloadPromise = page.waitForEvent('download');
  await page.click('#reqExportCsv');
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toBe('requests.csv');
  const stream = await download.createReadStream();
  const chunks: Buffer[] = [];
  for await (const chunk of stream!) chunks.push(chunk as Buffer);
  expect(Buffer.concat(chunks).toString('utf8')).toContain('dir,started_at,method,path');
  expect(errors).toEqual([]);
});

test('usage and quota charts render with seeded history', async ({ page }) => {
  await login(page);
  await page.click('#topNav a[data-tab=usage]');
  await expect(page.locator('#usageBody canvas').first()).toBeVisible();
  await expect(page.locator('#usageBody')).toContainText('mock-pro-1');

  await page.click('#topNav a[data-tab=quota]');
  await expect(page.locator('#quotaCurve canvas')).toBeVisible();
  // KPI cards from the seeded quota points + burn forecast.
  await expect(page.locator('#quotaKpis')).toContainText('日配额剩余');
});

test('system: diagnostic cards, config view/reload, debug toggle, process log', async ({
  page,
}) => {
  const errors = await collectPageErrors(page);
  await login(page);
  await page.click('#topNav a[data-tab=system]');

  // Rust diagnostic cards replace the Go-only goroutine/GC cards.
  await expect(page.locator('#sysKpis')).toContainText('RSS');
  await expect(page.locator('#sysKpis')).toContainText('运行时');
  await expect(page.locator('#sysKpis')).toContainText('Rust');
  await expect(page.locator('#sysKpis')).not.toContainText('goroutine');
  await expect(page.locator('#versionTag')).toHaveText('qa-panel-mock · rust');

  // Config view is the redacted effective view; reload applies hot fields.
  await page.click('#cfgViewBtn');
  await expect(page.locator('#cfgView')).toBeVisible();
  await expect(page.locator('#cfgView')).toContainText('mock.upstream.test');
  await expect(page.locator('#cfgView')).not.toContainText('mock-upstream-token');
  await page.click('#cfgReload');
  await expect(page.locator('#toastBox')).toContainText('已应用');

  // Debug toggle flips the request-log pipeline switch.
  const toggle = page.locator('#debugToggle');
  await expect(toggle).toBeVisible();
  await expect(toggle).toContainText('开');
  await toggle.click();
  await expect(toggle).toContainText('关');
  await toggle.click();
  await expect(toggle).toContainText('开');

  // Process log tail renders the seeded stderr lines.
  await expect(page.locator('#processLogView')).toContainText('mock panel server booted');
  expect(errors).toEqual([]);
});

test('expired session and upstream failure render real error states and recover', async ({
  page,
  request,
}) => {
  const errors = await collectPageErrors(page);
  await login(page);
  await expect(page.locator('#ovKpis .kpi').first()).toBeVisible();

  // Session revoked server-side: the next poll's 401 returns the panel to
  // the login page (same path as an expired cookie).
  await request.post(base() + '/mock/revoke-sessions');
  await expect(page.locator('#loginForm')).toBeVisible();

  // Recovery: log back in and the panel resumes.
  await page.fill('#password', 'pw');
  await page.click('#loginForm button[type=submit]');
  await expect(page.locator('#topNav')).toBeVisible();

  // Upstream failure: the status error surfaces as an alert banner instead
  // of a blank or stale dashboard.
  await request.post(base() + '/mock/upstream-failure', { data: { on: true } });
  await page.click('#topNav a[data-tab=system]');
  await page.click('#topNav a[data-tab=overview]');
  await expect(page.locator('#ovAlertBody')).toContainText('账户用量拉取失败');

  await request.post(base() + '/mock/upstream-failure', { data: { on: false } });
  await page.click('#topNav a[data-tab=system]');
  await page.click('#topNav a[data-tab=overview]');
  await expect(page.locator('#ovAlertPanel')).toBeHidden();
  await expect(page.locator('#gwText')).toHaveText('运行中');
  expect(errors).toEqual([]);
});

test.describe('screenshots', () => {
  test('desktop 1440x900', async ({ page, request }) => {
    // Earlier tests may have consumed the seeded in-flight request (abort);
    // re-seed so the overview renders its active panel.
    await request.post(base() + '/mock/spawn-active');
    await login(page);
    await expect(page.locator('#ovTrendChart canvas')).toBeVisible();
    await expect(page.locator('#ovHealth .mx-cells i').first()).toBeAttached();
    await expect(page.locator('#ovQuotaPanel .qbar').first()).toBeVisible();
    await expect(page.locator('#versionTag')).toHaveText('qa-panel-mock · rust');
    await expect(page.locator('#ovActivePanel')).toBeVisible();
    await chartSettled(page, 'ovTrendChart');
    await page.screenshot({ path: path.join(evidenceDir, 'desktop.png'), fullPage: true });
  });

  test('mobile 390x844', async ({ page, request }) => {
    await request.post(base() + '/mock/spawn-active');
    await page.setViewportSize({ width: 390, height: 844 });
    await login(page);
    await expect(page.locator('#ovTrendChart canvas')).toBeVisible();
    await expect(page.locator('#ovQuotaPanel .qbar').first()).toBeVisible();
    await chartSettled(page, 'ovTrendChart');
    // The in-flight mock request surfaces the active panel on the first poll;
    // wait for it so the scroll height is final before clipping.
    await expect(page.locator('#ovActivePanel')).toBeVisible();
    // The health matrix is horizontally scrollable by design (180 10s cells
    // cannot fit 390px); keep the 390 width and grow the viewport to the
    // full content height so the capture shows exactly what a phone user
    // sees, without the scrollWidth-wide blank canvas fullPage would make.
    const height = await page.evaluate(() => document.documentElement.scrollHeight);
    await page.setViewportSize({ width: 390, height: Math.min(height, 4000) });
    await chartSettled(page, 'ovTrendChart');
    await page.screenshot({ path: path.join(evidenceDir, 'mobile.png') });
  });
});
