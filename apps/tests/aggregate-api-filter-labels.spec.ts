import { expect, test, type Page } from "@playwright/test";

const LABELS = {
  "zh-CN": { status: "状态", type: "类型", allStatuses: "全部状态", allTypes: "全部类型", active: "已启用", disabled: "已禁用", compatible: "通用兼容（Codex + Claude）", missingRoute: "路由缺失", testRoute: "测试路由" },
  en: { status: "Status", type: "Type", allStatuses: "All statuses", allTypes: "All types", active: "Enabled", disabled: "Disabled", compatible: "Compatible (Codex + Claude)", missingRoute: "Route missing", testRoute: "Test route" },
  ko: { status: "상태", type: "유형", allStatuses: "전체 상태", allTypes: "전체 유형", active: "활성화됨", disabled: "비활성화됨", compatible: "호환 모드 (Codex + Claude)", missingRoute: "라우트 누락", testRoute: "라우트 테스트" },
  ru: { status: "Статус", type: "Тип", allStatuses: "Все статусы", allTypes: "Все типы", active: "Включено", disabled: "Отключено", compatible: "Совместимый режим (Codex + Claude)", missingRoute: "Маршрут отсутствует", testRoute: "Проверить маршрут" },
} as const;

type Locale = keyof typeof LABELS;

const CONNECTIONS = [
  { id: "codex-active", supplierName: "Codex active", providerType: "codex", status: "active", modelSlugs: [] },
  { id: "codex-disabled", supplierName: "Codex disabled", providerType: "codex", status: "disabled", modelSlugs: ["model-a"] },
  { id: "claude-active", supplierName: "Claude active", providerType: "claude", status: "active", modelSlugs: ["model-b"] },
  { id: "compatible-active", supplierName: "Compatible active", providerType: "compatible", status: "active", modelSlugs: ["model-c"] },
  { id: "gemini-active", supplierName: "Gemini active", providerType: "gemini", status: "active", modelSlugs: ["model-d"] },
].map((api, index) => ({ ...api, sort: index * 5, url: `https://${api.id}.invalid/v1`, authType: "bearer" }));

async function mockAggregateRuntime(page: Page, locale: Locale | "auto") {
  const aggregateCalls: string[] = [];
  await page.route(/\/api\/runtime\/?(?:\?.*)?$/, (route) => route.fulfill({
    json: { mode: "web-gateway", rpcBaseUrl: "/api/rpc", canManageService: false },
  }));
  await page.route(/\/api\/rpc\/?(?:\?.*)?$/, async (route) => {
    const { id, method } = route.request().postDataJSON();
    let result: unknown = {};
    if (method === "appSettings/get") {
      result = { locale, localeOptions: ["auto", "zh-CN", "en", "ko", "ru"], codexCliGuideDismissed: true, serviceAddr: "localhost:48760", theme: "tech", appearancePreset: "classic" };
    }
    if (method === "initialize") result = { userAgent: "codex_cli_rs/test", codexHome: "C:/Test/.codex" };
    if (method === "accountManager/session/current") {
      result = { mode: "none", role: "system_admin", permissions: [], distributionEnabled: false };
    }
    if (method.startsWith("aggregateApi/")) aggregateCalls.push(method);
    if (method === "aggregateApi/list") result = { items: CONNECTIONS };
    await route.fulfill({ json: { jsonrpc: "2.0", id, result } });
  });
  return aggregateCalls;
}

async function visibleSuppliers(page: Page) {
  return page.getByRole("main").locator("tbody tr").evaluateAll((rows) =>
    rows.map((row) => row.querySelector("td > div")?.textContent),
  );
}

test.use({ locale: "zh-CN" });

for (const locale of Object.keys(LABELS) as Locale[]) {
  test(`aggregate API filters show ${locale} labels and preserve filtering semantics`, async ({ page }) => {
    const aggregateCalls = await mockAggregateRuntime(page, locale);
    const labels = LABELS[locale];
    const pageErrors: Error[] = [];
    page.on("pageerror", (error) => pageErrors.push(error));
    await page.goto("/aggregate-api/");
    await expect(page.locator("html")).toHaveAttribute("lang", locale);
    const status = page.getByRole("combobox", { name: labels.status, exact: true });
    const provider = page.getByRole("combobox", { name: labels.type, exact: true });
    // Neither popup has mounted: Value must resolve labels from Root.items.
    await expect(status.locator('[data-slot="select-value"]')).toHaveText(labels.allStatuses);
    await expect(provider.locator('[data-slot="select-value"]')).toHaveText(labels.allTypes);
    await expect.poll(() => visibleSuppliers(page)).toEqual(CONNECTIONS.map((api) => api.supplierName));
    await expect(page.getByText(labels.missingRoute, { exact: true })).toBeVisible();
    await expect(page.getByText("missing route", { exact: true })).toHaveCount(0);
    await expect(page.getByRole("button", { name: labels.testRoute, exact: true })).toHaveCount(5);

    await provider.click();
    await page.getByRole("option", { name: "Codex", exact: true }).click();
    await expect(provider.locator('[data-slot="select-value"]')).toHaveText("Codex");
    await expect.poll(() => visibleSuppliers(page)).toEqual(["Codex active", "Codex disabled", "Compatible active"]);
    await status.click();
    await page.getByRole("option", { name: labels.active, exact: true }).click();
    await expect(status.locator('[data-slot="select-value"]')).toHaveText(labels.active);
    await expect.poll(() => visibleSuppliers(page)).toEqual(["Codex active", "Compatible active"]);
    await status.click();
    await page.getByRole("option", { name: labels.disabled, exact: true }).click();
    await expect(status.locator('[data-slot="select-value"]')).toHaveText(labels.disabled);
    await expect.poll(() => visibleSuppliers(page)).toEqual(["Codex disabled"]);
    await status.click();
    await page.getByRole("option", { name: labels.allStatuses, exact: true }).click();

    for (const [label, suppliers] of [
      [labels.compatible, ["Compatible active"]],
      ["Claude", ["Claude active", "Compatible active"]],
      ["Gemini", ["Gemini active"]],
      [labels.allTypes, CONNECTIONS.map((api) => api.supplierName)],
    ] as const) {
      await provider.click();
      await page.getByRole("option", { name: label, exact: true }).click();
      await expect(provider.locator('[data-slot="select-value"]')).toHaveText(label);
      await expect.poll(() => visibleSuppliers(page)).toEqual([...suppliers]);
    }
    expect(aggregateCalls).toEqual(["aggregateApi/list"]);
    expect(pageErrors).toEqual([]);
  });
}

test("closed aggregate filters retranslate on runtime language changes without resetting selection", async ({ page }) => {
  await mockAggregateRuntime(page, "auto");
  await page.goto("/aggregate-api/");
  const changeLanguage = async (locale: Locale) => {
    await page.evaluate((language) => {
      Object.defineProperty(navigator, "language", { configurable: true, value: language });
      window.dispatchEvent(new Event("languagechange"));
    }, locale);
    await expect(page.locator("html")).toHaveAttribute("lang", locale);
  };
  // Keep both selects closed through all four languages.
  for (const locale of Object.keys(LABELS) as Locale[]) {
    await changeLanguage(locale);
    const labels = LABELS[locale];
    await expect(page.getByRole("combobox", { name: labels.status, exact: true }).locator('[data-slot="select-value"]')).toHaveText(labels.allStatuses);
    await expect(page.getByRole("combobox", { name: labels.type, exact: true }).locator('[data-slot="select-value"]')).toHaveText(labels.allTypes);
  }
  await page.getByRole("combobox", { name: LABELS.ru.type, exact: true }).click();
  await page.getByRole("option", { name: "Codex", exact: true }).click();
  await page.getByRole("combobox", { name: LABELS.ru.status, exact: true }).click();
  await page.getByRole("option", { name: LABELS.ru.active, exact: true }).click();
  for (const locale of Object.keys(LABELS) as Locale[]) {
    await changeLanguage(locale);
    const labels = LABELS[locale];
    await expect(page.getByRole("combobox", { name: labels.status, exact: true }).locator('[data-slot="select-value"]')).toHaveText(labels.active);
    await expect(page.getByRole("combobox", { name: labels.type, exact: true }).locator('[data-slot="select-value"]')).toHaveText("Codex");
    await expect.poll(() => visibleSuppliers(page)).toEqual(["Codex active", "Compatible active"]);
  }
});
