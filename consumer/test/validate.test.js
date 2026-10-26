import assert from "node:assert/strict";
import test from "node:test";

import { validateReading } from "../src/validate.js";

function reading(overrides = {}) {
  return {
    schemaVersion: 1,
    series: "ETHUSDT",
    label: "ETH",
    observedAt: "2026-10-26T08:15:00.000Z",
    value: "3456.78",
    reference: "3400.00",
    high: "3500.00",
    low: "3300.00",
    activity: "12345.67",
    activityUnit: "USDT",
    ...overrides,
  };
}

test("acepta una lectura del contrato", () => {
  assert.equal(validateReading(reading()).ok, true);
});

test("rechaza otra schemaVersion", () => {
  assert.equal(validateReading(reading({ schemaVersion: 2 })).ok, false);
});

test("rechaza una unidad distinta de USDT", () => {
  assert.equal(validateReading(reading({ activityUnit: "EUR" })).ok, false);
});

test("rechaza un campo de más", () => {
  assert.equal(validateReading(reading({ extra: "no" })).ok, false);
});
