const SERIES = {
  BTCUSDT: "BTC",
  ETHUSDT: "ETH",
  SOLUSDT: "SOL",
  BNBUSDT: "BNB",
};

const DECIMAL = /^(?:0|[1-9]\d*)(?:\.\d+)?$/;

const FIELDS = [
  "schemaVersion",
  "series",
  "label",
  "observedAt",
  "value",
  "reference",
  "high",
  "low",
  "activity",
  "activityUnit",
];

export function validateReading(reading) {
  if (reading === null || typeof reading !== "object" || Array.isArray(reading)) {
    return { ok: false, reason: "payload" };
  }

  const keys = Object.keys(reading);
  if (keys.some((key) => !FIELDS.includes(key)) || FIELDS.some((key) => !keys.includes(key))) {
    return { ok: false, reason: "fields" };
  }

  if (reading.schemaVersion !== 1) {
    return { ok: false, reason: "schemaVersion" };
  }

  if (!Object.hasOwn(SERIES, reading.series) || reading.label !== SERIES[reading.series]) {
    return { ok: false, reason: "series" };
  }

  if (typeof reading.observedAt !== "string" || Number.isNaN(Date.parse(reading.observedAt))) {
    return { ok: false, reason: "observedAt" };
  }

  for (const field of ["value", "reference", "high", "low"]) {
    if (!isDecimal(reading[field], true)) {
      return { ok: false, reason: field };
    }
  }

  if (!isDecimal(reading.activity, false)) {
    return { ok: false, reason: "activity" };
  }

  if (Number(reading.high) < Number(reading.low)) {
    return { ok: false, reason: "high" };
  }

  if (reading.activityUnit !== "USDT") {
    return { ok: false, reason: "activityUnit" };
  }

  return { ok: true };
}

function isDecimal(raw, positive) {
  if (typeof raw !== "string" || !DECIMAL.test(raw)) {
    return false;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return false;
  }
  return positive ? value > 0 : value >= 0;
}
