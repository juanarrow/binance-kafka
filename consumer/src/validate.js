/**
 * Comprueba que un mensaje de Kafka sea una lectura del contrato versión 1.
 *
 * Las reglas son las mismas que en `producer/src/mapping.py`: cuatro series,
 * importes en cadena decimal, `activityUnit` fijo en USDT y ningún campo de
 * más. Un rechazo no tumba el consumidor; solo evita reenviarlo al panel.
 * El esquema está en
 * `specs/001-streaming-metrics-dashboard/contracts/reading.schema.json`.
 */

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

/**
 * @param {unknown} reading Mensaje JSON ya parseado del topic.
 * @returns {{ok: true} | {ok: false, reason: string}}
 */
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

/**
 * @param {unknown} raw
 * @param {boolean} positive `true` exige un precio mayor que cero.
 */
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
