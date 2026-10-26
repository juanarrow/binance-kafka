/**
 * Panel en el navegador. Se conecta a `/ws` del mismo origen y pinta cuatro
 * series. No habla con Kafka ni guarda histórico: la gráfica conserva como
 * máximo 60 precios de esta pestaña.
 *
 * El estado pasa a "Flujo interrumpido" si el socket sigue abierto pero no
 * llega ninguna lectura en 5 s. Binance emite cada serie cerca de una vez por
 * segundo, así que ese silencio no es la cadencia normal. Los precios no se
 * sustituyen por ceros durante el corte.
 */

const SERIES = [
  { code: "BTCUSDT", label: "BTC" },
  { code: "ETHUSDT", label: "ETH" },
  { code: "SOLUSDT", label: "SOL" },
  { code: "BNBUSDT", label: "BNB" },
];

const STALE_MS = 5000;
const MAX_POINTS = 60;

const board = document.querySelector("#board");
const statusEl = document.querySelector("#status");
const rateEl = document.querySelector("#rate");

const seriesState = new Map();
const arrivals = [];
let lastReadingAt = 0;
let socketState = "connecting";
let retryDelay = 1000;

for (const series of SERIES) {
  seriesState.set(series.code, { points: [], reading: null });
  board.append(createRow(series));
}

connect();
window.setInterval(() => {
  renderStatus();
  renderRate();
}, 500);

/** Crea la fila de una serie con guiones hasta que llegue la primera lectura. */
function createRow(series) {
  const row = document.createElement("article");
  row.className = "row";
  row.dataset.series = series.code;

  const symbol = document.createElement("div");
  symbol.className = "symbol";
  const title = document.createElement("h2");
  title.textContent = series.label;
  const code = document.createElement("p");
  code.textContent = series.code;
  symbol.append(title, code);

  const price = document.createElement("div");
  price.className = "price";
  const value = document.createElement("strong");
  value.dataset.field = "value";
  value.textContent = "—";
  const change = document.createElement("span");
  change.className = "change";
  change.dataset.field = "change";
  change.textContent = "—";
  price.append(value, change);

  const chart = document.createElement("canvas");
  chart.dataset.field = "chart";
  chart.width = 320;
  chart.height = 72;

  const facts = document.createElement("dl");
  facts.append(fact("Máximo", "high"), fact("Mínimo", "low"), fact("Actividad", "activity"));

  row.append(symbol, price, chart, facts);
  return row;
}

function fact(label, field) {
  const wrapper = document.createElement("div");
  const term = document.createElement("dt");
  term.textContent = label;
  const description = document.createElement("dd");
  description.dataset.field = field;
  description.textContent = "—";
  wrapper.append(term, description);
  return wrapper;
}

/** Abre el WebSocket y reintenta solo si se cierra, con espera hasta 10 s. */
function connect() {
  socketState = "connecting";
  renderStatus();
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${location.host}/ws`);

  socket.addEventListener("open", () => {
    socketState = "open";
    retryDelay = 1000;
    renderStatus();
  });

  socket.addEventListener("message", (event) => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }
    if (!message || message.type !== "reading" || !message.reading) {
      return;
    }
    applyReading(message.reading);
  });

  socket.addEventListener("close", () => {
    socketState = "closed";
    renderStatus();
    const wait = retryDelay;
    retryDelay = Math.min(retryDelay * 2, 10000);
    window.setTimeout(connect, wait);
  });
}

/** Guarda la lectura de una serie conocida y actualiza su fila. */
function applyReading(reading) {
  const state = seriesState.get(reading.series);
  if (!state) {
    return;
  }
  const value = Number(reading.value);
  if (!Number.isFinite(value)) {
    return;
  }

  state.reading = reading;
  state.points.push(value);
  if (state.points.length > MAX_POINTS) {
    state.points.shift();
  }
  lastReadingAt = Date.now();
  arrivals.push(lastReadingAt);
  paint(reading.series);
  renderStatus();
  renderRate();
}

function paint(series) {
  const state = seriesState.get(series);
  const row = board.querySelector(`[data-series="${series}"]`);
  const reading = state.reading;
  const change = percentChange(reading.value, reading.reference);
  const rising = change === null ? null : change >= 0;

  row.querySelector('[data-field="value"]').textContent = formatPrice(reading.value);
  const changeEl = row.querySelector('[data-field="change"]');
  changeEl.textContent = change === null ? "—" : formatPercent(change);
  changeEl.className = `change${change === null ? "" : rising ? " up" : " down"}`;
  row.querySelector('[data-field="high"]').textContent = formatPrice(reading.high);
  row.querySelector('[data-field="low"]').textContent = formatPrice(reading.low);
  row.querySelector('[data-field="activity"]').textContent = formatActivity(reading.activity);
  drawChart(row.querySelector('[data-field="chart"]'), state.points, rising);
}

function renderStatus() {
  const status = currentStatus();
  statusEl.textContent = status.label;
  statusEl.dataset.state = status.state;
}

/** Deriva la etiqueta visible a partir del socket y del silencio del flujo. */
function currentStatus() {
  if (socketState === "connecting") {
    return { label: "Conectando", state: "wait" };
  }
  if (socketState === "closed") {
    return { label: "Conexión del panel perdida", state: "down" };
  }
  if (!lastReadingAt) {
    return { label: "Esperando datos", state: "wait" };
  }
  if (Date.now() - lastReadingAt > STALE_MS) {
    return { label: "Flujo interrumpido", state: "down" };
  }
  return { label: "En vivo", state: "live" };
}

function renderRate() {
  if (!lastReadingAt) {
    rateEl.textContent = "—";
    return;
  }
  const cutoff = Date.now() - 1000;
  while (arrivals.length > 0 && arrivals[0] < cutoff) {
    arrivals.shift();
  }
  rateEl.textContent = String(arrivals.length);
}

/** Variación porcentual respecto al precio de apertura de la ventana de 24 h. */
function percentChange(value, reference) {
  const current = Number(value);
  const base = Number(reference);
  if (!Number.isFinite(current) || !Number.isFinite(base) || base === 0) {
    return null;
  }
  return ((current - base) / base) * 100;
}

function formatPrice(raw) {
  const value = Number(raw);
  return new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: 2,
    maximumFractionDigits: value >= 100 ? 2 : 4,
  }).format(value);
}

function formatPercent(value) {
  const formatted = new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signDisplay: "exceptZero",
  }).format(value);
  return `${formatted} %`;
}

function formatActivity(raw) {
  const value = Number(raw);
  const compact = new Intl.NumberFormat("es-ES", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(value);
  return `${compact} USDT`;
}

function drawChart(canvas, points, rising) {
  const context = canvas.getContext("2d");
  const { width, height } = canvas;
  context.clearRect(0, 0, width, height);
  if (points.length === 0) {
    return;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  context.beginPath();
  points.forEach((point, index) => {
    const x = points.length === 1 ? width / 2 : (index / (points.length - 1)) * (width - 4) + 2;
    const y = height - ((point - min) / span) * (height - 8) - 4;
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.lineWidth = 2;
  context.strokeStyle = rising === null ? "#f4efe4" : rising ? "#d6ff4a" : "#ff6b4a";
  context.stroke();
}
