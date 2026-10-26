# Contrato del panel

## HTTP

Origen único del navegador: `http://localhost:8088`.

| Método y ruta | Respuesta |
|---------------|-----------|
| `GET /` | `index.html` del panel. |
| `GET /styles.css` y `GET /app.js` | Estáticos del panel. |
| `GET /ws` | Nginx reescribe esta ruta a un WebSocket hacia `consumer:8081`. No es una petición HTTP de datos. |
| `GET /health` en el consumidor, red interna | `200` y cuerpo `ok` cuando el proceso está en marcha. No se publica al host. |
| `GET /health` en el productor, red interna | `200` y cuerpo `ok` cuando el proceso está en marcha. No se publica al host. |

No hay API REST de lecturas. El historial no se consulta: llega por el socket.

## WebSocket

URL desde el navegador: `ws://localhost:8088/ws` (el mismo host y puerto que la página).

Cada mensaje de servidor a cliente es un texto JSON:

```json
{
  "type": "reading",
  "reading": {
    "schemaVersion": 1,
    "series": "BTCUSDT",
    "label": "BTC",
    "observedAt": "2026-10-26T08:15:00.000Z",
    "value": "67234.12",
    "reference": "66800.00",
    "high": "68100.00",
    "low": "65990.50",
    "activity": "123456789.01",
    "activityUnit": "USDT"
  }
}
```

Reglas:

- `reading` cumple [reading.schema.json](./reading.schema.json).
- El consumidor no envía otro `type` en esta entrega.
- El cliente no envía mensajes de aplicación. Puede recibir ping del servidor y debe dejar que el navegador responda el pong.
- No hay instantánea inicial. Las tarjetas se rellenan con las lecturas que lleguen después de abrir el socket.
- Si el mensaje no es JSON o `type` no es `reading`, el cliente lo ignora.

## Kafka

| Propiedad | Valor |
|-----------|--------|
| Topic | `market.tickers` |
| Particiones | 3 |
| Clave | `series`, en UTF-8 |
| Valor | JSON UTF-8 de una lectura, sin envoltura |
| Grupo del panel | `panel` |
| Offset inicial | Último (`latest`). Un consumidor que arranca tarde no relee el pasado. |

Bootstrap interno: `kafka:19092`. Desde el host: `localhost:9092`.
