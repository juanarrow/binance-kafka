# Data Model: Panel de métricas en tiempo real

## Serie

Identidad estable de un par que el panel compara con los demás.

| Campo | Regla |
|-------|--------|
| code | Código de Binance en mayúsculas. Conjunto cerrado: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`. |
| label | Texto corto visible: `BTC`, `ETH`, `SOL`, `BNB`. Se deriva del código quitando el sufijo `USDT`. |

No se persiste. Existe como clave de la lectura y como tarjeta del panel.

## Lectura

Instantánea que viaja de la fuente al panel. Es el único mensaje del topic
`market.tickers`. Esquema normativo: [reading.schema.json](./contracts/reading.schema.json).

| Campo | Tipo | Regla |
|-------|------|--------|
| schemaVersion | entero | Obligatorio. Esta entrega solo acepta `1`. |
| series | string | Uno de los cuatro códigos. También es la clave Kafka del mensaje. |
| label | string | Etiqueta visible derivada de `series`. |
| observedAt | string fecha-hora | Instante del evento en UTC, formato RFC 3339 con milisegundos. Sale del campo `E` de la fuente. |
| value | string decimal | Último precio (`c`). Mayor que 0. |
| reference | string decimal | Precio de apertura de la ventana de 24 h (`o`). Mayor que 0. |
| high | string decimal | Máximo de la ventana (`h`). Mayor o igual que `low` y que 0. |
| low | string decimal | Mínimo de la ventana (`l`). Mayor que 0. |
| activity | string decimal | Volumen cotizado en USDT (`q`). Mayor o igual que 0. |
| activityUnit | string | Constante `USDT`. |

Validación en el productor, antes de publicar:

- Faltan `s`, `E`, `c`, `o`, `h`, `l` o `q`, o alguno de los precios no es un
  decimal positivo: descarte y log. No se publica.
- `series` fuera del conjunto cerrado: descarte y log.
- `activity` ausente o negativa: descarte. Cero está permitido.

El consumidor vuelve a validar el mismo contrato. Un mensaje que no lo cumple
no se reenvía al panel.

## Sesión de panel

Estado solo en el navegador. No se guarda en Kafka.

| Estado | Condición |
|--------|-----------|
| Conectando | El socket todavía no está abierto, o se está reabriendo. |
| Esperando datos | Socket abierto y aún no hay una lectura válida. |
| En vivo | La lectura válida más reciente tiene menos de 5 segundos. |
| Flujo interrumpido | Socket abierto y la última lectura válida supera los 5 segundos. |
| Conexión del panel perdida | El socket se cerró. El cliente reintenta con espera creciente hasta 10 segundos. |

Por serie, el navegador guarda como máximo las últimas 60 lecturas para la
gráfica. Al reconectar no recupera ese histórico: la gráfica se llena con lo
que llegue después.

## Transiciones relevantes

1. Evento de la fuente válido → lectura `schemaVersion` 1 → mensaje en
   `market.tickers` con clave `series`.
2. El consumidor entrega ese mensaje a todas las pestañas conectadas.
3. Evento inválido → log de descarte en el productor. No hay mensaje.
4. Mensaje de Kafka que no cumple el esquema → log de descarte en el
   consumidor. El panel no se entera.
5. Silencio de más de 5 segundos → la sesión pasa a «Flujo interrumpido» sin
   borrar los últimos valores ya mostrados.
