# Cinta de mercado

Laboratorio local de streaming. Una fuente pública de cotizaciones entra en
Kafka, un consumidor la reenvía y un panel en JavaScript muestra el precio, la
variación, la actividad, el máximo, el mínimo y el ritmo de llegada.

No hace falta API key ni cuenta.

## Requisitos

- Docker con Compose v2
- Un navegador de escritorio
- Los puertos `8088` y `9092` libres
- Salida a Internet hacia `data-stream.binance.vision`

## Arranque

```bash
docker compose up --build
```

Abre [http://localhost:8088](http://localhost:8088). En menos de tres minutos
deberías ver BTC, ETH, SOL y BNB moverse solos.

Para parar:

```bash
docker compose down
```

## Qué hace cada pieza

| Servicio | Papel |
| --- | --- |
| `producer` | Lee el mini ticker público de Binance y publica lecturas en el topic `market.tickers`. |
| `kafka` | Broker en modo KRaft. Desde el portátil escucha en `localhost:9092`. |
| `consumer` | Lee el grupo `panel` y reenvía cada lectura por WebSocket. |
| `panel` | Sirve la página y hace de proxy de `/ws`. El navegador no habla con Kafka. |

La URL de la fuente se puede sustituir con la variable `BINANCE_WS_URL` del
productor. El valor por defecto es el stream combinado de
`data-stream.binance.vision` para `btcusdt`, `ethusdt`, `solusdt` y `bnbusdt`.

## Ver el recorrido

Con el panel en «En vivo»:

```bash
docker compose stop producer
```

A los pocos segundos el panel pasa a «Flujo interrumpido» y conserva el último
precio. No hace falta recargar.

```bash
docker compose start producer
```

Para leer el topic desde el host:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.tickers \
  --max-messages 4
```

## Pruebas de unidad

```bash
python3 -m venv .venv
.venv/bin/pip install -r producer/requirements-dev.txt
.venv/bin/pytest producer/tests

cd consumer
npm test
```

## Especificación

El diseño vive en `specs/001-streaming-metrics-dashboard/`. La guía de
validación completa está en
[quickstart.md](specs/001-streaming-metrics-dashboard/quickstart.md).

Las cifras del panel son una demostración de streaming, no una recomendación
de inversión.
