# Quickstart de validación

Guía para comprobar la feature de extremo a extremo. El detalle de campos está
en [data-model.md](./data-model.md) y en [contracts/](./contracts/).

## Prerrequisitos

- Docker con Compose v2.
- Navegador de escritorio.
- Salida a Internet hacia `data-stream.binance.vision`.
- Puertos locales 8088 y 9092 libres.

## Arranque

Desde la raíz del repositorio:

```bash
docker compose up --build
```

Resultado esperado en menos de 3 minutos:

1. El servicio `kafka` queda healthy.
2. El productor loguea `result=published` para `BTCUSDT`, `ETHUSDT`, `SOLUSDT`
   y `BNBUSDT`.
3. `http://localhost:8088` responde con el panel en español.

## Panel

1. Abrir `http://localhost:8088`.
2. Comprobar el aviso de que las cifras no son una recomendación de inversión.
3. Sin recargar, las cuatro tarjetas pasan de «—» a un precio, una variación
   coloreada, actividad en USDT, máximo, mínimo y una gráfica que se mueve.
4. El estado pasa a «En vivo» y el ritmo muestra lecturas por segundo.
5. Recargar la pestaña. La conexión vuelve sola y las tarjetas se llenan de nuevo.

## Recorrido del dato

Con el panel abierto en «En vivo»:

```bash
docker compose stop producer
```

En unos 5 segundos el panel muestra «Flujo interrumpido» y conserva el último
valor, sin sustituirlo por ceros.

```bash
docker compose start producer
```

El estado vuelve a «En vivo» sin recargar.

Para ver el topic desde el host, con la pila en marcha:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.tickers \
  --max-messages 4
```

Cada línea es una lectura del contrato, con `schemaVersion` 1.

## Pruebas de unidad

No sustituyen al arranque. Cubren el mapeo y la validación del contrato:

```bash
python3 -m venv .venv
.venv/bin/pip install -r producer/requirements-dev.txt
.venv/bin/pytest producer/tests
cd consumer && npm test
```

## Parada

```bash
docker compose down
```

El topic desaparece con el contenedor de Kafka. El siguiente arranque lo crea
de nuevo.
