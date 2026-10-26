"""Productor: fuente pública de Binance hacia el topic ``market.tickers``.

Variables de entorno:

- ``KAFKA_BOOTSTRAP``: broker interno. Por defecto ``kafka:19092``.
- ``KAFKA_TOPIC``: topic de lecturas. Por defecto ``market.tickers``.
- ``BINANCE_WS_URL``: stream combinado. Por defecto los cuatro mini tickers
  de BTC, ETH, SOL y BNB frente a USDT, sin API key.
- ``HEALTH_PORT``: HTTP de salud para Compose. Por defecto ``8082``. No se
  publica al host.

La clave de cada mensaje es el código de serie, así cada par conserva el orden
dentro de su partición. Un evento inválido se descarta y el proceso sigue.
Si el socket de la fuente se cae, se reintenta con espera creciente.
"""

import asyncio  # Bucle de eventos: espera red sin bloquear el proceso.
import json  # El frame de Binance y el mensaje de Kafka son texto JSON.
import logging
import os  # Lee la configuración que inyecta docker-compose.
import signal  # SIGINT y SIGTERM avisan de que hay que cerrar.

import websockets  # Cliente del stream público de Binance.
from aiokafka import AIOKafkaProducer  # Publica en Kafka desde asyncio.
from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # Crea el topic al arrancar.
from aiokafka.errors import TopicAlreadyExistsError

from src.mapping import Discard, map_mini_ticker  # Traduce el ticker al contrato.

# Dirección interna del broker. Desde el portátil se usa localhost:9092.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
# Nombre del topic donde quedan las lecturas para el consumidor.
TOPIC = os.environ.get("KAFKA_TOPIC", "market.tickers")
# Cuatro series en un solo socket. El host no pide API key.
DEFAULT_WS_URL = (
    "wss://data-stream.binance.vision/stream?streams="
    "btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/bnbusdt@miniTicker"
)
# Permite cambiar la fuente sin tocar el código.
WS_URL = os.environ.get("BINANCE_WS_URL", DEFAULT_WS_URL)
# Puerto solo visible dentro de la red de Compose.
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8082"))

log = logging.getLogger("producer")


async def _handle_health(reader, writer):
    """Responde ``ok`` a cualquier petición. Compose solo comprueba el 200."""
    try:
        await reader.read(4096)  # Consume la petición HTTP para poder responder.
        body = b"ok"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        await writer.drain()  # Espera a que los bytes salgan al socket.
    finally:
        writer.close()
        await writer.wait_closed()


async def ensure_topic():
    """Crea el topic con 3 particiones y replicación 1 si todavía no existe.

    El broker puede estar healthy y aun así rechazar al admin los primeros
    segundos, por eso hay reintentos. Si otro proceso lo creó antes, se
    considera listo.
    """
    delay = 1  # Segundos antes del primer reintento. Luego se duplican.
    last_error = None
    for attempt in range(8):  # Ocho intentos bastan para el arranque de KRaft.
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            await admin.start()  # Abre la conexión de administración.
            await admin.create_topics(
                # 3 particiones y la clave por serie reparten los cuatro pares.
                # Replicación 1: hay un solo broker.
                [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
            )
            log.info("result=topic-ready topic=%s", TOPIC)
            return
        except TopicAlreadyExistsError:
            log.info("result=topic-ready topic=%s", TOPIC)
            return  # Otro arranque ya lo dejó creado.
        except Exception as exc:
            last_error = exc
            # aiokafka a veces envuelve "ya existe" en otra excepción.
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "TopicAlreadyExists" in type(exc).__name__:
                log.info("result=topic-ready topic=%s", TOPIC)
                return
            log.warning("result=topic-retry attempt=%s error=%s", attempt + 1, exc)
            await asyncio.sleep(delay)  # Cede el bucle mientras espera al broker.
            delay = min(delay * 2, 10)  # Tope de 10 s entre intentos.
        finally:
            try:
                await admin.close()  # Suelta el cliente aunque el intento falle.
            except Exception:
                log.warning("result=topic-admin-close-failed")
    raise RuntimeError(f"No se pudo preparar el topic {TOPIC}") from last_error


async def publish_forever(producer):
    """Mantiene el stream abierto y republica cada mensaje válido.

    La espera de reconexión empieza en 1 s y se duplica hasta 30 s. Binance
    cierra el socket como máximo a las 24 h y envía ping cada 20 s; la
    biblioteca ``websockets`` contesta el pong sola.
    """
    delay = 1
    while True:  # El corte de la fuente no termina el proceso.
        try:
            # ping_interval responde a Binance; ping_timeout corta un socket mudo.
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as socket:
                delay = 1  # Una conexión buena reinicia la espera.
                log.info("result=connected")
                async for raw in socket:  # Cada iteración es un frame de texto.
                    await _publish_message(producer, raw)
        except asyncio.CancelledError:
            raise  # La parada del proceso debe propagarse, no reintentarse.
        except Exception as exc:
            log.warning("result=disconnected retry_in=%s error=%s", delay, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)  # 1, 2, 4… hasta 30 s.


async def _publish_message(producer, raw):
    """Normaliza un frame del stream y lo envía con clave igual a la serie.

    El stream combinado envuelve el ticker en ``{"stream", "data"}``. Un
    frame suelto, sin envoltura, también se acepta. ``linger_ms=0`` en el
    productor evita retener el mensaje en un lote.
    """
    try:
        message = json.loads(raw)  # El frame llega como texto.
    except json.JSONDecodeError:
        log.warning("result=discarded series=- reason=json")
        return  # Un frame malformado no corta el stream.

    # El stream combinado mete el ticker en "data". Sin esa clave, el propio mensaje lo es.
    payload = message.get("data", message) if isinstance(message, dict) else message
    series = payload.get("s", "-") if isinstance(payload, dict) else "-"
    try:
        reading = map_mini_ticker(payload)  # Contrato versión 1, o Discard.
    except Discard as exc:
        log.warning("result=discarded series=%s reason=%s", series, exc.reason)
        return

    await producer.send_and_wait(
        TOPIC,
        json.dumps(reading, separators=(",", ":")).encode(),  # JSON compacto en UTF-8.
        key=reading["series"].encode(),  # La clave agrupa cada par en una partición.
    )
    log.info("result=published series=%s", reading["series"])


async def async_main():
    """Arranca salud, topic y bucle de publicación hasta SIGINT o SIGTERM."""
    logging.basicConfig(level=logging.INFO, format="[producer] %(message)s")
    # El healthcheck de Compose puede llamar mientras el topic aún se crea.
    await asyncio.start_server(_handle_health, "0.0.0.0", HEALTH_PORT)
    log.info("result=health port=%s", HEALTH_PORT)
    await ensure_topic()  # No publicamos hasta que el topic existe.

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",  # Espera el acuse del líder. Con un broker, es ese nodo.
        linger_ms=0,  # Envía ya: no agrupa mensajes para ir más rápido.
    )
    await producer.start()
    # La publicación corre en paralelo a la espera de la señal de parada.
    publish_task = asyncio.create_task(publish_forever(producer))
    stop = asyncio.Event()  # Se marca cuando llega SIGINT o SIGTERM.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)  # La señal despierta stop.wait().

    try:
        await stop.wait()  # El proceso se queda aquí mientras llegan lecturas.
    finally:
        publish_task.cancel()  # Corta el bucle del stream.
        await asyncio.gather(publish_task, return_exceptions=True)  # Espera a que termine.
        await producer.stop()  # Vacía y cierra la conexión con Kafka.


def main():
    """Punto de entrada del contenedor: ``python -m src.main``."""
    asyncio.run(async_main())  # Crea el bucle, ejecuta async_main y lo cierra.


if __name__ == "__main__":
    main()
