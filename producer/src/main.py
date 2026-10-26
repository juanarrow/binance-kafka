import asyncio
import json
import logging
import os
import signal

import websockets
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.mapping import Discard, map_mini_ticker

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "market.tickers")
DEFAULT_WS_URL = (
    "wss://data-stream.binance.vision/stream?streams="
    "btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/bnbusdt@miniTicker"
)
WS_URL = os.environ.get("BINANCE_WS_URL", DEFAULT_WS_URL)
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8082"))

log = logging.getLogger("producer")


async def _handle_health(reader, writer):
    try:
        await reader.read(4096)
        body = b"ok"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def ensure_topic():
    delay = 1
    last_error = None
    for attempt in range(8):
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            await admin.start()
            await admin.create_topics(
                [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
            )
            log.info("result=topic-ready topic=%s", TOPIC)
            return
        except TopicAlreadyExistsError:
            log.info("result=topic-ready topic=%s", TOPIC)
            return
        except Exception as exc:
            last_error = exc
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "TopicAlreadyExists" in type(exc).__name__:
                log.info("result=topic-ready topic=%s", TOPIC)
                return
            log.warning("result=topic-retry attempt=%s error=%s", attempt + 1, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)
        finally:
            try:
                await admin.close()
            except Exception:
                log.warning("result=topic-admin-close-failed")
    raise RuntimeError(f"No se pudo preparar el topic {TOPIC}") from last_error


async def publish_forever(producer):
    delay = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60) as socket:
                delay = 1
                log.info("result=connected")
                async for raw in socket:
                    await _publish_message(producer, raw)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("result=disconnected retry_in=%s error=%s", delay, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


async def _publish_message(producer, raw):
    try:
        message = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("result=discarded series=- reason=json")
        return

    payload = message.get("data", message) if isinstance(message, dict) else message
    series = payload.get("s", "-") if isinstance(payload, dict) else "-"
    try:
        reading = map_mini_ticker(payload)
    except Discard as exc:
        log.warning("result=discarded series=%s reason=%s", series, exc.reason)
        return

    await producer.send_and_wait(
        TOPIC,
        json.dumps(reading, separators=(",", ":")).encode(),
        key=reading["series"].encode(),
    )
    log.info("result=published series=%s", reading["series"])


async def async_main():
    logging.basicConfig(level=logging.INFO, format="[producer] %(message)s")
    await asyncio.start_server(_handle_health, "0.0.0.0", HEALTH_PORT)
    log.info("result=health port=%s", HEALTH_PORT)
    await ensure_topic()

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        linger_ms=0,
    )
    await producer.start()
    publish_task = asyncio.create_task(publish_forever(producer))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stop.wait()
    finally:
        publish_task.cancel()
        await asyncio.gather(publish_task, return_exceptions=True)
        await producer.stop()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
