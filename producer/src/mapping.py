"""Traduce un mini ticker de Binance a una lectura del contrato.

El productor publica solo el resultado de ``map_mini_ticker``. Un evento que no
cumple las reglas lanza ``Discard`` y no llega a Kafka. Los importes viajan
como cadenas decimales para no perder precisión al cruzar de Python a
JavaScript. El esquema normativo está en
``specs/001-streaming-metrics-dashboard/contracts/reading.schema.json``.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

SERIES = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "BNBUSDT": "BNB",
}

DECIMAL_PATTERN = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")


class Discard(Exception):
    """El evento de la fuente no es publicable. ``reason`` entra en el log."""

    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def map_mini_ticker(payload):
    """Convierte el objeto ``data`` de un mini ticker en una lectura versión 1.

    Campos de origen que se conservan: ``s`` (serie), ``E`` (instante en
    milisegundos), ``c`` (último precio), ``o`` (apertura de 24 h), ``h``,
    ``l`` y ``q`` (volumen cotizado en USDT). El resto de la fuente se ignora.
    """
    if not isinstance(payload, dict):
        raise Discard("payload")

    series = payload.get("s")
    if series not in SERIES:
        raise Discard("series")

    event_time = payload.get("E")
    # bool es subclase de int; un true no es un instante válido.
    if isinstance(event_time, bool) or not isinstance(event_time, int):
        raise Discard("observedAt")

    observed = datetime.fromtimestamp(event_time / 1000, tz=timezone.utc)
    observed_at = observed.strftime("%Y-%m-%dT%H:%M:%S.") + f"{observed.microsecond // 1000:03d}Z"

    value = _decimal(payload.get("c"), positive=True)
    reference = _decimal(payload.get("o"), positive=True)
    high = _decimal(payload.get("h"), positive=True)
    low = _decimal(payload.get("l"), positive=True)
    activity = _decimal(payload.get("q"), positive=False)

    if Decimal(high) < Decimal(low):
        raise Discard("high")

    return {
        "schemaVersion": 1,
        "series": series,
        "label": SERIES[series],
        "observedAt": observed_at,
        "value": value,
        "reference": reference,
        "high": high,
        "low": low,
        "activity": activity,
        "activityUnit": "USDT",
    }


def _decimal(raw, *, positive):
    """Normaliza una cadena decimal de la fuente.

    ``positive`` exige un importe mayor que cero (precios). La actividad
    admite cero porque una ventana puede no haber negociado todavía.
    """
    if not isinstance(raw, str):
        raise Discard("decimal")
    try:
        number = Decimal(raw)
    except InvalidOperation as exc:
        raise Discard("decimal") from exc
    if not number.is_finite():
        raise Discard("decimal")
    if positive and number <= 0:
        raise Discard("decimal")
    if not positive and number < 0:
        raise Discard("decimal")
    rendered = format(number, "f")
    if not DECIMAL_PATTERN.fullmatch(rendered):
        raise Discard("decimal")
    return rendered
