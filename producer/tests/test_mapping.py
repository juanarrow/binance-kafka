"""Casos del contrato de lectura: ticker válido, actividad cero y descartes."""

from decimal import Decimal

import pytest

from src.mapping import Discard, map_mini_ticker


def ticker(**overrides):
    payload = {
        "e": "24hrMiniTicker",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "c": "23000.10",
        "o": "22000.00",
        "h": "24000.00",
        "l": "21000.00",
        "v": "1000",
        "q": "18.50",
    }
    payload.update(overrides)
    return payload


def test_maps_valid_mini_ticker():
    reading = map_mini_ticker(ticker())

    assert reading["schemaVersion"] == 1
    assert reading["series"] == "BTCUSDT"
    assert reading["label"] == "BTC"
    assert reading["activityUnit"] == "USDT"
    assert Decimal(reading["value"]) == Decimal("23000.10")
    assert Decimal(reading["reference"]) == Decimal("22000.00")
    assert Decimal(reading["high"]) == Decimal("24000.00")
    assert Decimal(reading["low"]) == Decimal("21000.00")
    assert Decimal(reading["activity"]) == Decimal("18.50")
    assert reading["observedAt"].endswith("Z")
    assert "T" in reading["observedAt"]


def test_allows_zero_activity():
    reading = map_mini_ticker(ticker(q="0"))
    assert reading["activity"] == "0"


@pytest.mark.parametrize(
    "overrides",
    [
        {"c": None},
        {"o": ""},
        {"h": "no-es-numero"},
        {"l": "0"},
        {"c": "-1"},
        {"q": "-0.1"},
        {"E": "1672515782136"},
        {"s": "DOGEUSDT"},
        {"h": "20000", "l": "21000"},
    ],
)
def test_discards_incomplete_or_invalid_ticker(overrides):
    with pytest.raises(Discard):
        map_mini_ticker(ticker(**overrides))
