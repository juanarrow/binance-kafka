# Implementation Plan: Panel de métricas en tiempo real

**Branch**: `001-streaming-metrics-dashboard` | **Date**: 2026-10-26 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-streaming-metrics-dashboard/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

El alumnado arranca con un solo `docker compose up` un laboratorio local que
toma el mini ticker público de Binance (BTC, ETH, SOL y BNB frente a USDT),
lo normaliza a una lectura versionada y lo publica en Kafka. Un consumidor
distinto reenvía esas lecturas por WebSocket, y un panel estático en
JavaScript las dibuja en vivo: precio, variación, actividad, máximo, mínimo
y ritmo de llegada.

## Technical Context

**Language/Version**: Python 3.12 (productor) y Node.js 22 (consumidor). El panel es HTML, CSS y JavaScript sin paso de compilación.

**Primary Dependencies**: `aiokafka` y `websockets` en el productor; `kafkajs` y `ws` en el consumidor; imagen `apache/kafka:4.1.2` en modo KRaft; `nginx:1.27-alpine` para servir el panel.

**Storage**: Kafka es el único almacenamiento, efímero en el contenedor. No hay base de datos. El panel conserva en memoria las últimas 60 lecturas de cada serie.

**Testing**: `pytest` para el mapeo del productor; `node:test` para la validación del consumidor. La prueba de extremo a extremo es el arranque con Docker Compose descrito en `quickstart.md`.

**Target Platform**: Docker Compose en el portátil del alumno (Docker Desktop en macOS o equivalente Linux). El panel se abre en un navegador de escritorio.

**Project Type**: Laboratorio local de streaming con cuatro procesos de aplicación más el broker: productor, Kafka, consumidor y panel.

**Performance Goals**: Cuatro series a 1 lectura por segundo cada una. Al menos el 95 % de las lecturas visibles en el panel en menos de 2 segundos desde la fuente, en local.

**Constraints**: Sin API keys ni ficheros de secretos. Un solo comando de arranque. El navegador no habla el protocolo de Kafka. Interfaz en español y aviso de que no es asesoramiento financiero. Kafka con factor de replicación 1.

**Scale/Scope**: Una persona, cuatro series fijas, un topic, un grupo de consumo. Fuera de alcance: login, histórico, alertas, móvil y edición de series desde el panel.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principio | Resultado | Evidencia |
|-----------|-----------|-----------|
| I. Entorno reproducible | Pasa | `docker-compose.yml` levanta Kafka, productor, consumidor y panel con healthchecks. No hay instalación manual del broker. |
| II. Contrato de eventos explícito | Pasa | `contracts/reading.schema.json` versión 1. Productor y consumidor lo comparten. El panel solo lee esos campos. |
| III. Fuente pública sin secretos | Pasa | `wss://data-stream.binance.vision` no exige clave. No hay `.env` con secretos. |
| IV. Pipeline desacoplado | Pasa | Cuatro procesos: productor, broker, consumidor y nginx. El navegador solo abre HTTP y WebSocket. |
| V. Simplicidad didáctica | Pasa | Un topic, un esquema, sin registro de esquemas externo, sin interfaz de administración de Kafka y sin empaquetador de frontend. |

Revisión posterior al diseño: los mismos cinco puntos siguen cumpliéndose. `contracts/`, `data-model.md` y la separación de contenedores no añaden servicios fuera del mínimo exigido por el principio IV.

## Project Structure

### Documentation (this feature)

```text
specs/001-streaming-metrics-dashboard/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── reading.schema.json
│   └── panel-stream.md
└── tasks.md
```

### Source Code (repository root)

```text
docker-compose.yml
producer/
├── Dockerfile
├── requirements.txt
├── requirements-dev.txt
├── src/
│   ├── main.py
│   └── mapping.py
└── tests/
    └── test_mapping.py
consumer/
├── Dockerfile
├── package.json
├── src/
│   ├── server.js
│   └── validate.js
└── test/
    └── validate.test.js
panel/
├── Dockerfile
├── nginx.conf
└── public/
    ├── index.html
    ├── styles.css
    └── app.js
```

**Structure Decision**: Tres directorios de aplicación en la raíz, cada uno con
su imagen, más Compose. El contrato vive en `specs/` y el código de mapeo y
de validación queda en módulos puros para poder probarlo sin Docker.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No hay violaciones. Los cuatro procesos los exige el principio IV; no se añade
una consola de Kafka ni un registro de esquemas.
