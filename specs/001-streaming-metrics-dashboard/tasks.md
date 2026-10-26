---
description: "Task list for the realtime metrics panel"
---

# Tasks: Panel de métricas en tiempo real

**Input**: Design documents from `/specs/001-streaming-metrics-dashboard/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Incluidas porque `plan.md` fija `pytest` y `node:test`, y `quickstart.md` las usa como validación del contrato. No sustituyen al arranque con Compose.

**Organization**: Tareas agrupadas por historia de usuario para poder implementar y probar cada una por separado.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Puede ejecutarse en paralelo (otro fichero, sin depender de tareas incompletas)
- **[Story]**: Historia a la que pertenece (`US1`, `US2`, `US3`)

## Path Conventions

- `producer/` Python 3.12
- `consumer/` Node.js 22
- `panel/` estáticos servidos por Nginx
- `docker-compose.yml` en la raíz

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Esqueleto del repositorio y dependencias, sin lógica de negocio.

- [X] T001 Crear `.gitignore` en la raíz ignorando `.venv/`, `__pycache__/`, `consumer/node_modules/` y ficheros `.env`
- [X] T002 [P] Declarar `aiokafka` y `websockets` en `producer/requirements.txt`, y esas mismas más `pytest` en `producer/requirements-dev.txt`
- [X] T003 [P] Declarar `kafkajs` y `ws` en `consumer/package.json`, con el script `test` igual a `node --test`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Los cuatro procesos arrancan y se ven en la red de Compose. Bloquea todas las historias.

- [X] T004 Escribir `docker-compose.yml` con `kafka` (`apache/kafka:4.1.2`, KRaft, listener interno `kafka:19092` y listener de host `localhost:9092`), `producer`, `consumer` y `panel`, healthchecks y `depends_on` por salud
- [X] T005 [P] Escribir `producer/Dockerfile` sobre `python:3.12-slim` que instale `producer/requirements.txt` y arranque `python -m src.main`
- [X] T006 [P] Escribir `consumer/Dockerfile` sobre `node:22-alpine` que instale las dependencias de producción y arranque `node src/server.js`
- [X] T007 [P] Escribir `panel/Dockerfile` sobre `nginx:1.27-alpine` y `panel/nginx.conf` sirviendo `panel/public` en el puerto 80 y haciendo proxy de `/ws` a `consumer:8081`

**Checkpoint**: `docker compose config` es válido. Aún no hay lecturas ni panel útil.

---

## Phase 3: User Story 1 - Ver métricas que cambian solas (Priority: P1)

**Goal**: El panel muestra cuatro series en vivo con precio, variación, actividad, máximo, mínimo y gráfica, sin recargar.

**Independent Test**: Con la pila en marcha y la fuente disponible, abrir `http://localhost:8088` y ver las cuatro tarjetas cambiar solas.

### Tests for User Story 1

- [X] T008 [P] [US1] Cubrir en `producer/tests/test_mapping.py` el mapeo de un mini ticker válido a una lectura `schemaVersion` 1 y el descarte si falta un campo, si el decimal no es positivo o si `series` no está en `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`
- [X] T009 [P] [US1] Cubrir en `consumer/test/validate.test.js` la aceptación de una lectura del contrato y el rechazo si `schemaVersion` no es 1, si `activityUnit` no es `USDT` o si sobra un campo

### Implementation for User Story 1

- [X] T010 [P] [US1] Implementar `producer/src/mapping.py`: `series` cerrado en esos cuatro códigos, `label` sin el sufijo `USDT`, `observedAt` en RFC 3339 desde `E`, importes como cadenas decimales tomadas de `c`, `o`, `h`, `l` y `q`, `activityUnit` constante `USDT`, y descarte si falta un campo, si un precio no es un decimal mayor que 0 o si `activity` es negativa
- [X] T011 [P] [US1] Implementar `consumer/src/validate.js` con las mismas reglas de `contracts/reading.schema.json`, incluido `additionalProperties` rechazado
- [X] T012 [US1] Implementar `producer/src/main.py` para suscribirse al stream combinado, publicar en `market.tickers` con clave `series` y crear el topic con 3 particiones si no existe
- [X] T013 [US1] Implementar `consumer/src/server.js` para consumir el grupo `panel` desde el offset `latest` y reenviar `{ "type": "reading", "reading": ... }` por WebSocket en el puerto 8081
- [X] T014 [P] [US1] Implementar `panel/public/index.html`, `panel/public/styles.css` y `panel/public/app.js` con cuatro tarjetas, variación coloreada, actividad en USDT, máximo, mínimo, sparklines de hasta 60 puntos y el aviso de que no es una recomendación de inversión

**Checkpoint**: La historia 1 se demuestra abriendo el panel. No depende de las historias 2 y 3.

---

## Phase 4: User Story 2 - Poner en marcha el laboratorio sin cuentas (Priority: P2)

**Goal**: Un clon limpio arranca con la acción documentada y sin claves, y el panel recibe lecturas reales.

**Independent Test**: Seguir solo el README desde un directorio limpio hasta ver la primera lectura real en menos de 3 minutos.

### Implementation for User Story 2

- [X] T015 [US2] Documentar en `README.md` el único comando `docker compose up --build`, los puertos 8088 y 9092, la ausencia de API key y el enlace a `specs/001-streaming-metrics-dashboard/quickstart.md`
- [X] T016 [US2] Dejar en `producer/src/main.py` la URL por defecto `wss://data-stream.binance.vision/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/bnbusdt@miniTicker`, sustituible con `BINANCE_WS_URL`, sin fichero `.env` obligatorio

**Checkpoint**: Las historias 1 y 2 funcionan. El arranque no pide credenciales.

---

## Phase 5: User Story 3 - Entender el recorrido del dato (Priority: P3)

**Goal**: Se puede parar solo el productor y el panel lo explica; al volver, se recupera sin recargar. Los descartes quedan en el log.

**Independent Test**: `docker compose stop producer` lleva el panel a «Flujo interrumpido» en unos 5 segundos; `docker compose start producer` lo devuelve a «En vivo» sin recargar.

### Implementation for User Story 3

- [X] T017 [US3] Añadir en `producer/src/main.py` la reconexión con espera de 1 s duplicada hasta 30 s y logs `result=published` o `result=discarded` con la serie
- [X] T018 [US3] Añadir en `consumer/src/server.js` el log `result=discarded` cuando `validate.js` rechaza un mensaje, sin cerrar el proceso
- [X] T019 [US3] Completar en `panel/public/app.js` los estados «Conectando», «Esperando datos», «En vivo» (última lectura con menos de 5 s), «Flujo interrumpido» (silencio de más de 5 s, conservando el último valor y sin pintar ceros) y «Conexión del panel perdida» con reintento hasta 10 s

**Checkpoint**: Las tres historias se demuestran con el panel y los logs.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Cerrar la validación cruzada del quickstart.

- [X] T020 Ejecutar `pytest` sobre `producer/tests` y `npm test` en `consumer/`
- [X] T021 Ejecutar la validación de `specs/001-streaming-metrics-dashboard/quickstart.md` con Docker Compose y el panel en `http://localhost:8088`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: sin dependencias.
- **Foundational (Phase 2)**: depende de la fase 1 y bloquea las historias.
- **User Stories (Phase 3+)**: dependen de la fase 2. El orden de entrega es P1 → P2 → P3.
- **Polish**: depende de las historias que se quieran entregar.

### User Story Dependencies

- **User Story 1 (P1)**: empieza tras la fase 2. No depende de US2 ni US3.
- **User Story 2 (P2)**: empieza tras la fase 2. Usa el productor de US1 para fijar la URL pública y documentar el arranque.
- **User Story 3 (P3)**: empieza tras la fase 2. Añade resiliencia y estados sobre el panel de US1.

### Within Each User Story

- Los tests de US1 se escriben antes que el mapeo y la validación, y deben fallar antes de implementarlos.
- El mapeo y la validación van antes que los bucles de red.
- US1 queda demostrable antes de pasar a US2.

### Parallel Opportunities

- T002 y T003 en paralelo.
- T005, T006 y T007 en paralelo tras T004 si los Dockerfiles no cambian el Compose ya escrito; en la práctica T004 se cierra antes.
- T008 y T009 en paralelo.
- T010 y T011 en paralelo.
- T014 en paralelo con T012 y T013 una vez conocido el JSON del contrato.

---

## Parallel Example: User Story 1

```bash
# Tests de contrato, en paralelo:
Task: "T008 producer/tests/test_mapping.py"
Task: "T009 consumer/test/validate.test.js"

# Núcleos puros, en paralelo:
Task: "T010 producer/src/mapping.py"
Task: "T011 consumer/src/validate.js"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Completar la fase 1 y la fase 2.
2. Completar la fase 3.
3. Parar y abrir `http://localhost:8088`: las cuatro series se mueven solas.

### Incremental Delivery

1. Setup + foundational: la pila existe.
2. US1: el panel en vivo es el MVP.
3. US2: el README basta para repetir el laboratorio sin claves.
4. US3: parar el productor se entiende desde el panel.

### Parallel Team Strategy

1. Todo el equipo cierra las fases 1 y 2.
2. Después, una persona puede llevar el productor (T010, T012, T016, T017) y otra el consumidor y el panel (T011, T013, T014, T018, T019).

---

## Notes

- Las tareas con [P] tocan ficheros distintos.
- La etiqueta de historia sirve para parar en un checkpoint.
- No hay secretos que commitear.
- La validación final es T021, no solo los tests de unidad.
