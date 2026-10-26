# Configuración de Docker Compose

El laboratorio se levanta con el proyecto Compose `kafka-streaming-dashboard`
definido en [`docker-compose.yml`](docker-compose.yml). El flujo es:

```text
Binance  →  producer  →  kafka  →  consumer  →  panel  →  navegador
```

Productor y consumidor hablan con el broker por la red interna de Compose.
El navegador solo ve el panel. Kafka no se publica más que en el bucle local
del portátil, para poder inspeccionar el topic desde el host.

Solo `kafka`, `producer` y `consumer` definen `environment`. El panel no tiene
variables: nginx ya lleva el proxy a `consumer:8081` dentro de la imagen.
Compose inyecta cada clave como variable de entorno del contenedor en el
momento de crearlo.

## Puertos

| Quién | Dirección | Uso |
| --- | --- | --- |
| Host | `127.0.0.1:8088` → panel `:80` | Página y WebSocket (`/ws`) |
| Host | `127.0.0.1:9092` → kafka `:9092` | Clientes Kafka del portátil |
| Red interna | `kafka:19092` | Productor y consumidor |
| Red interna | `kafka:29093` | Controlador KRaft (no sale del contenedor) |
| Red interna | `producer:8082` | Salud del productor. Compose no lo publica |
| Red interna | `consumer:8081` | Salud y WebSocket. El panel hace de proxy |

Hacen falta libres en el host `8088` y `9092`. El panel y el broker quedan
ligados a `127.0.0.1`, así que no aceptan conexiones de otras máquinas.

## Orden de arranque

1. `kafka` pasa el healthcheck del API del broker.
2. `producer` y `consumer` arrancan en paralelo (`depends_on` con
   `condition: service_healthy`).
3. `panel` espera a que `consumer` responda en `/health`.

`producer` y `consumer` tienen `restart: unless-stopped`. `kafka` y `panel`
no: si se caen, Compose no los relanza solos.

Los logs del broker viven en `/tmp/kraft-combined-logs`, sin volumen. Un
`docker compose down` borra el topic y los offsets.

## kafka

| | |
| --- | --- |
| Imagen | `apache/kafka:4.1.2` |
| Hostname | `kafka` |
| Modo | Un solo nodo KRaft: broker y controlador en el mismo proceso |
| Datos | Efímeros, en `/tmp/kraft-combined-logs` |

El healthcheck ejecuta `kafka-broker-api-versions.sh` contra
`localhost:9092` cada 5 s (timeout 10 s, 24 reintentos, 25 s de gracia).

### Variables de entorno

La imagen `apache/kafka:4.1.2` traduce cada `KAFKA_*` a una propiedad de
`server.properties`: quita el prefijo, pasa a minúsculas y cambia `_` por `.`.
Así `KAFKA_NODE_ID` acaba en `node.id`. `CLUSTER_ID` no sigue esa regla: el
script de arranque de la imagen lo usa para formatear el directorio de datos
KRaft antes de lanzar el proceso.

El bloque describe un único nodo que es a la vez broker y controlador.

#### Identidad del nodo

- `KAFKA_NODE_ID: 1` es el identificador del proceso dentro del clúster. Tiene
  que coincidir con el votante del quórum.
- `KAFKA_PROCESS_ROLES: broker,controller` arranca el modo combinado de KRaft.
  El mismo proceso guarda los mensajes y la metadata. No hay ZooKeeper.
- `CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw` es el identificador del clúster, un UUID
  en base64 de 22 caracteres. La imagen formatea `/tmp/kraft-combined-logs`
  con ese valor la primera vez. Está fijo para que cada arranque del mismo
  contenedor reconozca el mismo clúster. Como ese directorio no tiene volumen,
  `docker compose down` lo borra y el siguiente `up` formatea de cero con el
  mismo id.

#### Listeners

El proceso abre tres sockets. El nombre de cada uno es libre; el mapa de
protocolos les da significado.

`KAFKA_LISTENERS` dice dónde escucha, dentro del contenedor:

| Nombre | Bind | Para quién |
| --- | --- | --- |
| `CONTROLLER` | `:29093` | Protocolo del controlador KRaft |
| `HOST` | `:9092` | Clientes del portátil. Compose publica ese puerto en `127.0.0.1:9092` |
| `INTERNAL` | `:19092` | Productor y consumidor, por la red de Compose |

`KAFKA_ADVERTISED_LISTENERS` es la dirección que el broker devuelve al cliente
después del primer contacto. El cliente se reconecta ahí, no a la dirección
por la que entró.

- `HOST://localhost:9092`: un cliente del portátil recibe «sigue en
  localhost:9092», que es justo el puerto publicado en el host.
- `INTERNAL://kafka:19092`: un cliente de otro contenedor recibe el hostname
  `kafka`, que resuelve el DNS de Compose. Por eso productor y consumidor usan
  `KAFKA_BOOTSTRAP: kafka:19092` y no `localhost:9092`. Dentro de su
  contenedor, `localhost` serían ellos mismos.

El listener `CONTROLLER` no se anuncia. Los miembros del quórum salen de la
lista de votantes, no de los advertised listeners.

`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,INTERNAL:PLAINTEXT`
asocia cada nombre con un protocolo. Los tres van en claro, sin TLS ni SASL.
Hace falta declararlo porque `HOST`, `INTERNAL` y `CONTROLLER` no son nombres
que Kafka conozca de serie.

`KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL` elige el canal del tráfico entre
brokers. Con un solo nodo no hay réplicas que mover, pero la propiedad es
obligatoria. Al dejarla en `INTERNAL`, ese tráfico no sale por
`localhost:9092`.

`KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER` marca qué listener habla el
protocolo del controlador.

`KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093` es la lista de votantes, en la
forma `id@host:puerto`. El id `1` es `KAFKA_NODE_ID`. El host `kafka` y el
puerto `29093` son el listener `CONTROLLER`. Un solo votante: si ese proceso
cae, no hay quórum.

#### Topics internos, ajustados a un solo broker

Kafka crea topics de sistema con factor de replicación 3. Con un nodo eso no
se puede cumplir, así que el compose los baja a 1.

- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` aplica a `__consumer_offsets`,
  donde el grupo `panel` guarda hasta dónde ha leído. Sin esto el coordinador
  de grupos no llega a formarse.
- `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` y
  `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` aplican a `__transaction_state`. El
  valor de serie del ISR mínimo es 2. Aunque este productor no abre
  transacciones, el broker inicializa ese topic igual.
- `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1` y
  `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1` hacen lo mismo con el topic
  del coordinador de share groups, que Kafka 4.1 crea al arrancar. El
  consumidor de este laboratorio usa el grupo clásico `panel`, no un share
  group.

#### Topics de la aplicación

- `KAFKA_NUM_PARTITIONS: 3` es el número de particiones al crear un topic si
  nadie indica otro. El productor crea `market.tickers` con 3 particiones por
  su cuenta; este valor deja el mismo criterio en el broker. Cuatro pares y la
  clave por serie se reparten en esas particiones.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` permite que el broker cree un topic
  la primera vez que alguien lo nombra. Va entre comillas para que Compose
  pase la cadena `true` y no un booleano de YAML. Es una red de seguridad: el
  productor ya lo crea por el API de administración.
- `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` quita la espera inicial del
  coordinador de grupos. El valor de serie es 3000 ms, pensado para que lleguen
  más miembros antes del primer reparto. Aquí el grupo `panel` tiene un solo
  consumidor y recibe las particiones enseguida.

#### Datos

`KAFKA_LOG_DIRS: /tmp/kraft-combined-logs` es el directorio de segmentos y de
la metadata KRaft. Está en `/tmp` del contenedor, sin volumen montado. Los
mensajes y los offsets duran lo que dura el contenedor.

## producer

| | |
| --- | --- |
| Build | [`producer/Dockerfile`](producer/Dockerfile) (`python:3.12-slim`) |
| Comando | `python -m src.main` |
| Broker | `kafka:19092` |
| Topic | `market.tickers` (lo crea al arrancar: 3 particiones, replicación 1) |

El healthcheck pide `http://127.0.0.1:8082/health` con Python cada 5 s
(timeout 3 s, 12 reintentos, 10 s de gracia). Ese puerto responde en cuanto
el proceso arranca, antes de que el topic esté listo.

El contenedor necesita salida a Internet hacia `data-stream.binance.vision`.

### Variables de entorno

`producer/src/main.py` lee estas dos variables con `os.environ`.

- `KAFKA_BOOTSTRAP: kafka:19092` es la dirección del primer contacto con el
  broker. Apunta al listener `INTERNAL` y al hostname del servicio. El
  productor la usa tanto para crear el topic como para publicar.
- `KAFKA_TOPIC: market.tickers` es el topic que crea (3 particiones,
  replicación 1) y en el que escribe cada lectura.

El proceso admite otras dos variables que Compose no fija, así que quedan en
su valor por defecto: `BINANCE_WS_URL` (el stream combinado de BTC, ETH, SOL y
BNB) y `HEALTH_PORT` (`8082`).

| Variable | En Compose | Si no está | Efecto |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:19092` | `kafka:19092` | Broker |
| `KAFKA_TOPIC` | `market.tickers` | `market.tickers` | Topic de lecturas |
| `BINANCE_WS_URL` | — | stream combinado de `btcusdt`, `ethusdt`, `solusdt` y `bnbusdt` en `data-stream.binance.vision` | Fuente pública, sin API key |
| `HEALTH_PORT` | — | `8082` | HTTP de salud, solo en la red interna |

## consumer

| | |
| --- | --- |
| Build | [`consumer/Dockerfile`](consumer/Dockerfile) (`node:22-alpine`) |
| Comando | `node src/server.js` |
| Broker | `kafka:19092` |
| Topic | `market.tickers` |
| Grupo | `panel` (offset inicial: el último; no relee el pasado) |
| Escucha | `0.0.0.0:8081` |

`/health` devuelve `200` y `ok`. El mismo puerto sirve el WebSocket que
nginx reenvía. El healthcheck hace `fetch('http://127.0.0.1:8081/health')`
cada 5 s (timeout 5 s, 12 reintentos, 10 s de gracia).

### Variables de entorno

`consumer/src/server.js` lee las mismas claves, con el mismo valor, para
suscribirse al topic que el productor llena.

- `KAFKA_BOOTSTRAP: kafka:19092` es el broker del cliente KafkaJS (`clientId`
  `panel-consumer`).
- `KAFKA_TOPIC: market.tickers` es el topic del `subscribe`. El grupo se llama
  `panel` y está escrito en el código, no en Compose.

`PORT` tampoco está en Compose. El proceso escucha en `8081`, que es el puerto
al que nginx envía `/ws` y el que usa el healthcheck.

| Variable | En Compose | Si no está | Efecto |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:19092` | `kafka:19092` | Broker |
| `KAFKA_TOPIC` | `market.tickers` | `market.tickers` | Topic |
| `PORT` | — | `8081` | HTTP y WebSocket |

Productor y consumidor tienen que llevar el mismo topic y el mismo bootstrap.
Si uno de los dos cambiara, publicarían o leerían en un sitio distinto y el
panel se quedaría sin lecturas.

## panel

| | |
| --- | --- |
| Build | [`panel/Dockerfile`](panel/Dockerfile) (`nginx:1.27-alpine`) |
| Configuración | [`panel/nginx.conf`](panel/nginx.conf) |
| Estáticos | `panel/public` → `/usr/share/nginx/html` |
| Puerto publicado | `127.0.0.1:8088` → `80` |

Nginx hace dos cosas:

- `/` sirve la página (`try_files` cae en `index.html`).
- `/ws` hace de proxy HTTP/1.1 con upgrade hacia `consumer:8081`. El
  navegador abre `ws://localhost:8088/ws` y no conoce al consumidor. La
  lectura del proxy no caduca hasta la hora (`proxy_read_timeout 3600s`) y
  no bufferiza, para no retener los ticks.

La imagen instala `wget` solo para el healthcheck, que pide
`http://127.0.0.1/` cada 5 s (timeout 3 s, 12 reintentos, 5 s de gracia).

## Uso

```bash
docker compose up --build
```

Abre [http://localhost:8088](http://localhost:8088).

```bash
docker compose down          # para y borra los contenedores (y el topic)
docker compose stop producer # corta la fuente; el panel pasa a «Flujo interrumpido»
docker compose start producer
```

Leer el topic desde el host, por el listener `HOST`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.tickers \
  --max-messages 4
```

Para cambiar la fuente hay que declararla en `environment` del productor (o
referenciar ahí una variable del host). Compose solo pasa al contenedor las
claves escritas en ese bloque. `BINANCE_WS_URL`, `HEALTH_PORT` y `PORT` usan
el valor por defecto del proceso mientras no aparezcan en el compose.
