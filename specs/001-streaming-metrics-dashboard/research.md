# Research: Panel de métricas en tiempo real

## Fuente pública

- Decision: Stream combinado de mini tickers de Binance Spot para `btcusdt`,
  `ethusdt`, `solusdt` y `bnbusdt`, en `wss://data-stream.binance.vision/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/bnbusdt@miniTicker`.
- Rationale: No pide API key. Cada serie llega aproximadamente una vez por
  segundo con último precio, apertura de la ventana de 24 h, máximo, mínimo y
  volumen en la moneda de cotización. El host `data-stream.binance.vision`
  publica solo datos de mercado. El dominio encaja con un panel de métricas
  comparables y se entiende en una clase sin contexto previo.
- Alternatives considered: Recent changes de Wikimedia (flujo real, pero no da
  métricas numéricas comparables). USGS de terremotos (público, pero es sondeo
  y no un chorro continuo). Cotizaciones tradicionales (suelen exigir clave).

## Broker

- Decision: Imagen oficial `apache/kafka:4.1.2`, un nodo en modo KRaft, sin
  ZooKeeper. Listener `INTERNAL://kafka:19092` para los contenedores y
  `HOST://localhost:9092` para inspeccionar desde el portátil. Topic
  `market.tickers`, 3 particiones, clave igual al código de serie, replicación
  1. Retención por defecto del broker; no se persiste el volumen entre
  recreaciones del contenedor.
- Rationale: La imagen oficial documenta este patrón de dos listeners. KRaft
  evita un segundo servicio que el alumnado no necesita para entender el flujo.
  La clave por serie mantiene el orden de cada par aunque haya varias
  particiones.
- Alternatives considered: Bitnami Kafka (más variables y cambios de licencia).
  Redpanda (otro producto; el encargo pide Kafka). Un único listener (impide a
  la vez el acceso interno por nombre de servicio y el acceso desde el host).

## Productor

- Decision: Python 3.12 con `websockets` y `aiokafka` en un solo bucle asyncio.
  Traduce el mini ticker al contrato y descarta eventos incompletos o no
  numéricos. Reconecta a la fuente con espera creciente hasta 30 segundos.
  Crea el topic al arrancar si no existe.
- Rationale: Python es el lenguaje habitual del módulo. Las dos bibliotecas son
  puras en el sentido operativo del contenedor (`aiokafka` no exige compilar
  `librdkafka`). Un solo proceso cubre la entrada.
- Alternatives considered: `confluent-kafka` (cliente más fiel al protocolo,
  pero añade la biblioteca nativa y complica la imagen). Productor en Node
  (unifica lenguaje con el panel, pero aleja el ejemplo del resto del módulo).

## Consumidor y panel

- Decision: El consumidor es Node.js 22 con `kafkajs` y `ws`. Valida el JSON
  contra el contrato y lo reenvía por WebSocket en `ws://consumer:8081` dentro
de la red de Compose. Nginx 1.27 sirve el estático en el puerto 8088 del host
y hace de proxy de `/ws` hacia el consumidor. El navegador solo habla con
`http://localhost:8088`.
- Rationale: El navegador no puede usar el protocolo de Kafka con un cliente
  habitual de clase. Separar Nginx del consumidor cumple el principio de
  procesos distintos y deja una sola URL. JavaScript sin empaquetador evita un
  paso de build y mantiene el panel legible.
- Alternatives considered: Un solo proceso Node que sirva estáticos y Kafka
  (menos contenedores, incumple la separación de panel y consumidor). Chart.js
  desde una CDN (depende de red en el aula y añade una biblioteca para cuatro
  sparklines que caben en un canvas).

## Estado del panel cuando no hay datos

- Decision: El cliente no pinta ceros. Muestra «Esperando datos» hasta la
  primera lectura válida, «En vivo» si la última lectura de cualquier serie
  tiene menos de 5 segundos, y «Flujo interrumpido» si el socket sigue abierto
  pero no llega nada en 5 segundos. Si el socket se cierra, reintenta solo y
  muestra «Conexión del panel perdida».
- Rationale: Binance emite cada serie cerca de una vez por segundo, así que 5
  segundos de silencio distinguen una pausa real de la cadencia normal sin un
  segundo topic de latidos.
- Alternatives considered: Heartbeat del productor (más mensajes y otro
  contrato para un caso que el silencio ya cubre).

## Formato numérico

- Decision: Los importes viajan como cadenas decimales. El panel calcula la
  variación `(valor - referencia) / referencia` y formatea en `es-ES`.
- Rationale: Evita perder precisión al pasar por JSON numérico y mantiene el
  contrato inequívoco entre Python y JavaScript.
- Alternatives considered: Números JSON (suficiente para una demo, peor como
  contrato didáctico de eventos).
