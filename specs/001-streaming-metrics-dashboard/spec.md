# Feature Specification: Panel de métricas en tiempo real

**Feature Branch**: `001-streaming-metrics-dashboard`

**Created**: 2026-10-26

**Status**: Draft

**Input**: User description: "Levantar un entorno local con un bus de mensajes, un productor y un consumidor. Conectarse a una fuente de datos en streaming pública de interés, alimentar el bus y visualizar distintas métricas en tiempo real en un panel."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ver métricas que cambian solas (Priority: P1)

Una persona abre el panel en el navegador y ve varias series en directo. Cada
serie muestra su valor actual, cómo se compara con el inicio de la ventana, la
actividad acumulada y los extremos de esa ventana. Los números y la gráfica se
actualizan solos, sin recargar la página.

**Why this priority**: El valor del laboratorio es ver datos vivos. Sin el
panel actualizándose, el resto del flujo no se puede demostrar.

**Independent Test**: Con el entorno ya en marcha y la fuente pública
disponible, abrir el panel y comprobar que al menos cuatro series cambian de
valor sin recargar.

**Acceptance Scenarios**:

1. **Given** el flujo está llegando y el panel está abierto, **When** la fuente
   publica una lectura nueva de una serie, **Then** el panel actualiza el valor,
   la variación, la actividad y la gráfica de esa serie sin recargar.
2. **Given** el panel acaba de abrirse y ya hay lecturas recientes, **When** la
   persona mira la vista principal, **Then** distingue en el primer minuto qué
   series suben, cuáles bajan y con qué intensidad.
3. **Given** llegan lecturas de varias series, **When** la persona observa el
   panel, **Then** ve al mismo tiempo al menos cuatro series y, en cada una, el
   valor actual, la variación, la actividad acumulada, el máximo y el mínimo.

---

### User Story 2 - Poner en marcha el laboratorio sin cuentas (Priority: P2)

Una persona clona el proyecto y pone en marcha el entorno con la acción única
documentada. No crea cuentas, no pega claves y no instala el bus a mano. Al
cabo de un momento el panel muestra lecturas reales de la fuente pública.

**Why this priority**: El público es alumnado. Si el arranque depende de claves
o de pasos ocultos, el laboratorio no se puede repetir en clase.

**Independent Test**: Desde un clon limpio, ejecutar la acción documentada,
abrir el panel y comprobar que los valores proceden de la fuente pública y no
de cifras fijas escritas en la página.

**Acceptance Scenarios**:

1. **Given** un clon limpio y los prerrequisitos documentados, **When** la
   persona ejecuta la acción única de arranque, **Then** el panel muestra la
   primera lectura real en menos de tres minutos.
2. **Given** el entorno está detenido, **When** la persona vuelve a arrancarlo,
   **Then** el panel vuelve a mostrar lecturas nuevas sin reconfigurar nada.

---

### User Story 3 - Entender el recorrido del dato (Priority: P3)

Una persona comprueba que los datos nacen en la fuente, quedan disponibles
para quien los consume y solo entonces aparecen en el panel. Puede reiniciar
la etapa de entrada o la de salida sin rehacer el resto, y entiende por qué el
panel se queda en espera o se recupera.

**Why this priority**: El objetivo formativo es el flujo de streaming, no solo
una gráfica. Separar las etapas permite señalar dónde se produce y dónde se
consume.

**Independent Test**: Con el panel abierto, detener solo la etapa que toma
datos de la fuente y comprobar que el panel deja de recibir lecturas nuevas y
lo indica; al reanudarla, las lecturas vuelven sin recargar.

**Acceptance Scenarios**:

1. **Given** el panel está conectado y recibe lecturas, **When** se detiene
   solo la etapa que lee la fuente pública, **Then** el panel deja de mover
   las series y muestra que el flujo de datos está interrumpido.
2. **Given** esa etapa se reanuda y la fuente responde, **When** llega la
   siguiente lectura válida, **Then** el panel vuelve a actualizarse sin que la
   persona recargue la página.
3. **Given** la etapa de salida arranca después de la de entrada, **When** la
   fuente sigue publicando, **Then** el panel recibe las lecturas nuevas y no
   exige que se reinicie la etapa de entrada.

---

### Edge Cases

- La fuente pública no responde al arrancar: el panel se abre, indica que aún
  no hay lecturas y se actualiza solo cuando la fuente vuelve.
- La fuente se corta a mitad de una sesión y se recupera antes de un minuto:
  el panel no exige recarga y no muestra cifras inventadas durante el corte.
- Llega un evento incompleto o con un valor que no es numérico: se descarta,
  queda constancia del descarte y las series válidas siguen actualizándose.
- La persona abre el panel antes de que exista ninguna lectura: ve el estado de
  espera, no una gráfica con ceros presentados como datos reales.
- Se cierra y se reabre la pestaña: la conexión en vivo se restablece y las
  lecturas posteriores vuelven a pintarse.
- La fuente envía una serie no prevista: el panel no se rompe; esa serie no
  sustituye a las cuatro series previstas.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El sistema MUST obtener un flujo continuo de una fuente pública
  de interés sin que la persona cree una cuenta ni aporte una clave.
- **FR-002**: El sistema MUST convertir cada evento válido de la fuente en una
  lectura con serie, instante, valor actual, valor de referencia de la ventana,
  máximo, mínimo y actividad acumulada.
- **FR-003**: Las lecturas nuevas MUST quedar disponibles para una etapa de
  salida que arranque después de la etapa de entrada, sin reiniciar la entrada.
- **FR-004**: El panel MUST mostrar a la vez al menos cuatro series distintas.
- **FR-005**: Cada serie del panel MUST mostrar el valor actual, la variación
  respecto al valor de referencia, la actividad acumulada, el máximo y el
  mínimo de la ventana.
- **FR-006**: El panel MUST actualizar esas métricas y una gráfica temporal por
  serie en cuanto llega una lectura nueva, sin recargar la página.
- **FR-007**: El panel MUST mostrar el estado de la conexión en vivo y un aviso
  comprensible cuando todavía no hay lecturas o cuando el flujo se interrumpe.
- **FR-008**: Si la fuente deja de responder, el sistema MUST reintentar la
  conexión por sí mismo y MUST NOT tumbar el panel por ese fallo.
- **FR-009**: El sistema MUST descartar eventos incompletos o no numéricos,
  MUST dejar constancia del descarte y MUST seguir procesando el resto.
- **FR-010**: Una persona MUST poder arrancar y detener todo el entorno con la
  acción única documentada, sin instalar el bus ni las etapas a mano.
- **FR-011**: El panel MUST abrirse en un navegador de escritorio moderno sin
  instalar complementos ni crear una sesión de usuario.
- **FR-012**: El panel MUST presentar la interfaz en español e MUST indicar que
  las cifras son una demostración de streaming, no una recomendación de
  inversión.
- **FR-013**: El ritmo de llegada de lecturas al panel MUST ser visible como
  una métrica propia, además de las métricas de cada serie.

### Key Entities *(include if feature involves data)*

- **Serie**: Flujo identificable que se compara con los demás. Tiene un nombre
  corto visible y un código estable. En esta entrega las series son cuatro
  pares de criptoactivos frente a una moneda de referencia estable.
- **Lectura**: Instantánea de una serie. Incluye instante de observación, valor
  actual, valor de referencia de la ventana, máximo, mínimo y actividad
  acumulada. Es la unidad que viaja desde la fuente hasta el panel.
- **Sesión de panel**: Conexión de una pestaña con el flujo en vivo. No
  identifica a una persona y no guarda preferencias.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Desde un clon limpio, la persona ve la primera lectura real en el
  panel en menos de 3 minutos, incluido el arranque del entorno.
- **SC-002**: Con la fuente disponible, al menos el 95 % de las lecturas
  observadas durante un minuto aparecen en el panel en menos de 2 segundos
  desde que la fuente las emite.
- **SC-003**: El panel muestra a la vez 4 series y 5 métricas por serie, más el
  ritmo de llegada de lecturas.
- **SC-004**: Tras un corte de la fuente de 30 segundos seguido de recuperación,
  el panel vuelve a moverse sin recargar la página.
- **SC-005**: En el primer minuto frente al panel, la persona señala qué series
  suben y cuáles bajan sin consultar documentación.

## Assumptions

- El público es alumnado de big data, con la herramienta de arranque
  documentada y un navegador de escritorio en el mismo ordenador. El panel no
  está pensado para el móvil en esta entrega.
- La fuente pública elegida son las cotizaciones al contado de BTC, ETH, SOL y
  BNB frente a USDT. Es continua, comparable y no pide clave. Las cifras no
  constituyen asesoramiento financiero.
- El conjunto de series es fijo en esta entrega. Cambiarlo requiere ajustar la
  configuración documentada, no un control dentro del panel.
- No hay inicio de sesión, alertas, historial de días anteriores ni exportación.
  El panel solo dibuja la ventana reciente de la sesión.
- Hay una sola persona usando el entorno en local. No se exige acceso
  simultáneo de un aula entera al mismo panel.
- La interfaz y los mensajes de estado están en español.
- Si la fuente pública cambia de formato o deja de existir, se sustituye la
  fuente manteniendo la misma lectura que ve el panel.
