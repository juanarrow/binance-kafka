# Constitución de Kafka Streaming Dashboard

## Core Principles

### I. Entorno reproducible
El sistema completo MUST levantarse con un único `docker compose up` desde un
clon limpio, sin pasos manuales de instalación de brokers ni de dependencias
en el host. Cada servicio MUST declarar imagen, puertos, variables y
comprobación de salud. Una persona MUST poder parar y volver a arrancar la
pila sin perder la capacidad de ver métricas en cuanto vuelva el flujo.

### II. Contrato de eventos explícito
Todo mensaje publicado en Kafka MUST ajustarse a un esquema versionado y
documentado en el repositorio. Productor y consumidor MUST compartir ese
contrato; un campo nuevo MUST ser compatible hacia atrás o MUST subir la
versión del esquema. El panel MUST consumir solo campos definidos en el
contrato, nunca el payload crudo de la fuente externa.

### III. Fuente pública sin secretos
La ingesta MUST usar una fuente de streaming pública que no exija API key,
cuenta ni credenciales. El repositorio MUST NOT contener secretos, tokens ni
ficheros `.env` con valores reales. Si una fuente deja de estar disponible, el
productor MUST registrarlo y reintentar sin tumbar el resto de la pila.

### IV. Pipeline desacoplado
Productor, broker, consumidor y panel MUST ser procesos distintos. El panel
JavaScript MUST NOT hablar el protocolo de Kafka; MUST recibir eventos ya
normalizados por un servicio consumidor a través de un canal apto para el
navegador. Cada etapa MUST poder reiniciarse sin reescribir las demás.

### V. Simplicidad didáctica
La pila MUST permanecer en el mínimo que demuestra el flujo de streaming:
un broker, un topic de métricas, un productor, un consumidor y un panel. Una
dependencia, un servicio o una abstracción adicional MUST justificarse porque
el flujo no puede enseñarse sin ella. Los nombres de topics, servicios y
campos MUST ser legibles sin documentación externa.

## Restricciones de plataforma

La pila corre en Docker Compose sobre el host del alumno. Kafka opera en modo
KRaft, sin ZooKeeper. El panel es una aplicación web estática en JavaScript,
servida por el mismo proceso que consume Kafka y reenvía eventos en tiempo
real. Los logs de productor y consumidor MUST identificar servicio, símbolo o
clave del evento y el resultado de publicar o entregar. Los puertos publicados
al host MUST estar documentados en un único sitio y no solaparse.

## Flujo de trabajo

Un cambio de comportamiento MUST actualizar la especificación o el plan antes
de modificar el código que lo implementa. El contrato de eventos se revisa en
el mismo cambio que altera el payload. Antes de dar una entrega por cerrada,
la pila MUST arrancar con Docker Compose y el panel MUST mostrar métricas
procedentes de la fuente pública, no de datos inventados en el cliente.

## Governance

Esta constitución prevalece sobre atajos locales y sobre convenciones no
escritas. Una enmienda MUST documentarse en este fichero, MUST incrementar la
versión según semver y MUST incluir la fecha de la enmienda. MAJOR elimina o
redefine un principio. MINOR añade un principio o una sección. PATCH aclara
redacción sin cambiar la regla. El cumplimiento se comprueba en la revisión:
entorno de un comando, contrato versionado, ausencia de secretos, separación
de etapas y ausencia de complejidad no justificada.

**Version**: 1.0.0 | **Ratified**: 2026-10-26 | **Last Amended**: 2026-10-26
