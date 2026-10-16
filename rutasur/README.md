# RutaSur · logística orientada a eventos

Plataforma de seguimiento de envíos en tiempo real: Apache Kafka (log de eventos), Node.js (servicios), MongoDB (modelo de lectura) y Next.js (interfaz). Implementa los cuatro casos de uso de la especificación y el simulador que los alimenta.

## Cómo correrlo

```bash
docker compose up --build        # un solo comando: broker KRaft, MongoDB (replica set), topics, índices, simulador, 5 servicios y web
```

- Web: http://localhost:3000 (torre de control, `/analitica`, `/demo`, `/envio/<id>`, `/tracking/<id>`)
- Simulador (control): http://localhost:4000/state · Kafka desde el host: `localhost:29092` · MongoDB: `localhost:27017`
- Repetir exactamente la misma corrida: `docker compose down -v` y volver a levantar (semilla fija `SEMILLA=12345`).

Comandos útiles (desde la raíz, tras `npm install` para los que corren en el host):

| Comando                                                       | Para qué                                                                                     |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `npm test`                                                    | 21 tests unitarios y de pipeline en memoria (no requieren Docker)                            |
| `npm run verify`                                              | verificación de aceptación contra el entorno levantado (ver más abajo)                       |
| `npm run lag`                                                 | lag por grupo de consumidores y partición                                                    |
| `npm run rebuild`                                             | borra `envios`, `alertas` y `notificaciones` y las reconstruye leyendo Kafka desde el inicio |
| `docker compose --profile replay up -d tiempos-hub`           | activa el cuarto indicador (grupo nuevo que relee el histórico)                              |
| `docker compose stop notificaciones` / `start notificaciones` | caída y recuperación de un servicio (paso 5 de la demo)                                      |

## Estructura

```
shared/domain/   lógica pura y testeable: reglas, esquema, proyección, métricas, monitor de SLA, planificador del simulador
shared/          kafka.js, mongo.js, consumidor.js (offsets manuales, idempotencia, DLQ), observabilidad.js
services/        simulador · proyector · notificaciones · reprogramacion · monitor-sla · analitica · tiempos-hub
scripts/         init (topics + índices) · lag · rebuild · verify
web/             Next.js 14 (App Router): páginas en servidor + componentes cliente con SSE
test/            node:test
```

## Decisiones de diseño (las de la especificación, aplicadas)

- **Al menos una vez + consumidores idempotentes.** `eventos_procesados` (índice único consumidor+eventId, TTL 7 días). En proyector y analítica la marca y el efecto van en **la misma transacción** de MongoDB; el resto de los servicios usa eventId **deterministas** (`rep-<id>`, `notif-<id>`, `alerta-<envío>-<regla>`), así que un reintento nunca duplica el efecto aguas abajo.
- **Offsets confirmados a mano, después de escribir en MongoDB.** Un fallo a mitad de proceso reintenta el evento.
- **3 particiones** en cada topic, clave = `shipmentId` (orden por envío). **JSON** con validación de esquema en cada consumidor; lo inválido va a `envios.estados.dlq` con el motivo y el consumo sigue.
- **Monitor de SLA propio**: estado en memoria respaldado en `monitor_estado`. Orden de efectos: _publicar alerta → guardar estado → confirmar offset_; si algo falla se reintenta y no hay pérdida ni repetición. Usa el **reloj simulado** (mayor `ocurridoEn` visto) y barre todos los envíos abiertos cada minuto de reloj simulado.
- **Tiempo real en la interfaz**: change streams de MongoDB + Server-Sent Events. Next.js nunca consume Kafka. La torre refresca los contadores con un `fetch` a `/api/torre` (máximo cada 500 ms) y aplica directamente alertas y últimos eventos.
- **Reconstrucción determinista**: la proyección deriva `situacion` y `proximoIntento` del contenido (no del orden de llegada entre topics); el test `RECONSTRUCCIÓN` lo comprueba con 200 intercalados aleatorios.

## Supuestos (donde la especificación no define)

1. `ZONA_INACCESIBLE` no tiene regla propia: se trata como `AUSENTE` (día hábil siguiente, franja opuesta).
2. Franjas: `MANANA` = antes de las 13:00 UTC, `TARDE` = desde las 13:00; un nuevo intento arranca a las 08:00 o a las 13:00. Días hábiles: lunes a viernes. Todo el reloj simulado está en UTC.
3. `ocurridoEn` es tiempo simulado, así que la «demora de sincronización del repartidor» (diferencia con el timestamp de Kafka) no se modela; la latencia que se muestra es timestamp de Kafka → proyección.
4. El simulador conoce las reglas de reprogramación (comparte la función con el servicio) para planificar los reintentos del «mundo real», porque no consume Kafka.
5. Se notifica al destinatario ante cada `ENTREGA_FALLIDA` y ante alertas de prioridad Alta o Crítica.
6. Los envíos que inyecta `scripts/verify.js` usan zona `ZTEST` y hub `HTEST`; aparecen en la analítica como una zona más.
7. Los escenarios largos (reintento «al día hábil siguiente») se aceleran con el botón _Avanzar el reloj_ del panel de demo.

## Guion de la demo (15 min)

1. **Arranque**: `docker compose up --build`; abrir `/`.
2. **Envío feliz** en `/demo`, seguirlo en su tracking público.
3. **Ausente**: en el detalle aparecen aviso, reprogramación y analítica.
4. **Demorado**: la alerta _Alta_ (y luego _Crítica_) aparece en la cola con el reloj acelerado.
5. **Caída**: `docker compose stop notificaciones`, mirar `npm run lag`, volver a `start`.
6. **Evento inválido** en `/demo`: aparece en la DLQ.
7. **Analítica**: `/analitica` identifica la zona y la franja con más fallos.
8. **Reprocesamiento**: `docker compose --profile replay up -d tiempos-hub` y recargar `/analitica`.

## Alcance y límites

Dentro: todo lo de la tabla «Dentro del alcance». Fuera (como indica la especificación): GPS, rutas, autenticación, envío real de correos, nube, pruebas de carga. Extensiones opcionales no implementadas.
