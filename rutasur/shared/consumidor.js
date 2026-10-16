import { kafka, crearProductor } from './kafka.js';
import { conectarMongo, dormir } from './mongo.js';
import { crearLogger } from './logger.js';
import { validar } from './domain/schema.js';
import { T } from './domain/constantes.js';

/**
 * Consumidor con las decisiones de diseño de la especificación:
 *  - al menos una vez + idempotencia (eventos_procesados por consumidor+eventId);
 *  - offsets confirmados a mano, DESPUÉS de escribir en MongoDB;
 *  - JSON inválido o que no cumple el esquema -> DLQ con el motivo y el consumo continúa;
 *  - fallos transitorios (Mongo/Kafka caídos) se reintentan sin avanzar el offset.
 * `transaccional`: marca de procesado + efecto en una sola transacción de MongoDB.
 */
export async function correrConsumidor({ nombre, topics, transaccional = false, manejar, alIniciar }) {
  const log = crearLogger(nombre);
  const { client, db } = await conectarMongo();
  const procesados = db.collection('eventos_procesados');
  const productor = await crearProductor();
  const consumer = kafka.consumer({ groupId: nombre, sessionTimeout: 15000, heartbeatInterval: 3000, maxWaitTimeInMs: 100 });
  const base = { nombre, log, db, client, productor };
  if (alIniciar) await alIniciar(base);

  async function aDlq({ topic, partition, offset, clave, raw, ev, motivo }) {
    const shipmentId = clave ?? ev?.shipmentId ?? 'desconocido';
    const registro = { eventId: `dlq-${nombre}-${topic}-${partition}-${offset}`, shipmentId, tipo: 'DLQ', ocurridoEn: new Date().toISOString(),
      version: 1, consumidor: nombre, topicOrigen: topic, particion: partition, offset, motivo, payloadOriginal: raw };
    await productor.send({ topic: T.DLQ, acks: -1, messages: [{ key: shipmentId, value: JSON.stringify(registro) }] });
  }

  async function ejecutar(ev, meta) {
    const marca = { consumidor: nombre, eventId: ev.eventId };
    if (transaccional) {
      let duplicado = false;
      await client.withSession((session) => session.withTransaction(async () => {
        duplicado = false;
        if (await procesados.findOne(marca, { session })) { duplicado = true; return; }
        await manejar(ev, { ...base, ...meta, session });
        await procesados.insertOne({ ...marca, procesadoEn: new Date() }, { session });
      }));
      return duplicado;
    }
    if (await procesados.findOne(marca)) return true;
    await manejar(ev, { ...base, ...meta });
    await procesados.updateOne(marca, { $setOnInsert: { procesadoEn: new Date() } }, { upsert: true });
    return false;
  }

  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const offset = message.offset;
      const clave = message.key?.toString() ?? null;
      const raw = message.value?.toString() ?? '';
      const confirmar = () => consumer.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);
      let ev = null, motivo = null;
      try { ev = JSON.parse(raw); } catch { motivo = 'JSON inválido'; }
      if (!motivo) motivo = validar(topic, ev);
      if (motivo) {
        await aDlq({ topic, partition, offset, clave, raw, ev, motivo });
        log.warn('evento inválido enviado a la DLQ', { topic, partition, offset, motivo });
        await confirmar();
        return;
      }
      const meta = { topic, partition, offset, timestamp: Number(message.timestamp) };
      for (let intento = 1; ; intento++) {
        try {
          const dup = await ejecutar(ev, meta);
          log.info(dup ? 'duplicado descartado' : 'evento procesado', { eventId: ev.eventId, tipo: ev.tipo, topic, partition, offset });
          break;
        } catch (e) {
          if (e.permanente) { await aDlq({ topic, partition, offset, clave, raw, ev, motivo: e.message }); break; }
          log.error('fallo transitorio, se reintenta sin confirmar el offset', { eventId: ev.eventId, intento, error: e.message });
          await heartbeat();
          await dormir(Math.min(5000, 300 * intento));
        }
      }
      await confirmar();
    },
  });
  log.info('consumidor iniciado', { grupo: nombre, topics });

  const parar = async () => { try { await consumer.disconnect(); await productor.disconnect(); await client.close(); } catch { /* cierre */ } process.exit(0); };
  process.on('SIGTERM', parar); process.on('SIGINT', parar);
  consumer.on(consumer.events.CRASH, ({ payload }) => { log.error('el consumidor se cayó', { error: payload.error?.message }); if (!payload.restart) process.exit(1); });
  return { ...base, consumer };
}
