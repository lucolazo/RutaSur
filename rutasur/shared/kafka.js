import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { cfg } from './config.js';
import { dormir } from './mongo.js';

export const kafka = new Kafka({ clientId: 'rutasur', brokers: cfg.brokers, logLevel: logLevel.WARN, retry: { retries: 30, initialRetryTime: 300 } });

export async function crearProductor() {
  const p = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, idempotent: true, maxInFlightRequests: 1 });
  for (let i = 0; ; i++) {
    try { await p.connect(); return p; } catch (e) { if (i > 60) throw e; await dormir(2000); }
  }
}

/** Publica eventos con clave = shipmentId (garantiza orden por envío dentro de la partición). */
export const publicar = (productor, topic, eventos) =>
  productor.send({ topic, acks: -1, messages: eventos.map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })) });

export const publicarCrudo = (productor, topic, clave, valor) =>
  productor.send({ topic, acks: -1, messages: [{ key: clave, value: valor }] });
