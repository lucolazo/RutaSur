import { kafka } from '../shared/kafka.js';
import { conectarMongo, dormir } from '../shared/mongo.js';
import { TOPICS } from '../shared/domain/constantes.js';

const admin = kafka.admin();
for (let i = 0; ; i++) { try { await admin.connect(); break; } catch (e) { if (i > 60) throw e; await dormir(2000); } }
await admin.createTopics({
  waitForLeaders: true,
  topics: TOPICS.map((t) => ({
    topic: t.topic, numPartitions: 3, replicationFactor: 1,
    configEntries: t.compactado ? [{ name: 'cleanup.policy', value: 'compact' }] : [{ name: 'retention.ms', value: String(t.retencionS * 1000) }],
  })),
});
console.log('topics listos:', TOPICS.map((t) => t.topic).join(', '));
await admin.disconnect();

const { client, db } = await conectarMongo();
const ix = (c, spec, opt = {}) => db.collection(c).createIndex(spec, opt);
await Promise.all([
  ix('envios', { shipmentId: 1 }, { unique: true }), ix('envios', { estado: 1, zonaId: 1 }), ix('envios', { 'promesa.venceEn': 1 }), ix('envios', { estadoDesde: -1 }),
  ix('alertas', { estado: 1, prioridad: 1 }), ix('alertas', { shipmentId: 1 }), ix('alertas', { estado: 1, prioridadRank: -1, abiertaEn: -1 }),
  ix('notificaciones', { shipmentId: 1 }), ix('notificaciones', { origenEventId: 1 }),
  ix('metricas_diarias', { fecha: 1, zonaId: 1, franja: 1 }, { unique: true }),
  ix('metricas_tiempos_hub', { fecha: 1, hubId: 1 }, { unique: true }),
  ix('eventos_procesados', { consumidor: 1, eventId: 1 }, { unique: true }),
  ix('eventos_procesados', { procesadoEn: 1 }, { expireAfterSeconds: 7 * 86400 }),
]);
// Garantiza que las colecciones existen (las transacciones multi-colección lo requieren en versiones viejas)
console.log('índices de MongoDB listos');
await client.close();
