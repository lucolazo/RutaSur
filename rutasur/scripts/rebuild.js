// Reconstrucción del modelo de lectura del proyector: borra sus colecciones y vuelve a leer Kafka desde el inicio.
// El proyector debe estar detenido (grupo sin miembros). Uso: sh scripts/rebuild.sh
import { kafka } from '../shared/kafka.js';
import { conectarMongo, dormir } from '../shared/mongo.js';
import { GRUPOS } from '../shared/domain/constantes.js';

const admin = kafka.admin(); await admin.connect();
for (const topic of GRUPOS.proyector) {
  for (let i = 0; ; i++) {
    try { await admin.resetOffsets({ groupId: 'proyector', topic, earliest: true }); break; }
    catch (e) { if (i > 30) throw e; console.log(`esperando que el grupo proyector quede vacío (${e.type ?? e.message})`); await dormir(2000); }
  }
}
await admin.disconnect();
const { client, db } = await conectarMongo();
for (const c of ['envios', 'alertas', 'notificaciones']) console.log(c, 'borrados:', (await db.collection(c).deleteMany({})).deletedCount);
console.log('marcas de procesado:', (await db.collection('eventos_procesados').deleteMany({ consumidor: 'proyector' })).deletedCount);
await client.close();
console.log('listo: al arrancar, el proyector reprocesa el log completo');
