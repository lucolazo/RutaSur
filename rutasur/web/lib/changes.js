import { getDb } from './db';

// Un único conjunto de change streams por proceso; cada conexión SSE se suscribe al reparto interno.
const COLECCIONES = [['envios', 'envio'], ['alertas', 'alerta'], ['notificaciones', 'notificacion']];

function abrir(db, hub, coleccion, tipo) {
  let cs;
  const reabrir = () => { try { cs?.close(); } catch {} setTimeout(() => abrir(db, hub, coleccion, tipo), 1000); };
  cs = db.collection(coleccion).watch([{ $match: { operationType: { $in: ['insert', 'update', 'replace'] } } }], { fullDocument: 'updateLookup' });
  cs.on('change', (c) => {
    const doc = c.fullDocument;
    if (!doc) return;
    const msg = { tipo, shipmentId: doc.shipmentId, doc };
    hub.oyentes.forEach((fn) => fn(msg));
  });
  let reabierto = false;
  cs.on('error', () => { if (!reabierto) { reabierto = true; reabrir(); } });
}

export async function suscribir(fn) {
  if (!globalThis.__hub) {
    globalThis.__hub = (async () => {
      const db = await getDb();
      const hub = { oyentes: new Set() };
      for (const [c, t] of COLECCIONES) abrir(db, hub, c, t);
      return hub;
    })().catch((e) => { globalThis.__hub = null; throw e; });
  }
  const hub = await globalThis.__hub;
  hub.oyentes.add(fn);
  return () => hub.oyentes.delete(fn);
}

/** Vista pública: solo estado actual y fecha estimada, sin datos internos. */
export function publico(doc) {
  const f = doc.proximoIntento?.fecha ?? doc.promesa?.venceEn ?? null;
  return { shipmentId: doc.shipmentId, estado: doc.estado, situacion: doc.situacion, estadoDesde: doc.estadoDesde,
    fechaEstimada: doc.situacion === 'RETENIDO' || doc.situacion === 'DEVOLUCION' ? null : f };
}
