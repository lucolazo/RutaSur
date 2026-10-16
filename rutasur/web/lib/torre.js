import { getDb, plano } from './db';

export async function resumenTorre() {
  const db = await getDb();
  const [porEstado, alertas, ultimos] = await Promise.all([
    db.collection('envios').aggregate([{ $group: { _id: { estado: '$estado', situacion: '$situacion' }, n: { $sum: 1 } } }]).toArray(),
    db.collection('alertas').find({ estado: 'ABIERTA' }).sort({ prioridadRank: -1, abiertaEn: -1 }).limit(30).toArray(),
    db.collection('envios').find({}, { projection: { historial: 0, reprogramaciones: 0 } }).sort({ estadoDesde: -1 }).limit(15).toArray(),
  ]);
  const conteos = {};
  for (const r of porEstado) { const k = r._id.estado === 'ENTREGA_FALLIDA' ? r._id.situacion ?? 'PENDIENTE' : r._id.estado; conteos[k] = (conteos[k] ?? 0) + r.n; }
  return plano({ conteos, alertas, ultimos });
}
