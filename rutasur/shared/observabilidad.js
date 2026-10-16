import { GRUPOS } from './domain/constantes.js';

/** Lag por grupo y partición: fin del log - offset confirmado (o inicio si nunca confirmó). */
export async function lagDeGrupo(admin, grupo, topics = GRUPOS[grupo]) {
  const confirmados = await admin.fetchOffsets({ groupId: grupo, topics });
  const filas = [];
  for (const { topic, partitions } of confirmados) {
    const fin = await admin.fetchTopicOffsets(topic);
    for (const p of partitions) {
      const f = fin.find((x) => x.partition === p.partition);
      const base = Number(p.offset) < 0 ? Number(f.low) : Number(p.offset);
      filas.push({ grupo, topic, particion: p.partition, confirmado: Number(p.offset), fin: Number(f.offset), lag: Number(f.offset) - base });
    }
  }
  return filas;
}
export const lagTotal = async (admin, grupo, topics) => (await lagDeGrupo(admin, grupo, topics)).reduce((s, f) => s + f.lag, 0);
