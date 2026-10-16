import { correrConsumidor } from '../../shared/consumidor.js';
import { T } from '../../shared/domain/constantes.js';
import { incrementosDe } from '../../shared/domain/metricas.js';

// Incrementos atómicos ($inc) + marca de procesado en la misma transacción: ni pierde ni cuenta dos veces.
await correrConsumidor({
  nombre: 'analitica', topics: [T.ESTADOS], transaccional: true,
  manejar: async (ev, { db, session }) => {
    const m = incrementosDe(ev);
    if (!m) return;
    await db.collection('metricas_diarias').updateOne(m.filtro, { $inc: m.inc, $setOnInsert: { hubId: m.hubId } }, { upsert: true, session });
  },
});
