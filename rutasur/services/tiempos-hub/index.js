import { correrConsumidor } from '../../shared/consumidor.js';
import { T } from '../../shared/domain/constantes.js';
import { fechaDe } from '../../shared/domain/reglas.js';

// Cuarto indicador (reprocesamiento): grupo NUEVO que lee envios.estados desde el inicio del log
// y completa el histórico sin tocar a los demás servicios.
await correrConsumidor({
  nombre: 'analitica-tiempos', topics: [T.ESTADOS], transaccional: true,
  manejar: async (ev, { db, session }) => {
    const tiempos = db.collection('tiempos_envio');
    if (ev.tipo === 'INGRESADO_HUB') {
      await tiempos.updateOne({ _id: ev.shipmentId }, { $min: { ingresoEn: ev.ocurridoEn }, $setOnInsert: { hubId: ev.hubId, medido: false } }, { upsert: true, session });
    } else if (ev.tipo === 'EN_REPARTO') {
      const d = await tiempos.findOne({ _id: ev.shipmentId }, { session });
      if (!d || d.medido || !d.ingresoEn) return;
      const ms = Date.parse(ev.ocurridoEn) - Date.parse(d.ingresoEn);
      if (ms < 0) return;
      await db.collection('metricas_tiempos_hub').updateOne({ fecha: fechaDe(ev.ocurridoEn), hubId: d.hubId }, { $inc: { sumaMs: ms, cantidad: 1 } }, { upsert: true, session });
      await tiempos.updateOne({ _id: ev.shipmentId }, { $set: { medido: true } }, { session });
    }
  },
});
