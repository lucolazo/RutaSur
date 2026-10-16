import { correrConsumidor } from '../../shared/consumidor.js';
import { T } from '../../shared/domain/constantes.js';
import { aplicarEstado, aplicarReprogramacion, aplicarAlerta, aplicarNotificacion } from '../../shared/domain/proyeccion.js';

await correrConsumidor({
  nombre: 'proyector', topics: [T.ESTADOS, T.REPROG, T.ALERTAS, T.NOTIF], transaccional: true,
  manejar: async (ev, { topic, session, db, timestamp, log }) => {
    if (topic === T.ESTADOS || topic === T.REPROG) {
      const envios = db.collection('envios');
      const actual = await envios.findOne({ _id: ev.shipmentId }, { session });
      const r = topic === T.ESTADOS ? aplicarEstado(actual, ev) : aplicarReprogramacion(actual, ev);
      if (!r.aplicado) { log.info('evento descartado', { eventId: ev.eventId, motivo: r.motivo }); return; }
      // _meta es operativo (latencia de proyección); no forma parte del estado derivado
      await envios.replaceOne({ _id: ev.shipmentId }, { ...r.doc, _meta: { latenciaMs: Date.now() - timestamp, aplicadoEn: new Date().toISOString() } }, { upsert: true, session });
    } else if (topic === T.ALERTAS) {
      const alertas = db.collection('alertas');
      const id = `${ev.shipmentId}:${ev.regla}`;
      const nuevo = aplicarAlerta(await alertas.findOne({ _id: id }, { session }), ev);
      await alertas.replaceOne({ _id: id }, nuevo, { upsert: true, session });
    } else {
      const n = aplicarNotificacion(ev);
      await db.collection('notificaciones').replaceOne({ _id: n._id }, n, { upsert: true, session });
    }
  },
});
