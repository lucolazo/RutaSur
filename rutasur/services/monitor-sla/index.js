import { correrConsumidor } from '../../shared/consumidor.js';
import { publicar } from '../../shared/kafka.js';
import { T } from '../../shared/domain/constantes.js';
import { MonitorSla } from '../../shared/domain/monitor.js';

// Estado en memoria con respaldo en MongoDB (monitor_estado / monitor_meta).
// Orden de efectos: publicar alerta -> guardar estado -> confirmar offset.
// Ante un fallo en cualquier punto se reintenta el mismo evento y los eventId deterministas evitan duplicados.
let monitor, ctxGlobal;
let cola = Promise.resolve();
const exclusivo = (fn) => { const p = cola.then(fn); cola = p.catch(() => {}); return p; };

async function persistir() {
  const { db } = ctxGlobal;
  const ids = [...monitor.tocados];
  const ops = ids.filter((i) => i !== '__reloj__' && monitor.envios.has(i)).map((i) => ({
    replaceOne: { filter: { _id: i }, replacement: monitor.envios.get(i), upsert: true } }));
  if (ops.length) await db.collection('monitor_estado').bulkWrite(ops, { ordered: false });
  await db.collection('monitor_meta').updateOne({ _id: 'reloj' }, { $max: { reloj: monitor.reloj, ultimoBarrido: monitor.ultimoBarrido } }, { upsert: true });
  ids.forEach((i) => monitor.tocados.delete(i));
}

const ctx = await correrConsumidor({
  nombre: 'monitor-sla', topics: [T.ESTADOS, T.PROMESAS],
  alIniciar: async (c) => {
    ctxGlobal = c;
    const envios = Object.fromEntries((await c.db.collection('monitor_estado').find().toArray()).map((d) => [d._id, d]));
    const meta = (await c.db.collection('monitor_meta').findOne({ _id: 'reloj' })) ?? {};
    monitor = new MonitorSla({ envios, reloj: meta.reloj ?? 0, ultimoBarrido: meta.ultimoBarrido ?? 0,
      emitir: (evs) => publicar(c.productor, T.ALERTAS, evs) });
    c.log.info('estado restaurado', { envios: Object.keys(envios).length, reloj: meta.reloj ? new Date(meta.reloj).toISOString() : null });
  },
  manejar: (ev, { topic }) => exclusivo(async () => {
    if (topic === T.PROMESAS) await monitor.alPromesa(ev); else await monitor.alEstado(ev);
    await monitor.barrer(); // cada minuto de reloj simulado, sobre todos los envíos abiertos
    await persistir();
  }),
});
// Además del barrido por reloj de eventos, un temporizador real revisa si el reloj simulado avanzó un minuto.
setInterval(() => exclusivo(async () => { await monitor.barrer(); await persistir(); }).catch((e) => ctx.log.error('barrido fallido', { error: e.message })), 2000);
