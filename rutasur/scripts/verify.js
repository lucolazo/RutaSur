// Verificación de aceptación contra el entorno levantado (docker compose up --build -d).
// Uso: node scripts/verify.js [--no-docker] [--seccion=ciclo,fallida,sla,analitica,reconstruccion,entorno]
// Inyecta envíos de prueba propios (zona ZTEST, hub HTEST) con tiempos <= reloj simulado, para no alterar el reloj del monitor.
import { execSync } from 'node:child_process';
import { kafka, crearProductor } from '../shared/kafka.js';
import { conectarMongo, dormir } from '../shared/mongo.js';
import { validar } from '../shared/domain/schema.js';
import { lagTotal } from '../shared/observabilidad.js';
import { T } from '../shared/domain/constantes.js';
import { aplanar } from '../shared/domain/metricas.js';
import { cfg } from '../shared/config.js';

const arg = process.argv.slice(2);
const SIN_DOCKER = arg.includes('--no-docker');
const SOLO = (arg.find((a) => a.startsWith('--seccion=')) ?? '').split('=')[1]?.split(',').filter(Boolean);
const WEB = process.env.WEB_URL || 'http://localhost:3000';
const RUN = Date.now().toString(36);
const resultados = [];
const check = (sec, nombre, ok, detalle = '') => { resultados.push({ sec, nombre, ok: !!ok, detalle }); console.log(`${ok ? '✔' : '✘'} [${sec}] ${nombre}${detalle ? ` — ${detalle}` : ''}`); };
const omitido = (sec, nombre, motivo) => { resultados.push({ sec, nombre, ok: null, detalle: motivo }); console.log(`↷ [${sec}] ${nombre} — omitido: ${motivo}`); };
const dc = (a) => execSync(`docker compose ${a}`, { stdio: 'pipe', encoding: 'utf8' });
const iso = (ms) => new Date(ms).toISOString();
const estable = (x) => JSON.stringify(x, (k, v) => (v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.keys(v).sort().map((q) => [q, v[q]])) : v));

async function esperar(fn, { timeout = 40000, cada = 400 } = {}) {
  const t = Date.now();
  for (;;) { try { const r = await fn(); if (r) return r; } catch { /* reintento */ } if (Date.now() - t > timeout) return null; await dormir(cada); }
}
const simApi = async (p, m = 'GET') => { const r = await fetch(cfg.simUrl + p, { method: m }); const j = await r.json(); if (!r.ok) throw new Error(j.error); return j; };
const ahora = async () => Date.parse((await simApi('/state')).simNow);

const productor = await crearProductor();
const admin = kafka.admin(); await admin.connect();
const { client, db } = await conectarMongo();
try { await simApi('/state'); } catch { console.error(`No responde el simulador en ${cfg.simUrl}. ¿Está levantado el entorno?`); process.exit(2); }

const pub = (topic, id, evs) => productor.send({ topic, acks: -1, messages: evs.map((e) => ({ key: id, value: typeof e === 'string' ? e : JSON.stringify(e) })) });
const drenar = async (grupos, timeout = 90000) => esperar(async () => { for (const g of grupos) if ((await lagTotal(admin, g)) > 0) return false; return true; }, { timeout, cada: 700 });

async function leerTodo(topic) {
  const fin = await admin.fetchTopicOffsets(topic);
  const falta = new Map(fin.filter((p) => Number(p.offset) > Number(p.low)).map((p) => [p.partition, Number(p.offset) - 1]));
  const out = []; if (!falta.size) return out;
  const c = kafka.consumer({ groupId: `verify-${RUN}-${Math.random().toString(36).slice(2)}` });
  await c.connect(); await c.subscribe({ topic, fromBeginning: true });
  await new Promise((res) => {
    const timer = setTimeout(res, 90000);
    c.run({ eachMessage: async ({ partition, message }) => {
      out.push({ partition, offset: Number(message.offset), key: message.key?.toString(), value: message.value?.toString() });
      if (Number(message.offset) >= falta.get(partition)) falta.delete(partition);
      if (!falta.size) { clearTimeout(timer); res(); }
    } });
  });
  await c.disconnect().catch(() => {}); return out;
}

/** Envío de prueba: constructor de eventos con secuencia y tiempos crecientes (siempre <= reloj simulado). */
async function envio(nombre) {
  const id = `VERIFY-${RUN}-${nombre}`; let seq = 0; let t = (await ahora()) - 600000;
  const ev = (tipo, extra = {}) => { seq += 1; t += 1000; return { eventId: `${id}-e${seq}`, shipmentId: id, tipo, ocurridoEn: iso(t), secuencia: seq,
    hubId: 'HTEST', zonaId: 'ZTEST', repartidorId: tipo === 'CREADO' || tipo === 'INGRESADO_HUB' ? null : 'RTEST', version: 1, ...extra }; };
  const promesa = (venceEnMs) => ({ eventId: `${id}-promesa`, shipmentId: id, tipo: 'PROMESA', venceEn: iso(venceEnMs), ocurridoEn: iso(t), hubId: 'HTEST', zonaId: 'ZTEST', version: 1 });
  return { id, ev, promesa, get t() { return t; } };
}
const hastaReparto = (e, venceEnMs) => [e.ev('CREADO', { promesaVenceEn: iso(venceEnMs) }), e.ev('INGRESADO_HUB'), e.ev('CLASIFICADO'), e.ev('EN_REPARTO')];
const envioDoc = (id) => db.collection('envios').findOne({ _id: id });
const sumaZtest = async () => {
  const s = {}; for (const d of await db.collection('metricas_diarias').find({ zonaId: 'ZTEST' }).toArray()) for (const [k, v] of Object.entries(aplanar(d))) s[k] = (s[k] ?? 0) + v;
  return s;
};
const H = 3600e3;
const secciones = {};

// ───────────────────────────── CICLO DE VIDA ─────────────────────────────
secciones.ciclo = async () => {
  const S = 'Ciclo de vida';
  const r = await simApi('/scenario/feliz', 'POST');
  const doc = await esperar(async () => { const d = await envioDoc(r.shipmentId); return d?.estado === 'ENTREGADO' ? d : null; }, { timeout: 120000 });
  check(S, 'Un envío simulado llega a la torre con su estado correcto (ENTREGADO)', doc, r.shipmentId);
  if (doc) {
    check(S, 'Su historial tiene los 5 eventos en orden de secuencia', estable(doc.historial.map((h) => h.secuencia)) === '[1,2,3,4,5]');
    try {
      const [torre, trk, det] = await Promise.all([fetch(`${WEB}/`), fetch(`${WEB}/tracking/${r.shipmentId}`), fetch(`${WEB}/envio/${r.shipmentId}`)]);
      const html = await trk.text();
      check(S, 'Torre de control y detalle responden', torre.ok && det.ok);
      check(S, 'El tracking público muestra «Entregado»', trk.ok && html.includes('Entregado'));
      check(S, 'El tracking público no expone datos internos (repartidor, zona, hub)', !html.includes(doc.repartidorId) && !html.includes('"zonaId"') && !html.includes('"hubId"'));
    } catch (e) { check(S, 'Frontend accesible', false, `${WEB}: ${e.message}`); }
  }
  const e = await envio('ciclo'); const evs = hastaReparto(e, (await ahora()) + 30 * H);
  await pub(T.ESTADOS, e.id, evs);
  check(S, 'Envío de prueba alcanza EN_REPARTO', await esperar(async () => (await envioDoc(e.id))?.estado === 'EN_REPARTO'));
  await pub(T.ESTADOS, e.id, [evs[2]]); await drenar(['proyector']);
  let d = await envioDoc(e.id);
  check(S, 'Un evento duplicado no agrega una entrada repetida al historial', d.historial.length === 4 && new Set(d.historial.map((h) => h.eventId)).size === 4);
  await pub(T.ESTADOS, e.id, [{ ...evs[1], eventId: `${e.id}-viejo` }]); await drenar(['proyector']);
  d = await envioDoc(e.id);
  check(S, 'Un evento fuera de orden no hace retroceder el estado', d.estado === 'EN_REPARTO' && d.ultimaSecuencia === 4 && d.historial.length === 4);
  await pub(T.ESTADOS, e.id, [`{"eventId":"${e.id}-roto","shipmentId":"${e.id}"}`]); await drenar(['proyector', 'analitica']);
  const dlq = (await leerTodo(T.DLQ)).map((m) => { try { return JSON.parse(m.value); } catch { return null; } }).filter((x) => x?.payloadOriginal?.includes(`${e.id}-roto`));
  check(S, 'Un evento malformado termina en la DLQ con su motivo', dlq.length > 0 && dlq.every((x) => x.motivo), dlq[0]?.motivo);
  await pub(T.ESTADOS, e.id, [e.ev('ENTREGADO', { intento: 1, promesaVenceEn: iso(Date.parse(evs[0].promesaVenceEn)) })]);
  check(S, 'Tras la DLQ el consumo continúa (el envío llega a ENTREGADO)', await esperar(async () => (await envioDoc(e.id))?.estado === 'ENTREGADO'));
};

// ───────────────────────────── ENTREGA FALLIDA ─────────────────────────────
const sigHabil = (f) => { const d = new Date(`${f}T00:00:00Z`); do { d.setUTCDate(d.getUTCDate() + 1); } while ([0, 6].includes(d.getUTCDay())); return d.toISOString().slice(0, 10); };
secciones.fallida = async () => {
  const S = 'Entrega fallida';
  await drenar(['analitica']);
  const antes = await sumaZtest();
  const casos = {}; const venc = (await ahora()) + 30 * H;
  for (const motivo of ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE']) {
    const e = await envio(`falla-${motivo}`); const evs = hastaReparto(e, venc); const f = e.ev('ENTREGA_FALLIDA', { intento: 1, motivo });
    casos[motivo] = { e, fallo: f }; await pub(T.ESTADOS, e.id, [...evs, f]);
  }
  for (const [motivo, c] of Object.entries(casos)) {
    c.doc = await esperar(async () => { const d = await envioDoc(c.e.id); return d?.reprogramaciones?.['1'] ? d : null; });
    check(S, `${motivo}: se registra la decisión de reprogramación`, c.doc);
  }
  const rg = (m) => casos[m].doc?.reprogramaciones?.['1']; const fa = casos.AUSENTE.fallo.ocurridoEn;
  const esperada = `${sigHabil(fa.slice(0, 10))}T${String(new Date(fa).getUTCHours() < 13 ? 13 : 8).padStart(2, '0')}:00:00.000Z`;
  check(S, 'AUSENTE → día hábil siguiente, franja opuesta', rg('AUSENTE')?.tipo === 'REPROGRAMADO' && rg('AUSENTE').fechaNuevoIntento === esperada, `${rg('AUSENTE')?.fechaNuevoIntento} (esperada ${esperada})`);
  check(S, 'DIRECCION_INCORRECTA → retenido, no se reprograma solo', rg('DIRECCION_INCORRECTA')?.tipo === 'RETENIDO' && !rg('DIRECCION_INCORRECTA').fechaNuevoIntento && casos.DIRECCION_INCORRECTA.doc.situacion === 'RETENIDO');
  check(S, 'RECHAZADO → marcado para devolución', rg('RECHAZADO')?.tipo === 'DEVOLUCION');
  check(S, 'ZONA_INACCESIBLE → mismo criterio que AUSENTE (supuesto documentado)', rg('ZONA_INACCESIBLE')?.tipo === 'REPROGRAMADO');
  await pub(T.ESTADOS, casos.AUSENTE.e.id, [casos.AUSENTE.fallo]); // el mismo fallo, duplicado
  await drenar(['proyector', 'notificaciones', 'reprogramacion', 'analitica']);
  const reps = (await leerTodo(T.REPROG)).map((m) => JSON.parse(m.value));
  for (const [motivo, c] of Object.entries(casos)) {
    const n = await db.collection('notificaciones').countDocuments({ origenEventId: c.fallo.eventId });
    const r = reps.filter((x) => x.origenEventId === c.fallo.eventId).length;
    check(S, `${motivo}: exactamente una notificación y una reprogramación (también con el fallo duplicado)`, n === 1 && r === 1, `notificaciones=${n}, reprogramaciones=${r}`);
  }
  const desp = await sumaZtest(); const dlt = (k) => (desp[k] ?? 0) - (antes[k] ?? 0);
  check(S, 'Analítica suma cada fallo una sola vez, por motivo', dlt('fallos.total') === 4 && ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'].every((m) => dlt(`fallos.motivos.${m}`) === 1), `fallos.total +${dlt('fallos.total')}`);

  const t3 = await envio('tercer'); const venc3 = (await ahora()) + 30 * H;
  const evs3 = [...hastaReparto(t3, venc3), t3.ev('ENTREGA_FALLIDA', { intento: 1, motivo: 'AUSENTE' }),
    t3.ev('CLASIFICADO'), t3.ev('EN_REPARTO'), t3.ev('ENTREGA_FALLIDA', { intento: 2, motivo: 'AUSENTE' }),
    t3.ev('CLASIFICADO'), t3.ev('EN_REPARTO'), t3.ev('ENTREGA_FALLIDA', { intento: 3, motivo: 'AUSENTE' })];
  await pub(T.ESTADOS, t3.id, evs3);
  const d3 = await esperar(async () => { const d = await envioDoc(t3.id); return d?.reprogramaciones?.['3'] ? d : null; });
  check(S, 'El tercer fallo marca el envío para devolución', d3?.reprogramaciones['3'].tipo === 'DEVOLUCION' && d3.situacion === 'DEVOLUCION' && d3.reprogramaciones['1'].tipo === 'REPROGRAMADO' && d3.reprogramaciones['2'].tipo === 'REPROGRAMADO');

  if (SIN_DOCKER) return omitido(S, 'Detener un servicio no afecta a los otros dos', '--no-docker');
  await drenar(['notificaciones']); dc('stop notificaciones');
  const aisl = await envio('aislamiento'); const evsA = [...hastaReparto(aisl, venc), aisl.ev('ENTREGA_FALLIDA', { intento: 1, motivo: 'AUSENTE' })];
  const base = await sumaZtest(); await pub(T.ESTADOS, aisl.id, evsA);
  const dA = await esperar(async () => { const d = await envioDoc(aisl.id); return d?.estado === 'ENTREGA_FALLIDA' && d.reprogramaciones?.['1'] ? d : null; });
  await drenar(['analitica']);
  const post = await sumaZtest();
  check(S, 'Con notificaciones detenido: el proyector y la reprogramación siguen funcionando', dA);
  check(S, 'Con notificaciones detenido: analítica sigue sumando', (post['fallos.total'] ?? 0) - (base['fallos.total'] ?? 0) === 1);
  check(S, 'Con notificaciones detenido: no hay aviso y el lag del grupo crece', (await db.collection('notificaciones').countDocuments({ shipmentId: aisl.id })) === 0 && (await lagTotal(admin, 'notificaciones')) > 0);
  dc('start notificaciones');
  const ok1 = await esperar(async () => (await db.collection('notificaciones').countDocuments({ shipmentId: aisl.id })) === 1, { timeout: 60000 });
  await dormir(3000);
  check(S, 'Al reiniciar procesa lo pendiente sin duplicar avisos', ok1 && (await db.collection('notificaciones').countDocuments({ shipmentId: aisl.id })) === 1);
};

// ───────────────────────────── RIESGO DE SLA ─────────────────────────────
secciones.sla = async () => {
  const S = 'Riesgo de SLA';
  const ahoraMs = await ahora();
  const montar = async (nombre, venceEnMs, tramo) => {
    const e = await envio(nombre); const evs = tramo(e, venceEnMs); await pub(T.PROMESAS, e.id, [e.promesa(venceEnMs)]); await pub(T.ESTADOS, e.id, evs); return { e, evs, venceEnMs };
  };
  const casos = {
    SIN_INGRESO: await montar('sla-ingreso', ahoraMs + 9 * H, (e, v) => [e.ev('CREADO', { promesaVenceEn: iso(v) })]),
    SIN_SALIDA: await montar('sla-salida', ahoraMs + 3 * H, (e, v) => [e.ev('CREADO', { promesaVenceEn: iso(v) }), e.ev('INGRESADO_HUB')]),
    VENCIDO: await montar('sla-vencido', ahoraMs - H, (e, v) => hastaReparto(e, v)),
  };
  const fechaFallo = iso(ahoraMs - 600000).slice(0, 10); // el fallo ocurre ese mismo día (UTC)
  const vFallo = Date.parse(`${fechaFallo}T23:59:59.000Z`);
  const fl2 = await envio('sla-fallo'); const evsF2 = [...hastaReparto(fl2, vFallo), fl2.ev('ENTREGA_FALLIDA', { intento: 1, motivo: 'AUSENTE' })];
  await pub(T.PROMESAS, fl2.id, [fl2.promesa(vFallo)]); await pub(T.ESTADOS, fl2.id, evsF2);
  casos.FALLO_DIA_LIMITE = { e: fl2, evs: evsF2, venceEnMs: vFallo };
  const esperado = { SIN_INGRESO: 'MEDIA', SIN_SALIDA: 'ALTA', FALLO_DIA_LIMITE: 'ALTA', VENCIDO: 'CRITICA' };
  for (const [regla, c] of Object.entries(casos)) {
    const a = await esperar(() => db.collection('alertas').findOne({ _id: `${c.e.id}:${regla}` }), { timeout: 45000 });
    check(S, `${regla} genera su alerta con prioridad ${esperado[regla]}`, a && a.prioridad === esperado[regla] && a.estado === 'ABIERTA', a ? a.prioridad : 'sin alerta');
  }
  const cierre = casos.VENCIDO; const ent = cierre.e.ev('ENTREGADO', { intento: 1, promesaVenceEn: iso(cierre.venceEnMs) });
  await pub(T.ESTADOS, cierre.e.id, [ent]);
  check(S, 'La alerta se cierra cuando el envío se entrega', await esperar(async () => { const as = await db.collection('alertas').find({ shipmentId: cierre.e.id }).toArray(); return as.length && as.every((a) => a.estado === 'CERRADA'); }));
  const conteos = async () => { const m = {}; for (const x of await leerTodo(T.ALERTAS)) { const v = JSON.parse(x.value); if (v.tipo === 'ALERTA_ABIERTA' && v.shipmentId.startsWith(`VERIFY-${RUN}`)) m[`${v.shipmentId}|${v.regla}`] = (m[`${v.shipmentId}|${v.regla}`] ?? 0) + 1; } return m; };
  await drenar(['monitor-sla', 'proyector']);
  let m = await conteos();
  check(S, 'Una misma regla no alerta dos veces sobre el mismo envío', Object.keys(m).length >= 4 && Object.values(m).every((n) => n === 1), JSON.stringify(m).slice(0, 160));

  if (SIN_DOCKER) return omitido(S, 'Tras reiniciar el monitor no se pierden ni se repiten alertas', '--no-docker');
  dc('restart monitor-sla'); await dormir(8000);
  const post = await montar('sla-post-reinicio', (await ahora()) - H, (e, v) => hastaReparto(e, v));
  check(S, 'Tras reiniciar el monitor sigue alertando', await esperar(() => db.collection('alertas').findOne({ _id: `${post.e.id}:VENCIDO` }), { timeout: 60000 }));
  dc('stop monitor-sla');
  const mientras = await montar('sla-monitor-caido', (await ahora()) - H, (e, v) => hastaReparto(e, v));
  await dormir(4000);
  const sinAlerta = !(await db.collection('alertas').findOne({ _id: `${mientras.e.id}:VENCIDO` }));
  dc('start monitor-sla');
  const recuperada = await esperar(() => db.collection('alertas').findOne({ _id: `${mientras.e.id}:VENCIDO` }), { timeout: 90000 });
  check(S, 'Tras reiniciar el monitor no se pierden alertas (la del período caído aparece)', sinAlerta && recuperada);
  await drenar(['monitor-sla', 'proyector']); m = await conteos();
  check(S, 'Tras reiniciar el monitor no se repiten alertas', Object.values(m).every((n) => n === 1), `${Object.keys(m).length} alertas distintas`);
};

// ───────────────────────────── ANALÍTICA Y REPROCESAMIENTO ─────────────────────────────
const TODOS = ['proyector', 'notificaciones', 'reprogramacion', 'monitor-sla', 'analitica'];
async function eventosUnicos() {
  const vistos = new Set(), out = [];
  for (const m of await leerTodo(T.ESTADOS)) { let ev; try { ev = JSON.parse(m.value); } catch { continue; } if (validar(T.ESTADOS, ev) || vistos.has(ev.eventId)) continue; vistos.add(ev.eventId); out.push(ev); }
  return out;
}
secciones.analitica = async () => {
  const S = 'Analítica y reprocesamiento';
  await simApi('/flow/pause', 'POST'); await dormir(3000); await drenar(TODOS);
  const evs = await eventosUnicos();
  // Recuento "manual": bucles simples sobre el log, sin reutilizar el código de los servicios
  const esp = {}; const suma = (e, p) => { const k = `${e.ocurridoEn.slice(0, 10)}|${e.zonaId}|${new Date(e.ocurridoEn).getUTCHours() < 13 ? 'MANANA' : 'TARDE'}`; esp[k] ??= {}; esp[k][p] = (esp[k][p] ?? 0) + 1; };
  for (const e of evs) {
    if (e.tipo === 'ENTREGADO') { suma(e, 'entregas'); suma(e, 'sla.total'); suma(e, Date.parse(e.ocurridoEn) <= Date.parse(e.promesaVenceEn) ? 'sla.enTiempo' : 'sla.tarde'); if (e.intento === 1) { suma(e, 'primerIntento.total'); suma(e, 'primerIntento.entregados'); } }
    if (e.tipo === 'ENTREGA_FALLIDA') { suma(e, 'fallos.total'); suma(e, `fallos.motivos.${e.motivo}`); if (e.intento === 1) suma(e, 'primerIntento.total'); }
  }
  const real = {}; for (const d of await db.collection('metricas_diarias').find().toArray()) real[`${d.fecha}|${d.zonaId}|${d.franja}`] = aplanar(d);
  const dif = []; for (const k of new Set([...Object.keys(esp), ...Object.keys(real)])) if (estable(esp[k] ?? {}) !== estable(real[k] ?? {})) dif.push(k);
  check(S, 'Los tres indicadores coinciden con el recuento manual sobre el log', dif.length === 0 && evs.length > 0, `${evs.length} eventos únicos, ${Object.keys(esp).length} celdas, ${dif.length} diferencias${dif[0] ? ` (ej. ${dif[0]})` : ''}`);

  if (SIN_DOCKER) return omitido(S, 'El cuarto indicador se completa con el histórico sin reiniciar los demás servicios', '--no-docker');
  try { dc('stop tiempos-hub'); } catch { /* no estaba levantado */ }
  for (const c of ['metricas_tiempos_hub', 'tiempos_envio']) await db.collection(c).deleteMany({});
  await db.collection('eventos_procesados').deleteMany({ consumidor: 'analitica-tiempos' });
  await admin.deleteGroups(['analitica-tiempos']).catch(() => {});
  const iniciado = (s) => execSync(`docker inspect -f '{{.State.StartedAt}}' $(docker compose ps -q ${s})`, { encoding: 'utf8' }).trim();
  const antes = Object.fromEntries(TODOS.map((s) => [s, iniciado(s)]));
  dc('--profile replay up -d tiempos-hub');
  await esperar(async () => (await db.collection('metricas_tiempos_hub').countDocuments()) > 0 && (await lagTotal(admin, 'analitica-tiempos')) === 0, { timeout: 180000 });
  const ing = {}, medido = new Set(), expT = {};
  for (const e of evs) {
    if (e.tipo === 'INGRESADO_HUB' && !(e.shipmentId in ing)) ing[e.shipmentId] = Date.parse(e.ocurridoEn);
    if (e.tipo === 'EN_REPARTO' && e.shipmentId in ing && !medido.has(e.shipmentId)) {
      medido.add(e.shipmentId); const k = `${e.ocurridoEn.slice(0, 10)}|${e.hubId}`; const ms = Date.parse(e.ocurridoEn) - ing[e.shipmentId];
      expT[k] ??= { sumaMs: 0, cantidad: 0 }; expT[k].sumaMs += ms; expT[k].cantidad += 1;
    }
  }
  const realT = {}; for (const d of await db.collection('metricas_tiempos_hub').find().toArray()) realT[`${d.fecha}|${d.hubId}`] = { sumaMs: d.sumaMs, cantidad: d.cantidad };
  check(S, 'El cuarto indicador se completa con datos históricos (grupo nuevo, lectura desde el inicio)', estable(expT) === estable(realT) && Object.keys(realT).length > 0, `${Object.keys(realT).length} celdas hub/día`);
  check(S, 'Los demás servicios no se reiniciaron', TODOS.every((s) => iniciado(s) === antes[s]));
};

// ───────────────────────────── RECONSTRUCCIÓN ─────────────────────────────
secciones.reconstruccion = async () => {
  const S = 'Reconstrucción';
  if (SIN_DOCKER) return omitido(S, 'Borrar envios y reprocesar deja la colección igual', '--no-docker');
  await simApi('/flow/pause', 'POST'); await dormir(3000); await drenar(TODOS);
  const foto = async () => { const o = {}; for (const c of ['envios', 'alertas', 'notificaciones']) o[c] = (await db.collection(c).find().sort({ _id: 1 }).toArray()).map(({ _meta, ...r }) => r); return o; };
  const a = await foto();
  dc('stop proyector'); execSync('node scripts/rebuild.js', { stdio: 'inherit' }); dc('start proyector');
  await esperar(async () => (await db.collection('envios').countDocuments()) >= a.envios.length && (await lagTotal(admin, 'proyector')) === 0, { timeout: 240000, cada: 1500 });
  await dormir(2000);
  const b = await foto();
  for (const c of ['envios', 'alertas', 'notificaciones']) check(S, `Borrar ${c} y reprocesar la deja en el mismo estado`, estable(a[c]) === estable(b[c]) && a[c].length > 0, `${a[c].length} → ${b[c].length} documentos`);
};

// ───────────────────────────── ENTORNO Y NO FUNCIONALES ─────────────────────────────
secciones.entorno = async () => {
  const S = 'Entorno';
  await simApi('/flow/resume', 'POST');
  const e = await envio('latencia'); const id = e.id;
  try {
    const ctrl = new AbortController(); const res = await fetch(`${WEB}/api/stream?id=${id}`, { signal: ctrl.signal });
    const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = '';
    const esperarSeq = async (seq) => { for (;;) { const { value, done } = await reader.read(); if (done) throw new Error('SSE cerrado'); buf += dec.decode(value);
      if (buf.split('\n\n').some((b) => b.startsWith('data:') && JSON.parse(b.slice(5)).doc?.ultimaSecuencia === seq)) return; } };
    await reader.read(); await dormir(1500);
    const lat = []; const evs = hastaReparto(e, (await ahora()) + 30 * H);
    for (const ev of evs) { const t = Date.now(); await pub(T.ESTADOS, id, [ev]); await Promise.race([esperarSeq(ev.secuencia), dormir(8000).then(() => { throw new Error('timeout SSE'); })]); lat.push(Date.now() - t); }
    ctrl.abort();
    check(S, 'Latencia evento → pantalla (SSE) < 2 s con el flujo continuo', Math.max(...lat) < 2000, `${lat.join(' / ')} ms`);
  } catch (err) { check(S, 'Latencia evento → pantalla (SSE) < 2 s', false, err.message); }
  if (SIN_DOCKER) return omitido(S, 'Servicios levantados, logs y lag', '--no-docker');
  const vivos = dc('ps --services --filter status=running').split('\n').filter(Boolean);
  const faltan = ['kafka', 'mongo', 'simulador', 'proyector', 'notificaciones', 'reprogramacion', 'monitor-sla', 'analitica', 'web'].filter((s) => !vivos.includes(s));
  check(S, 'Todos los servicios están levantados', faltan.length === 0, faltan.join(', '));
  const logs = dc('logs --no-color --tail=300 proyector');
  check(S, 'Cada servicio registra eventId, partición y offset', /"eventId":"[^"]+"/.test(logs) && /"partition":\d/.test(logs) && /"offset":"?\d/.test(logs));
  let lagOk = false; try { lagOk = /lag total por grupo/.test(execSync('node scripts/lag.js', { encoding: 'utf8' })); } catch { /* sin lag */ }
  check(S, 'Se puede consultar el lag de cada grupo (npm run lag)', lagOk);
  omitido(S, 'Todo el sistema se levanta con un solo comando en una máquina limpia', 'prueba manual: docker compose down -v && docker compose up --build');
};

const orden = ['ciclo', 'fallida', 'sla', 'analitica', 'reconstruccion', 'entorno'];
try {
  for (const s of orden) { if (SOLO && !SOLO.includes(s)) continue; console.log(`\n── ${s} ──`); try { await secciones[s](); } catch (e) { check(s, 'La sección terminó sin errores', false, e.message); } }
} finally {
  await simApi('/flow/resume', 'POST').catch(() => {});
  const ok = resultados.filter((r) => r.ok === true).length, mal = resultados.filter((r) => r.ok === false).length, om = resultados.filter((r) => r.ok === null).length;
  console.log(`\nResumen: ${ok} correctas, ${mal} con fallas, ${om} omitidas`);
  await admin.disconnect(); await productor.disconnect(); await client.close();
  process.exit(mal ? 1 : 0);
}
