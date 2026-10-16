import test from 'node:test';
import assert from 'node:assert/strict';
import { planificarEnvio, PARAMS_DEFECTO } from '../shared/domain/simulacion.js';
import { validar } from '../shared/domain/schema.js';
import { T } from '../shared/domain/constantes.js';
import { aplicarEstado, aplicarReprogramacion, aplicarAlerta, aplicarNotificacion } from '../shared/domain/proyeccion.js';
import { reaccionReprogramacion, reaccionNotificacion } from '../shared/domain/reglas.js';
import { incrementosDe, aplanar } from '../shared/domain/metricas.js';
import { MonitorSla } from '../shared/domain/monitor.js';

const P = { ...PARAMS_DEFECTO };
const t0 = P.inicioSimMs;

function plan(n, params = P) {
  const msgs = [];
  for (let i = 1; i <= n; i++) msgs.push(...planificarEnvio({ shipmentId: `RS-${String(i).padStart(6, '0')}`, creadoEnMs: t0 + i * 3000, params }));
  return msgs.map((m, i) => ({ m, i })).sort((a, b) => a.m.t - b.m.t || a.i - b.i).map((x) => x.m);
}

test('simulador: misma semilla => misma corrida; otra semilla => distinta', () => {
  const a = JSON.stringify(plan(300)), b = JSON.stringify(plan(300));
  assert.equal(a, b);
  assert.notEqual(a, JSON.stringify(plan(300, { ...P, semilla: 99 })));
});

test('simulador: eventos válidos, secuencia creciente, tasas cercanas a las configuradas', () => {
  const msgs = plan(4000);
  let malformados = 0, rechazados = 0, dup = 0, fuera = 0;
  const ult = {};
  const vistos = new Set();
  for (const m of msgs) {
    if (m.malformado) { malformados++; let ev = null; try { ev = JSON.parse(m.raw); } catch {} if (validar(T.ESTADOS, ev)) rechazados++; continue; }
    assert.equal(validar(m.topic, m.evento), null, JSON.stringify(m.evento));
    if (m.topic !== T.ESTADOS) continue;
    if (vistos.has(m.evento.eventId)) { dup++; continue; }
    vistos.add(m.evento.eventId);
    const prev = ult[m.evento.shipmentId] ?? 0;
    if (m.evento.secuencia < prev) fuera++;
    ult[m.evento.shipmentId] = Math.max(prev, m.evento.secuencia);
  }
  assert.equal(rechazados, malformados, 'todo malformado debe ser rechazado por el esquema');
  assert.ok(Math.abs(malformados / 4000 - 0.01) < 0.01, `malformados ${malformados}`);
  assert.ok(fuera > 30 && fuera < 150, `fuera de orden ${fuera}`); // ~2% de 4000
  assert.ok(dup > 100, `duplicados ${dup}`);
  const primer = msgs.filter((m) => m.evento?.tipo === 'ENTREGA_FALLIDA' && m.evento.intento === 1 && !m.duplicado).length;
  const total = msgs.filter((m) => m.evento && (m.evento.tipo === 'ENTREGADO' || m.evento.tipo === 'ENTREGA_FALLIDA') && m.evento.intento === 1 && !m.duplicado).length;
  assert.ok(primer / total > 0.12 && primer / total < 0.26, `fallo primer intento ${(primer / total).toFixed(3)}`);
});

test('escenarios a pedido', () => {
  const ev = (f) => planificarEnvio({ shipmentId: 'DEMO-X', creadoEnMs: t0, params: P, forzado: f }).filter((m) => m.topic === T.ESTADOS).map((m) => m.evento);
  assert.deepEqual(ev({ modo: 'feliz' }).map((e) => e.tipo), ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO']);
  const aus = ev({ modo: 'ausente', resultados: ['AUSENTE'] });
  assert.equal(aus.filter((e) => e.tipo === 'ENTREGA_FALLIDA').length, 1); assert.equal(aus.at(-1).tipo, 'ENTREGADO'); assert.equal(aus.at(-1).intento, 2);
  const tf = ev({ modo: 'tercerFallo', resultados: ['AUSENTE', 'AUSENTE', 'AUSENTE'] });
  assert.equal(tf.filter((e) => e.tipo === 'ENTREGA_FALLIDA').length, 3); assert.equal(tf.at(-1).tipo, 'ENTREGA_FALLIDA');
  const dem = ev({ modo: 'demorado' }); const venc = Date.parse(dem[0].promesaVenceEn);
  assert.ok(Date.parse(dem.find((e) => e.tipo === 'CLASIFICADO').ocurridoEn) > venc, 'detenido en el hub pasado el vencimiento');
});

/** Bus en memoria: cada "topic" es una cola FIFO por clave (equivale a una partición por envío). */
async function correrPipeline(msgs) {
  const log = { [T.ESTADOS]: [], [T.PROMESAS]: [], [T.REPROG]: [], [T.ALERTAS]: [], [T.NOTIF]: [], [T.DLQ]: [] };
  const envios = new Map(), alertas = new Map(), notifs = new Map(), metricas = new Map(), vistos = { proyector: new Set(), analitica: new Set(), reprog: new Set(), notif: new Set(), monitor: new Set() };
  const monitor = new MonitorSla({ emitir: async (e) => { for (const x of e) pend.push({ topic: T.ALERTAS, ev: x }); } });
  const pend = [];
  const alEvento = async (topic, ev) => {
    const err = validar(topic, ev);
    if (err) { log[T.DLQ].push({ topic, motivo: err }); return; }
    log[topic].push(ev);
    // proyector
    if ([T.ESTADOS, T.REPROG, T.ALERTAS, T.NOTIF].includes(topic) && !vistos.proyector.has(ev.eventId)) {
      vistos.proyector.add(ev.eventId);
      if (topic === T.ESTADOS || topic === T.REPROG) {
        const r = topic === T.ESTADOS ? aplicarEstado(envios.get(ev.shipmentId) ?? null, ev) : aplicarReprogramacion(envios.get(ev.shipmentId) ?? null, ev);
        if (r.aplicado) envios.set(ev.shipmentId, r.doc);
      } else if (topic === T.ALERTAS) { const id = `${ev.shipmentId}:${ev.regla}`; alertas.set(id, aplicarAlerta(alertas.get(id) ?? null, ev)); }
      else notifs.set(ev.eventId, aplicarNotificacion(ev));
    }
    if (topic === T.ESTADOS) {
      const rp = vistos.reprog.has(ev.eventId) ? null : reaccionReprogramacion(ev); vistos.reprog.add(ev.eventId);
      if (rp) pend.push({ topic: T.REPROG, ev: rp });
      if (!vistos.analitica.has(ev.eventId)) {
        vistos.analitica.add(ev.eventId);
        const inc = incrementosDe(ev);
        if (inc) { const k = JSON.stringify(inc.filtro); const acc = metricas.get(k) ?? {}; for (const [p, n] of Object.entries(inc.inc)) acc[p] = (acc[p] ?? 0) + n; metricas.set(k, acc); }
      }
      if (!vistos.monitor.has(ev.eventId)) { vistos.monitor.add(ev.eventId); await monitor.alEstado(ev); await monitor.barrer(); }
    }
    if (topic === T.PROMESAS) { await monitor.alPromesa(ev); await monitor.barrer(); }
    const nt = (topic === T.ESTADOS || topic === T.ALERTAS) && !vistos.notif.has(ev.eventId) ? reaccionNotificacion(topic, ev) : null;
    if (topic === T.ESTADOS || topic === T.ALERTAS) vistos.notif.add(ev.eventId);
    if (nt) pend.push({ topic: T.NOTIF, ev: nt });
  };
  for (const m of msgs) {
    let ev = m.evento;
    if (m.raw !== undefined) { try { ev = JSON.parse(m.raw); } catch { log[T.DLQ].push({ topic: m.topic, motivo: 'JSON inválido' }); continue; } }
    await alEvento(m.topic, ev);
    while (pend.length) { const p = pend.shift(); await alEvento(p.topic, p.ev); }
  }
  return { log, envios, alertas, notifs, metricas };
}

test('pipeline completo: criterios de aceptación de CU1–CU4 sobre 1500 envíos', async () => {
  const msgs = plan(1500);
  const r = await correrPipeline(msgs);
  // CU1: todos presentes, sin retroceso ni historial repetido, DLQ recibe malformados
  assert.equal(r.envios.size, 1500);
  for (const d of r.envios.values()) {
    const ids = d.historial.map((h) => h.eventId); assert.equal(new Set(ids).size, ids.length);
    const seqs = d.historial.filter((h) => h.fuente === 'estado').map((h) => h.secuencia);
    assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b));
    assert.ok(d.historial.length < 15);
  }
  const malf = msgs.filter((m) => m.malformado).length;
  assert.equal(r.log[T.DLQ].length, malf); assert.ok(malf > 0);
  // CU2: exactamente una notificación y una reprogramación por cada ENTREGA_FALLIDA única
  const fallos = new Map(); for (const e of r.log[T.ESTADOS]) if (e.tipo === 'ENTREGA_FALLIDA') fallos.set(e.eventId, e);
  assert.ok(fallos.size > 100);
  for (const f of fallos.values()) {
    assert.equal([...r.notifs.values()].filter((n) => n.origenEventId === f.eventId).length, 1);
    assert.equal(r.log[T.REPROG].filter((x) => x.origenEventId === f.eventId).length, 1);
  }
  const tipos = { REPROGRAMADO: 0, RETENIDO: 0, DEVOLUCION: 0 }; for (const x of r.log[T.REPROG]) tipos[x.tipo]++;
  assert.ok(tipos.REPROGRAMADO > 0 && tipos.RETENIDO > 0 && tipos.DEVOLUCION > 0, JSON.stringify(tipos));
  // CU3: aparecen las cuatro reglas, nunca dos veces la misma sobre un envío, y se cierran al entregar
  const abiertas = r.log[T.ALERTAS].filter((a) => a.tipo === 'ALERTA_ABIERTA');
  assert.equal(new Set(abiertas.map((a) => a.regla)).size, 4, 'las cuatro reglas generan alertas');
  assert.equal(new Set(abiertas.map((a) => a.eventId)).size, abiertas.length);
  for (const a of r.alertas.values()) { const d = r.envios.get(a.shipmentId); if (d?.estado === 'ENTREGADO') assert.equal(a.estado, 'CERRADA'); }
  // CU4: recuento manual = agregados
  let ent1 = 0, tot1 = 0, tarde = 0, enT = 0; const vistos = new Set();
  for (const e of r.log[T.ESTADOS]) { if (vistos.has(e.eventId)) continue; vistos.add(e.eventId);
    if ((e.tipo === 'ENTREGADO' || e.tipo === 'ENTREGA_FALLIDA') && e.intento === 1) { tot1++; if (e.tipo === 'ENTREGADO') ent1++; }
    if (e.tipo === 'ENTREGADO') (Date.parse(e.ocurridoEn) <= Date.parse(e.promesaVenceEn) ? enT++ : tarde++); }
  const sum = (p) => [...r.metricas.values()].reduce((s, m) => s + (m[p] ?? 0), 0);
  assert.equal(sum('primerIntento.total'), tot1); assert.equal(sum('primerIntento.entregados'), ent1);
  assert.equal(sum('sla.enTiempo'), enT); assert.equal(sum('sla.tarde'), tarde);
  assert.equal(aplanar({ a: { b: 2, c: 0 }, hubId: 'x' })['a.b'], 2);
});
