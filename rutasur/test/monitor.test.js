import test from 'node:test';
import assert from 'node:assert/strict';
import { MonitorSla } from '../shared/domain/monitor.js';

const H = 3600e3;
const t0 = Date.parse('2026-03-03T08:00:00.000Z');
const iso = (ms) => new Date(ms).toISOString();
const mk = (extra = {}) => { const salida = []; const m = new MonitorSla({ emitir: async (e) => { salida.push(...e); }, ...extra }); return { m, salida }; };
const est = (id, seq, tipo, ms, extra = {}) => ({ eventId: `${id}-e${seq}`, shipmentId: id, tipo, secuencia: seq, ocurridoEn: iso(ms), hubId: 'H1', zonaId: 'Z01', ...extra });

test('SIN_SALIDA se emite una sola vez y se cierra al entregar', async () => {
  const { m, salida } = mk();
  await m.alEstado(est('A', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 3 * H) }));
  const reglas = salida.map((e) => e.regla).sort();
  assert.deepEqual(reglas, ['SIN_INGRESO', 'SIN_SALIDA']);
  await m.alEstado(est('X', 1, 'CREADO', t0 + 5 * 60e3, { promesaVenceEn: iso(t0 + 30 * H) })); // avanza el reloj, otro envío
  await m.barrer(); await m.barrer();
  assert.equal(salida.filter((e) => e.shipmentId === 'A').length, 2, 'sin repetición');
  await m.alEstado(est('A', 2, 'ENTREGADO', t0 + H, { intento: 1 }));
  const cierres = salida.filter((e) => e.tipo === 'ALERTA_CERRADA' && e.shipmentId === 'A');
  assert.equal(cierres.length, 2);
});
test('VENCIDO aparece por paso del tiempo (reloj simulado), sin evento del propio envío', async () => {
  const { m, salida } = mk();
  await m.alEstado(est('B', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 12 * H) }));
  await m.alEstado(est('B', 2, 'INGRESADO_HUB', t0 + H));
  await m.alEstado(est('B', 3, 'CLASIFICADO', t0 + 2 * H));
  await m.alEstado(est('B', 4, 'EN_REPARTO', t0 + 3 * H));
  assert.equal(salida.length, 0);
  await m.alEstado(est('OTRO', 1, 'CREADO', t0 + 13 * H, { promesaVenceEn: iso(t0 + 40 * H) })); // el reloj pasa el vencimiento
  await m.barrer();
  assert.ok(salida.some((e) => e.shipmentId === 'B' && e.regla === 'VENCIDO' && e.prioridad === 'CRITICA'));
});
test('evento fuera de orden no retrocede el estado y duplicados no repiten alertas', async () => {
  const { m, salida } = mk();
  await m.alEstado(est('C', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 20 * H) }));
  await m.alEstado(est('C', 4, 'EN_REPARTO', t0 + 3 * H));
  await m.alEstado(est('C', 2, 'INGRESADO_HUB', t0 + H));
  assert.equal(m.envios.get('C').estado, 'EN_REPARTO');
  await m.alEstado(est('C', 4, 'EN_REPARTO', t0 + 3 * H));
  assert.equal(salida.length, 0);
});
test('reinicio: el estado serializado no repite ni pierde alertas', async () => {
  const a = mk();
  await a.m.alEstado(est('D', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 3 * H) }));
  const n = a.salida.length; assert.ok(n >= 1);
  const snap = JSON.parse(JSON.stringify({ envios: Object.fromEntries(a.m.envios), reloj: a.m.reloj, ultimoBarrido: a.m.ultimoBarrido }));
  const b = mk(snap); // "reinicio"
  await b.m.alEstado(est('D', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 3 * H) })); // reentrega del mismo mensaje
  assert.equal(b.salida.length, 0, 'no repite');
  await b.m.alEstado(est('E', 1, 'CREADO', t0 + 6 * H, { promesaVenceEn: iso(t0 + 30 * H) }));
  await b.m.barrer();
  assert.ok(b.salida.some((e) => e.shipmentId === 'D' && e.regla === 'VENCIDO'), 'no pierde: dispara lo que corresponde tras el reinicio');
});
test('si la publicación falla, la alerta no se marca como emitida y se reintenta', async () => {
  let falla = true; const salida = [];
  const m = new MonitorSla({ emitir: async (e) => { if (falla) throw new Error('kafka caído'); salida.push(...e); } });
  const ev = est('F', 1, 'CREADO', t0, { promesaVenceEn: iso(t0 + 3 * H) });
  await assert.rejects(m.alEstado(ev));
  falla = false; await m.alEstado(ev);
  assert.ok(salida.length >= 1);
});
