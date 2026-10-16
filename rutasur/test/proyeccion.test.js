import test from 'node:test';
import assert from 'node:assert/strict';
import { aplicarEstado, aplicarReprogramacion, aplicarAlerta } from '../shared/domain/proyeccion.js';
import { reaccionReprogramacion } from '../shared/domain/reglas.js';

const iso = (m) => new Date(Date.parse('2026-03-03T08:00:00.000Z') + m * 60e3).toISOString();
const ev = (seq, tipo, min, extra = {}) => ({ eventId: `S-e${seq}`, shipmentId: 'S', tipo, secuencia: seq, ocurridoEn: iso(min), hubId: 'H1', zonaId: 'Z01',
  repartidorId: seq > 2 ? 'R001' : null, ...(seq === 1 && { promesaVenceEn: iso(1800) }), ...extra });
const ciclo = [ev(1, 'CREADO', 0), ev(2, 'INGRESADO_HUB', 10), ev(3, 'CLASIFICADO', 20), ev(4, 'EN_REPARTO', 30),
  ev(5, 'ENTREGA_FALLIDA', 60, { intento: 1, motivo: 'AUSENTE' })];

const correr = (evs) => evs.reduce((d, e) => { const r = aplicarEstado(d, e); return r.doc; }, null);

test('refleja último estado e historial completo', () => {
  const d = correr(ciclo);
  assert.equal(d.estado, 'ENTREGA_FALLIDA'); assert.equal(d.historial.length, 5); assert.equal(d.ultimaSecuencia, 5);
});
test('duplicado no agrega entrada repetida', () => {
  const d = correr([...ciclo, ciclo[3], ciclo[4]]);
  assert.equal(d.historial.length, 5);
  assert.equal(new Set(d.historial.map((h) => h.eventId)).size, 5);
});
test('evento fuera de orden no retrocede el estado', () => {
  const d = correr([ciclo[0], ciclo[1], ciclo[3], ciclo[2]]); // 4 llega antes que 3
  assert.equal(d.estado, 'EN_REPARTO'); assert.equal(d.ultimaSecuencia, 4); assert.equal(d.historial.length, 3);
});
test('situación derivada: reprogramado, retenido, devolución', () => {
  let d = correr(ciclo);
  d = aplicarReprogramacion(d, reaccionReprogramacion(ciclo[4])).doc;
  assert.equal(d.situacion, 'REPROGRAMADO'); assert.equal(d.proximoIntento.franja, 'TARDE');
  const dir = correr([...ciclo.slice(0, 4), ev(5, 'ENTREGA_FALLIDA', 60, { intento: 1, motivo: 'DIRECCION_INCORRECTA' })]);
  const r = aplicarReprogramacion(dir, reaccionReprogramacion({ ...ciclo[4], motivo: 'DIRECCION_INCORRECTA' })).doc;
  assert.equal(r.situacion, 'RETENIDO');
});
test('RECONSTRUCCIÓN: cualquier intercalado entre topics produce el mismo documento', () => {
  const evs = [...ciclo,
    ev(6, 'CLASIFICADO', 1500), ev(7, 'EN_REPARTO', 1530), ev(8, 'ENTREGADO', 1600, { intento: 2, promesaVenceEn: iso(1800) })];
  const reps = [ciclo[4]].map((f) => reaccionReprogramacion(f));
  let s = 7;
  const rnd = () => (s = (s * 16807) % 2147483647) / 2147483647;
  const base = JSON.stringify(finalDe(evs, reps, () => 0));
  for (let i = 0; i < 200; i++) {
    assert.equal(JSON.stringify(finalDe(evs, reps, rnd)), base);
  }
  function finalDe(estados, reprogs, azar) { // mezcla preservando el orden dentro de cada topic
    const a = [...estados], b = [...reprogs]; let d = null;
    while (a.length || b.length) {
      const tomaA = a.length && (!b.length || azar() < 0.5);
      d = tomaA ? aplicarEstado(d, a.shift()).doc : aplicarReprogramacion(d, b.shift()).doc;
    }
    return d;
  }
});
test('alertas: una cerrada no se reabre', () => {
  const a = { shipmentId: 'S', regla: 'VENCIDO', prioridad: 'CRITICA', causa: 'x', ocurridoEn: iso(1), hubId: 'H1', zonaId: 'Z01' };
  let d = aplicarAlerta(null, { ...a, tipo: 'ALERTA_ABIERTA' });
  d = aplicarAlerta(d, { ...a, tipo: 'ALERTA_CERRADA', ocurridoEn: iso(5) });
  d = aplicarAlerta(d, { ...a, tipo: 'ALERTA_ABIERTA' });
  assert.equal(d.estado, 'CERRADA');
});
