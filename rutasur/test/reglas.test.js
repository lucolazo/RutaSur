import test from 'node:test';
import assert from 'node:assert/strict';
import { decidirReprogramacion, reglasActivas, reaccionNotificacion, reaccionReprogramacion } from '../shared/domain/reglas.js';
import { validar } from '../shared/domain/schema.js';
import { T } from '../shared/domain/constantes.js';

const H = 3600e3;

test('AUSENTE: día hábil siguiente en la franja opuesta', () => {
  // martes 2026-03-03 10:00 (mañana) -> miércoles 13:00 (tarde)
  const d = decidirReprogramacion({ motivo: 'AUSENTE', intentoFallido: 1, ocurridoEn: '2026-03-03T10:00:00.000Z' });
  assert.equal(d.tipo, 'REPROGRAMADO'); assert.equal(d.franja, 'TARDE'); assert.equal(d.fechaNuevoIntento, '2026-03-04T13:00:00.000Z');
  // tarde -> mañana
  const e = decidirReprogramacion({ motivo: 'AUSENTE', intentoFallido: 1, ocurridoEn: '2026-03-03T15:00:00.000Z' });
  assert.equal(e.franja, 'MANANA'); assert.equal(e.fechaNuevoIntento, '2026-03-04T08:00:00.000Z');
});
test('viernes salta al lunes', () => {
  const d = decidirReprogramacion({ motivo: 'AUSENTE', intentoFallido: 1, ocurridoEn: '2026-03-06T10:00:00.000Z' });
  assert.equal(d.fechaNuevoIntento, '2026-03-09T13:00:00.000Z');
});
test('DIRECCION_INCORRECTA retiene, RECHAZADO devuelve, tercer fallo devuelve', () => {
  const o = '2026-03-03T10:00:00.000Z';
  assert.equal(decidirReprogramacion({ motivo: 'DIRECCION_INCORRECTA', intentoFallido: 1, ocurridoEn: o }).tipo, 'RETENIDO');
  assert.equal(decidirReprogramacion({ motivo: 'RECHAZADO', intentoFallido: 1, ocurridoEn: o }).tipo, 'DEVOLUCION');
  for (const motivo of ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'])
    assert.equal(decidirReprogramacion({ motivo, intentoFallido: 3, ocurridoEn: o }).tipo, 'DEVOLUCION');
});
test('reprogramación y notificación son deterministas (mismo eventId ante reintento)', () => {
  const ev = { eventId: 'S1-e5', shipmentId: 'S1', tipo: 'ENTREGA_FALLIDA', motivo: 'AUSENTE', intento: 1, ocurridoEn: '2026-03-03T10:00:00.000Z', hubId: 'H1', zonaId: 'Z01' };
  assert.deepEqual(reaccionReprogramacion(ev), reaccionReprogramacion(ev));
  assert.equal(reaccionNotificacion(T.ESTADOS, ev).eventId, 'notif-S1-e5');
  assert.equal(reaccionNotificacion(T.ESTADOS, { ...ev, tipo: 'EN_REPARTO' }), null);
});
test('reglas de SLA: umbrales y prioridades', () => {
  const now = Date.parse('2026-03-03T08:00:00.000Z');
  const s = (o) => ({ venceEnMs: now + 20 * H, estado: 'CREADO', ingresado: false, entregado: false, falloEn: null, ...o });
  const nombres = (x) => reglasActivas(x, now).map((r) => `${r.regla}:${r.prioridad}`);
  assert.deepEqual(nombres(s({})), []);
  assert.deepEqual(nombres(s({ venceEnMs: now + 9 * H })), ['SIN_INGRESO:MEDIA']);
  assert.deepEqual(nombres(s({ venceEnMs: now + 3 * H, estado: 'INGRESADO_HUB', ingresado: true })), ['SIN_SALIDA:ALTA']);
  assert.deepEqual(nombres(s({ venceEnMs: now + 3 * H, estado: 'EN_REPARTO', ingresado: true })), []);
  assert.ok(nombres(s({ venceEnMs: Date.parse('2026-03-03T20:00:00.000Z'), estado: 'ENTREGA_FALLIDA', ingresado: true, falloEn: '2026-03-03T07:00:00.000Z' })).includes('FALLO_DIA_LIMITE:ALTA'));
  assert.ok(nombres(s({ venceEnMs: now - H, estado: 'EN_REPARTO', ingresado: true })).includes('VENCIDO:CRITICA'));
  assert.deepEqual(nombres(s({ venceEnMs: now - H, entregado: true })), []);
});
test('validación de esquema', () => {
  const ok = { eventId: 'a', shipmentId: 's', tipo: 'CREADO', ocurridoEn: '2026-03-03T10:00:00.000Z', secuencia: 1, hubId: 'H1', zonaId: 'Z01', repartidorId: null, promesaVenceEn: '2026-03-04T20:00:00.000Z', version: 1 };
  assert.equal(validar(T.ESTADOS, ok), null);
  assert.match(validar(T.ESTADOS, { ...ok, tipo: 'TELEPORTADO' }), /desconocido/);
  assert.match(validar(T.ESTADOS, { ...ok, secuencia: undefined }), /secuencia/);
  assert.match(validar(T.ESTADOS, { ...ok, ocurridoEn: 'ayer' }), /ocurridoEn/);
  assert.match(validar(T.ESTADOS, { ...ok, tipo: 'ENTREGA_FALLIDA', intento: 1, repartidorId: 'R001', motivo: 'X' }), /motivo/);
  assert.match(validar(T.ESTADOS, { eventId: 'x' }), /obligatorios/);
});
