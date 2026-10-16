import { T, HUBS, ZONAS } from './constantes.js';
import { decidirReprogramacion, diaHabilSiguiente, franjaDe } from './reglas.js';

const H = 3600e3, MIN = 60e3;
export const TASA_FALLO = { Z01: 0.05, Z02: 0.08, Z03: 0.12, Z04: 0.18, Z05: 0.30, Z06: 0.22, Z07: 0.15, Z08: 0.10, Z09: 0.25 };
export const PARAMS_DEFECTO = {
  semilla: 12345, enviosPorMinuto: 20, segundosSimPorSegundoReal: 120, // 1 h simulada = 30 s reales
  probDemoraHub: 0.10, probDuplicado: 0.02, probFueraDeOrden: 0.02, probMalformado: 0.01,
  tickMs: 1000, inicioSimMs: Date.parse('2026-03-02T06:00:00.000Z'),
};

export function mulberry32(a) {
  return () => { a |= 0; a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}
export function hash(s) { let h = 2166136261; for (const c of s) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; }

function motivoAleatorio(rng) { const r = rng(); return r < 0.60 ? 'AUSENTE' : r < 0.80 ? 'DIRECCION_INCORRECTA' : r < 0.95 ? 'RECHAZADO' : 'ZONA_INACCESIBLE'; }

/**
 * Planifica TODO el recorrido de un envío (incluidos reintentos) de forma determinista:
 * el generador se siembra con semilla + shipmentId, así la corrida es repetible aunque
 * cambie el orden o el ritmo real. `forzado` = {modo, resultados:[motivo...], swap} para los escenarios.
 * Devuelve mensajes {topic, clave, t (instante de emisión, ms simulados), evento | raw}.
 */
export function planificarEnvio({ shipmentId, creadoEnMs, params = PARAMS_DEFECTO, forzado = null }) {
  const rng = mulberry32(hash(`${params.semilla}:${shipmentId}`));
  const uni = (a, b) => a + rng() * (b - a);
  const zona = ZONAS[Math.floor(rng() * ZONAS.length)];
  const hubIdx = Object.keys(HUBS).indexOf(zona.hubId);
  const repartidorId = 'R' + String(hubIdx * 40 + 1 + Math.floor(rng() * 40)).padStart(3, '0');
  const iso = (ms) => new Date(ms).toISOString();
  const msgs = []; let seq = 0;
  const estado = (tipo, t, extra = {}) => {
    seq += 1;
    const evento = { eventId: `${shipmentId}-e${seq}`, shipmentId, tipo, ocurridoEn: iso(t), secuencia: seq,
      hubId: zona.hubId, zonaId: zona.zonaId,
      repartidorId: tipo === 'CREADO' || tipo === 'INGRESADO_HUB' ? null : repartidorId, version: 1, ...extra };
    msgs.push({ topic: T.ESTADOS, clave: shipmentId, t, evento, tipo });
  };

  const t0 = creadoEnMs;
  let venceEn, ingreso, clasif, reparto, resultado;
  const modo = forzado?.modo ?? null;
  if (modo === 'demorado') { venceEn = t0 + 6 * H; ingreso = t0 + 10 * MIN; clasif = venceEn + 2 * H; reparto = clasif + 30 * MIN; resultado = reparto + H; }
  else if (modo) { venceEn = t0 + 30 * H; ingreso = t0 + 6 * MIN; clasif = t0 + 12 * MIN; reparto = t0 + 18 * MIN; resultado = t0 + 30 * MIN; }
  else {
    const dia1 = diaHabilSiguiente(iso(t0)).getTime();
    venceEn = dia1 + 20 * H;
    ingreso = t0 + uni(0.5, 3) * H; clasif = ingreso + uni(0.5, 2) * H;
    reparto = Math.max(clasif + H, dia1 + uni(8, 11) * H); resultado = reparto + uni(1, 4) * H;
    if (rng() < params.probDemoraHub) { // demora en el hub: dispara alertas de SLA
      if (rng() < 0.5) { ingreso = venceEn - uni(2, 9) * H; clasif = ingreso + 30 * MIN; reparto = clasif + H; resultado = reparto + uni(1, 3) * H; }
      else { clasif = venceEn - uni(1, 3) * H; reparto = clasif + 30 * MIN; resultado = reparto + uni(1, 2) * H; }
    }
  }

  estado('CREADO', t0, { promesaVenceEn: iso(venceEn) });
  msgs.push({ topic: T.PROMESAS, clave: shipmentId, t: t0, evento: { eventId: `${shipmentId}-promesa`, shipmentId, tipo: 'PROMESA',
    venceEn: iso(venceEn), ocurridoEn: iso(t0), hubId: zona.hubId, zonaId: zona.zonaId, version: 1 } });
  estado('INGRESADO_HUB', ingreso);
  estado('CLASIFICADO', clasif);
  let intento = 1;
  for (;;) {
    estado('EN_REPARTO', reparto);
    let motivo = null;
    if (forzado) motivo = forzado.resultados?.[intento - 1] ?? null;
    else {
      const p = Math.min(0.9, TASA_FALLO[zona.zonaId] * (franjaDe(iso(resultado)) === 'MANANA' ? 1.2 : 0.8));
      if (rng() < p) motivo = motivoAleatorio(rng);
    }
    if (!motivo) { estado('ENTREGADO', resultado, { intento, promesaVenceEn: iso(venceEn) }); break; }
    estado('ENTREGA_FALLIDA', resultado, { intento, motivo });
    const dec = decidirReprogramacion({ motivo, intentoFallido: intento, ocurridoEn: iso(resultado) });
    if (dec.tipo !== 'REPROGRAMADO') break;
    intento += 1;
    const slot = Date.parse(dec.fechaNuevoIntento);
    estado('CLASIFICADO', slot - 30 * MIN);
    reparto = slot + uni(0, 1.5) * H; resultado = reparto + uni(1, 4) * H;
  }

  // Defectos del mundo real: fuera de orden, duplicados y mensajes malformados.
  const est = msgs.filter((m) => m.topic === T.ESTADOS);
  const swap = forzado ? !!forzado.swap : rng() < params.probFueraDeOrden;
  if (swap) { // solo estados intermedios (nunca CREADO ni el resultado)
    const a = forzado ? 2 : (rng() < 0.5 ? 1 : 2);
    [est[a].t, est[a + 1].t] = [est[a + 1].t, est[a].t];
  }
  if (!forzado) {
    for (const m of est) if (rng() < params.probDuplicado) msgs.push({ ...m, duplicado: true });
    if (rng() < params.probMalformado) {
      const m = est[Math.floor(rng() * est.length)];
      const v = Math.floor(rng() * 4);
      const e = m.evento;
      const raw = [
        `{"eventId":"${e.eventId}","shipmentId":"${shipmentId}",`,
        JSON.stringify({ ...e, tipo: 'TELEPORTADO', eventId: `${e.eventId}-x` }),
        JSON.stringify({ ...e, secuencia: undefined, eventId: `${e.eventId}-y` }),
        JSON.stringify({ ...e, ocurridoEn: 'ayer', eventId: `${e.eventId}-z` }),
      ][v];
      msgs.push({ topic: T.ESTADOS, clave: shipmentId, t: m.t, raw, malformado: true });
    }
  }
  return msgs.map((m, i) => ({ m, i })).sort((a, b) => a.m.t - b.m.t || a.i - b.i).map((x) => x.m);
}
