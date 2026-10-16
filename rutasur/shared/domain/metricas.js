import { fechaDe, franjaDe } from './reglas.js';

/** Incrementos atómicos de metricas_diarias (CU4) para un evento de estado. */
export function incrementosDe(ev) {
  if (ev.tipo !== 'ENTREGADO' && ev.tipo !== 'ENTREGA_FALLIDA') return null;
  const inc = {};
  if (ev.tipo === 'ENTREGADO') {
    inc.entregas = 1;
    if (ev.intento === 1) { inc['primerIntento.total'] = 1; inc['primerIntento.entregados'] = 1; }
    inc['sla.total'] = 1;
    inc[Date.parse(ev.ocurridoEn) <= Date.parse(ev.promesaVenceEn) ? 'sla.enTiempo' : 'sla.tarde'] = 1;
  } else {
    inc['fallos.total'] = 1;
    inc[`fallos.motivos.${ev.motivo}`] = 1;
    if (ev.intento === 1) inc['primerIntento.total'] = 1;
  }
  return { filtro: { fecha: fechaDe(ev.ocurridoEn), zonaId: ev.zonaId, franja: franjaDe(ev.ocurridoEn) }, hubId: ev.hubId, inc };
}

/** Aplana un documento de metricas_diarias a contadores con ruta "a.b.c" (sin ceros). */
export function aplanar(doc, prefijo = '', out = {}) {
  for (const [k, v] of Object.entries(doc)) {
    if (!prefijo && ['_id', 'fecha', 'zonaId', 'franja', 'hubId'].includes(k)) continue;
    if (v && typeof v === 'object') aplanar(v, `${prefijo}${k}.`, out);
    else if (typeof v === 'number' && v !== 0) out[`${prefijo}${k}`] = v;
  }
  return out;
}
