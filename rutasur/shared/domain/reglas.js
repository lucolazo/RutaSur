import { T, PRIORIDADES } from './constantes.js';

const H = 3600e3;
export const fechaDe = (iso) => new Date(iso).toISOString().slice(0, 10);
export const franjaDe = (iso) => (new Date(iso).getUTCHours() < 13 ? 'MANANA' : 'TARDE');
export const INICIO_FRANJA = { MANANA: 8, TARDE: 13 };

/** 00:00 UTC del siguiente día hábil (lunes a viernes). */
export function diaHabilSiguiente(iso) {
  const d = new Date(fechaDe(iso) + 'T00:00:00.000Z');
  do { d.setUTCDate(d.getUTCDate() + 1); } while ([0, 6].includes(d.getUTCDay()));
  return d;
}

/**
 * Reglas de reprogramación (CU2). Se evalúa primero "¿tercer intento?" y luego el motivo,
 * igual que el diagrama de flujo. ZONA_INACCESIBLE no tiene regla propia en la
 * especificación: se trata como AUSENTE (supuesto documentado en el README).
 */
export function decidirReprogramacion({ motivo, intentoFallido, ocurridoEn }) {
  if (intentoFallido >= 3) return { tipo: 'DEVOLUCION', razon: 'TERCER_INTENTO_FALLIDO' };
  if (motivo === 'DIRECCION_INCORRECTA') return { tipo: 'RETENIDO', razon: 'DIRECCION_INCORRECTA' };
  if (motivo === 'RECHAZADO') return { tipo: 'DEVOLUCION', razon: 'RECHAZADO' };
  const franja = franjaDe(ocurridoEn) === 'MANANA' ? 'TARDE' : 'MANANA';
  const f = diaHabilSiguiente(ocurridoEn);
  f.setUTCHours(INICIO_FRANJA[franja]);
  return { tipo: 'REPROGRAMADO', razon: motivo, franja, fechaNuevoIntento: f.toISOString() };
}

export function reaccionReprogramacion(ev) {
  if (ev.tipo !== 'ENTREGA_FALLIDA') return null;
  const d = decidirReprogramacion({ motivo: ev.motivo, intentoFallido: ev.intento, ocurridoEn: ev.ocurridoEn });
  return {
    eventId: `rep-${ev.eventId}`, shipmentId: ev.shipmentId, tipo: d.tipo, razon: d.razon,
    motivo: ev.motivo, intentoFallido: ev.intento,
    ...(d.franja && { franja: d.franja, fechaNuevoIntento: d.fechaNuevoIntento }),
    origenEventId: ev.eventId, ocurridoEn: ev.ocurridoEn, hubId: ev.hubId, zonaId: ev.zonaId, version: 1,
  };
}

/** Notificaciones (CU2): una por ENTREGA_FALLIDA y una por alerta Alta/Crítica. EventId determinista. */
export function reaccionNotificacion(topic, ev) {
  const base = (origen, motivoNotificacion, mensaje, opciones) => ({
    eventId: `notif-${origen}`, shipmentId: ev.shipmentId, tipo: 'NOTIFICACION', motivoNotificacion,
    canal: 'EMAIL', destinatario: `destinatario-${ev.shipmentId}@ejemplo.test`, mensaje, opciones,
    origenEventId: origen, ocurridoEn: ev.ocurridoEn, hubId: ev.hubId, zonaId: ev.zonaId, version: 1,
  });
  if (topic === T.ESTADOS && ev.tipo === 'ENTREGA_FALLIDA') {
    return base(ev.eventId, 'ENTREGA_FALLIDA', `No pudimos entregar tu envío (${ev.motivo}). Elegí cómo continuar.`,
      ['REPROGRAMAR', 'RETIRAR_EN_SUCURSAL', 'CORREGIR_DIRECCION']);
  }
  if (topic === T.ALERTAS && ev.tipo === 'ALERTA_ABIERTA' && PRIORIDADES[ev.prioridad] >= PRIORIDADES.ALTA) {
    return base(ev.eventId, 'ALERTA_SLA', 'Tu envío podría llegar más tarde de lo prometido. Ya estamos trabajando en eso.', []);
  }
  return null;
}

/** Reglas de SLA (CU3). `s` = estado interno del envío; `ahoraMs` = reloj simulado (watermark). */
export function reglasActivas(s, ahoraMs) {
  if (s.entregado || s.venceEnMs == null || !s.estado) return [];
  const resta = s.venceEnMs - ahoraMs;
  const out = [];
  if (resta < 10 * H && !s.ingresado) out.push({ regla: 'SIN_INGRESO', prioridad: 'MEDIA', causa: 'Faltan menos de 10 h y no ingresó al hub' });
  if (resta < 4 * H && s.estado !== 'EN_REPARTO') out.push({ regla: 'SIN_SALIDA', prioridad: 'ALTA', causa: 'Faltan menos de 4 h y no está en reparto' });
  if (s.estado === 'ENTREGA_FALLIDA' && s.falloEn && fechaDe(s.falloEn) === fechaDe(new Date(s.venceEnMs).toISOString()))
    out.push({ regla: 'FALLO_DIA_LIMITE', prioridad: 'ALTA', causa: 'Entrega fallida y la promesa vence hoy' });
  if (resta < 0) out.push({ regla: 'VENCIDO', prioridad: 'CRITICA', causa: 'Pasó el vencimiento sin ENTREGADO' });
  return out;
}
