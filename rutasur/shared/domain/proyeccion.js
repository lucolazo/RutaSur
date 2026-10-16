import { PRIORIDADES } from './constantes.js';

const ORDEN_FUENTE = { estado: 0, reprogramacion: 1 };

function nuevo(shipmentId) {
  return {
    _id: shipmentId, shipmentId, estado: null, estadoDesde: null, hubId: null, zonaId: null, repartidorId: null,
    promesa: null, intentos: 0, motivoUltimoFallo: null, ultimaSecuencia: 0, historial: [],
    reprogramaciones: {}, situacion: null, proximoIntento: null,
  };
}
const t = (iso) => Date.parse(iso);
function ordenar(h) {
  h.sort((a, b) => (t(a.ocurridoEn) - t(b.ocurridoEn)) || (ORDEN_FUENTE[a.fuente] - ORDEN_FUENTE[b.fuente])
    || ((a.secuencia ?? 0) - (b.secuencia ?? 0)) || (a.eventId < b.eventId ? -1 : 1));
}
/** Campos derivados: no dependen del orden de llegada entre topics, solo del contenido. */
function derivar(d) {
  if (d.estado === 'ENTREGA_FALLIDA') {
    const r = d.reprogramaciones[String(d.intentos)];
    d.situacion = r ? r.tipo : 'PENDIENTE';
    d.proximoIntento = r?.tipo === 'REPROGRAMADO' ? { fecha: r.fechaNuevoIntento, franja: r.franja } : null;
  } else { d.situacion = null; d.proximoIntento = null; }
}

export function aplicarEstado(doc, ev) {
  if (doc && ev.secuencia <= doc.ultimaSecuencia) {
    return { doc, aplicado: false, motivo: ev.secuencia === doc.ultimaSecuencia ? 'secuencia repetida' : 'evento fuera de orden' };
  }
  const d = doc ? structuredClone(doc) : nuevo(ev.shipmentId);
  d.estado = ev.tipo; d.estadoDesde = ev.ocurridoEn; d.ultimaSecuencia = ev.secuencia;
  d.hubId = ev.hubId; d.zonaId = ev.zonaId;
  if (ev.repartidorId) d.repartidorId = ev.repartidorId;
  if (ev.promesaVenceEn) d.promesa = { venceEn: ev.promesaVenceEn };
  if (ev.intento) d.intentos = Math.max(d.intentos, ev.intento);
  if (ev.tipo === 'ENTREGA_FALLIDA') d.motivoUltimoFallo = ev.motivo;
  d.historial.push({
    fuente: 'estado', eventId: ev.eventId, tipo: ev.tipo, ocurridoEn: ev.ocurridoEn, secuencia: ev.secuencia,
    ...(ev.motivo && { motivo: ev.motivo }), ...(ev.intento && { intento: ev.intento }),
    ...(ev.repartidorId && { repartidorId: ev.repartidorId }),
  });
  ordenar(d.historial); derivar(d);
  return { doc: d, aplicado: true };
}

export function aplicarReprogramacion(doc, ev) {
  const d = doc ? structuredClone(doc) : nuevo(ev.shipmentId);
  if (d.reprogramaciones[String(ev.intentoFallido)]?.eventId === ev.eventId) return { doc, aplicado: false, motivo: 'duplicado' };
  d.hubId ??= ev.hubId; d.zonaId ??= ev.zonaId;
  d.reprogramaciones[String(ev.intentoFallido)] = {
    eventId: ev.eventId, tipo: ev.tipo, razon: ev.razon ?? null, ocurridoEn: ev.ocurridoEn,
    ...(ev.fechaNuevoIntento && { fechaNuevoIntento: ev.fechaNuevoIntento, franja: ev.franja }),
  };
  d.historial.push({
    fuente: 'reprogramacion', eventId: ev.eventId, tipo: ev.tipo, ocurridoEn: ev.ocurridoEn,
    intentoFallido: ev.intentoFallido, razon: ev.razon ?? null,
    ...(ev.fechaNuevoIntento && { fechaNuevoIntento: ev.fechaNuevoIntento, franja: ev.franja }),
  });
  ordenar(d.historial); derivar(d);
  return { doc: d, aplicado: true };
}

export function aplicarAlerta(doc, ev) {
  const d = doc ? { ...doc } : {
    _id: `${ev.shipmentId}:${ev.regla}`, shipmentId: ev.shipmentId, regla: ev.regla, prioridad: ev.prioridad,
    prioridadRank: PRIORIDADES[ev.prioridad], causa: ev.causa ?? null, estado: 'ABIERTA', abiertaEn: null, cerradaEn: null,
    hubId: ev.hubId ?? null, zonaId: ev.zonaId ?? null,
  };
  if (ev.tipo === 'ALERTA_ABIERTA') d.abiertaEn ??= ev.ocurridoEn;
  else { d.estado = 'CERRADA'; d.cerradaEn = ev.ocurridoEn; d.abiertaEn ??= ev.abiertaEn ?? null; }
  return d; // una alerta cerrada nunca se reabre
}

export function aplicarNotificacion(ev) {
  return {
    _id: ev.eventId, shipmentId: ev.shipmentId, motivoNotificacion: ev.motivoNotificacion, canal: ev.canal,
    destinatario: ev.destinatario, mensaje: ev.mensaje, opciones: ev.opciones ?? [], origenEventId: ev.origenEventId,
    ocurridoEn: ev.ocurridoEn,
  };
}
