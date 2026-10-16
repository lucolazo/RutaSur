import { T, ESTADOS, MOTIVOS, PRIORIDADES, REGLAS_SLA, TIPOS_REPROG } from './constantes.js';

const str = (v) => typeof v === 'string' && v.length > 0;
const iso = (v) => typeof v === 'string' && !Number.isNaN(Date.parse(v));
const ent = (v) => Number.isInteger(v) && v >= 1;

/** Devuelve null si el evento es válido, o el motivo del rechazo (va a la DLQ). */
export function validar(topic, ev) {
  if (!ev || typeof ev !== 'object' || Array.isArray(ev)) return 'el evento no es un objeto JSON';
  const falta = ['eventId', 'shipmentId', 'tipo'].filter((k) => !str(ev[k]));
  if (falta.length) return `campos obligatorios ausentes o inválidos: ${falta.join(', ')}`;
  if (!iso(ev.ocurridoEn)) return 'ocurridoEn ausente o no es una fecha válida';
  if (ev.version !== 1) return `versión de esquema no soportada: ${ev.version}`;
  switch (topic) {
    case T.ESTADOS: {
      if (!ESTADOS.includes(ev.tipo)) return `tipo de estado desconocido: ${ev.tipo}`;
      if (!ent(ev.secuencia)) return 'secuencia ausente o inválida';
      if (!str(ev.hubId) || !str(ev.zonaId)) return 'hubId y zonaId son obligatorios';
      if (ev.tipo !== 'CREADO' && ev.tipo !== 'INGRESADO_HUB' && !str(ev.repartidorId)) return 'repartidorId obligatorio desde CLASIFICADO';
      if (ev.tipo === 'CREADO' && !iso(ev.promesaVenceEn)) return 'CREADO requiere promesaVenceEn';
      if (ev.tipo === 'ENTREGADO' && (!ent(ev.intento) || !iso(ev.promesaVenceEn))) return 'ENTREGADO requiere intento y promesaVenceEn';
      if (ev.tipo === 'ENTREGA_FALLIDA') {
        if (!ent(ev.intento)) return 'ENTREGA_FALLIDA requiere intento';
        if (!MOTIVOS.includes(ev.motivo)) return `motivo de fallo inválido: ${ev.motivo}`;
      }
      return null;
    }
    case T.PROMESAS:
      return ev.tipo === 'PROMESA' && iso(ev.venceEn) ? null : 'promesa inválida';
    case T.REPROG:
      if (!TIPOS_REPROG.includes(ev.tipo)) return `tipo de reprogramación inválido: ${ev.tipo}`;
      if (!ent(ev.intentoFallido)) return 'intentoFallido inválido';
      if (ev.tipo === 'REPROGRAMADO' && !iso(ev.fechaNuevoIntento)) return 'REPROGRAMADO requiere fechaNuevoIntento';
      return null;
    case T.ALERTAS:
      if (!['ALERTA_ABIERTA', 'ALERTA_CERRADA'].includes(ev.tipo)) return `tipo de alerta inválido: ${ev.tipo}`;
      if (!REGLAS_SLA.includes(ev.regla)) return `regla de SLA inválida: ${ev.regla}`;
      if (!(ev.prioridad in PRIORIDADES)) return `prioridad inválida: ${ev.prioridad}`;
      return null;
    case T.NOTIF:
      return ev.tipo === 'NOTIFICACION' && str(ev.origenEventId) ? null : 'notificación inválida';
    default:
      return null;
  }
}
