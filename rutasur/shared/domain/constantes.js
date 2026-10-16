export const T = {
  ESTADOS: 'envios.estados',
  PROMESAS: 'envios.promesas',
  REPROG: 'envios.reprogramaciones',
  ALERTAS: 'alertas.sla',
  NOTIF: 'notificaciones.enviadas',
  DLQ: 'envios.estados.dlq',
};
export const ESTADOS = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];
export const RANK = { CREADO: 0, INGRESADO_HUB: 1, CLASIFICADO: 2, EN_REPARTO: 3, ENTREGADO: 4, ENTREGA_FALLIDA: 4 };
export const MOTIVOS = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];
export const PRIORIDADES = { MEDIA: 1, ALTA: 2, CRITICA: 3 };
export const REGLAS_SLA = ['SIN_INGRESO', 'SIN_SALIDA', 'FALLO_DIA_LIMITE', 'VENCIDO'];
export const TIPOS_REPROG = ['REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'];

// Grupo de consumidores -> topics que lee (catálogo de eventos de la especificación)
export const GRUPOS = {
  proyector: [T.ESTADOS, T.REPROG, T.ALERTAS, T.NOTIF],
  notificaciones: [T.ESTADOS, T.ALERTAS],
  reprogramacion: [T.ESTADOS],
  'monitor-sla': [T.ESTADOS, T.PROMESAS],
  analitica: [T.ESTADOS],
  'analitica-tiempos': [T.ESTADOS],
};

const DIA = 86400;
export const TOPICS = [
  { topic: T.ESTADOS, retencionS: 30 * DIA },
  { topic: T.PROMESAS, compactado: true },
  { topic: T.REPROG, retencionS: 30 * DIA },
  { topic: T.ALERTAS, retencionS: 7 * DIA },
  { topic: T.NOTIF, retencionS: 7 * DIA },
  { topic: T.DLQ, retencionS: 30 * DIA },
];

export const HUBS = { H1: ['Z01', 'Z02', 'Z03'], H2: ['Z04', 'Z05', 'Z06'], H3: ['Z07', 'Z08', 'Z09'] };
export const ZONAS = Object.entries(HUBS).flatMap(([hubId, zs]) => zs.map((zonaId) => ({ zonaId, hubId })));
