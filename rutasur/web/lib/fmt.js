// Todo en UTC y con formato fijo para que servidor y navegador rendericen igual (sin errores de hidratación).
export const hora = (iso) => (iso ? new Date(iso).toISOString().replace('T', ' ').slice(0, 16) + ' UTC' : '—');
export const dia = (iso) => (iso ? new Date(iso).toISOString().slice(0, 10) : '—');
export const ESTADO_TXT = {
  CREADO: 'Creado', INGRESADO_HUB: 'En el hub', CLASIFICADO: 'Clasificado', EN_REPARTO: 'En reparto', ENTREGADO: 'Entregado',
  ENTREGA_FALLIDA: 'Entrega fallida', PENDIENTE: 'Fallo sin decisión aún', REPROGRAMADO: 'Reprogramado', RETENIDO: 'Retenido', DEVOLUCION: 'En devolución',
};
export const MOTIVO_TXT = { AUSENTE: 'Ausente', DIRECCION_INCORRECTA: 'Dirección incorrecta', RECHAZADO: 'Rechazado', ZONA_INACCESIBLE: 'Zona inaccesible' };
export const PRIORIDAD_TXT = { MEDIA: 'Media', ALTA: 'Alta', CRITICA: 'Crítica' };
export const REGLA_TXT = { SIN_INGRESO: 'Sin ingreso al hub', SIN_SALIDA: 'Sin salida a reparto', FALLO_DIA_LIMITE: 'Fallo en el día límite', VENCIDO: 'Vencido' };
export const FRANJA_TXT = { MANANA: 'Mañana', TARDE: 'Tarde' };
export const estadoVisible = (d) => (d.estado === 'ENTREGA_FALLIDA' ? ESTADO_TXT[d.situacion] ?? ESTADO_TXT.ENTREGA_FALLIDA : ESTADO_TXT[d.estado] ?? '—');
