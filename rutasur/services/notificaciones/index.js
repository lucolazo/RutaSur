import { correrConsumidor } from '../../shared/consumidor.js';
import { publicar } from '../../shared/kafka.js';
import { T } from '../../shared/domain/constantes.js';
import { reaccionNotificacion } from '../../shared/domain/reglas.js';

// Se registra la notificación (no hay envío real de mail/SMS). EventId determinista => reintentos inocuos.
await correrConsumidor({
  nombre: 'notificaciones', topics: [T.ESTADOS, T.ALERTAS],
  manejar: async (ev, { topic, productor }) => {
    const n = reaccionNotificacion(topic, ev);
    if (n) await publicar(productor, T.NOTIF, [n]);
  },
});
