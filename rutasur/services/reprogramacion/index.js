import { correrConsumidor } from '../../shared/consumidor.js';
import { publicar } from '../../shared/kafka.js';
import { T } from '../../shared/domain/constantes.js';
import { reaccionReprogramacion } from '../../shared/domain/reglas.js';

await correrConsumidor({
  nombre: 'reprogramacion', topics: [T.ESTADOS],
  manejar: async (ev, { productor }) => {
    const r = reaccionReprogramacion(ev);
    if (r) await publicar(productor, T.REPROG, [r]);
  },
});
