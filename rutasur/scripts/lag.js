import { kafka } from '../shared/kafka.js';
import { GRUPOS } from '../shared/domain/constantes.js';
import { lagDeGrupo } from '../shared/observabilidad.js';

const admin = kafka.admin(); await admin.connect();
const filas = [];
for (const g of Object.keys(GRUPOS)) { try { filas.push(...(await lagDeGrupo(admin, g))); } catch (e) { console.error(`grupo ${g}: ${e.message}`); } }
console.table(filas);
const tot = {}; for (const f of filas) tot[f.grupo] = (tot[f.grupo] ?? 0) + f.lag;
console.log('lag total por grupo:', tot);
await admin.disconnect();
