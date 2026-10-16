import http from 'node:http';
import { crearProductor, publicarCrudo } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/logger.js';
import { planificarEnvio, PARAMS_DEFECTO } from '../../shared/domain/simulacion.js';
import { T } from '../../shared/domain/constantes.js';

const log = crearLogger('simulador');
const num = (v, d) => (v === undefined || v === '' ? d : Number(v));
const params = {
  ...PARAMS_DEFECTO,
  semilla: num(process.env.SEMILLA, PARAMS_DEFECTO.semilla),
  enviosPorMinuto: num(process.env.ENVIOS_POR_MINUTO, PARAMS_DEFECTO.enviosPorMinuto),
  segundosSimPorSegundoReal: 3600 / num(process.env.SEGUNDOS_REALES_POR_HORA_SIM, 30),
  probDemoraHub: num(process.env.PROB_DEMORA_HUB, PARAMS_DEFECTO.probDemoraHub),
  probDuplicado: num(process.env.PROB_DUPLICADO, PARAMS_DEFECTO.probDuplicado),
  probFueraDeOrden: num(process.env.PROB_FUERA_DE_ORDEN, PARAMS_DEFECTO.probFueraDeOrden),
  probMalformado: num(process.env.PROB_MALFORMADO, PARAMS_DEFECTO.probMalformado),
};
const productor = await crearProductor();

let simNowMs = params.inicioSimMs; // reloj simulado: avanza solo por ticks (determinista)
let acumulador = 0, contador = 0, publicados = 0, pausado = false, ocupado = false, orden = 0, ultimoEstado = null;
const cola = []; // mensajes pendientes ordenados por (t, orden)

function insertar(m) {
  m.n = orden++;
  let lo = 0, hi = cola.length;
  while (lo < hi) { const mid = (lo + hi) >> 1; (cola[mid].t < m.t || (cola[mid].t === m.t && cola[mid].n < m.n)) ? (lo = mid + 1) : (hi = mid); }
  cola.splice(lo, 0, m);
}
const crear = (shipmentId, creadoEnMs, forzado) => planificarEnvio({ shipmentId, creadoEnMs, params, forzado }).forEach(insertar);

async function vaciar() {
  let k = 0; while (k < cola.length && cola[k].t <= simNowMs) k++;
  if (!k) return;
  const listos = cola.splice(0, k);
  const porTopic = new Map();
  for (const m of listos) {
    if (!porTopic.has(m.topic)) porTopic.set(m.topic, []);
    porTopic.get(m.topic).push({ key: m.clave, value: m.raw ?? JSON.stringify(m.evento) });
    if (m.topic === T.ESTADOS && m.evento) ultimoEstado = m;
  }
  for (const [topic, messages] of porTopic) {
    for (let i = 0; i < messages.length; i += 500) await productor.send({ topic, acks: -1, messages: messages.slice(i, i + 500) });
  }
  publicados += listos.length;
}

async function tick() {
  if (pausado || ocupado) return;
  ocupado = true;
  try {
    simNowMs += params.segundosSimPorSegundoReal * params.tickMs;
    acumulador += (params.enviosPorMinuto * params.tickMs) / 60000;
    while (acumulador >= 1) { acumulador -= 1; contador += 1; crear(`RS-${String(contador).padStart(6, '0')}`, simNowMs, null); }
    await vaciar();
  } catch (e) { log.error('tick fallido', { error: e.message }); } finally { ocupado = false; }
}
setInterval(tick, params.tickMs);

const ESCENARIOS = {
  feliz: { modo: 'feliz' },
  ausente: { modo: 'ausente', resultados: ['AUSENTE'] },
  tercerFallo: { modo: 'tercerFallo', resultados: ['AUSENTE', 'AUSENTE', 'AUSENTE'] },
  demorado: { modo: 'demorado' },
  fueraDeOrden: { modo: 'feliz', swap: true },
};
async function escenario(nombre) {
  const tag = Date.now().toString(36);
  if (nombre === 'rafaga') {
    for (let i = 0; i < 500; i++) crear(`RAF-${tag}-${String(i).padStart(3, '0')}`, simNowMs, null);
    await vaciar();
    return { ok: true, creados: 500 };
  }
  if (!ESCENARIOS[nombre]) throw new Error(`escenario desconocido: ${nombre}`);
  const shipmentId = `DEMO-${nombre.toUpperCase()}-${tag}`;
  crear(shipmentId, simNowMs, ESCENARIOS[nombre]);
  await vaciar();
  return { ok: true, shipmentId };
}
async function inyectar(tipo) {
  if (tipo === 'malformado') {
    const id = `DEMO-MALFORMADO-${Date.now().toString(36)}`;
    await publicarCrudo(productor, T.ESTADOS, id, `{"eventId":"${id}","shipmentId":"${id}","tipo":"CREADO"`); // JSON truncado
    return { ok: true, shipmentId: id };
  }
  if (tipo === 'duplicado') {
    if (!ultimoEstado) throw new Error('todavía no se publicó ningún evento');
    await publicarCrudo(productor, T.ESTADOS, ultimoEstado.clave, JSON.stringify(ultimoEstado.evento));
    return { ok: true, eventId: ultimoEstado.evento.eventId };
  }
  throw new Error(`inyección desconocida: ${tipo}`);
}
async function avanzar(horas) {
  simNowMs += horas * 3600e3; // salto de reloj: emite en orden cronológico todo lo que vence en ese lapso
  await vaciar();
  return { ok: true, simNow: new Date(simNowMs).toISOString() };
}
const estado = () => ({ simNow: new Date(simNowMs).toISOString(), pausado, enviosCreados: contador, mensajesPublicados: publicados,
  pendientes: cola.length, parametros: params });

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const responder = (code, body) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
  try {
    const [, a, b] = url.pathname.split('/');
    if (req.method === 'GET' && (a === 'state' || a === 'health')) return responder(200, estado());
    if (req.method !== 'POST') return responder(404, { error: 'no encontrado' });
    if (a === 'scenario') return responder(200, await escenario(b));
    if (a === 'inject') return responder(200, await inyectar(b));
    if (a === 'advance') return responder(200, await avanzar(Number(url.searchParams.get('horas') ?? 24)));
    if (a === 'flow' && (b === 'pause' || b === 'resume')) { pausado = b === 'pause'; return responder(200, estado()); }
    return responder(404, { error: 'no encontrado' });
  } catch (e) { return responder(400, { error: e.message }); }
}).listen(4000, () => log.info('simulador listo', { puerto: 4000, ...estado() }));
