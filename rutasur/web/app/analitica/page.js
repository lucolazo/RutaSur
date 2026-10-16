import { getDb } from '../../lib/db';
import { FRANJA_TXT, MOTIVO_TXT, dia } from '../../lib/fmt';
export const dynamic = 'force-dynamic';

const pct = (a, b) => (b ? Math.round((1000 * a) / b) / 10 : null);
const g = (o, p) => p.split('.').reduce((x, k) => x?.[k], o) ?? 0;

export default async function Analitica({ searchParams }) {
  const { zona = '', hub = '', desde = '', hasta = '' } = searchParams;
  const db = await getDb();
  const filtro = {};
  if (zona) filtro.zonaId = zona;
  if (hub) filtro.hubId = hub;
  if (desde || hasta) filtro.fecha = { ...(desde && { $gte: desde }), ...(hasta && { $lte: hasta }) };
  const [docs, todas, tiempos] = await Promise.all([
    db.collection('metricas_diarias').find(filtro).toArray(),
    db.collection('metricas_diarias').aggregate([{ $group: { _id: { z: '$zonaId', h: '$hubId' } } }]).toArray(),
    db.collection('metricas_tiempos_hub').find({ ...(hub && { hubId: hub }), ...(filtro.fecha && { fecha: filtro.fecha }) }).toArray(),
  ]);
  const zonas = [...new Set(todas.map((x) => x._id.z))].sort(), hubs = [...new Set(todas.map((x) => x._id.h))].sort();

  // 1) primer intento por zona x franja y por día
  const zf = {}, porDia = {};
  for (const d of docs) {
    const k = `${d.zonaId}|${d.franja}`; zf[k] ??= { t: 0, e: 0 }; zf[k].t += g(d, 'primerIntento.total'); zf[k].e += g(d, 'primerIntento.entregados');
    porDia[d.fecha] ??= { t: 0, e: 0 }; porDia[d.fecha].t += g(d, 'primerIntento.total'); porDia[d.fecha].e += g(d, 'primerIntento.entregados');
  }
  const zonasVis = [...new Set(docs.map((d) => d.zonaId))].sort();
  const peor = Object.entries(zf).filter(([, v]) => v.t >= 10).map(([k, v]) => ({ k, fallo: 100 - pct(v.e, v.t), n: v.t })).sort((a, b) => b.fallo - a.fallo)[0];
  // 2) motivos por zona
  const mot = {}; for (const d of docs) for (const m of Object.keys(MOTIVO_TXT)) { mot[d.zonaId] ??= {}; mot[d.zonaId][m] = (mot[d.zonaId][m] ?? 0) + g(d, `fallos.motivos.${m}`); }
  // 3) SLA por hub y día
  const sla = {}; for (const d of docs) { const k = `${d.hubId}|${d.fecha}`; sla[k] ??= { t: 0, ok: 0 }; sla[k].t += g(d, 'sla.total'); sla[k].ok += g(d, 'sla.enTiempo'); }
  const slaFilas = Object.entries(sla).sort();
  // 4) tiempo medio hub -> reparto (reprocesamiento)
  const th = {}; for (const t of tiempos) { th[t.hubId] ??= { s: 0, c: 0 }; th[t.hubId].s += t.sumaMs; th[t.hubId].c += t.cantidad; }
  const color = (f) => (f == null ? '#eef2f6' : `rgba(192,57,43,${Math.min(0.85, 0.12 + f / 45)})`);

  return (
    <>
      <h1>Analítica de mejora</h1>
      <p className="sub">Tres indicadores calculados con incrementos atómicos sobre <code>metricas_diarias</code>. Se actualizan al cargar la página.</p>
      <form className="filtros" method="GET">
        <label>Zona<select name="zona" defaultValue={zona}><option value="">Todas</option>{zonas.map((z) => <option key={z}>{z}</option>)}</select></label>
        <label>Hub<select name="hub" defaultValue={hub}><option value="">Todos</option>{hubs.map((h) => <option key={h}>{h}</option>)}</select></label>
        <label>Desde<input type="date" name="desde" defaultValue={desde} /></label>
        <label>Hasta<input type="date" name="hasta" defaultValue={hasta} /></label>
        <button type="submit">Aplicar filtros</button>
      </form>
      {docs.length === 0 && <div className="aviso">Todavía no hay métricas para este filtro. Con el simulador en marcha aparecen en pocos minutos.</div>}
      {peor && <div className="aviso"><b>Dónde y cuándo fallamos más:</b> zona {peor.k.split('|')[0]}, franja {FRANJA_TXT[peor.k.split('|')[1]].toLowerCase()} ({peor.fallo.toFixed(1)} % de fallos en primer intento, {peor.n} intentos).{peor.k.endsWith('MANANA') ? ' Propuesta: ofrecer franja vespertina en esa zona.' : ''}</div>}

      <div className="panel"><h2>Tasa de entrega en primer intento · ¿dónde y cuándo fallamos más?</h2>
        <table><thead><tr><th>Zona</th>{Object.keys(FRANJA_TXT).map((f) => <th key={f} className="num">{FRANJA_TXT[f]}</th>)}</tr></thead><tbody>
          {zonasVis.map((z) => (<tr key={z}><td>{z}</td>{Object.keys(FRANJA_TXT).map((f) => { const v = zf[`${z}|${f}`]; const e = v ? pct(v.e, v.t) : null;
            return <td key={f}><div className="calor" style={{ background: color(e == null ? null : 100 - e) }}>{e == null ? '—' : `${e} %`} <small>({v?.t ?? 0})</small></div></td>; })}</tr>))}
        </tbody></table>
        <h2 style={{ marginTop: 18 }}>Por día</h2>
        <table><thead><tr><th>Día</th><th className="num">Intentos</th><th className="num">Entregados a la primera</th><th className="num">Tasa</th></tr></thead><tbody>
          {Object.entries(porDia).sort().map(([f, v]) => <tr key={f}><td>{dia(f)}</td><td className="num">{v.t}</td><td className="num">{v.e}</td><td className="num">{pct(v.e, v.t)} %</td></tr>)}
        </tbody></table>
      </div>

      <div className="panel"><h2>Distribución de motivos de fallo · ¿por qué fallamos?</h2>
        <table><thead><tr><th>Zona</th><th>Reparto</th>{Object.values(MOTIVO_TXT).map((m) => <th key={m} className="num">{m}</th>)}</tr></thead><tbody>
          {Object.entries(mot).sort().map(([z, m]) => { const tot = Object.values(m).reduce((s, x) => s + x, 0);
            return (<tr key={z}><td>{z}</td><td><div className="barra">{Object.keys(MOTIVO_TXT).map((k, i) => <i key={k} className={`c${i}`} style={{ width: `${tot ? (100 * m[k]) / tot : 0}%` }} title={`${MOTIVO_TXT[k]}: ${m[k]}`} />)}</div></td>
              {Object.keys(MOTIVO_TXT).map((k) => <td key={k} className="num">{m[k]}</td>)}</tr>); })}
        </tbody></table>
      </div>

      <div className="panel"><h2>Cumplimiento de SLA · ¿qué hub llega tarde?</h2>
        <table><thead><tr><th>Hub</th><th>Día</th><th className="num">Entregas</th><th className="num">En tiempo</th><th className="num">Cumplimiento</th></tr></thead><tbody>
          {slaFilas.map(([k, v]) => { const [h, f] = k.split('|'); return <tr key={k}><td>{h}</td><td>{dia(f)}</td><td className="num">{v.t}</td><td className="num">{v.ok}</td><td className="num">{pct(v.ok, v.t)} %</td></tr>; })}
        </tbody></table>
      </div>

      <div className="panel"><h2>Tiempo medio entre «En el hub» y «En reparto» · por hub</h2>
        {Object.keys(th).length === 0 ? (
          <p className="vacio">Este indicador todavía no está activo. Se activa sin tocar los demás servicios, con un grupo de consumidores nuevo que relee el histórico:<br /><code>docker compose --profile replay up -d tiempos-hub</code></p>
        ) : (
          <table><thead><tr><th>Hub</th><th className="num">Envíos medidos</th><th className="num">Tiempo medio</th></tr></thead><tbody>
            {Object.entries(th).sort().map(([h, v]) => <tr key={h}><td>{h}</td><td className="num">{v.c}</td><td className="num">{(v.s / v.c / 3600e3).toFixed(2)} h</td></tr>)}
          </tbody></table>
        )}
      </div>
    </>
  );
}
