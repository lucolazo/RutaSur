'use client';
import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { ESTADO_TXT, PRIORIDAD_TXT, REGLA_TXT, hora, estadoVisible } from '../lib/fmt';

const RUTA = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO'];
const DESVIO = ['PENDIENTE', 'REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'];
const orden = (a, b) => b.prioridadRank - a.prioridadRank || (a.abiertaEn < b.abiertaEn ? 1 : -1);

export default function TorreLive({ inicial }) {
  const [conteos, setConteos] = useState(inicial.conteos);
  const [alertas, setAlertas] = useState(inicial.alertas);
  const [feed, setFeed] = useState(inicial.ultimos);
  const [vivo, setVivo] = useState(false);
  const [latencia, setLatencia] = useState(null);
  const pendiente = useRef(null);

  useEffect(() => {
    const es = new EventSource('/api/stream');
    es.onopen = () => setVivo(true);
    es.onerror = () => setVivo(false);
    es.onmessage = (m) => {
      const msg = JSON.parse(m.data);
      if (msg.tipo === 'envio') {
        setFeed((f) => [msg.doc, ...f.filter((x) => x.shipmentId !== msg.doc.shipmentId)].slice(0, 15));
        setLatencia(msg.doc._meta?.latenciaMs ?? null);
        if (!pendiente.current) { // los contadores se refrescan como máximo cada 500 ms
          pendiente.current = setTimeout(async () => {
            pendiente.current = null;
            const r = await fetch('/api/torre'); if (r.ok) setConteos((await r.json()).conteos);
          }, 500);
        }
      } else if (msg.tipo === 'alerta') {
        setAlertas((a) => { const resto = a.filter((x) => x._id !== msg.doc._id); return msg.doc.estado === 'ABIERTA' ? [msg.doc, ...resto].sort(orden).slice(0, 30) : resto; });
      }
    };
    return () => { es.close(); clearTimeout(pendiente.current); };
  }, []);

  const n = (k) => conteos[k] ?? 0;
  return (
    <>
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h2>Envíos por estado</h2>
          <span className={`vivo ${vivo ? 'on' : ''}`}><i />{vivo ? 'En vivo' : 'Reconectando…'}{latencia != null && ` · último evento proyectado en ${latencia} ms`}</span>
        </div>
        <div className="ruta">{RUTA.map((k) => <div className="tramo" key={k}><div className="nodo" /><b>{n(k)}</b><span>{ESTADO_TXT[k]}</span></div>)}</div>
        <div className="desvio">
          {DESVIO.map((k) => <div className="ficha" key={k}><b>{n(k)}</b><span>{k === 'PENDIENTE' ? 'Fallo, esperando decisión' : ESTADO_TXT[k]}</span></div>)}
        </div>
      </div>
      <div className="cols">
        <div className="panel">
          <h2>Alertas de SLA abiertas ({alertas.length})</h2>
          {alertas.length === 0 ? <p className="vacio">Sin alertas abiertas. Dispará el escenario «Demorado» desde el panel de demo.</p> : (
            <table><thead><tr><th>Prioridad</th><th>Envío</th><th>Regla</th><th>Desde</th></tr></thead><tbody>
              {alertas.map((a) => (
                <tr key={a._id}><td><span className={`etiqueta p-${a.prioridad}`}>{PRIORIDAD_TXT[a.prioridad]}</span></td>
                  <td><Link href={`/envio/${a.shipmentId}`}>{a.shipmentId}</Link></td><td>{REGLA_TXT[a.regla]}</td><td>{hora(a.abiertaEn)}</td></tr>
              ))}
            </tbody></table>
          )}
        </div>
        <div className="panel">
          <h2>Últimos eventos</h2>
          <table><thead><tr><th>Envío</th><th>Estado</th><th>Zona</th></tr></thead><tbody>
            {feed.map((d) => (
              <tr key={d.shipmentId}><td><Link href={`/envio/${d.shipmentId}`}>{d.shipmentId}</Link><br /><small>{hora(d.estadoDesde)}</small></td>
                <td>{estadoVisible(d)}</td><td>{d.zonaId}</td></tr>
            ))}
          </tbody></table>
        </div>
      </div>
    </>
  );
}
