'use client';
import { useEffect, useState } from 'react';
import { ESTADO_TXT, MOTIVO_TXT, PRIORIDAD_TXT, REGLA_TXT, FRANJA_TXT, hora, estadoVisible } from '../lib/fmt';

function texto(h) {
  if (h.fuente === 'reprogramacion') {
    if (h.tipo === 'REPROGRAMADO') return `Reprogramado para ${hora(h.fechaNuevoIntento)} (franja ${FRANJA_TXT[h.franja].toLowerCase()})`;
    return h.tipo === 'RETENIDO' ? 'Retenido hasta corregir la dirección' : 'Marcado para devolución al comercio';
  }
  return ESTADO_TXT[h.tipo] + (h.motivo ? ` · ${MOTIVO_TXT[h.motivo]}` : '') + (h.intento ? ` (intento ${h.intento})` : '');
}

export default function DetalleLive({ id, inicial }) {
  const [envio, setEnvio] = useState(inicial.envio);
  const [alertas, setAlertas] = useState(inicial.alertas);
  const [notifs, setNotifs] = useState(inicial.notificaciones);
  const [vivo, setVivo] = useState(false);

  useEffect(() => {
    const es = new EventSource(`/api/stream?id=${encodeURIComponent(id)}`);
    es.onopen = () => setVivo(true); es.onerror = () => setVivo(false);
    es.onmessage = (m) => {
      const msg = JSON.parse(m.data);
      const upsert = (lista, doc) => [doc, ...lista.filter((x) => x._id !== doc._id)];
      if (msg.tipo === 'envio') setEnvio(msg.doc);
      if (msg.tipo === 'alerta') setAlertas((l) => upsert(l, msg.doc));
      if (msg.tipo === 'notificacion') setNotifs((l) => upsert(l, msg.doc));
    };
    return () => es.close();
  }, [id]);

  if (!envio) return <p className="vacio">Todavía no hay eventos para este envío. Esta pantalla se completa sola cuando llegue el primero.</p>;
  return (
    <>
      <div className="panel">
        <h2>{estadoVisible(envio)} <span className={`vivo ${vivo ? 'on' : ''}`} style={{ marginLeft: 10 }}><i />{vivo ? 'En vivo' : 'Reconectando…'}</span></h2>
        <table><tbody>
          <tr><th>Zona / hub</th><td>{envio.zonaId} / {envio.hubId}</td><th>Repartidor</th><td>{envio.repartidorId ?? 'Sin asignar'}</td></tr>
          <tr><th>Promesa de entrega</th><td>{hora(envio.promesa?.venceEn)}</td><th>Intentos</th><td>{envio.intentos}</td></tr>
          {envio.proximoIntento && <tr><th>Próximo intento</th><td colSpan={3}>{hora(envio.proximoIntento.fecha)} · franja {FRANJA_TXT[envio.proximoIntento.franja].toLowerCase()}</td></tr>}
        </tbody></table>
      </div>
      <div className="cols">
        <div className="panel">
          <h2>Línea de tiempo</h2>
          <ul className="linea-tiempo">{envio.historial.map((h) => (
            <li key={h.eventId} className={h.fuente === 'reprogramacion' ? 'reprog' : ''}>{texto(h)}<small>{hora(h.ocurridoEn)}</small></li>))}</ul>
        </div>
        <div>
          <div className="panel"><h2>Alertas de SLA</h2>
            {alertas.length === 0 ? <p className="vacio">Sin alertas para este envío.</p> : (
              <table><tbody>{alertas.map((a) => (
                <tr key={a._id}><td><span className={`etiqueta p-${a.prioridad}`}>{PRIORIDAD_TXT[a.prioridad]}</span></td>
                  <td>{REGLA_TXT[a.regla]}<br /><small>{a.causa}</small></td><td>{a.estado === 'ABIERTA' ? 'Abierta' : <span className="ok">Cerrada</span>}</td></tr>))}</tbody></table>)}
          </div>
          <div className="panel"><h2>Notificaciones al destinatario</h2>
            {notifs.length === 0 ? <p className="vacio">Todavía no se registró ninguna notificación.</p> : (
              <table><tbody>{notifs.map((n) => (<tr key={n._id}><td>{n.mensaje}<br /><small>{n.canal} a {n.destinatario} · {hora(n.ocurridoEn)}</small></td></tr>))}</tbody></table>)}
          </div>
        </div>
      </div>
    </>
  );
}
