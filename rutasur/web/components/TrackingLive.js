'use client';
import { useEffect, useState } from 'react';
import { ESTADO_TXT, dia, estadoVisible } from '../lib/fmt';

export default function TrackingLive({ id, inicial }) {
  const [d, setD] = useState(inicial);
  useEffect(() => {
    const es = new EventSource(`/api/stream?public=1&id=${encodeURIComponent(id)}`);
    es.onmessage = (m) => { const msg = JSON.parse(m.data); if (msg.tipo === 'envio') setD(msg.doc); };
    return () => es.close();
  }, [id]);
  if (!d) return <p>Todavía no encontramos este envío. Probá de nuevo en un momento: la página se actualiza sola.</p>;
  const texto = d.estado === 'ENTREGADO' ? 'Tu paquete fue entregado.'
    : d.situacion === 'RETENIDO' ? 'Necesitamos corregir la dirección para volver a intentar la entrega.'
    : d.situacion === 'DEVOLUCION' ? 'Tu paquete vuelve al comercio.'
    : d.fechaEstimada ? `Fecha estimada: ${dia(d.fechaEstimada)}` : 'Estamos procesando tu envío.';
  return (<><p style={{ margin: 0 }}>Estado actual</p><p className="grande">{estadoVisible(d)}</p><p>{texto}</p>
    {d.estado === 'ENTREGA_FALLIDA' && d.situacion === 'REPROGRAMADO' && <p>Vamos a intentarlo de nuevo.</p>}
    <p className="vivo on"><i />Se actualiza en vivo</p></>);
}
