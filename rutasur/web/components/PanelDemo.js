'use client';
import Link from 'next/link';
import { useEffect, useState } from 'react';

const ACCIONES = [
  { a: 'feliz', t: 'Envío feliz', d: 'Un envío recorre todos los estados sin incidentes. Seguilo en el tracking público.' },
  { a: 'ausente', t: 'Ausente', d: 'Un envío falla por ausencia: aparecen aviso, reprogramación y el incremento en analítica.' },
  { a: 'demorado', t: 'Demorado', d: 'Un envío queda detenido en el hub hasta generar una alerta de SLA.' },
  { a: 'tercerFallo', t: 'Tercer fallo', d: 'Un envío agota sus intentos y pasa a devolución. Usá «Avanzar 24 h» entre intentos.' },
  { a: 'rafaga', t: 'Ráfaga de 500 envíos', d: 'Mirá cómo crece y se recupera el lag de los grupos de consumidores.' },
  { a: 'fueraDeOrden', t: 'Evento fuera de orden', d: 'Un envío con dos eventos intercambiados: el estado no retrocede.' },
  { a: 'duplicado', t: 'Evento duplicado', d: 'Se vuelve a publicar el último evento: no se agrega una entrada repetida.' },
  { a: 'malformado', t: 'Evento inválido', d: 'Se inyecta un mensaje roto: va a la DLQ con su motivo y el flujo sigue.' },
];

export default function PanelDemo() {
  const [estado, setEstado] = useState(null);
  const [ocupado, setOcupado] = useState(false);
  const [ultimo, setUltimo] = useState(null);
  const [horas, setHoras] = useState(24);
  const leer = async () => { try { const r = await fetch('/api/demo'); setEstado(await r.json()); } catch { setEstado({ error: 'sin conexión' }); } };
  useEffect(() => { leer(); const t = setInterval(leer, 3000); return () => clearInterval(t); }, []);
  async function disparar(accion, extra = {}) {
    setOcupado(true);
    try { const r = await fetch('/api/demo', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ accion, ...extra }) }); setUltimo({ accion, ...(await r.json()) }); }
    finally { setOcupado(false); leer(); }
  }
  return (
    <>
      <div className="panel">
        <h2>Simulador</h2>
        {estado?.error ? <p className="mal">{estado.error}. Verificá que el servicio «simulador» esté levantado.</p> : estado ? (
          <p>Reloj simulado: <b>{estado.simNow.replace('T', ' ').slice(0, 16)} UTC</b> · envíos creados: <b>{estado.enviosCreados}</b> · mensajes publicados: <b>{estado.mensajesPublicados}</b> · {estado.pausado ? <b className="mal">flujo en pausa</b> : <b className="ok">flujo continuo</b>}</p>
        ) : <p className="vacio">Conectando con el simulador…</p>}
        <div className="filtros" style={{ marginBottom: 0 }}>
          <button className="sec" disabled={ocupado} onClick={() => disparar(estado?.pausado ? 'reanudar' : 'pausar')}>{estado?.pausado ? 'Reanudar flujo' : 'Pausar flujo'}</button>
          <label>Horas simuladas<input type="number" min="1" max="72" value={horas} onChange={(e) => setHoras(e.target.value)} style={{ width: 90 }} /></label>
          <button disabled={ocupado} onClick={() => disparar('avanzar', { horas })}>Avanzar el reloj</button>
        </div>
      </div>
      {ultimo && (
        <div className="aviso">
          {ultimo.error ? `No se pudo ejecutar: ${ultimo.error}` : <>Listo: <b>{ultimo.accion}</b>.{' '}
            {ultimo.shipmentId && <>Seguilo en <Link href={`/envio/${ultimo.shipmentId}`}>el detalle</Link> o en el <Link href={`/tracking/${ultimo.shipmentId}`}>tracking público</Link> ({ultimo.shipmentId}).</>}</>}
        </div>
      )}
      <div className="demo-grid">{ACCIONES.map((x) => (
        <div className="panel" key={x.a}><h2>{x.t}</h2><p>{x.d}</p><button disabled={ocupado} onClick={() => disparar(x.a)}>Disparar</button></div>))}</div>
    </>
  );
}
