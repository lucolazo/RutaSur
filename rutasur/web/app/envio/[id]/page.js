import DetalleLive from '../../../components/DetalleLive';
import { getDb, plano } from '../../../lib/db';
export const dynamic = 'force-dynamic';

export default async function Detalle({ params }) {
  const id = decodeURIComponent(params.id);
  const db = await getDb();
  const [envio, alertas, notificaciones] = await Promise.all([
    db.collection('envios').findOne({ _id: id }),
    db.collection('alertas').find({ shipmentId: id }).sort({ prioridadRank: -1 }).toArray(),
    db.collection('notificaciones').find({ shipmentId: id }).sort({ ocurridoEn: -1 }).toArray(),
  ]);
  return (
    <>
      <h1>Envío {id}</h1>
      <p className="sub">Vista del supervisor. Enlace de seguimiento para el destinatario: <a href={`/tracking/${encodeURIComponent(id)}`}>/tracking/{id}</a></p>
      <DetalleLive id={id} inicial={plano({ envio, alertas, notificaciones })} />
    </>
  );
}
