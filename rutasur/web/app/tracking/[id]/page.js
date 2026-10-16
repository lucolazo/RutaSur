import TrackingLive from '../../../components/TrackingLive';
import { getDb } from '../../../lib/db';
import { publico } from '../../../lib/changes';
export const dynamic = 'force-dynamic';

export default async function Tracking({ params }) {
  const id = decodeURIComponent(params.id);
  const doc = await (await getDb()).collection('envios').findOne({ _id: id });
  const inicial = doc ? JSON.parse(JSON.stringify(publico(doc))) : null;
  return (
    <div className="publico">
      <style>{'.riel{display:none} body{display:block} main{max-width:none;padding:0}'}</style>
      <div className="panel"><h1>Seguimiento de tu envío</h1><p className="sub">Código {id}</p><TrackingLive id={id} inicial={inicial} /></div>
    </div>
  );
}
