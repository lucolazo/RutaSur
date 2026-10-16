import TorreLive from '../components/TorreLive';
import Buscador from '../components/Buscador';
import { resumenTorre } from '../lib/torre';
export const dynamic = 'force-dynamic';

export default async function Torre() {
  const inicial = await resumenTorre();
  return (
    <>
      <h1>Torre de control</h1>
      <p className="sub">Estado de la operación mientras ocurre: cada cambio de estado de un envío llega como evento y se refleja acá en menos de dos segundos.</p>
      <Buscador />
      <TorreLive inicial={inicial} />
    </>
  );
}
