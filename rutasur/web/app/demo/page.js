import PanelDemo from '../../components/PanelDemo';
export const dynamic = 'force-dynamic';

export default function Demo() {
  return (
    <>
      <h1>Panel de demo</h1>
      <p className="sub">Escenarios a pedido del simulador para recorrer los cuatro casos de uso. Para la caída de un servicio y el cuarto indicador se usan comandos de Docker (ver README).</p>
      <PanelDemo />
    </>
  );
}
