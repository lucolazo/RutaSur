export const dynamic = 'force-dynamic';
const SIM = process.env.SIM_URL || 'http://localhost:4000';
const RUTAS = {
  feliz: '/scenario/feliz', ausente: '/scenario/ausente', demorado: '/scenario/demorado', tercerFallo: '/scenario/tercerFallo',
  rafaga: '/scenario/rafaga', fueraDeOrden: '/scenario/fueraDeOrden', duplicado: '/inject/duplicado', malformado: '/inject/malformado',
  pausar: '/flow/pause', reanudar: '/flow/resume',
};
export async function GET() {
  try { return Response.json(await (await fetch(`${SIM}/state`, { cache: 'no-store' })).json()); }
  catch { return Response.json({ error: 'El simulador no responde' }, { status: 503 }); }
}
export async function POST(req) {
  const { accion, horas } = await req.json();
  const ruta = accion === 'avanzar' ? `/advance?horas=${Number(horas) || 24}` : RUTAS[accion];
  if (!ruta) return Response.json({ error: 'acción desconocida' }, { status: 400 });
  try {
    const r = await fetch(`${SIM}${ruta}`, { method: 'POST', cache: 'no-store' });
    return Response.json(await r.json(), { status: r.status });
  } catch { return Response.json({ error: 'El simulador no responde' }, { status: 503 }); }
}
