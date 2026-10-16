import { suscribir, publico } from '../../../lib/changes';
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

// Server-Sent Events alimentados por los change streams de MongoDB (el frontend nunca toca Kafka).
export async function GET(req) {
  const url = new URL(req.url);
  const id = url.searchParams.get('id');
  const esPublico = url.searchParams.get('public') === '1';
  const enc = new TextEncoder();
  let baja = () => {}, latido;
  const stream = new ReadableStream({
    async start(controller) {
      const enviar = (obj) => { try { controller.enqueue(enc.encode(`data: ${JSON.stringify(obj)}\n\n`)); } catch {} };
      const limpiar = () => { baja(); clearInterval(latido); try { controller.close(); } catch {} };
      controller.enqueue(enc.encode(': conectado\n\n'));
      baja = await suscribir((msg) => {
        if (id && msg.shipmentId !== id) return;
        if (esPublico) { if (msg.tipo === 'envio') enviar({ tipo: 'envio', doc: publico(msg.doc) }); return; }
        enviar(msg);
      });
      latido = setInterval(() => { try { controller.enqueue(enc.encode(': latido\n\n')); } catch {} }, 15000);
      req.signal.addEventListener('abort', limpiar);
    },
    cancel() { baja(); clearInterval(latido); },
  });
  return new Response(stream, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' } });
}
