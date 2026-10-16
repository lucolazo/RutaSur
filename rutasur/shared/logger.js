export function crearLogger(servicio) {
  const out = (nivel, msg, extra) => console.log(JSON.stringify({ ts: new Date().toISOString(), servicio, nivel, msg, ...extra }));
  return { info: (m, e) => out('info', m, e), warn: (m, e) => out('warn', m, e), error: (m, e) => out('error', m, e) };
}
