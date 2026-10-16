import { reglasActivas } from './reglas.js';

const MINUTO = 60e3;

/**
 * Monitor de SLA (CU3) sin E/S: estado por envío + reloj simulado (watermark = mayor ocurridoEn visto).
 * `emitir(eventos)` publica y SOLO si resuelve se marca la regla como emitida; así un fallo de
 * publicación se reintenta sin perder la alerta. Los eventId son deterministas, por eso un
 * reintento o un reinicio nunca duplica el efecto aguas abajo.
 */
export class MonitorSla {
  constructor({ envios = {}, reloj = 0, ultimoBarrido = 0, emitir }) {
    this.envios = new Map(Object.entries(envios));
    this.reloj = reloj;
    this.ultimoBarrido = ultimoBarrido;
    this.emitir = emitir;
    this.tocados = new Set();
  }

  _get(id) {
    if (!this.envios.has(id)) {
      this.envios.set(id, { _id: id, shipmentId: id, venceEnMs: null, estado: null, ingresado: false, entregado: false,
        falloEn: null, hubId: null, zonaId: null, ultimaSecuencia: 0, emitidas: [], cierresEmitidos: false });
    }
    return this.envios.get(id);
  }
  _avanzar(iso) { const ms = Date.parse(iso); if (ms > this.reloj) this.reloj = ms; }

  async alPromesa(ev) {
    this._avanzar(ev.ocurridoEn);
    const s = this._get(ev.shipmentId);
    s.venceEnMs = Date.parse(ev.venceEn);
    await this._reevaluar(s);
  }

  async alEstado(ev) {
    this._avanzar(ev.ocurridoEn);
    const s = this._get(ev.shipmentId);
    if (ev.secuencia > s.ultimaSecuencia) { // regla de orden: lo viejo o repetido no cambia el estado
      s.ultimaSecuencia = ev.secuencia;
      s.estado = ev.tipo; s.hubId = ev.hubId; s.zonaId = ev.zonaId;
      if (ev.tipo !== 'CREADO') s.ingresado = true;
      if (ev.tipo === 'ENTREGADO') { s.entregado = true; s.entregadoEn = ev.ocurridoEn; }
      if (ev.tipo === 'ENTREGA_FALLIDA') s.falloEn = ev.ocurridoEn;
      if (ev.promesaVenceEn && s.venceEnMs == null) s.venceEnMs = Date.parse(ev.promesaVenceEn);
    }
    await this._reevaluar(s);
  }

  /** Barrido de todos los envíos abiertos cada minuto de reloj simulado. */
  async barrer() {
    if (this.reloj - this.ultimoBarrido < MINUTO) return;
    this.ultimoBarrido = this.reloj;
    this.tocados.add('__reloj__');
    for (const s of this.envios.values()) {
      if (s.entregado || s.emitidas.includes('VENCIDO')) continue;
      await this._reevaluar(s);
    }
  }

  async _reevaluar(s) {
    const evs = [];
    const base = { shipmentId: s.shipmentId, hubId: s.hubId, zonaId: s.zonaId, version: 1 };
    if (s.entregado) {
      if (!s.cierresEmitidos) {
        for (const regla of s.emitidas) {
          evs.push({ ...base, eventId: `cierre-${s.shipmentId}-${regla}`, tipo: 'ALERTA_CERRADA', regla,
            prioridad: PRIORIDAD_DE[regla], causa: 'Envío entregado', abiertaEn: s.abiertas?.[regla] ?? null, ocurridoEn: s.entregadoEn });
        }
        if (evs.length) await this.emitir(evs);
        s.cierresEmitidos = true; this.tocados.add(s._id);
      }
      return;
    }
    for (const r of reglasActivas(s, this.reloj)) {
      if (s.emitidas.includes(r.regla)) continue; // sin repetición
      evs.push({ ...base, eventId: `alerta-${s.shipmentId}-${r.regla}`, tipo: 'ALERTA_ABIERTA', regla: r.regla,
        prioridad: r.prioridad, causa: r.causa, ocurridoEn: new Date(this.reloj).toISOString() });
    }
    if (!evs.length) return;
    await this.emitir(evs);
    s.abiertas ??= {};
    for (const e of evs) { s.emitidas.push(e.regla); s.abiertas[e.regla] = e.ocurridoEn; }
    this.tocados.add(s._id);
  }
}
const PRIORIDAD_DE = { SIN_INGRESO: 'MEDIA', SIN_SALIDA: 'ALTA', FALLO_DIA_LIMITE: 'ALTA', VENCIDO: 'CRITICA' };
