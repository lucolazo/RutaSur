'use client';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

export default function Buscador() {
  const router = useRouter();
  const [id, setId] = useState('');
  return (
    <form className="filtros" onSubmit={(e) => { e.preventDefault(); if (id.trim()) router.push(`/envio/${encodeURIComponent(id.trim())}`); }}>
      <label>Buscar un envío por su identificador
        <input value={id} onChange={(e) => setId(e.target.value)} placeholder="Ej.: RS-000042" size={26} />
      </label>
      <button type="submit">Abrir detalle</button>
    </form>
  );
}
