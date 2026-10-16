import './globals.css';
import Link from 'next/link';

export const metadata = { title: 'RutaSur · Centro de control', description: 'Seguimiento de envíos en tiempo real' };

export default function Layout({ children }) {
  return (
    <html lang="es">
      <body>
        <aside className="riel">
          <Link href="/" className="marca"><span className="marca-ruta" aria-hidden="true" />RutaSur</Link>
          <nav>
            <Link href="/">Torre de control</Link>
            <Link href="/analitica">Analítica</Link>
            <Link href="/demo">Panel de demo</Link>
          </nav>
          <p className="riel-pie">Los tiempos están en el reloj simulado (UTC).</p>
        </aside>
        <main>{children}</main>
      </body>
    </html>
  );
}
