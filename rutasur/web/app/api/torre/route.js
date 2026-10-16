import { resumenTorre } from '../../../lib/torre';
export const dynamic = 'force-dynamic';
export async function GET() { return Response.json(await resumenTorre()); }
