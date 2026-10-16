import { MongoClient } from 'mongodb';

const url = process.env.MONGO_URL || 'mongodb://localhost:27017/?directConnection=true';
export function getDb() {
  if (!globalThis.__mongo) {
    globalThis.__mongo = new MongoClient(url).connect().catch((e) => { globalThis.__mongo = null; throw e; });
  }
  return globalThis.__mongo.then((c) => c.db(process.env.MONGO_DB || 'rutasur'));
}
export const plano = (x) => JSON.parse(JSON.stringify(x));
