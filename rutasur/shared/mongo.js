import { MongoClient } from 'mongodb';
import { cfg } from './config.js';

export const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

export async function conectarMongo() {
  for (let i = 0; ; i++) {
    try {
      const client = new MongoClient(cfg.mongoUrl, { serverSelectionTimeoutMS: 5000 });
      await client.connect();
      await client.db('admin').command({ ping: 1 });
      return { client, db: client.db(cfg.dbName) };
    } catch (e) {
      if (i > 90) throw e;
      await dormir(2000);
    }
  }
}
