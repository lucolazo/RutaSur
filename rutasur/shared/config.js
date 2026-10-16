const env = process.env;
export const cfg = {
  brokers: (env.KAFKA_BROKERS || 'localhost:29092').split(','),
  mongoUrl: env.MONGO_URL || 'mongodb://localhost:27017/?directConnection=true',
  dbName: env.MONGO_DB || 'rutasur',
  simUrl: env.SIM_URL || 'http://localhost:4000',
};
