import 'dotenv/config';

import { loadConfig } from './config.js';
import { createDbPool } from './db.js';
import { startMcpServer } from './server.js';

async function main() {
  const config = loadConfig();
  const pool = createDbPool(config);

  const shutdown = async () => {
    await pool.end();
    process.exit(0);
  };

  process.on('SIGINT', () => {
    void shutdown();
  });

  process.on('SIGTERM', () => {
    void shutdown();
  });

  await startMcpServer(pool, config);
}

main().catch((error) => {
  console.error('[bany_mcp] fatal error', error);
  process.exit(1);
});
