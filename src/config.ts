import { z } from 'zod';

const envSchema = z.object({
  DB_HOST: z.string().min(1),
  DB_PORT: z.coerce.number().int().positive().default(3306),
  DB_USER: z.string().min(1),
  DB_PASSWORD: z.string().default(''),
  DB_NAME: z.string().min(1),
  DB_CONNECTION_LIMIT: z.coerce.number().int().positive().default(10),
  DEFAULT_OWNER_ID: z.string().optional(),
  DEFAULT_LOOKBACK_DAYS: z.coerce.number().int().min(1).max(365).default(30),
  MCP_SERVER_NAME: z.string().min(1).default('bany-mcp'),
  MCP_SERVER_VERSION: z.string().min(1).default('0.2.0'),
});

export type AppConfig = z.infer<typeof envSchema>;

export function loadConfig(): AppConfig {
  return envSchema.parse(process.env);
}
