# Estrutura

Stack:

- Node.js 20+.
- TypeScript ESM.
- `@modelcontextprotocol/sdk`.
- `mysql2/promise`.
- `dotenv`.
- Zod.

Arquivos:

- `src/index.ts`: entrypoint, carrega config, cria pool MySQL e inicia servidor MCP.
- `src/server.ts`: instancia MCP Server, declara tools, valida inputs e executa chamadas.
- `src/analytics.ts`: consultas e calculos de transacoes/feedbacks.
- `src/config.ts`: schema Zod das variaveis de ambiente.
- `src/db.ts`: criacao do pool MySQL.
- `README.md`: setup e inventario de tools.

Artefatos:

- `dist/` e gerado por build.
- `node_modules/` nao deve ser editado.
