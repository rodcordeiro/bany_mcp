# Runtime

Execucao:

- Desenvolvimento: `pnpm run dev`.
- Build: `pnpm run build`.
- Producao local: `pnpm run start`.
- Typecheck: `pnpm run typecheck`.

Transporte:

- `src/server.ts` usa `StdioServerTransport`.
- stdout deve ser reservado para JSON-RPC.
- Logs humanos devem ir para stderr; evite `console.log` em fluxo MCP.

Configuracao:

- `src/config.ts` valida ambiente com Zod.
- Variaveis exigidas: host, porta, usuario, senha e nome do banco.
- Variaveis opcionais: owner padrao, janela padrao, limite de conexao e metadata do servidor.

Shutdown:

- `src/index.ts` fecha o pool MySQL em `SIGINT` e `SIGTERM`.
