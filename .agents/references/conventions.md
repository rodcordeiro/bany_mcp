# Convencoes

Mudancas:

- `src/server.ts` e dono dos contratos MCP.
- `src/analytics.ts` e dono da consulta/transformacao de dados.
- `src/config.ts` e dono do contrato de ambiente.
- `src/db.ts` deve permanecer pequeno e focado no pool.

Ao adicionar tool:

- Criar schema Zod.
- Declarar `inputSchema` correspondente.
- Validar limites numericos e filtros.
- Retornar JSON estruturado estavel.
- Garantir erro sem segredo.
- Atualizar README e `contracts.md`.

Validacao:

- `pnpm run typecheck`
- `pnpm run build`
- Smoke MCP via host/cliente apropriado quando disponivel.
