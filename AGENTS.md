# bany-mcp

Servidor MCP TypeScript via stdio para analise read-only de transacoes e
feedbacks do ecossistema Banky. O checkout local chama `bany_mcp`; no Knowledge
Repo Nero o projeto foi registrado como `bany-mcp`.

## Como Usar Este Contexto

| Quando | Ler |
| --- | --- |
| Glossario (Net vs Saldo, Feedback legado) | `CONTEXT.md` |
| Spec grelhada de *contrato* | `docs/spec-contrato-e-dominio.md` |
| Entender entrypoint e pastas | `.agents/references/structure.md` |
| Entender runtime, transporte e config | `.agents/references/runtime.md` |
| Entender tools e contratos MCP | `.agents/references/contracts.md` |
| Consultar dados pelas tools Banky | `$banky-mcp-tools` em `.agents/skills/banky-mcp-tools/` |
| Entender fronteiras de seguranca | `.agents/references/security.md` |
| Mudar queries, tools ou validacao | `.agents/references/conventions.md` |
| Avaliar gaps conhecidos | `.agents/references/tech-debt.md` |
| Aplicar guideline Nero | `$nero` -> `references/guidelines/mcp-guidelines.md` |

## Regras Rapidas

- Use `$nero` para contexto de knowledge.
- Use `$banky-mcp-tools` para consultas de transacoes e feedbacks via MCP Banky.
- Preserve o transporte stdio; stdout pertence ao JSON-RPC.
- Nao logue segredos, connection strings, valores de `.env` ou payloads sensiveis.
- Tools devem manter schema explicito e erro sem ecoar valor sensivel.
- Queries devem permanecer parametrizadas.
- Prefira `pnpm`, pois o repo possui `pnpm-lock.yaml`.

## Comandos

- `pnpm run dev`
- `pnpm run typecheck`
- `pnpm run build`
- `pnpm run start`

## Skills Condicionais

- Sempre: `$nero`.
- MCP: `$nero` -> `references/guidelines/mcp-guidelines.md`.
- Analytics Banky: `$banky-mcp-tools`.
