# Contratos MCP

Capability:

- `tools`.

Tools declaradas:

- `transactions_overview`
- `transactions_timeline`
- `transactions_list`
- `transactions_detect_anomalies`
- `transactions_credit_card_spending`
- `feedbacks_overview`
- `feedbacks_list`
- `feedbacks_quality`
- `feedbacks_training_queue`

Uso por agentes:

- A skill local `$banky-mcp-tools` documenta selecao, limites, exemplos e regras de interpretacao em `.agents/skills/banky-mcp-tools/`.
- O prefixo exposto pelo host e `mcp__banky__`; os nomes registrados pelo servidor permanecem sem esse prefixo.

Padrao de input:

- Schemas Zod internos validam argumentos recebidos.
- `inputSchema` MCP limita propriedades e ranges numericos.
- `ownerId` pode vir da chamada ou da configuracao padrao, conforme a tool.

Padrao de output:

- Sucesso retorna `content[type=text]` com JSON formatado.
- Erro esperado retorna `isError: true` com mensagem textual.
- Erro Zod retorna lista de issues serializada.

Banco:

- Queries usam placeholders parametrizados.
- `analytics.ts` verifica existencia de tabelas/colunas para tolerar diferencas de schema.
