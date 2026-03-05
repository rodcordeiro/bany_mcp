# bany_mcp

MCP server em Node.js + TypeScript para analise e acompanhamento de transacoes do ecossistema Banky.

## Recursos

- Interface MCP via `stdio`
- Conexao direta em MySQL (`bk_tb_transactions`, `bk_tb_categories`, `bk_tb_accounts`)
- Ferramentas:
  - `transactions_overview`: resumo financeiro do periodo
  - `transactions_timeline`: serie diaria de entradas/saidas/saldo
  - `transactions_list`: listagem paginada com filtros
  - `transactions_detect_anomalies`: deteccao de anomalias de gasto
  - `transactions_credit_card_spending`: total gasto em contas de cartao de credito no periodo
  - `feedbacks_overview`: volume de feedbacks, distribuicao por status, cobertura de treino e tendencia diaria
  - `feedbacks_list`: listagem paginada de feedbacks com filtros por status, uso em treino e busca textual
  - `feedbacks_quality`: qualidade por campo comparando `predictedJson` vs `userCorrectedJson`
  - `feedbacks_training_queue`: fila apta para treino (`status != pending` e `usedForTraining = false`)

## Requisitos

- Node.js 20+
- Banco MySQL acessivel com o schema do Banky

## Configuracao

1. Copie `.env.example` para `.env`.
2. Preencha as variaveis de conexao:
   - `DB_HOST`
   - `DB_PORT`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_NAME`
3. Opcional:
   - `DEFAULT_OWNER_ID`
   - `DEFAULT_LOOKBACK_DAYS`

## Execucao

```bash
pnpm install
pnpm run dev
```

Build:

```bash
pnpm run build
pnpm run start
```

## Integracao MCP

Exemplo de comando do servidor:

```json
{
  "command": "node",
  "args": ["dist/index.js"],
  "cwd": "D:/projetos/personal/banky/bany_mcp"
}
```

Em desenvolvimento:

```json
{
  "command": "pnpm",
  "args": ["run", "dev"],
  "cwd": "D:/projetos/personal/banky/bany_mcp"
}
```
