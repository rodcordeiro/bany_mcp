# Catalogo de tools

Todas as propriedades sao opcionais no protocolo, mas `ownerId` e obrigatorio para tools de transacao quando `DEFAULT_OWNER_ID` nao estiver configurado. `days` usa `DEFAULT_LOOKBACK_DAYS` quando omitido.

## Transacoes

| Tool | Usar para | Inputs | Padroes e limites |
| --- | --- | --- | --- |
| `mcp__banky__transactions_overview` | Totais de Income, Expense, Net do periodo (nao e Saldo da Account), sync pendente e principais Categories | `ownerId`, `days` | `days`: 1-365 |
| `mcp__banky__transactions_timeline` | Serie diaria de entrada, saida e saldo | `ownerId`, `days` | `days`: 1-365 |
| `mcp__banky__transactions_list` | Detalhes, busca, status e paginacao | `ownerId`, `days`, `status`, `search`, `limit`, `offset` | `status`: `all`, `pending`, `processing`, `done`, `error`; `limit`: 1-200, padrao 50; `offset`: >= 0, padrao 0 |
| `mcp__banky__transactions_detect_anomalies` | Aumentos atipicos de despesa por categoria | `ownerId`, `recentDays`, `baselineDays`, `minIncreasePercent`, `minAbsoluteDelta`, `limit` | recentes 1-60, padrao 7; baseline 7-365, padrao 30 e maior que recentes; aumento 1-1000%, padrao 40; delta 0-1.000.000, padrao 10; `limit` 1-50, padrao 10 |
| `mcp__banky__transactions_credit_card_spending` | Total e distribuicao de despesas em contas de cartao | `ownerId`, `days`, `limit` | `days`: 1-365; `limit`: 1-100, padrao 10 |

## Feedbacks

| Tool | Usar para | Inputs | Padroes e limites |
| --- | --- | --- | --- |
| `mcp__banky__feedbacks_overview` | Volume, status, cobertura de treino e tendencia | `ownerId`, `days` | `days`: 1-365 |
| `mcp__banky__feedbacks_list` | Amostras e paginacao com filtros | `ownerId`, `days`, `status`, `usedForTraining`, `search`, `limit`, `offset` | `status`: `all`, `pending`, `validated`, `corrected`; `limit`: 1-200, padrao 50; `offset`: >= 0, padrao 0 |
| `mcp__banky__feedbacks_quality` | Acuracia de intent, category, account e value | `ownerId`, `days` | Compara apenas registros com correcao; `days`: 1-365 |
| `mcp__banky__feedbacks_training_queue` | Volume e amostras elegiveis para treino | `ownerId`, `days`, `limit` | Elegivel: status diferente de `pending` e ainda nao usado; `limit`: 1-100, padrao 20 |

## Semantica importante

- `transactions_overview.net = income - expense` no periodo. Nao e o Saldo (`ammount`) da Account.
- Tools de Transaction exigem `ownerId` ou `DEFAULT_OWNER_ID`. Tools de Feedback hoje aceitam omitir owner e, nesse caso, nao filtram dono.
- `transactions_list` retorna uma pagina, nao uma contagem total garantida.
- Filtro `status` de Transaction e Sync status (`pending|processing|done|error`), nao Status de negocio. Sem coluna `sync_status`, o filtro nao aplica e a listagem preenche `pending`.
- A deteccao de anomalias compara totais de Expense por Category (recente vs baseline). Sinal estatistico, nao fraude.
- Card spending usa heuristica no nome do Payment type; nao e o fluxo Credit payment da API.
- Feedback tools leem `bk_nlp_feedback` (`predictedJson` / `userCorrectedJson`). A API principal grava `bk_tb_feedback` em colunas; tabela legado ausente → erro.
- `feedbacks_quality.totalCompared` inclui feedbacks com `userCorrectedJson`; cada `accuracy` e `matches / totalCompared` (0 a 1).
- A fila de treino e uma leitura de elegibilidade. Consultar a tool nao altera `usedForTraining`.
- Sucesso chega como JSON serializado em `content[type=text]`; erro esperado usa `isError: true`.
